// SPDX-License-Identifier: MIT
package sessionkit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/sdk/go/internal/rpc"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestTransferActionsNeverPassPathsToGenericCaller(t *testing.T) {
	var generic atomic.Int32
	call := func(context.Context, string, any) (json.RawMessage, error) {
		generic.Add(1)
		return nil, errors.New("generic call must not see local paths")
	}
	legacy := NewCaller(call)
	if _, err := legacy.Action(context.Background(), "transfer.offer", json.RawMessage(`{"target":"receiver@local","source":"/private/source"}`)); !errors.Is(err, ErrTransferUnsupported) {
		t.Fatal(err)
	}
	local, remote := net.Pipe()
	defer local.Close()
	defer remote.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	transfers := newTransfers(nil)
	var wire *rpc.Conn
	wire = rpc.New(local, true, func(_ context.Context, request *rpc.Request) {
		if !transfers.handle(wire, request) {
			t.Error("unexpected machine callback")
		}
	})
	defer wire.Close()
	var received atomic.Int32
	var server *rpc.Conn
	server = rpc.New(remote, false, func(_ context.Context, r *rpc.Request) {
		go func() {
			received.Add(1)
			p, ok := r.Params.(*protocol.TransferOffer)
			if !ok || p.Target != "receiver@local" || p.Name != "source" {
				t.Error("wrong path-free offer", r.Params)
			}
			raw, _ := protocol.EncodeParams(r.Method, r.Params)
			if strings.Contains(string(raw), "/private/") {
				t.Error("local source path went over wire")
			}
			_ = server.Result(r, protocol.TransferDescriptor{ID: "one", Sender: "sender@local", Receiver: "receiver@local", CreatedBy: "sender@local", Name: "source", State: "offered"})
		}()
	})
	defer server.Close()
	transfers.attach(wire, ctx, true)
	caller := NewCallerWithTransfers(call, transfers)
	raw, err := caller.Action(ctx, "transfer.offer", json.RawMessage(`{"target":"receiver@local","source":"/private/source"}`))
	if err != nil {
		t.Fatal(err)
	}
	var result protocol.TransferDescriptor
	if err = json.Unmarshal(raw, &result); err != nil || result.ID != "one" {
		t.Fatal(string(raw), err)
	}
	for _, raw := range []string{
		`{"target":"receiver@local","source":"/private/source","extra":true}`,
		`{"target":"receiver@local","source":""}`,
	} {
		if _, err = caller.Action(ctx, "transfer.offer", json.RawMessage(raw)); err == nil {
			t.Fatal("invalid local action was accepted")
		}
	}
	preCancelled, stop := context.WithCancel(ctx)
	stop()
	if _, err = caller.Action(preCancelled, "transfer.offer", json.RawMessage(`{"target":"receiver@local","source":"/private/source"}`)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if received.Load() != 1 || generic.Load() != 0 {
		t.Fatal("local action fell back to generic RPC", received.Load(), generic.Load())
	}
	transfers.wait(wire)
}

func TestTransferAcceptRejectsChangedLocalPathOrOverwrite(t *testing.T) {
	g, b, _ := localTransferBinding(t, nil)
	g.owner.current = g
	b.path = "/local/bound"
	b.role = "destination"
	for _, change := range []struct {
		path      string
		overwrite bool
	}{{"/local/different", false}, {b.path, true}} {
		if _, err := g.owner.Accept(context.Background(), b.id, change.path, change.overwrite); !errors.Is(err, ErrTransferBinding) {
			t.Fatal("changed local binding reached RPC", err)
		}
	}
	b.role = "source"
	if _, err := g.owner.Accept(context.Background(), b.id, b.path, true); !errors.Is(err, ErrTransferBinding) {
		t.Fatal("source chose receiver overwrite", err)
	}
}

type latePublicationDestination struct {
	entered, release chan struct{}
	aborts           atomic.Int32
}

func (x *latePublicationDestination) Write(raw []byte) (int, error) { return len(raw), nil }
func (x *latePublicationDestination) Commit() (bool, error) {
	close(x.entered)
	<-x.release
	return true, nil
}
func (x *latePublicationDestination) Abort() error { x.aborts.Add(1); return nil }
func TestCapturedReceiverProjectionWaitsOffReaderForPublicationFact(t *testing.T) {
	g, b, _ := localTransferBinding(t, nil)
	destination := &latePublicationDestination{entered: make(chan struct{}), release: make(chan struct{})}
	b.role = "destination"
	b.started = true
	b.prepared = true
	b.destination = destination
	b.hash = sha256.New()
	b.hash.Write([]byte("data"))
	b.size = 4
	b.offset = 4
	path := "/private/destination"
	b.destinationProjection.Store(&path)
	wire := &rpc.Conn{}
	g.wire = wire
	g.owner.current = g
	sum := sha256.Sum256([]byte("data"))
	finished := make(chan error, 1)
	go func() {
		_, err := g.finish(protocol.TransferFinish{ID: b.id, Bytes: 4, SHA256: hex.EncodeToString(sum[:])})
		finished <- err
	}()
	<-destination.entered
	d := protocol.DeliveryRequest{MessageID: "transfer-notice-owned", Body: `{"kind":"terminal","id":"owned","outcome":"unknown","bytes":4,"reason":"link_lost"}`}
	project := g.owner.ProjectDelivery(wire, &d)
	if project == nil {
		t.Fatal("receiver projection missing")
	}
	projected := make(chan error, 1)
	go func() { projected <- project(context.Background(), &d) }()
	select {
	case <-projected:
		t.Fatal("projection invented publication before confirmation")
	case <-time.After(time.Millisecond):
	}
	close(destination.release)
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	if err := <-projected; err != nil {
		t.Fatal(err)
	}
	var body struct{ Outcome, Destination, SHA256 string }
	if err := json.Unmarshal([]byte(d.Body), &body); err != nil {
		t.Fatal(err)
	}
	if body.Outcome != "completed" || body.Destination != path || body.SHA256 != hex.EncodeToString(sum[:]) {
		t.Fatal(d.Body)
	}
	b.destinationProjection.Store(nil)
	if g.owner.ProjectDelivery(wire, &d) != nil {
		t.Fatal("source gained receiver path")
	}
}

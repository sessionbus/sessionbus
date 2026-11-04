// SPDX-License-Identifier: MIT

package sessionkit

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/sdk/go/internal/rpc"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestSameWireHelloUsesNativeIdentityLifetime(t *testing.T) {
	left, right := net.Pipe()
	wire := NewConnection(left, func(context.Context, *Request) {})
	var server *rpc.Conn
	var offers atomic.Int32
	server = rpc.New(right, false, func(_ context.Context, r *rpc.Request) {
		go func() {
			switch r.Method {
			case "session.hello":
				_ = server.Result(r, protocol.HelloResult{Capabilities: []string{protocol.TransferCapability}})
			case "transfer.offer":
				_ = server.Result(r, protocol.TransferDescriptor{ID: fmt.Sprint(offers.Add(1)), Sender: "source@local", Receiver: "other@local", CreatedBy: "source", Name: "file", State: "offered"})
			}
		}()
	})
	transfers, _ := NewConnectionTransfers(nil)
	a, endA := context.WithCancel(context.Background())
	b, endB := context.WithCancel(context.Background())
	t.Cleanup(func() {
		endA()
		endB()
		_ = wire.Close()
		_ = server.Close()
		transfers.CloseConnectionGeneration(nil)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	identity := PeerIdentity{Product: "fixture", SessionID: "first@local", Groups: []string{"test"}}
	if err := transfers.HelloObserved(ctx, a, wire, identity, nil); err != nil {
		t.Fatal(err)
	}
	first, _ := transfers.generation()
	pending, err := transfers.Offer(ctx, "other@local", "/private/source", "file")
	if err != nil {
		t.Fatal(err)
	}
	// Context wrappers keep the same cancellation lifetime, including a
	// non-comparable implementation which must not be compared as an interface.
	type nonComparableContext struct {
		context.Context
		values []string
	}
	metadata := nonComparableContext{context.WithValue(a, struct{}{}, "metadata"), []string{"unchanged lifetime"}}
	if err := transfers.HelloObserved(ctx, metadata, wire, identity, nil); err != nil {
		t.Fatal(err)
	}
	if current, _ := transfers.generation(); current != first || first.binding(pending.ID) == nil {
		t.Fatal("metadata rehello retired unchanged lifetime or its pending work")
	}
	stale, endStale := context.WithCancel(context.Background())
	endStale()
	for _, enabled := range []bool{false, true} {
		if transfers.attach(wire, stale, enabled) != nil {
			t.Fatal("cancelled replacement attached")
		}
		if current, _ := transfers.generation(); current != first {
			t.Fatal("cancelled replacement displaced live identity")
		}
	}
	identity.SessionID = "next@local"
	if err := transfers.HelloObserved(ctx, b, wire, identity, nil); err != nil {
		t.Fatal(err)
	}
	endA()
	select {
	case <-first.finished:
	case <-ctx.Done():
		t.Fatal("old identity resources did not retire")
	}
	if _, err := transfers.Offer(ctx, "other@local", "/private/next-source", "file"); err != nil {
		t.Fatal("new identity inherited old cancellation", err)
	}
	current, _ := transfers.generation()
	if current == first || first.binding(pending.ID) != nil {
		t.Fatal("new identity reused old generation or retained its binding")
	}
	endB()
	select {
	case <-current.finished:
	case <-ctx.Done():
		t.Fatal("new identity resources did not retire")
	}
	if _, err := transfers.Offer(ctx, "other@local", "/private/ended-source", "file"); !errors.Is(err, ErrTransferUnsupported) {
		t.Fatal("ended current identity still usable", err)
	}
	transfers.mu.Lock()
	defer transfers.mu.Unlock()
	if transfers.pending != 0 || transfers.active != 0 {
		t.Fatal("overlapping generations leaked quota", transfers.pending, transfers.active)
	}
}

// SPDX-License-Identifier: MIT
package sessionkit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/sdk/go/internal/rpc"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

type blockingTransferExecutor struct{ entered chan struct{} }

func (x *blockingTransferExecutor) OpenTransferSource(ctx context.Context, _ string) (TransferSource, error) {
	close(x.entered)
	<-ctx.Done()
	return nil, ctx.Err()
}
func (x *blockingTransferExecutor) OpenTransferDestination(context.Context, string, bool) (TransferDestination, error) {
	return nil, errors.New("fixture path rejected")
}

type unknownTransferDestination struct{ aborts atomic.Int32 }

func (x *unknownTransferDestination) Write(b []byte) (int, error) { return len(b), nil }
func (x *unknownTransferDestination) Commit() (bool, error)       { return false, ErrTransferCommitUnknown }
func (x *unknownTransferDestination) Abort() error {
	x.aborts.Add(1)
	return errors.New("destructive abort must not be called")
}
func localTransferBinding(t *testing.T, executor TransferExecutor) (*transferGeneration, *transferBinding, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	owner := newTransfers(executor)
	g := &transferGeneration{owner: owner, ctx: ctx, cancel: cancel, enabled: true, bindings: map[string]*transferBinding{}}
	b := &transferBinding{id: "owned", role: "source", path: "local-only"}
	if e := g.reserve(b); e != nil {
		t.Fatal(e)
	}
	g.provisional--
	g.bindings[b.id] = b
	return g, b, cancel
}
func TestTransferTerminalCancelsBlockedPrepareOffReader(t *testing.T) {
	executor := &blockingTransferExecutor{entered: make(chan struct{})}
	g, b, _ := localTransferBinding(t, executor)
	prepared := make(chan error, 1)
	go func() { _, err := g.prepare(protocol.TransferPrepare{ID: b.id, Role: "source"}); prepared <- err }()
	<-executor.entered
	terminal := make(chan error, 1)
	go func() {
		_, err := g.notice(protocol.TransferNotice{ID: b.id, Kind: "terminal", Outcome: "cancelled"})
		terminal <- err
	}()
	select {
	case err := <-prepared:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("terminal could not cancel blocked permission prepare")
	}
	select {
	case err := <-terminal:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("cleanup did not join")
	}
	g.owner.mu.Lock()
	defer g.owner.mu.Unlock()
	if g.owner.active != 0 || g.owner.pending != 0 {
		t.Fatal("local admission leaked")
	}
}
func TestTransferUnknownNativePublicationNeverAbortsOrRetries(t *testing.T) {
	g, b, cancel := localTransferBinding(t, nil)
	dest := &unknownTransferDestination{}
	b.role = "destination"
	b.started, b.prepared, b.active = true, true, true
	b.destination = dest
	b.size, b.offset = 4, 4
	b.hash = sha256.New()
	b.hash.Write([]byte("data"))
	g.active = 1
	g.owner.pending = 0
	g.owner.active = 1
	digest := sha256.Sum256([]byte("data"))
	result, err := g.finish(protocol.TransferFinish{ID: b.id, Bytes: 4, SHA256: hex.EncodeToString(digest[:])})
	if err != nil || result.Outcome != "unknown" {
		t.Fatal(result, err)
	}
	if _, err = g.notice(protocol.TransferNotice{ID: b.id, Kind: "terminal", Outcome: "unknown"}); !errors.Is(err, ErrTransferCommitUnknown) {
		t.Fatal("UNKNOWN lost", err)
	}
	if dest.aborts.Load() != 0 || g.owner.active != 1 {
		t.Fatal("destructive abort/early release after UNKNOWN")
	}
	cancel()
	g.shutdown()
	if dest.aborts.Load() != 0 || g.owner.active != 0 {
		t.Fatal("generation cleanup retried publication or destructively aborted")
	}
}
func TestRawConnectionTransferHookNegotiatesWithoutLegacyProbe(t *testing.T) {
	for _, capable := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy-daemon", true: "capable-daemon"}[capable], func(t *testing.T) {
			local, remote := net.Pipe()
			var server *rpc.Conn
			var opted atomic.Bool
			var observed atomic.Bool
			var wire *Connection
			executor := &blockingTransferExecutor{entered: make(chan struct{})}
			transfers, err := NewConnectionTransfers(executor)
			if err != nil {
				t.Fatal(err)
			}
			assigned := make(chan struct{})
			wire = NewConnection(local, func(ctx context.Context, r *Request) {
				<-assigned
				if !observed.Load() {
					t.Error("incoming callback before observed identity install")
				}
				if !transfers.HandleConnectionRequest(wire, r) {
					t.Error("unhandled transfer callback")
				}
			})
			close(assigned)
			server = rpc.New(remote, false, func(ctx context.Context, r *rpc.Request) {
				go func() {
					switch r.Method {
					case "session.hello":
						hello := r.Params.(*protocol.PeerHello)
						opted.Store(hello.Info[protocol.TransferInfoKey] == float64(1))
						reply := protocol.HelloResult{}
						if capable {
							reply.Capabilities = []string{protocol.TransferCapability}
						}
						_ = server.Result(r, reply)
					case "session.list":
						_ = server.Result(r, protocol.SessionListResult{Sessions: []protocol.SessionSummary{}})
					default:
						t.Error("unexpected discovery/transfer method", r.Method)
					}
				}()
			})
			t.Cleanup(func() { _ = wire.Close(); _ = server.Close(); transfers.CloseConnectionGeneration(wire) })
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			identity := PeerIdentity{Product: "fixture", SessionID: "raw", Groups: []string{"test"}, Info: map[string]any{}}
			if err = transfers.HelloObserved(ctx, wire.Context(), wire, identity, func() error {
				_, readyErr := transfers.generation()
				if capable && readyErr != nil || !capable && !errors.Is(readyErr, ErrTransferUnsupported) {
					t.Error("identity observer published before usable negotiated generation", readyErr)
				}
				observed.Store(true)
				return nil
			}); err != nil || !opted.Load() {
				t.Fatal("hello negotiation", err)
			}
			if !capable {
				if _, err = transfers.Offer(ctx, "other@host", "private-source", "file"); !errors.Is(err, ErrTransferUnsupported) {
					t.Fatal(err)
				}
			} else {
				var empty struct{}
				from := protocol.DeliverySource{SessionID: "other@host", Product: "fixture", Groups: []string{"test"}}
				if err = server.Call(ctx, "transfer.notice", protocol.TransferNotice{ID: "pending", Kind: "offer", Name: "file", From: &from}, &empty); err != nil {
					t.Fatal(err)
				}
			}
			var listed protocol.SessionListResult
			if err = wire.Call(ctx, "session.list", protocol.SessionListRequest{}, &listed); err != nil {
				t.Fatal("ordinary call lost", err)
			}
		})
	}
}

func TestRejectedHelloObserverDoesNotDisableANewerConnection(t *testing.T) {
	left, right := net.Pipe()
	successorLocal, successorRemote := net.Pipe()
	wire := NewConnection(left, func(context.Context, *Request) {})
	successor := NewConnection(successorLocal, func(context.Context, *Request) {})
	var server *rpc.Conn
	server = rpc.New(right, false, func(_ context.Context, r *rpc.Request) {
		go func() {
			_ = server.Result(r, protocol.HelloResult{Capabilities: []string{protocol.TransferCapability}})
		}()
	})
	transfers, _ := NewConnectionTransfers(nil)
	defer func() {
		_ = wire.Close()
		_ = server.Close()
		_ = successor.Close()
		_ = successorRemote.Close()
		transfers.CloseConnectionGeneration(wire)
		transfers.CloseConnectionGeneration(successor)
	}()
	rejected := errors.New("stale identity rejected")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := transfers.HelloObserved(ctx, wire.Context(), wire, PeerIdentity{Product: "fixture", SessionID: "raw", Groups: []string{"test"}}, func() error {
		// The observer can release its broker mutex while another identity wins.
		transfers.attach(successor, successor.Context(), true)
		return rejected
	})
	if !errors.Is(err, rejected) {
		t.Fatal("observer refusal lost", err)
	}
	g, err := transfers.generation()
	if err != nil || g.wire != successor {
		t.Fatal("stale observer disabled newer usable generation", g, err)
	}

	stale, cancelStale := context.WithCancel(context.Background())
	cancelStale()
	for _, enabled := range []bool{true, false} {
		if transfers.attach(wire, stale, enabled) != nil {
			t.Fatal("cancelled identity attached")
		}
		current, err := transfers.generation()
		if err != nil || current != g {
			t.Fatal("cancelled identity displaced current generation", current, err)
		}
	}
}

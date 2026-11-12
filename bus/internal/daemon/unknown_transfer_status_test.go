// SPDX-License-Identifier: GPL-3.0-only
package daemon

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/federation"
	"github.com/antst/sessionbus/bus/internal/transfer"
	sessionkit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
	"github.com/antst/sessionbus/bus/sdk/go/testsocket"
)

type unknownStatusPeer struct {
	wire      *sessionkit.Connection
	transfers *sessionkit.Transfers
	identity  protocol.PeerHello
	terminal  <-chan protocol.TransferNotice
}

// Only test-owned SDK callbacks are delayed, off the socket reader. Holding the
// real terminal cleanup ACK keeps the coordinator/bridge record until release.
func connectUnknownStatusPeer(t *testing.T, socket, id string, hold bool) *unknownStatusPeer {
	t.Helper()
	fd, err := net.Dial("unix", socket)
	must(t, err)
	tx, err := sessionkit.NewConnectionTransfers(nil)
	must(t, err)
	life, end := context.WithCancel(context.Background())
	gate := make(chan struct{})
	entered := make(chan protocol.TransferNotice, 1)
	if !hold {
		close(gate)
	}
	var wire *sessionkit.Connection
	assigned := make(chan struct{})
	var helpers sync.WaitGroup
	wire = sessionkit.NewConnection(fd, func(ctx context.Context, r *sessionkit.Request) {
		<-assigned
		if n, ok := r.Params.(*protocol.TransferNotice); ok && r.Method == "transfer.notice" && n.Kind == "terminal" && hold {
			entered <- *n
			helpers.Add(1)
			go func() {
				defer helpers.Done()
				select {
				case <-gate:
				case <-ctx.Done():
				}
				if !tx.HandleConnectionRequest(wire, r) {
					t.Error("held SDK terminal was not handled")
				}
			}()
			return
		}
		if tx.HandleConnectionRequest(wire, r) {
			return
		}
		if r.Method == "message.deliver" {
			go func() { _ = wire.Result(r, protocol.DeliveryReceipt{Disposition: "written"}) }()
			return
		}
		t.Errorf("unexpected private-fixture callback %s", r.Method)
	})
	close(assigned)
	q := &unknownStatusPeer{wire: wire, transfers: tx, terminal: entered, identity: protocol.PeerHello{Protocol: 1, Product: "fixture-client", SessionID: id, Groups: []string{"shared"}, Info: map[string]any{}}}
	t.Cleanup(func() {
		if hold {
			close(gate)
		}
		end()
		_ = wire.Close()
		helpers.Wait()
		tx.CloseConnectionGeneration(wire)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	must(t, tx.HelloObserved(ctx, life, wire, q.identity, nil))
	return q
}
func (q *unknownStatusPeer) optIn(ctx context.Context, enabled bool) error {
	hello := q.identity
	hello.Info = map[string]any{"sessionbus_transfer": 1}
	if enabled {
		hello.Info["sessionbus_unknown_transfer"] = 1
	}
	var result protocol.HelloResult
	return q.wire.Call(ctx, "session.hello", hello, &result)
}
func requireUnknownStatusReply(t *testing.T, q *unknownStatusPeer, ctx context.Context, id string, opted bool) {
	t.Helper()
	var value protocol.TransferDescriptor
	err := q.wire.Call(ctx, "transfer.status", protocol.TransferTarget{ID: id}, &value)
	code, message := -32019, "transfer_failed"
	if opted {
		code, message = -32020, "unknown_transfer"
	}
	var refusal *protocol.RPCError
	if !errors.As(err, &refusal) || refusal.Code != code || refusal.Message != message || len(refusal.Data) != 0 {
		t.Fatalf("status refusal: got %v, want %d/%s", err, code, message)
	}
	var listed protocol.SessionListResult
	must(t, q.wire.Call(ctx, "session.list", protocol.SessionListRequest{}, &listed))
}

func TestUnknownTransferStatusLocalAndForwardedEnded(t *testing.T) {
	for _, remote := range []bool{false, true} {
		t.Run(map[bool]string{false: "local", true: "forwarded"}[remote], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			address := ""
			secrets := map[string]string{}
			if remote {
				for _, host := range []string{"alpha", "beta"} {
					secret, err := federation.NewSecret()
					must(t, err)
					secrets[host] = secret
				}
				tlsConfig, err := federation.ServerTLS(secrets)
				must(t, err)
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				must(t, err)
				hub := federation.StartHub(listener, tlsConfig, io.Discard)
				t.Cleanup(hub.Close)
				address = listener.Addr().String()
			}
			start := func(host string) (*Daemon, string) {
				dir := testsocket.Directory(t)
				socket := filepath.Join(dir, "status.sock")
				config := Config{Host: host, SocketPath: socket, TablePath: filepath.Join(dir, "rows")}
				if remote {
					config.HubAddress = address
					config.HubSecret = secrets[host]
				}
				d, err := Start(config)
				must(t, err)
				t.Cleanup(func() { _ = d.Close() })
				return d, socket
			}
			origin, alpha := start("alpha")
			beta := alpha
			targetHost := "alpha"
			if remote {
				_, beta = start("beta")
				targetHost = "beta"
			}
			creator := connectUnknownStatusPeer(t, alpha, "creator", true)
			_ = connectUnknownStatusPeer(t, beta, "other", false)
			if remote {
				for {
					var listed protocol.SessionListResult
					err := creator.wire.Call(ctx, "session.list", protocol.SessionListRequest{Host: "beta"}, &listed)
					if err == nil {
						break
					}
					select {
					case <-ctx.Done():
						t.Fatal("federation fixture admission", err)
					case <-time.After(time.Millisecond):
					}
				}
			}
			descriptor, err := creator.transfers.Offer(ctx, "other@"+targetHost, "/private/not-opened", "file")
			must(t, err)
			var live protocol.TransferDescriptor
			must(t, creator.wire.Call(ctx, "transfer.status", protocol.TransferTarget{ID: descriptor.ID}, &live))
			if live.ID != descriptor.ID || live.State != "offered" {
				t.Fatal("real live coordinator descriptor", live)
			}
			origin.directory.mu.Lock()
			item := origin.directory.connected["creator@alpha"]
			origin.directory.mu.Unlock()
			actor, err := origin.directory.transferEndpoint(item)
			must(t, err)
			var bridge *transferBridge
			if remote {
				origin.directory.mu.Lock()
				bridge = origin.federation.bridge
				origin.directory.mu.Unlock()
			}
			owns := func(t *testing.T) {
				t.Helper()
				if remote {
					if !bridge.owns(actor.Key, descriptor.ID) {
						t.Fatal("forwarded ID no longer owned by exact origin bridge")
					}
					if _, err := origin.transfers.Status(actor.Key, descriptor.ID); !errors.Is(err, transfer.ErrUnknown) {
						t.Fatal("local coordinator unexpectedly owns hub ID", err)
					}
				}
			}
			owns(t)
			// Cancel's authoritative terminal proves ended; its actual cleanup ACK below
			// remains held, so this is an ended record, not only a post-removal lookup.
			terminal, err := creator.transfers.Cancel(ctx, descriptor.ID)
			must(t, err)
			if terminal.ID != descriptor.ID || terminal.Outcome != "cancelled" {
				t.Fatal("coordinator did not end actual record", terminal)
			}
			select {
			case notice := <-creator.terminal:
				if notice.ID != descriptor.ID || notice.Outcome != "cancelled" {
					t.Fatal("wrong held terminal", notice)
				}
			case <-ctx.Done():
				t.Fatal("real cleanup ACK did not reach gate")
			}
			for _, enabled := range []bool{false, true} {
				t.Run(map[bool]string{false: "legacy", true: "opted"}[enabled], func(t *testing.T) {
					must(t, creator.optIn(ctx, enabled))
					t.Run("ended", func(t *testing.T) {
						owns(t)
						requireUnknownStatusReply(t, creator, ctx, descriptor.ID, enabled)
						owns(t)
					})
					t.Run("absent", func(t *testing.T) {
						// No binding exists for a fabricated ID, even on a federated
						// host: this is the documented local no-live fallback.
						requireUnknownStatusReply(t, creator, ctx, "absent", enabled)
					})
					t.Run("other-methods", func(t *testing.T) {
						for _, method := range []string{"transfer.accept", "transfer.cancel"} {
							var result any = &protocol.TransferDescriptor{}
							if method == "transfer.cancel" {
								result = &protocol.TransferTerminal{}
							}
							err := creator.wire.Call(ctx, method, protocol.TransferTarget{ID: "absent"}, result)
							if rpcCode(err) != -32019 {
								t.Fatalf("%s semantic refusal changed: %v", method, err)
							}
						}
					})
				})
			}
		})
	}
}

// A status already waiting on the exact bridge generation must remain an
// unavailable operation when that generation ends. No later local fallback is
// used as evidence about the in-flight transport outcome.
func TestUnknownTransferInFlightBridgeLossRemainsUnavailable(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "opted"}[enabled], func(t *testing.T) {
			d, s, peer := reviewSession(t)
			d.host = "local"
			s.supportsTransfer = true
			item, _, _, ok := d.directory.installPeer(s, &protocol.PeerHello{Protocol: 1, Product: "fixture", SessionID: "caller", Groups: []string{"shared"}, Info: map[string]any{"sessionbus_transfer": 1}}, d.host)
			if !ok {
				t.Fatal("private caller install")
			}
			s.identity = item
			// Set negotiation using the real peer rehello path, not the new session flag.
			info := map[string]any{"sessionbus_transfer": 1}
			if enabled {
				info["sessionbus_unknown_transfer"] = 1
			}
			hello := &protocol.PeerHello{Protocol: 1, Product: "fixture", SessionID: "caller", Groups: []string{"shared"}, Info: info}
			s.peerHello(protocol.Frame{ID: 1, Method: "session.hello"}, hello)
			// Drain its real reply before starting the status under test.
			reader := bufio.NewReader(peer)
			must(t, peer.SetReadDeadline(time.Now().Add(time.Second)))
			if _, err := reader.ReadBytes('\n'); err != nil {
				t.Fatal(err)
			}
			actor, err := d.directory.transferEndpoint(item)
			must(t, err)
			ctx, end := context.WithCancel(context.Background())
			defer end()
			link := &federationLink{transfer: true, inbox: make(chan any, 1)}
			link.bridge = newTransferBridge(d, link, ctx)
			binding := &bridgeBinding{endpoint: actor, id: "id"}
			link.bridge.ids["id"] = binding
			d.federation = link
			s.transferRequest(protocol.Frame{ID: 2, Method: "transfer.status"}, &protocol.TransferTarget{ID: "id"})
			select {
			case event := <-link.inbox:
				call, ok := event.(federation.TransferCall)
				if !ok || call.Method != "transfer.status" {
					t.Fatalf("not an in-flight status: %T", event)
				}
			case <-time.After(time.Second):
				t.Fatal("status never entered actual bridge call")
			}
			end()
			select {
			case event := <-s.inbox:
				s.handleEvent(event)
			case <-time.After(time.Second):
				t.Fatal("cancelled bridge did not settle original status")
			}
			must(t, peer.SetReadDeadline(time.Now().Add(time.Second)))
			raw, err := reader.ReadBytes('\n')
			must(t, err)
			frame, err := protocol.DecodeFrame(raw)
			must(t, err)
			if frame.ID != 2 || frame.Error == nil || frame.Error.Code != protocol.Internal || string(frame.Error.Data) != `"transfer_unavailable"` {
				t.Fatalf("in-flight link loss became semantic status refusal: %s", raw)
			}
			s.list(protocol.Frame{ID: 3, Method: "session.list"}, &protocol.SessionListRequest{SessionID: "caller@local"})
			must(t, peer.SetReadDeadline(time.Now().Add(time.Second)))
			raw, err = reader.ReadBytes('\n')
			must(t, err)
			frame, err = protocol.DecodeFrame(raw)
			must(t, err)
			if frame.ID != 3 || frame.Error != nil {
				t.Fatalf("local ordinary work did not continue: %s", raw)
			}
			// This fixture is the actual bridge/cancellation/local-wire boundary; it does
			// not claim an independently observed TLS socket drop or a model effect.
		})
	}
}

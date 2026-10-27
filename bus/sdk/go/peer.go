// SPDX-License-Identifier: MIT

package sessionkit

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"reflect"
	"sync"
	"time"

	"github.com/antst/sessionbus/bus/sdk/go/internal/rpc"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

const defaultPeerReconnectInterval = 2 * time.Second

var peerReconnectInterval = defaultPeerReconnectInterval
var dialPeer = (&net.Dialer{}).DialContext

type DeliverFunc func(context.Context, PeerIdentity, DeliveryRequest) (DeliveryReceipt, error)

type Peer struct {
	Transfers     *Transfers
	Caller        *Caller
	mu            sync.Mutex
	identity      PeerIdentity
	admitted      PeerIdentity
	generation    uint64
	deliver       DeliverFunc
	socket        string
	wire          *rpc.Conn
	ctx           context.Context
	cancel        context.CancelFunc
	identityCtx   context.Context
	identityStop  context.CancelFunc
	ready, closed chan struct{}
	terminal      error
}

func ConnectPeer(identity PeerIdentity, deliver DeliverFunc) (*Peer, error) {
	return ConnectPeerContext(context.Background(), identity, deliver)
}

// ConnectPeerContext binds the peer connection and every reconnect attempt to ctx.
func ConnectPeerContext(ctx context.Context, identity PeerIdentity, deliver DeliverFunc) (*Peer, error) {
	return connectPeerContext(ctx, identity, deliver, nil)
}

// ConnectPeerWithTransfersContext opts into machine transfers only when the
// supplied executor can enforce effective native permission on actual handles.
func ConnectPeerWithTransfersContext(ctx context.Context, identity PeerIdentity, deliver DeliverFunc, executor TransferExecutor) (*Peer, error) {
	if executor == nil {
		return nil, ErrTransferScope
	}
	return connectPeerContext(ctx, identity, deliver, executor)
}
func connectPeerContext(ctx context.Context, identity PeerIdentity, deliver DeliverFunc, executor TransferExecutor) (*Peer, error) {
	if ctx == nil {
		return nil, errors.New("peer context is nil")
	}
	socket, _, err := sessionEnvironment(false)
	if err != nil {
		return nil, err
	}
	identity, err = snapshotIdentity(identity)
	if err != nil {
		return nil, err
	}
	peerCtx, cancel := context.WithCancel(ctx)
	p := &Peer{identity: identity, deliver: deliver, socket: socket, ctx: peerCtx, cancel: cancel, ready: make(chan struct{}), closed: make(chan struct{})}
	if executor != nil {
		p.Transfers = newTransfers(executor)
	}
	p.Caller = NewCaller(p.Call)
	p.Caller.transfers = p.Transfers
	go p.connect()
	return p, nil
}

func (p *Peer) Ready() <-chan struct{}  { return p.ready }
func (p *Peer) Closed() <-chan struct{} { return p.closed }
func (p *Peer) Err() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.terminal
}

// Rehello replaces the name and info; an empty name removes the native name.
func (p *Peer) Rehello(ctx context.Context, name string, info map[string]any) error {
	next, wire, generation, unchanged, err := p.desire(Identity{Name: name, Info: info}, false)
	if err != nil {
		return err
	}
	if unchanged {
		return nil
	}
	if wire == nil {
		return errNotConnected
	}
	done, acknowledged := make(chan error, 1), make(chan struct{})
	go func() {
		helloErr := p.helloObserved(p.ctx, wire, next, generation, func() { close(acknowledged) })
		if !p.failHello(helloErr) && helloErr != nil && wire.Context().Err() != nil {
			helloErr = errNotConnected
		}
		done <- helloErr
	}()
	select {
	case err = <-done:
		return err
	default:
	}
	select {
	case err = <-done:
		return err
	case <-ctx.Done():
		select {
		case <-acknowledged:
			return <-done
		case err = <-done:
			return err
		default:
			return ctx.Err()
		}
	}
}

func (p *Peer) Replace(ctx context.Context, identity Identity) error {
	identity, wire, generation, _, err := p.desire(identity, true)
	if err != nil {
		return err
	}
	if wire == nil {
		return errNotConnected
	}
	if err = p.hello(ctx, wire, identity, generation); err != nil {
		p.failHello(err)
		_ = wire.Close()
	}
	return err
}

func (p *Peer) desire(identity Identity, replacement bool) (Identity, *rpc.Conn, uint64, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ctx.Err() != nil {
		return Identity{}, nil, 0, false, &ProtocolError{Code: protocol.Superseded, Message: "superseded"}
	}
	if !replacement {
		identity.Product, identity.SessionID, identity.Groups = p.identity.Product, p.identity.SessionID, p.identity.Groups
	}
	identity, err := snapshotIdentity(identity)
	if err != nil {
		return Identity{}, nil, 0, false, err
	}
	if replacement && identity.SessionID == p.identity.SessionID {
		return Identity{}, nil, 0, false, &ProtocolError{Code: protocol.InvalidHello, Message: "invalid_hello"}
	}
	if !replacement && reflect.DeepEqual(identity, p.identity) {
		return identity, p.wire, p.generation, true, nil
	}
	p.identity, p.generation = identity, p.generation+1
	wire := p.wire
	if replacement {
		if p.identityStop != nil {
			p.identityStop()
		}
		p.wire = nil
		p.Caller.disconnected()
	}
	return identity, wire, p.generation, false, nil
}

func (p *Peer) Shutdown() {
	p.cancel()
	if wire := p.connection(); wire != nil {
		_ = wire.Close()
	}
}

func (p *Peer) Call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	var result json.RawMessage
	err := p.call(ctx, method, params, &result)
	return result, err
}

func (p *Peer) call(ctx context.Context, method string, params, result any) error {
	wire := p.connection()
	if wire == nil {
		return errNotConnected
	}
	return wire.Call(ctx, method, params, result)
}

func (p *Peer) connection() *rpc.Conn {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.wire
}

func (p *Peer) connect() {
	defer close(p.closed)
	defer p.Transfers.wait(nil)
	ready := p.ready
	for {
		fd, err := dialPeer(p.ctx, "unix", p.socket)
		if err == nil {
			var wire *rpc.Conn
			wire = rpc.New(fd, true, func(_ context.Context, request *rpc.Request) { p.handle(wire, request) })
			p.mu.Lock()
			identity, generation := p.identity, p.generation
			p.mu.Unlock()
			helloErr := p.hello(p.ctx, wire, identity, generation)
			if helloErr == nil {
				if ready != nil {
					close(ready)
					ready = nil
				}
				select {
				case <-wire.Done():
				case <-p.ctx.Done():
					_ = wire.Close()
					<-wire.Done()
				}
				p.mu.Lock()
				if p.wire == wire {
					p.wire = nil
					if p.identityStop != nil {
						p.identityStop()
						p.identityStop = nil
						p.identityCtx = nil
					}
				}
				p.mu.Unlock()
			}
			_ = wire.Close()
			// Old-generation cleanup remains charged, but never blocks ordinary
			// reconnection. Explicit peer disposal joins every owned generation above.
			if p.failHello(helloErr) {
				return
			}
		}
		if p.ctx.Err() != nil {
			return
		}
		select {
		case <-time.After(peerReconnectInterval):
		case <-p.ctx.Done():
			return
		}
	}
}

func (p *Peer) failHello(err error) bool {
	var failure *ProtocolError
	if !errors.As(err, &failure) || failure.Code != protocol.InvalidHello {
		return false
	}
	p.mu.Lock()
	p.terminal = failure
	p.mu.Unlock()
	p.cancel()
	return true
}

func (p *Peer) hello(ctx context.Context, wire *rpc.Conn, identity PeerIdentity, generation uint64) error {
	return p.helloObserved(ctx, wire, identity, generation, nil)
}

func (p *Peer) helloObserved(ctx context.Context, wire *rpc.Conn, identity PeerIdentity, generation uint64, acknowledged func()) error {
	for {
		installed := false
		hello := identity
		hello.Info = make(map[string]any, len(identity.Info)+1)
		for key, value := range identity.Info {
			if key != protocol.TransferInfoKey {
				hello.Info[key] = value
			}
		}
		if p.Transfers != nil {
			hello.Info[protocol.TransferInfoKey] = 1
		}
		var helloResult protocol.HelloResult
		err := wire.CallObserved(ctx, "session.hello", hello, &helloResult, func() error {
			p.mu.Lock()
			defer p.mu.Unlock()
			if p.ctx.Err() != nil {
				return rpc.ErrClosed
			}
			if generation == p.generation {
				if p.wire != wire {
					p.identityCtx, p.identityStop = context.WithCancel(wire.Context())
				}
				p.admitted, p.wire, installed = identity, wire, true
				if p.Transfers != nil {
					enabled := false
					for _, capability := range helloResult.Capabilities {
						if capability == protocol.TransferCapability {
							enabled = true
						}
					}
					p.Transfers.attach(wire, p.identityCtx, enabled)
				}
			} else {
				identity, generation = p.identity, p.generation
			}
			return nil
		})
		if err != nil {
			return err
		}
		if installed {
			if acknowledged != nil {
				acknowledged()
			}
			return nil
		}
	}
}

func (p *Peer) handle(wire *rpc.Conn, request *rpc.Request) {
	if p.Transfers.handle(wire, request) {
		return
	}
	switch request.Method {
	case "session.superseded":
		p.mu.Lock()
		if p.wire == wire {
			p.wire = nil
			p.terminal = &ProtocolError{Code: protocol.Superseded, Message: "superseded"}
			if p.identityStop != nil {
				p.identityStop()
			}
		}
		p.mu.Unlock()
		p.Caller.disconnected()
		go func() { _ = wire.Result(request, struct{}{}); p.cancel(); _ = wire.Close() }()
	case "message.deliver":
		project := p.Transfers.ProjectDelivery(wire, request.Params.(*DeliveryRequest))
		p.mu.Lock()
		identity, err := snapshotIdentity(p.admitted)
		identityCtx := p.identityCtx
		current := p.wire == wire && identityCtx != nil
		p.mu.Unlock()
		go func() {
			receipt := DeliveryReceipt{Disposition: "rejected", Reason: "closing"}
			if current && err == nil && project != nil {
				err = project(identityCtx, request.Params.(*DeliveryRequest))
			}
			if current && err == nil {
				receipt, err = p.deliver(identityCtx, identity, *request.Params.(*DeliveryRequest))
			}
			var failure *ProtocolError
			if errors.As(err, &failure) && failure.Code == protocol.Internal {
				if wire.Error(request, failure.Code, failure.Data) != nil {
					_ = wire.Close()
				}
				return
			}
			if err != nil {
				receipt = DeliveryReceipt{Disposition: "rejected", Reason: err.Error()}
			}
			if wire.Result(request, receipt) != nil {
				_ = wire.Close()
			}
		}()
	}
}

var errNotConnected = &ProtocolError{Code: protocol.NotConnected, Message: "not_connected"}

func snapshotIdentity(identity PeerIdentity) (PeerIdentity, error) {
	identity.Protocol = 1
	raw, err := protocol.EncodeParams("session.hello", identity)
	var snapshot PeerIdentity
	if err == nil {
		err = json.Unmarshal(raw, &snapshot)
	}
	return snapshot, err
}

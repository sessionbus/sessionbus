// SPDX-License-Identifier: GPL-3.0-only
package daemon

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"slices"
	"sync"
	"time"

	"github.com/antst/sessionbus/bus/internal/federation"
	"github.com/antst/sessionbus/bus/internal/transfer"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

// One bridge belongs to one hub attachment. Reconnect creates another bridge;
// neither opaque bindings nor pending calls are adopted or replayed.
type transferBridge struct {
	daemon *Daemon
	link   *federationLink
	ctx    context.Context
	mu     sync.Mutex
	tokens map[string]*bridgeBinding
	ids    map[string]*bridgeBinding
	closed bool
}
type bridgeBinding struct {
	armOnce, endOnce       sync.Once
	armed                  bool
	bridge                 *transferBridge
	mu                     sync.Mutex
	holds                  sync.WaitGroup
	token, id, role, phase string
	endpoint               transfer.Endpoint
	reservation            *transfer.Reservation
	ctx                    context.Context
	cancel                 context.CancelFunc
	expires                time.Time
	timer                  *time.Timer
	held, ended            bool
	retired                chan struct{}
	confirmed              *protocol.TransferTerminal
}

func newTransferBridge(d *Daemon, link *federationLink, ctx context.Context) *transferBridge {
	b := &transferBridge{daemon: d, link: link, ctx: ctx, tokens: map[string]*bridgeBinding{}, ids: map[string]*bridgeBinding{}}
	go func() { <-ctx.Done(); b.shutdown() }()
	return b
}
func (b *transferBridge) shutdown() {
	b.mu.Lock()
	b.closed = true
	all := make([]*bridgeBinding, 0, len(b.tokens))
	for _, entry := range b.tokens {
		all = append(all, entry)
	}
	b.mu.Unlock()
	for _, entry := range all {
		go func(x *bridgeBinding) {
			_ = x.terminal(protocol.TransferNotice{ID: x.transferID(), Kind: "terminal", Outcome: "unknown", Reason: "hub_generation_ended"})
		}(entry)
	}
}
func (x *bridgeBinding) transferID() string { x.mu.Lock(); defer x.mu.Unlock(); return x.id }
func (b *transferBridge) call(ctx context.Context, method string, params, result any) error {
	reply := make(chan federation.Reply, 1)
	call := federation.TransferCall{Method: method, Params: params, Reply: reply}
	if method == "transfer.chunk" {
		select {
		case b.link.transferBulk <- call:
		case <-ctx.Done():
			return ctx.Err()
		case <-b.ctx.Done():
			return transfer.ErrUnsupported
		}
	} else {
		select {
		case b.link.inbox <- call:
		case <-ctx.Done():
			return ctx.Err()
		case <-b.ctx.Done():
			return transfer.ErrUnsupported
		}
	}
	select {
	case response := <-reply:
		if response.Error != nil {
			return response.Error
		}
		return json.Unmarshal(response.Result, result)
	case <-ctx.Done():
		return ctx.Err()
	case <-b.ctx.Done():
		return transfer.ErrUnsupported
	}
}
func (b *transferBridge) bind(ctx context.Context, p federation.TransferBind) (federation.TransferBound, error) {
	d := b.daemon.directory
	d.mu.Lock()
	items := matchingEntries(d.connected, p.Target, p.From.Groups)
	var item *entry
	if len(items) == 1 {
		item = items[0]
	}
	if item != nil && p.Generation != "" {
		if item.row.SessionID != p.From.SessionID || item.transferKey != p.Generation || item.lifetime == nil || item.lifetime.token != p.From.OwnerLifetime || item.row.Product != p.From.Product || !slices.Equal(item.row.Groups, p.From.Groups) {
			item = nil
		}
	}
	d.mu.Unlock()
	endpoint, err := d.transferEndpoint(item)
	if err != nil {
		return federation.TransferBound{}, err
	}
	reservation, err := b.daemon.transferBudget.PendingFor(endpoint.Key)
	if err != nil {
		return federation.TransferBound{}, err
	}
	var entropy [16]byte
	if _, err = rand.Read(entropy[:]); err != nil {
		reservation.Release()
		return federation.TransferBound{}, err
	}
	token := hex.EncodeToString(entropy[:])
	child, cancel := context.WithCancel(b.ctx)
	entry := &bridgeBinding{bridge: b, token: token, endpoint: endpoint, reservation: reservation, ctx: child, cancel: cancel, phase: "pending", expires: time.Now().Add(10 * time.Minute), retired: make(chan struct{})}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		cancel()
		reservation.Release()
		return federation.TransferBound{}, transfer.ErrUnsupported
	}
	b.tokens[token] = entry
	entry.timer = time.AfterFunc(10*time.Minute, func() {
		_ = entry.terminal(protocol.TransferNotice{Kind: "terminal", Reason: "deadline_elapsed"})
	})
	b.mu.Unlock()

	return federation.TransferBound{Binding: token, Generation: endpoint.Key, Identity: endpoint.Identity}, nil
}
func (b *transferBridge) lookupToken(token string) (*bridgeBinding, error) {
	b.mu.Lock()
	entry := b.tokens[token]
	b.mu.Unlock()
	if entry == nil {
		return nil, transfer.ErrUnknown
	}
	return entry, nil
}
func (b *transferBridge) lookupID(id string) (*bridgeBinding, error) {
	b.mu.Lock()
	entry := b.ids[id]
	b.mu.Unlock()
	if entry == nil {
		return nil, transfer.ErrUnknown
	}
	return entry, nil
}
func (b *transferBridge) associate(entry *bridgeBinding, id string) error {
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if entry.ended || entry.id != "" && entry.id != id {
		return transfer.ErrState
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if other := b.ids[id]; other != nil && other != entry {
		return transfer.ErrState
	}
	entry.id = id
	b.ids[id] = entry
	return nil
}
func (x *bridgeBinding) terminal(n protocol.TransferNotice) error {
	x.mu.Lock()
	if x.ended {
		x.mu.Unlock()
		select {
		case <-x.retired:
			return nil
		default:
			return transfer.ErrState
		}
	}
	// Classify local close/deadline while holding the SAME phase lock used to
	// enter committing. Transport loss keeps its separate UNKNOWN projection.
	if x.confirmed != nil {
		known := x.confirmed
		n.Outcome, n.Reason, n.Bytes, n.SHA256 = known.Outcome, known.Reason, &known.Bytes, known.SHA256
	} else if n.Reason == "endpoint_closing" || n.Reason == "deadline_elapsed" {
		if x.phase == "committing" {
			n.Outcome = "unknown"
			if n.Reason == "deadline_elapsed" {
				n.Reason = "commit_unconfirmed"
			}
		} else if n.Reason == "endpoint_closing" {
			n.Outcome = "cancelled"
		} else {
			switch x.phase {
			case "pending":
				n.Outcome, n.Reason = "expired", "expired"
			case "preparing":
				n.Outcome, n.Reason = "failed", "prepare_timeout"
			default:
				n.Outcome, n.Reason = "failed", "progress_timeout"
			}
		}
	}
	x.ended = true
	x.timer.Stop()
	id := x.id
	x.mu.Unlock()
	// No new hold can enter after ended. Join existing owner round-trips
	// outside mu so a shared-reader ingress gate never waits on that owner.
	x.holds.Wait()
	x.mu.Lock()
	held := x.held
	x.mu.Unlock()
	var err error
	if id != "" {
		n.ID = id
		if n.Bytes == nil {
			bytes := protocol.Integer(0)
			n.Bytes = &bytes
		}
		err = x.endpoint.Notice(x.bridge.daemon.traceContext, n)
	}
	finish := func() {
		if held {
			x.endpoint.Hold(x.bridge.daemon.traceContext, id, true)
		}
		b := x.bridge
		b.mu.Lock()
		if b.tokens[x.token] == x {
			delete(b.tokens, x.token)
		}
		if b.ids[id] == x {
			delete(b.ids, id)
		}
		b.mu.Unlock()
		x.reservation.Release()
		x.signalEnd()
		x.cancel()
		close(x.retired)
	}
	if err == nil {
		finish()
		return nil
	}
	// No lost callback/tool response is cleanup proof. Keep the original charge
	// until the exact native generation or this daemon's lifetime actually ends.
	go func() {
		select {
		case <-x.endpoint.Done:
		case <-x.bridge.daemon.traceContext.Done():
		}
		finish()
	}()
	return err
}
func (b *transferBridge) server(ctx context.Context, method string, raw json.RawMessage) (json.RawMessage, error) {
	params, err := federation.DecodeTransferParams(method, raw)
	if err != nil {
		return nil, err
	}
	var value any
	switch p := params.(type) {
	case *federation.TransferBind:
		value, err = b.bind(ctx, *p)
	case *federation.TransferBindingTarget:
		if method == federation.TransferBindReadyMethod {
			var entry *bridgeBinding
			entry, err = b.lookupToken(p.Binding)
			if err == nil {
				err = entry.arm()
			}
			value = struct{}{}
			break
		}
		var entry *bridgeBinding
		entry, err = b.lookupToken(p.Binding)
		if err == nil {
			entry.mu.Lock()
			pending := entry.phase == "pending"
			entry.mu.Unlock()
			if !pending {
				return nil, transfer.ErrState
			}
			err = entry.terminal(protocol.TransferNotice{Kind: "terminal", Outcome: "cancelled", Reason: "creation_refused"})
			value = struct{}{}
		}
	case *federation.TransferPreparedRoute:
		var entry *bridgeBinding
		entry, err = b.lookupToken(p.Binding)
		if err != nil {
			break
		}
		if err = b.associate(entry, p.Prepare.ID); err != nil {
			break
		}
		entry.mu.Lock()
		if entry.ended {
			entry.mu.Unlock()
			err = transfer.ErrState
			break
		}
		if entry.role != "" && entry.role != p.Prepare.Role {
			entry.mu.Unlock()
			err = transfer.ErrRole
			break
		}
		entry.role = p.Prepare.Role
		err = entry.reservation.Prepare()
		if err == nil {
			entry.phase = "preparing"
			remaining := time.Until(entry.expires)
			if remaining > 60*time.Second {
				remaining = 60 * time.Second
			}
			if remaining <= 0 {
				err = transfer.ErrState
			} else {
				entry.timer.Reset(remaining)
			}
		}
		entry.mu.Unlock()
		if err != nil {
			break
		}
		var prepared protocol.TransferPrepared
		err = entry.endpoint.Call(ctx, "transfer.prepare", p.Prepare, &prepared)
		value = prepared
	case *federation.TransferHold:
		var entry *bridgeBinding
		entry, err = b.lookupToken(p.Binding)
		if err != nil {
			break
		}
		if err = b.associate(entry, p.ID); err != nil {
			break
		}
		entry.mu.Lock()
		if entry.ended {
			entry.mu.Unlock()
			value = federation.TransferHeld{Held: false}
			break
		}
		entry.holds.Add(1)
		if !p.Terminal {
			// Even a lost/refused response can follow an applied owner event.
			// Keep the release obligation until a positive terminal reply.
			entry.held = true
		}
		entry.mu.Unlock()
		held := entry.endpoint.Hold(ctx, p.ID, p.Terminal)
		entry.mu.Lock()
		if held {
			entry.held = !p.Terminal
		}
		entry.mu.Unlock()
		entry.holds.Done()
		value = federation.TransferHeld{Held: held}
	case *federation.TransferNoticeRoute:
		var entry *bridgeBinding
		entry, err = b.lookupToken(p.Binding)
		if err != nil {
			break
		}
		if err = b.associate(entry, p.Notice.ID); err != nil {
			break
		}
		if p.Notice.Kind == "terminal" {
			err = entry.terminal(p.Notice)
		} else {
			err = entry.endpoint.Notice(ctx, p.Notice)
		}
		value = struct{}{}
	case *protocol.TransferStart:
		var entry *bridgeBinding
		entry, err = b.lookupID(p.ID)
		if err != nil {
			break
		}
		entry.mu.Lock()
		if entry.ended || entry.role != p.Role {
			entry.mu.Unlock()
			err = transfer.ErrRole
			break
		}
		entry.phase = "streaming"
		entry.timer.Reset(60 * time.Second)
		entry.mu.Unlock()
		var started protocol.TransferStarted
		err = entry.endpoint.Call(ctx, method, p, &started)
		value = started
	case *protocol.TransferChunk:
		var entry *bridgeBinding
		entry, err = b.lookupID(p.ID)
		if err != nil {
			break
		}
		entry.mu.Lock()
		valid := !entry.ended && entry.role == "destination" && entry.phase == "streaming"
		entry.mu.Unlock()
		if !valid {
			err = transfer.ErrRole
			break
		}
		var credit protocol.TransferCredit
		err = entry.endpoint.Call(ctx, method, p, &credit)
		value = credit
		if err == nil {
			entry.mu.Lock()
			if !entry.ended {
				entry.timer.Reset(60 * time.Second)
			}
			entry.mu.Unlock()
		}
	case *protocol.TransferFinish:
		var entry *bridgeBinding
		entry, err = b.lookupID(p.ID)
		if err != nil {
			break
		}
		entry.mu.Lock()
		valid := !entry.ended && entry.role == "destination" && entry.phase == "streaming"
		if valid {
			entry.phase = "committing"
		}
		entry.mu.Unlock()
		if !valid {
			err = transfer.ErrRole
			break
		}
		var terminal protocol.TransferTerminal
		err = entry.endpoint.Call(ctx, method, p, &terminal)
		if err == nil {
			entry.observeCommit(*p, terminal)
		}
		value = terminal
	default:
		err = transfer.ErrUnsupported
	}
	if err != nil {
		return nil, err
	}
	return json.Marshal(value)
}
func (b *transferBridge) canChunk(actor, id, role string) bool {
	entry, err := b.lookupID(id)
	if err != nil {
		return false
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	return !entry.ended && entry.phase == "streaming" && entry.role == role && (actor == "" || entry.endpoint.Key == actor)
}
func (b *transferBridge) destinationGate(frame protocol.Frame) bool {
	var p protocol.TransferTarget
	if json.Unmarshal(frame.Params, &p) != nil {
		return false
	}
	return b.canChunk("", p.ID, "destination")
}
func (b *transferBridge) owns(actor, id string) bool {
	entry, err := b.lookupID(id)
	if err != nil {
		return false
	}
	return entry.endpoint.Key == actor
}
func (b *transferBridge) forward(ctx context.Context, actor string, method string, params any) (any, error) {
	var id string
	switch p := params.(type) {
	case *protocol.TransferTarget:
		id = p.ID
	case *protocol.TransferAccept:
		id = p.ID
	case *protocol.TransferChunk:
		id = p.ID
	case *protocol.TransferFinish:
		id = p.ID
	case *protocol.TransferFailure:
		id = p.ID
	default:
		return nil, transfer.ErrUnsupported
	}
	entry, err := b.lookupID(id)
	if err != nil || entry.endpoint.Key != actor {
		return nil, transfer.ErrUnknown
	}
	var result any
	switch method {
	case "transfer.accept", "transfer.status":
		result = &protocol.TransferDescriptor{}
	case "transfer.cancel", "transfer.finish", "transfer.fail":
		result = &protocol.TransferTerminal{}
	case "transfer.chunk":
		result = &protocol.TransferCredit{}
	default:
		return nil, transfer.ErrUnsupported
	}
	if method == "transfer.finish" {
		entry.mu.Lock()
		entry.phase = "committing"
		entry.mu.Unlock()
	}
	err = b.call(ctx, method, params, result)
	if err == nil && method == "transfer.finish" {
		entry.observeCommit(*params.(*protocol.TransferFinish), *result.(*protocol.TransferTerminal))
	}
	if err == nil && method == "transfer.chunk" {
		entry.mu.Lock()
		if !entry.ended {
			entry.timer.Reset(60 * time.Second)
		}
		entry.mu.Unlock()
	}
	return result, err
}
func (x *bridgeBinding) observeCommit(p protocol.TransferFinish, result protocol.TransferTerminal) {
	if result.Outcome != "completed" || result.ID != p.ID || result.Bytes != p.Bytes || result.SHA256 != p.SHA256 {
		return
	}
	x.mu.Lock()
	x.confirmed = &result
	x.mu.Unlock()
}
func (b *transferBridge) endEndpoint(key string) {
	b.mu.Lock()
	var all []*bridgeBinding
	for _, entry := range b.tokens {
		if entry.endpoint.Key == key {
			all = append(all, entry)
		}
	}
	b.mu.Unlock()
	for _, entry := range all {
		go func(x *bridgeBinding) {
			_ = x.terminal(protocol.TransferNotice{Kind: "terminal", Reason: "endpoint_closing"})
		}(entry)
	}
}

func (x *bridgeBinding) signalEnd() {
	x.mu.Lock()
	armed := x.armed
	x.mu.Unlock()
	if !armed {
		return
	}
	x.endOnce.Do(func() {
		go func() {
			var empty struct{}
			_ = x.bridge.call(x.bridge.ctx, federation.TransferEndMethod, federation.TransferBindingTarget{Binding: x.token}, &empty)
		}()
	})
}
func (x *bridgeBinding) arm() error {
	x.mu.Lock()
	if x.ended {
		x.mu.Unlock()
		return transfer.ErrUnknown
	}
	x.armed = true
	x.mu.Unlock()
	x.armOnce.Do(func() {
		go func() {
			select {
			case <-x.endpoint.Done:
				x.signalEnd()
				_ = x.terminal(protocol.TransferNotice{ID: x.transferID(), Kind: "terminal", Outcome: "unknown", Reason: "native_generation_ended"})
			case <-x.ctx.Done():
			}
		}()
	})
	return nil
}

func (s *session) forwardTransfer(frame protocol.Frame, params any, origin transfer.Endpoint) bool {
	var target string
	kind, name, what := "", "", ""
	switch p := params.(type) {
	case *protocol.TransferOffer:
		target, kind, name = p.Target, "offer", p.Name
	case *protocol.TransferRequest:
		target, kind, name, what = p.Target, "request", p.Name, p.What
	}
	var canonicalID, remoteHost string
	if target != "" {
		var code int
		canonicalID, remoteHost, code = canonicalTarget(target, s.daemon.host, validIDPart)
		if code != 0 {
			return false
		}
		if remoteHost == s.daemon.host {
			return false
		}
	}
	s.daemon.directory.mu.Lock()
	link := s.daemon.federation
	s.daemon.directory.mu.Unlock()
	if target == "" {
		var id string
		switch p := params.(type) {
		case *protocol.TransferTarget:
			id = p.ID
		case *protocol.TransferAccept:
			id = p.ID
		case *protocol.TransferChunk:
			id = p.ID
		case *protocol.TransferFinish:
			id = p.ID
		case *protocol.TransferFailure:
			id = p.ID
		}
		if link == nil || !link.transfer || !link.bridge.owns(origin.Key, id) {
			return false
		}
	} else if link == nil || !link.transfer {
		s.error(frame, protocol.UnsupportedOpen, "unsupported_transfer")
		return true
	}
	caller := s.federationCaller()
	reply := make(chan answer, 1)
	s.requests[frame.ID] = &requestState{frame: frame, pending: 1}
	s.await(frame.ID, 0, reply, s.identity.done)
	go func() {
		response := answer{}
		if target != "" {
			var created federation.TransferCreated
			err := link.bridge.call(link.bridge.ctx, federation.TransferCreateMethod, federation.TransferCreate{From: caller, Generation: origin.Key, Target: canonicalID, Kind: kind, Name: name, What: what}, &created)
			if err == nil {
				var binding *bridgeBinding
				binding, err = link.bridge.lookupToken(created.OriginBinding)
				if err == nil && binding.endpoint.Key != origin.Key {
					err = transfer.ErrRole
				}
				if err == nil {
					err = link.bridge.associate(binding, created.Transfer.ID)
				}
			}
			if err != nil {
				response.code, response.data = protocol.Internal, "transfer_unavailable"
			} else {
				response.value = created.Transfer
				response.after = func() {
					var empty struct{}
					_ = link.bridge.call(link.bridge.ctx, federation.TransferAnnounceMethod, protocol.TransferTarget{ID: created.Transfer.ID}, &empty)
				}
			}
		} else {
			value, err := link.bridge.forward(link.bridge.ctx, origin.Key, frame.Method, params)
			response.value = value
			if err != nil {
				response.code, response.data = protocol.Internal, "transfer_unavailable"
			}
		}
		reply <- response
	}()
	return true
}

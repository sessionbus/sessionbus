// SPDX-License-Identifier: GPL-3.0-only
package federation

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/antst/sessionbus/bus/internal/transfer"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

type hubTransfers struct {
	ctx      context.Context
	engine   *transfer.Engine
	budget   *transfer.Budget
	mu       sync.Mutex
	routes   map[string]*hubTransferRoute
	bindings map[string]*hubTransferBinding
}
type hubTransferBinding struct {
	hub     *hubTransfers
	link    *hostLink
	bound   TransferBound
	ctx     context.Context
	cancel  context.CancelFunc
	key     string
	mu      sync.Mutex
	cleaned bool
	route   *hubTransferRoute
}
type hubTransferRoute struct {
	id             string
	creator, other *hubTransferBinding
	mu             sync.Mutex
	cleaned        int
}

func newHubTransfers(ctx context.Context) *hubTransfers {
	budget := transfer.HubBudget()
	return &hubTransfers{ctx: ctx, budget: budget, engine: transfer.NewEngine(ctx, budget), routes: map[string]*hubTransferRoute{}, bindings: map[string]*hubTransferBinding{}}
}
func bindingMapKey(link *hostLink, binding string) string { return link.attachment + ":" + binding }
func (l *hostLink) transferCall(ctx context.Context, method string, params, result any) error {
	if !SupportsTransfer(l.fd) {
		return transfer.ErrUnsupported
	}
	reply := make(chan Reply, 1)
	call := TransferCall{Method: method, Params: params, Reply: reply}
	var target chan TransferCall
	if method == "transfer.chunk" {
		target = l.transferBulk
	}
	if target != nil {
		select {
		case target <- call:
		case <-ctx.Done():
			return ctx.Err()
		case <-l.done:
			return transfer.ErrUnsupported
		}
	} else {
		select {
		case l.inbox <- call:
		case <-ctx.Done():
			return ctx.Err()
		case <-l.done:
			return transfer.ErrUnsupported
		}
	}
	select {
	case r := <-reply:
		if r.Error != nil {
			return r.Error
		}
		return json.Unmarshal(r.Result, result)
	case <-ctx.Done():
		return ctx.Err()
	case <-l.done:
		return transfer.ErrUnsupported
	}
}
func (h *hubTransfers) bind(ctx context.Context, link *hostLink, input TransferBind) (*hubTransferBinding, error) {
	var bound TransferBound
	if err := link.transferCall(ctx, TransferBindMethod, input, &bound); err != nil {
		return nil, err
	}
	if !ownedPart(bound.Identity.SessionID, link.host, false) {
		return nil, transfer.ErrUnsupported
	}
	child, cancel := context.WithCancel(h.ctx)
	b := &hubTransferBinding{hub: h, link: link, bound: bound, ctx: child, cancel: cancel, key: link.attachment + ":" + bound.Generation}
	h.mu.Lock()
	mapKey := bindingMapKey(link, bound.Binding)
	if h.bindings[mapKey] != nil {
		h.mu.Unlock()
		cancel()
		return nil, transfer.ErrState
	}
	h.bindings[mapKey] = b
	h.mu.Unlock()
	go func() {
		select {
		case <-link.done:
			cancel()
		case <-child.Done():
		}
	}()
	var ready struct{}
	if err := link.transferCall(ctx, TransferBindReadyMethod, TransferBindingTarget{Binding: bound.Binding}, &ready); err != nil {
		h.forgetBinding(b)
		return nil, err
	}
	return b, nil
}
func (b *hubTransferBinding) endpoint() transfer.Endpoint {
	ep := transfer.Endpoint{Key: b.key, Identity: b.bound.Identity, Done: b.ctx.Done()}
	ep.Call = func(ctx context.Context, method string, params, result any) error {
		select {
		case <-b.ctx.Done():
			return transfer.ErrUnsupported
		default:
		}
		if method == "transfer.prepare" {
			return b.link.transferCall(ctx, TransferPrepareMethod, TransferPreparedRoute{Binding: b.bound.Binding, Prepare: params.(protocol.TransferPrepare)}, result)
		}
		return b.link.transferCall(ctx, method, params, result)
	}
	ep.Hold = func(ctx context.Context, id string, terminal bool) bool {
		b.mu.Lock()
		cleaned := b.cleaned
		b.mu.Unlock()
		if terminal && cleaned {
			return true
		}
		var held TransferHeld
		err := b.link.transferCall(ctx, TransferHoldMethod, TransferHold{Binding: b.bound.Binding, ID: id, Terminal: terminal}, &held)
		return err == nil && held.Held
	}
	ep.Notice = func(ctx context.Context, n protocol.TransferNotice) error {
		var empty struct{}
		err := b.link.transferCall(ctx, TransferNoticeMethod, TransferNoticeRoute{Binding: b.bound.Binding, Notice: n}, &empty)
		if err == nil && n.Kind == "terminal" {
			b.retire()
		}
		return err
	}
	return ep
}
func (h *hubTransfers) forgetBinding(b *hubTransferBinding) {
	h.mu.Lock()
	key := bindingMapKey(b.link, b.bound.Binding)
	if h.bindings[key] == b {
		delete(h.bindings, key)
	}
	h.mu.Unlock()
	b.cancel()
}
func (h *hubTransfers) endLink(link *hostLink) {
	h.mu.Lock()
	var all []*hubTransferBinding
	for _, b := range h.bindings {
		if b.link == link {
			all = append(all, b)
		}
	}
	h.mu.Unlock()
	for _, b := range all {
		b.retire()
	}
}
func (h *hubTransfers) endBinding(link *hostLink, token string) {
	h.mu.Lock()
	b := h.bindings[bindingMapKey(link, token)]
	h.mu.Unlock()
	if b != nil {
		b.retire()
	}
}
func (h *hubTransfers) chunkAdmitted(link *hostLink, frame protocol.Frame) bool {
	var p protocol.TransferTarget
	if json.Unmarshal(frame.Params, &p) != nil {
		return false
	}
	h.mu.Lock()
	route := h.routes[p.ID]
	h.mu.Unlock()
	if route == nil {
		return false
	}
	b := route.creator
	if b.link != link {
		b = route.other
	}
	if b.link != link {
		return false
	}
	return h.engine.CanChunk(b.key, p.ID)
}
func (h *hubTransfers) create(origin, destination *hostLink, p TransferCreate) (TransferCreated, error) {
	if destination == nil || destination == origin || !SupportsTransfer(origin.fd) || !SupportsTransfer(destination.fd) {
		return TransferCreated{}, transfer.ErrUnsupported
	}
	reservation, err := h.budget.PendingFor(origin.attachment + ":" + p.Generation)
	if err != nil {
		return TransferCreated{}, err
	}
	adopted := false
	defer func() {
		if !adopted {
			reservation.Release()
		}
	}()
	source, err := h.bind(h.ctx, origin, TransferBind{From: p.From, Target: p.From.SessionID, Generation: p.Generation})
	if err != nil {
		return TransferCreated{}, err
	}
	var target *hubTransferBinding
	defer func() {
		if !adopted {
			for _, b := range []*hubTransferBinding{source, target} {
				if b != nil {
					var empty struct{}
					_ = b.link.transferCall(h.ctx, TransferDropMethod, TransferBindingTarget{Binding: b.bound.Binding}, &empty)
					h.forgetBinding(b)
				}
			}
		}
	}()
	target, err = h.bind(h.ctx, destination, TransferBind{From: p.From, Target: p.Target})
	if err != nil {
		return TransferCreated{}, err
	}
	// One bounded metadata copy per hub engine: refuse an unrepresentable scope
	// before any file handle opens. Ordinary messaging remains available.
	metadata, _ := json.Marshal(struct {
		Source, Destination TransferBound
		Name, What          string
	}{source.bound, target.bound, p.Name, p.What})
	if len(metadata) > protocol.TransferPendingCopyBytes {
		return TransferCreated{}, transfer.ErrCapacity
	}
	descriptor, err := h.engine.CreateReserved(source.endpoint(), target.endpoint(), p.Kind == "offer", p.Name, p.What, reservation)
	if err != nil {
		return TransferCreated{}, err
	}
	route := &hubTransferRoute{id: descriptor.ID, creator: source, other: target}
	source.mu.Lock()
	source.route = route
	if source.cleaned {
		route.mu.Lock()
		route.cleaned++
		route.mu.Unlock()
	}
	source.mu.Unlock()
	target.mu.Lock()
	target.route = route
	if target.cleaned {
		route.mu.Lock()
		route.cleaned++
		route.mu.Unlock()
	}
	target.mu.Unlock()
	route.mu.Lock()
	h.mu.Lock()
	if route.cleaned < 2 {
		h.routes[descriptor.ID] = route
	}
	h.mu.Unlock()
	route.mu.Unlock()
	adopted = true
	return TransferCreated{Transfer: descriptor, OriginBinding: source.bound.Binding}, nil
}
func (h *hubTransfers) action(origin *hostLink, method string, params any) (any, error) {
	if p, ok := params.(*TransferBindingTarget); ok && method == TransferEndMethod {
		h.endBinding(origin, p.Binding)
		return struct{}{}, nil
	}
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
	h.mu.Lock()
	route := h.routes[id]
	h.mu.Unlock()
	if route == nil {
		return nil, transfer.ErrUnknown
	}
	actor := route.creator
	if actor.link != origin {
		actor = route.other
	}
	if actor.link != origin {
		return nil, transfer.ErrRole
	}
	switch method {
	case TransferAnnounceMethod:
		if actor != route.creator {
			return nil, transfer.ErrRole
		}
		h.engine.Announce(id)
		return struct{}{}, nil
	case "transfer.accept":
		return h.engine.Accept(actor.key, *params.(*protocol.TransferAccept))
	case "transfer.status":
		return h.engine.Status(actor.key, id)
	case "transfer.cancel":
		return h.engine.Cancel(actor.key, id)
	case "transfer.chunk":
		return h.engine.Chunk(actor.key, *params.(*protocol.TransferChunk))
	case "transfer.finish":
		return h.engine.Finish(actor.key, *params.(*protocol.TransferFinish))
	case "transfer.fail":
		return h.engine.Fail(actor.key, *params.(*protocol.TransferFailure))
	}
	return nil, transfer.ErrUnsupported
}
func (h *hubTransfers) submit(link *hostLink, frame connTransferFrame) {
	params, err := decodeTransferParams(frame.value.Method, frame.value.Params)
	if err != nil {
		if !h.reply(link, frame, nil, err) {
			link.wire.Close()
		}
		return
	}
	go func() {
		value, err := h.action(link, frame.value.Method, params)
		if !h.reply(link, frame, value, err) {
			link.wire.Close()
		}
	}()
}

type connTransferFrame struct {
	value   protocol.Frame
	release func()
}

func (h *hubTransfers) reply(link *hostLink, frame connTransferFrame, value any, err error) bool {
	reply := transferReply(value, err)
	event := originReply{id: frame.value.ID, value: reply, origin: link, destination: link, release: frame.release}
	select {
	case <-link.done:
	case <-h.ctx.Done():
	default:
		// This can run on that inbox's own owner or the registry. Never wait
		// for capacity from either loop; the caller applies normal overload close.
		select {
		case link.inbox <- event:
			return true
		default:
		}
	}
	if frame.release != nil {
		frame.release()
	}
	return false
}

func (b *hubTransferBinding) retire() {
	b.mu.Lock()
	first := !b.cleaned
	b.cleaned = true
	route := b.route
	b.mu.Unlock()
	if !first {
		return
	}
	b.hub.forgetBinding(b)
	if route != nil {
		route.mu.Lock()
		route.cleaned++
		all := route.cleaned == 2
		route.mu.Unlock()
		if all {
			b.hub.mu.Lock()
			if b.hub.routes[route.id] == route {
				delete(b.hub.routes, route.id)
			}
			b.hub.mu.Unlock()
		}
	}
}

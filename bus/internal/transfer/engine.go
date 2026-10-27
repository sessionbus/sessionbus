// SPDX-License-Identifier: GPL-3.0-only

package transfer

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"time"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

var ErrUnknown = errors.New("unknown_transfer")
var ErrRole = errors.New("transfer_role_mismatch")
var ErrState = errors.New("transfer_state_mismatch")
var ErrUnsupported = errors.New("unsupported_transfer")

// Endpoint is an authenticated attachment, never an endpoint-supplied sender
// assertion. Key must change on every replacement/connection generation. Call
// and Notice must be bounded by their connection and use separate bulk slots.
// Notice performs machine cleanup, then the normal daemon stage/wake projection.
type Endpoint struct {
	Key      string
	Identity protocol.DeliverySource
	Done     <-chan struct{}
	Call     func(context.Context, string, any, any) error
	Hold     func(context.Context, string, bool) bool
	Notice   func(context.Context, protocol.TransferNotice) error
}

type Engine struct {
	mu                                  sync.Mutex
	ctx                                 context.Context
	budget                              *Budget
	live                                map[string]*record
	endpoints                           map[string]endpointUsage
	pendingTTL, progressTTL, prepareTTL time.Duration
}
type endpointUsage struct{ pending, active int }
type record struct {
	expires                                   time.Time
	creationQueued                            chan struct{}
	acceptDone                                chan struct{}
	active                                    bool
	mu                                        sync.Mutex
	io                                        sync.Mutex
	inFlight                                  sync.WaitGroup
	engine                                    *Engine
	ctx                                       context.Context
	cancel                                    context.CancelFunc
	descriptor                                protocol.TransferDescriptor
	source, destination, creator              Endpoint
	what                                      string
	reservation                               *Reservation
	timer                                     *time.Timer
	announced, accepting, ended               bool
	sourceHoldStarted, destinationHoldStarted bool
	bytes                                     int64
	terminal                                  protocol.TransferTerminal
	stage                                     Stage
}

func NewEngine(ctx context.Context, budget *Budget) *Engine {
	e := &Engine{ctx: ctx, budget: budget, live: map[string]*record{}, endpoints: map[string]endpointUsage{}, pendingTTL: 10 * time.Minute, progressTTL: 60 * time.Second, prepareTTL: 60 * time.Second}
	context.AfterFunc(ctx, func() {
		e.mu.Lock()
		all := make([]*record, 0, len(e.live))
		for _, r := range e.live {
			all = append(all, r)
		}
		e.mu.Unlock()
		for _, r := range all {
			r.terminate("unknown", "coordinator_ended", true)
		}
	})
	return e
}
func validEndpoint(p Endpoint) bool {
	return p.Key != "" && p.Identity.SessionID != "" && p.Call != nil && p.Hold != nil && p.Notice != nil && p.Done != nil
}
func (e *Engine) Create(creator, other Endpoint, offer bool, name, what string) (protocol.TransferDescriptor, error) {
	return e.create(creator, other, offer, name, what, nil)
}

// CreateReserved consumes a pre-ID reservation already charged by federation.
// The caller must release it on error; successful creation owns it until cleanup.
func (e *Engine) CreateReserved(creator, other Endpoint, offer bool, name, what string, reservation *Reservation) (protocol.TransferDescriptor, error) {
	if reservation == nil || reservation.budget != e.budget {
		return protocol.TransferDescriptor{}, ErrCapacity
	}
	return e.create(creator, other, offer, name, what, reservation)
}
func (e *Engine) create(creator, other Endpoint, offer bool, name, what string, reservation *Reservation) (protocol.TransferDescriptor, error) {
	if !validEndpoint(creator) || !validEndpoint(other) || creator.Key == other.Key {
		return protocol.TransferDescriptor{}, ErrUnsupported
	}
	method := "transfer.request"
	var params any = protocol.TransferRequest{Target: other.Identity.SessionID, Name: name, What: what}
	if offer {
		method = "transfer.offer"
		params = protocol.TransferOffer{Target: other.Identity.SessionID, Name: name}
	}
	if _, err := protocol.EncodeParams(method, params); err != nil {
		return protocol.TransferDescriptor{}, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.ctx.Err() != nil {
		return protocol.TransferDescriptor{}, e.ctx.Err()
	}
	for _, p := range []Endpoint{creator, other} {
		u := e.endpoints[p.Key]
		if u.pending >= protocol.MaxPendingTransfersPerConnection {
			return protocol.TransferDescriptor{}, ErrCapacity
		}
		select {
		case <-p.Done:
			return protocol.TransferDescriptor{}, ErrUnsupported
		default:
		}
	}
	var err error
	if reservation == nil {
		reservation, err = e.budget.PendingFor(creator.Key, other.Key)
	} else {
		err = reservation.BindPendingEndpoints(creator.Key, other.Key)
	}
	if err != nil {
		return protocol.TransferDescriptor{}, err
	}
	var entropy [16]byte
	if _, err = rand.Read(entropy[:]); err != nil {
		reservation.Release()
		return protocol.TransferDescriptor{}, err
	}
	id := hex.EncodeToString(entropy[:])
	ctx, cancel := context.WithCancel(e.ctx)
	r := &record{creationQueued: make(chan struct{}), engine: e, ctx: ctx, cancel: cancel, source: other, destination: creator, creator: creator, what: what, reservation: reservation}
	r.expires = time.Now().Add(e.pendingTTL)
	if offer {
		r.source, r.destination = creator, other
		r.what = ""
	}
	r.descriptor = protocol.TransferDescriptor{ID: id, Sender: r.source.Identity.SessionID, Receiver: r.destination.Identity.SessionID, Name: name, CreatedBy: creator.Identity.SessionID, State: "pending"}
	if name == "" {
		r.descriptor.Name = "file"
	}
	if offer {
		r.descriptor.State = "offered"
	} else {
		r.descriptor.State = "requested"
	}
	e.live[id] = r
	for _, p := range []Endpoint{creator, other} {
		u := e.endpoints[p.Key]
		u.pending++
		e.endpoints[p.Key] = u
	}
	r.timer = time.AfterFunc(e.pendingTTL, func() { r.terminate("expired", "expired", false) })
	go func() {
		select {
		case <-r.source.Done:
			r.terminate("failed", "source_binding_ended", false)
		case <-r.destination.Done:
			r.terminate("failed", "destination_binding_ended", false)
		case <-r.ctx.Done():
		}
	}()
	return cloneDescriptor(r.descriptor), nil
}

// Announce must be called after creation's result has been queued to its SDK.
// The origin's CallObserved binding then precedes any prepare on that SAME wire.
func (e *Engine) Announce(id string) {
	e.mu.Lock()
	r := e.live[id]
	e.mu.Unlock()
	if r == nil {
		return
	}
	r.mu.Lock()
	if r.announced {
		r.mu.Unlock()
		return
	}
	r.announced = true
	close(r.creationQueued)
	if r.ended {
		r.mu.Unlock()
		return
	}
	other := r.destination
	kind := "offer"
	if r.creator.Key == r.destination.Key {
		other = r.source
		kind = "request"
	}
	from := r.creator.Identity
	notice := protocol.TransferNotice{Kind: kind, ID: id, From: &from, Name: r.descriptor.Name, What: r.what}
	r.mu.Unlock()
	if other.Notice(r.ctx, notice) != nil {
		r.terminate("failed", "notice_unavailable", false)
	}
}
func (e *Engine) get(actor, id string) (*record, error) {
	e.mu.Lock()
	r := e.live[id]
	e.mu.Unlock()
	if r == nil {
		return nil, ErrUnknown
	}
	if actor != r.source.Key && actor != r.destination.Key {
		return nil, ErrUnknown
	}
	return r, nil
}

// CanChunk is a no-I/O ingress gate against the previously admitted binding.
// Detailed offset/credit validation still occurs on the serialized engine.
func (e *Engine) CanChunk(actor, id string) bool {
	r, err := e.get(actor, id)
	if err != nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return actor == r.source.Key && !r.ended && r.descriptor.State == "streaming"
}

// EndEndpoint is the existing explicit-close/native-owner-loss boundary. It
// cancels only records pinned to that generation, including persistent lanes.
// Committing records retain UNKNOWN until confirmed publication knowledge.
func (e *Engine) EndEndpoint(actor string) {
	e.mu.Lock()
	var all []*record
	for _, r := range e.live {
		if r.source.Key == actor || r.destination.Key == actor {
			all = append(all, r)
		}
	}
	e.mu.Unlock()
	for _, r := range all {
		r.terminate("cancelled", "endpoint_closing", false)
	}
}
func (e *Engine) Status(actor, id string) (protocol.TransferDescriptor, error) {
	r, err := e.get(actor, id)
	if err != nil {
		return protocol.TransferDescriptor{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ended {
		return protocol.TransferDescriptor{}, ErrUnknown
	}
	return cloneDescriptor(r.descriptor), nil
}
func cloneDescriptor(d protocol.TransferDescriptor) protocol.TransferDescriptor {
	if d.Size != nil {
		x := *d.Size
		d.Size = &x
	}
	return d
}
func (e *Engine) Accept(actor string, p protocol.TransferAccept) (protocol.TransferDescriptor, error) {
	r, err := e.get(actor, p.ID)
	if err != nil {
		return protocol.TransferDescriptor{}, err
	}
	r.mu.Lock()
	if actor == r.creator.Key {
		r.mu.Unlock()
		return protocol.TransferDescriptor{}, ErrRole
	}
	if r.ended || !r.announced {
		r.mu.Unlock()
		return protocol.TransferDescriptor{}, ErrUnknown
	}
	if r.accepting {
		done := r.acceptDone
		r.mu.Unlock()
		select {
		case <-done:
		case <-r.ctx.Done():
			return protocol.TransferDescriptor{}, ErrUnknown
		}
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.ended {
			return protocol.TransferDescriptor{}, ErrUnknown
		}
		if p.Size != nil && (r.descriptor.Size == nil || *p.Size != *r.descriptor.Size) {
			return protocol.TransferDescriptor{}, ErrState
		}
		return cloneDescriptor(r.descriptor), nil
	}
	if r.descriptor.State != "offered" && r.descriptor.State != "requested" {
		r.mu.Unlock()
		return protocol.TransferDescriptor{}, ErrState
	}
	r.accepting = true
	r.acceptDone = make(chan struct{})
	r.mu.Unlock()
	defer close(r.acceptDone)
	return e.prepareAccepted(r, p)
}

func (e *Engine) prepareAccepted(r *record, p protocol.TransferAccept) (protocol.TransferDescriptor, error) {
	var err error
	r.mu.Lock()
	if r.ended {
		r.mu.Unlock()
		return protocol.TransferDescriptor{}, ErrUnknown
	}
	e.mu.Lock()
	for _, ep := range []Endpoint{r.source, r.destination} {
		if e.endpoints[ep.Key].active >= protocol.MaxActiveTransfersPerConnection {
			err = ErrCapacity
		}
	}
	if err == nil {
		err = r.reservation.Prepare()
	}
	if err == nil {
		for _, ep := range []Endpoint{r.source, r.destination} {
			u := e.endpoints[ep.Key]
			u.pending--
			u.active++
			e.endpoints[ep.Key] = u
		}
		r.descriptor.State = "preparing"
		r.active = true
	}
	e.mu.Unlock()
	if err != nil {
		r.mu.Unlock()
		r.terminate("failed", "capacity_exhausted", false)
		return protocol.TransferDescriptor{}, err
	}
	preparingFor := time.Until(r.expires)
	if preparingFor > e.prepareTTL {
		preparingFor = e.prepareTTL
	}
	if preparingFor <= 0 {
		r.mu.Unlock()
		r.terminate("expired", "expired", false)
		return protocol.TransferDescriptor{}, ErrState
	}
	r.timer.Reset(preparingFor)
	r.mu.Unlock()
	var src, dst protocol.TransferPrepared
	if err = r.source.Call(r.ctx, "transfer.prepare", protocol.TransferPrepare{ID: p.ID, Role: "source"}, &src); err != nil || src.ID != p.ID || src.Size == nil {
		r.terminate("failed", "source_prepare_failed", false)
		return protocol.TransferDescriptor{}, ErrState
	}
	size := int64(*src.Size)
	if size < 0 || size > protocol.MaxTransferBytes || p.Size != nil && *p.Size != *src.Size {
		r.terminate("failed", "source_size_invalid", false)
		return protocol.TransferDescriptor{}, ErrState
	}
	if err = r.destination.Call(r.ctx, "transfer.prepare", protocol.TransferPrepare{ID: p.ID, Role: "destination", Size: src.Size}, &dst); err != nil || dst.ID != p.ID || !dst.Ready {
		r.terminate("failed", "destination_prepare_failed", false)
		return protocol.TransferDescriptor{}, ErrState
	}
	r.mu.Lock()
	if r.ended {
		r.mu.Unlock()
		return protocol.TransferDescriptor{}, ErrState
	}
	r.descriptor.Size = src.Size
	r.mu.Unlock()

	// Admit activation under the terminal gate, but never wait for an owner
	// round-trip with that gate locked: the owner can itself be closing us.
	// Terminal cleanup joins this lease before inspecting/releasing late grants.
	holdCtx, stopHold := context.WithTimeout(r.ctx, e.prepareTTL)
	defer stopHold()
	r.mu.Lock()
	if r.ended {
		r.mu.Unlock()
		return protocol.TransferDescriptor{}, ErrState
	}
	r.inFlight.Add(1)
	r.sourceHoldStarted = true
	r.mu.Unlock()
	sourceHeld := r.source.Hold(holdCtx, p.ID, false)
	r.mu.Lock()
	activateDestination := sourceHeld && !r.ended
	if activateDestination {
		r.destinationHoldStarted = true
	}
	r.mu.Unlock()
	destinationHeld := false
	if activateDestination {
		destinationHeld = r.destination.Hold(holdCtx, p.ID, false)
	}
	r.mu.Lock()
	ready := !r.ended && sourceHeld && destinationHeld
	r.mu.Unlock()
	r.inFlight.Done()
	if !ready {
		r.terminate("failed", "participant_closing", false)
		return protocol.TransferDescriptor{}, ErrState
	}

	var start protocol.TransferStarted
	if err = r.destination.Call(r.ctx, "transfer.start", protocol.TransferStart{ID: p.ID, Role: "destination", Size: *src.Size}, &start); err != nil || start.ID != p.ID || !start.Ready {
		r.terminate("failed", "destination_start_failed", false)
		return protocol.TransferDescriptor{}, ErrState
	}
	r.mu.Lock()
	if r.ended {
		r.mu.Unlock()
		return protocol.TransferDescriptor{}, ErrState
	}
	r.descriptor.State = "streaming"
	r.timer.Reset(e.progressTTL)
	result := cloneDescriptor(r.descriptor)
	r.mu.Unlock()
	credit := protocol.Integer(protocol.TransferCreditBytes)
	if err = r.source.Call(r.ctx, "transfer.start", protocol.TransferStart{ID: p.ID, Role: "source", Size: *src.Size, Credit: &credit}, &start); err != nil || start.ID != p.ID || !start.Started {
		r.terminate("unknown", "source_start_unconfirmed", true)
		return protocol.TransferDescriptor{}, ErrState
	}
	return result, nil
}
func (e *Engine) Chunk(actor string, p protocol.TransferChunk) (protocol.TransferCredit, error) {
	r, err := e.get(actor, p.ID)
	if err != nil {
		return protocol.TransferCredit{}, err
	}
	if actor != r.source.Key {
		return protocol.TransferCredit{}, ErrRole
	}
	if err = protocol.ValidateTransferParams("transfer.chunk", p); err != nil {
		r.terminate("failed", "invalid_chunk", false)
		return protocol.TransferCredit{}, err
	}
	r.mu.Lock()
	if r.ended || r.descriptor.State != "streaming" {
		r.mu.Unlock()
		return protocol.TransferCredit{}, ErrState
	}
	lease, err := r.stage.Reserve(len(p.Data)+128, Ingress)
	if err != nil {
		r.mu.Unlock()
		return protocol.TransferCredit{}, err
	}
	// Add is serialized against ended/cleanup's Wait, before retaining a chunk
	// behind the I/O lock. Cancellation must join every queued lease, not merely
	// pass one mutex barrier and release the combined reservation too early.
	r.inFlight.Add(1)
	r.mu.Unlock()
	r.io.Lock()
	defer func() { lease.Release(); r.io.Unlock(); r.inFlight.Done() }()
	r.mu.Lock()
	n := len(p.Data) / 4 * 3
	if p.Data[len(p.Data)-1] == '=' {
		n--
		if p.Data[len(p.Data)-2] == '=' {
			n--
		}
	}
	if r.ended || r.descriptor.State != "streaming" || int64(p.Offset) != r.bytes || r.descriptor.Size == nil || r.bytes+int64(n) > int64(*r.descriptor.Size) {
		r.mu.Unlock()
		r.terminate("failed", "invalid_offset_or_size", false)
		return protocol.TransferCredit{}, ErrState
	}
	r.mu.Unlock()
	var credit protocol.TransferCredit
	if err = r.destination.Call(r.ctx, "transfer.chunk", p, &credit); err != nil || int64(credit.Offset) != int64(p.Offset)+int64(n) {
		r.terminate("failed", "destination_write_unconfirmed", false)
		return protocol.TransferCredit{}, ErrState
	}
	r.mu.Lock()
	if r.ended {
		r.mu.Unlock()
		return protocol.TransferCredit{}, ErrState
	}
	r.bytes += int64(n)
	r.descriptor.Bytes = credit.Offset
	r.timer.Reset(e.progressTTL)
	r.mu.Unlock()
	return credit, nil
}
func (e *Engine) Finish(actor string, p protocol.TransferFinish) (protocol.TransferTerminal, error) {
	r, err := e.get(actor, p.ID)
	if err != nil {
		return protocol.TransferTerminal{}, err
	}
	if actor != r.source.Key {
		return protocol.TransferTerminal{}, ErrRole
	}
	r.io.Lock()
	defer r.io.Unlock()
	r.mu.Lock()
	if r.ended || r.descriptor.State != "streaming" || r.descriptor.Size == nil || r.bytes != int64(p.Bytes) || r.bytes != int64(*r.descriptor.Size) {
		r.mu.Unlock()
		r.terminate("failed", "invalid_finish", false)
		return protocol.TransferTerminal{}, ErrState
	}
	r.descriptor.State = "committing"
	r.inFlight.Add(1)
	r.mu.Unlock()
	defer r.inFlight.Done()
	var result protocol.TransferTerminal
	err = r.destination.Call(r.ctx, "transfer.finish", p, &result)
	if err != nil || result.ID != p.ID || result.Bytes != p.Bytes || result.Outcome == "completed" && result.SHA256 != p.SHA256 {
		r.terminate("unknown", "commit_unconfirmed", true)
		return protocol.TransferTerminal{ID: p.ID, Outcome: "unknown", Bytes: p.Bytes, Reason: "commit_unconfirmed"}, nil
	}
	if result.Outcome != "completed" && result.Outcome != "failed" && result.Outcome != "unknown" {
		r.terminate("unknown", "commit_unconfirmed", true)
		return protocol.TransferTerminal{}, ErrState
	}
	r.finishTerminal(result)
	return result, nil
}

// Fail reports only source read/verification failure before publication was
// requested. It shares finish/cancel serialization, never overrides a commit.
func (e *Engine) Fail(actor string, p protocol.TransferFailure) (protocol.TransferTerminal, error) {
	if _, err := protocol.EncodeParams("transfer.fail", p); err != nil {
		return protocol.TransferTerminal{}, err
	}
	r, err := e.get(actor, p.ID)
	if err != nil {
		return protocol.TransferTerminal{}, err
	}
	if actor != r.source.Key {
		return protocol.TransferTerminal{}, ErrRole
	}
	r.io.Lock()
	defer r.io.Unlock()
	r.mu.Lock()
	if r.ended {
		result := r.terminal
		r.mu.Unlock()
		return result, nil
	}
	phase := r.descriptor.State
	r.mu.Unlock()
	if phase == "committing" {
		r.terminate("unknown", "commit_unconfirmed", true)
	} else if phase == "streaming" {
		r.terminate("failed", p.Reason, false)
	} else {
		return protocol.TransferTerminal{}, ErrState
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.terminal, nil
}
func (e *Engine) Cancel(actor, id string) (protocol.TransferTerminal, error) {
	r, err := e.get(actor, id)
	if err != nil {
		return protocol.TransferTerminal{}, err
	}
	r.io.Lock()
	defer r.io.Unlock()
	r.mu.Lock()
	if r.ended {
		v := r.terminal
		r.mu.Unlock()
		return v, nil
	}
	committing := r.descriptor.State == "committing"
	r.mu.Unlock()
	if committing {
		r.terminate("unknown", "commit_unconfirmed", true)
	} else {
		r.terminate("cancelled", "cancelled", false)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.terminal, nil
}
func (r *record) terminate(outcome, reason string, uncertain bool) {
	r.mu.Lock()
	if r.ended {
		r.mu.Unlock()
		return
	}
	if reason == "expired" {
		switch r.descriptor.State {
		case "preparing":
			outcome, reason = "failed", "prepare_timeout"
		case "streaming":
			outcome, reason = "failed", "progress_timeout"
		case "committing":
			outcome, reason = "unknown", "commit_unconfirmed"
		}
	}
	if r.descriptor.State == "committing" || uncertain {
		outcome = "unknown"
	}
	result := protocol.TransferTerminal{ID: r.descriptor.ID, Outcome: outcome, Bytes: protocol.Integer(r.bytes), Reason: reason}
	r.finishTerminalLocked(result)
}
func (r *record) finishTerminal(result protocol.TransferTerminal) {
	r.mu.Lock()
	r.finishTerminalLocked(result)
}
func (r *record) finishTerminalLocked(result protocol.TransferTerminal) {
	if r.ended {
		r.mu.Unlock()
		return
	}
	r.ended = true
	r.terminal = result
	r.timer.Stop()
	r.cancel()
	wasActive := r.active
	r.mu.Unlock()
	// Do not release uncertain accepted bindings on a tool response timeout.
	// Each positive machine-cleanup acknowledgment (or exact generation end)
	// retires its resources before host/endpoint accounting is released.
	go func() {
		// Creation/terminal races must not deliver cleanup before the creator's
		// sole observed result can install its local path binding. Announce is
		// sent only after that result is queued, not after a tool context timeout.
		select {
		case <-r.creationQueued:
		case <-r.creator.Done:
		case <-r.engine.ctx.Done():
		}
		// ended forbids new Add calls; join all admitted queued/in-write chunks and
		// the sole finish before releasing handles, holds or the reservation.
		r.inFlight.Wait()

		settled := make(chan struct{}, 2)
		for _, ep := range []Endpoint{r.destination, r.source} {
			go func(participant Endpoint) {
				defer func() { settled <- struct{}{} }()
				done := make(chan error, 1)
				go func() {
					done <- participant.Notice(r.engine.ctx, protocol.TransferNotice{Kind: "terminal", ID: result.ID, Outcome: result.Outcome, Bytes: &result.Bytes, SHA256: result.SHA256, Reason: result.Reason})
				}()
				select {
				case err := <-done:
					if err != nil && wasActive {
						select {
						case <-participant.Done:
						case <-r.engine.ctx.Done():
						}
					}
				case <-participant.Done:
				case <-r.engine.ctx.Done():
				}
				r.mu.Lock()
				// A cancelled/lost Hold reply does not prove its queued owner
				// event never applied. Terminal release is idempotent by ID.
				held := r.sourceHoldStarted
				if participant.Key == r.destination.Key {
					held = r.destinationHoldStarted
				}
				r.mu.Unlock()
				if held {
					participant.Hold(r.engine.ctx, result.ID, true)
				}
			}(ep)
		}
		<-settled
		<-settled

		e := r.engine
		e.mu.Lock()
		if e.live[result.ID] == r {
			delete(e.live, result.ID)
			for _, ep := range []Endpoint{r.source, r.destination} {
				u := e.endpoints[ep.Key]
				if wasActive {
					u.active--
				} else {
					u.pending--
				}
				if u.pending == 0 && u.active == 0 {
					delete(e.endpoints, ep.Key)
				} else {
					e.endpoints[ep.Key] = u
				}
			}
		}
		e.mu.Unlock()
		r.reservation.Release()
	}()
}

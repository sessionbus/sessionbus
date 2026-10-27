// SPDX-License-Identifier: MIT

package sessionkit

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"unicode/utf8"

	"github.com/antst/sessionbus/bus/sdk/go/internal/rpc"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

// TransferExecutor owns endpoint-local filesystem handles, off the RPC reader.
// Nil selects FilesystemTransferExecutor, with direct current-user access in all
// products/modes. Custom executors remain available for controlled integrations.
// At most four filesystem FDs aggregate both roles may be retained per transfer;
// custom processes, pipes and JSON have separately bounded costs. Terminal
// cleanup must close/join executor-owned resources even when reporting residue.
type TransferExecutor interface {
	OpenTransferSource(context.Context, string) (TransferSource, error)
	OpenTransferDestination(context.Context, string, bool) (TransferDestination, error)
}

// TransferSource owns one checked regular-file handle. Verify checks the SAME
// handle's identity and size after streaming. This is not snapshot isolation;
// same-size in-place modification may be undetectable by final stat checks.
type TransferSource interface {
	io.Reader
	Size() int64
	Verify() error
	Close() error
}

// TransferDestination serializes publication against Abort. Commit's bool is
// the atomic-publication fact, even when subsequent temp cleanup returns error.
// Abort must not retract a committed destination and must join its own writer.
// Return ErrTransferCommitUnknown if publication may have occurred but its
// acknowledgment was lost. Abort must never unlink the final destination,
// including after UNKNOWN. Neither interface implies fsync/durability.
// No-replace must be atomic. On ErrTransferCommitUnknown, Commit must have
// closed its executor-owned handles and joined its admitted I/O before return;
// Abort will not be called and terminal retirement cannot retain a live slot.
type TransferDestination interface {
	io.Writer
	Commit() (published bool, cleanup error)
	Abort() error
}

var ErrTransferUnsupported = errors.New("unsupported_transfer")
var ErrTransferBinding = errors.New("transfer_binding_conflict")
var ErrTransferCommitUnknown = errors.New("transfer_commit_unknown")

// Transfers keeps source/destination paths entirely local. Its generation owns
// accepted work independently of a tool-call context. A lost call result never
// proves cancellation and is never replayed. Ordinary notices are staged by the
// daemon using its normal delivery policy, not by this manager.
type Transfers struct {
	pending, active int
	executor        TransferExecutor
	mu              sync.Mutex
	current         *transferGeneration
	generations     map[*transferGeneration]struct{}
}

type transferGeneration struct {
	finished            chan struct{}
	owner               *Transfers
	wire                *rpc.Conn
	ctx                 context.Context
	cancel              context.CancelFunc
	mu                  sync.Mutex
	enabled             bool
	closed              bool
	provisional, active int
	bindings            map[string]*transferBinding
	operations          sync.WaitGroup
}

type transferBinding struct {
	publication                         atomic.Pointer[protocol.TransferTerminal]
	commitFinished                      chan struct{}
	finishAttempted                     bool
	destinationProjection               atomic.Pointer[string]
	commitStarted                       atomic.Bool
	commitUnknown                       bool
	retired                             chan struct{}
	cleanupError                        error
	cleanupReason                       string
	ctx                                 context.Context
	prepareAttempted                    bool
	prepareError                        error
	mu                                  sync.Mutex
	id, role, path                      string
	overwrite                           bool
	prepared, started, ended, published bool
	active                              bool
	size, offset                        int64
	hash                                hash.Hash
	source                              TransferSource
	destination                         TransferDestination
	cancel                              context.CancelFunc
	worker                              chan struct{}
}

func newTransfers(executor TransferExecutor) *Transfers {
	if executor == nil {
		executor = FilesystemTransferExecutor{}
	}
	return &Transfers{executor: executor, generations: map[*transferGeneration]struct{}{}}
}
func (t *Transfers) attach(wire *rpc.Conn, ctx context.Context, enabled bool) *transferGeneration {
	if !enabled {
		t.mu.Lock()
		if ctx.Err() != nil || wire.Context().Err() != nil {
			t.mu.Unlock()
			return nil
		}
		old := t.current
		t.current = nil
		t.mu.Unlock()
		if old != nil {
			old.cancel()
		}
		return nil
	}
	child, cancel := context.WithCancel(ctx)
	stopWire := context.AfterFunc(wire.Context(), cancel)
	g := &transferGeneration{finished: make(chan struct{}), owner: t, wire: wire, ctx: child, cancel: cancel, enabled: enabled, bindings: map[string]*transferBinding{}}
	t.mu.Lock()
	if ctx.Err() != nil || wire.Context().Err() != nil {
		t.mu.Unlock()
		cancel()
		stopWire()
		return nil
	}
	if t.current != nil && t.current.wire == wire && t.current.enabled == enabled && t.current.ctx.Err() == nil {
		current := t.current
		t.mu.Unlock()
		cancel()
		stopWire()
		return current
	}
	old := t.current
	t.current = g
	t.generations[g] = struct{}{}
	t.mu.Unlock()
	if old != nil {
		old.cancel()
	}
	go func() {
		defer close(g.finished)
		defer stopWire()
		<-child.Done()
		g.shutdown()
		t.mu.Lock()
		delete(t.generations, g)
		t.mu.Unlock()
	}()
	return g
}
func (t *Transfers) generation() (*transferGeneration, error) {
	if t == nil {
		return nil, ErrTransferUnsupported
	}
	t.mu.Lock()
	g := t.current
	t.mu.Unlock()
	if g == nil || !g.enabled || g.ctx.Err() != nil {
		return nil, ErrTransferUnsupported
	}
	return g, nil
}
func (g *transferGeneration) shutdown() {
	g.mu.Lock()
	g.closed = true
	all := make([]*transferBinding, 0, len(g.bindings))
	for _, b := range g.bindings {
		all = append(all, b)
	}
	g.mu.Unlock()
	for _, b := range all {
		g.end(b)
	}
	for _, b := range all {
		<-b.retired
	}
	g.operations.Wait()
}
func (g *transferGeneration) end(b *transferBinding) {
	// Cancellation must reach a blocked prepare/read without waiting for its
	// handle mutex. Once native publication starts, wait for its fact/UNKNOWN;
	// never cancel that operation as an inferred pre-publication rollback.
	if !b.commitStarted.Load() && b.cancel != nil {
		b.cancel()
	}
	b.mu.Lock()
	if b.ended {
		b.mu.Unlock()
		return
	}
	b.ended = true
	source, dest, worker := b.source, b.destination, b.worker
	commitUnknown := b.commitUnknown
	b.source = nil
	b.destination = nil
	b.mu.Unlock()
	var cleanupError error
	if source != nil {
		cleanupError = source.Close()
	}
	if commitUnknown {
		// No destructive abort or late publication retry after an unconfirmed
		// native publish. Commit has relinquished executor-owned resources;
		// the publication fact remains UNKNOWN independently of slot retirement.
		cleanupError = ErrTransferCommitUnknown
	} else if dest != nil {
		if err := dest.Abort(); err != nil {
			cleanupError = err
		}
	}
	b.mu.Lock()
	b.cleanupError = cleanupError
	if cleanupError != nil && !errors.Is(cleanupError, ErrTransferCommitUnknown) {
		b.cleanupReason = "cleanup_failed"
	}
	b.mu.Unlock()
	// The source can be waiting for finish's reply while terminal notice arrives.
	// Join off the reader/reply path; capacity remains charged until it exits.
	go func() {
		if worker != nil {
			<-worker
		}
		// Cleanup errors describe closed-handle residue, not live work. Retire
		// only after the admitted worker joins, without waiting for disconnect.
		if b.cancel != nil {
			b.cancel()
		}
		g.mu.Lock()
		defer g.mu.Unlock()
		if g.bindings[b.id] == b {
			delete(g.bindings, b.id)
			g.owner.mu.Lock()
			if b.active {
				g.active--
				g.owner.active--
			} else {
				g.owner.pending--
			}
			g.owner.mu.Unlock()
			close(b.retired)
		}
	}()
}
func (g *transferGeneration) reserve(b *transferBinding) error {
	if len(b.path) == 0 || len(b.path) > 4096 {
		return ErrTransferBinding
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return ErrTransferUnsupported
	}
	g.owner.mu.Lock()
	defer g.owner.mu.Unlock()
	if g.owner.pending >= protocol.MaxPendingTransfersPerConnection {
		return &ProtocolError{Code: protocol.Busy, Message: "busy"}
	}
	g.provisional++
	g.owner.pending++
	b.retired = make(chan struct{})
	b.commitFinished = make(chan struct{})
	b.ctx, b.cancel = context.WithCancel(g.ctx)
	if b.role == "destination" {
		path := b.path
		b.destinationProjection.Store(&path)
	}
	return nil
}
func (g *transferGeneration) create(ctx context.Context, method string, params any, b *transferBinding) (protocol.TransferDescriptor, error) {
	if err := ctx.Err(); err != nil {
		return protocol.TransferDescriptor{}, err
	}
	if err := g.reserve(b); err != nil {
		return protocol.TransferDescriptor{}, err
	}
	g.mu.Lock()
	if g.closed {
		g.provisional--
		g.owner.mu.Lock()
		g.owner.pending--
		g.owner.mu.Unlock()
		g.mu.Unlock()
		b.cancel()
		return protocol.TransferDescriptor{}, ErrTransferUnsupported
	}
	g.operations.Add(1)
	g.mu.Unlock()
	done := make(chan error, 1)
	var result protocol.TransferDescriptor
	// The bounded provisional slot survives ambiguous cancellation until the sole
	// observed result or this generation's end. No files are opened in this call.
	go func() {
		defer g.operations.Done()
		installed := false
		err := g.wire.CallObserved(g.ctx, method, params, &result, func() error {
			g.mu.Lock()
			defer g.mu.Unlock()
			if g.closed {
				return ErrTransferUnsupported
			}
			if _, ok := g.bindings[result.ID]; ok {
				return ErrTransferBinding
			}
			b.id = result.ID
			g.bindings[result.ID] = b
			g.provisional--
			installed = true
			return nil
		})
		if !installed {
			g.mu.Lock()
			g.provisional--
			g.owner.mu.Lock()
			g.owner.pending--
			g.owner.mu.Unlock()
			g.mu.Unlock()
		}
		done <- err
	}()
	select {
	case err := <-done:
		return result, err
	case <-ctx.Done():
		return protocol.TransferDescriptor{}, ctx.Err()
	}
}
func (t *Transfers) Offer(ctx context.Context, target, source, name string) (protocol.TransferDescriptor, error) {
	g, e := t.generation()
	if e != nil {
		return protocol.TransferDescriptor{}, e
	}
	return g.create(ctx, "transfer.offer", protocol.TransferOffer{Target: target, Name: name}, &transferBinding{role: "source", path: source})
}

// Action handles local-only path fields before constructing strict path-free
// wire DTOs. It deliberately cannot fall back to a generic RPC action.
func (t *Transfers) Action(ctx context.Context, action string, args json.RawMessage) (json.RawMessage, error) {
	if t == nil {
		return nil, ErrTransferUnsupported
	}
	switch action {
	case "transfer.offer":
		return localAction(args, func(p struct {
			Target string `json:"target"`
			Source string `json:"source"`
			Name   string `json:"name,omitempty"`
		}) (protocol.TransferDescriptor, error) {
			name := p.Name
			if name == "" {
				name = filepath.Base(p.Source)
			}
			return t.Offer(ctx, p.Target, p.Source, name)
		})
	case "transfer.request":
		return localAction(args, func(p struct {
			Target      string `json:"target"`
			What        string `json:"what"`
			Destination string `json:"destination"`
			Name        string `json:"name,omitempty"`
			Overwrite   bool   `json:"overwrite,omitempty"`
		}) (protocol.TransferDescriptor, error) {
			return t.Request(ctx, p.Target, p.What, p.Destination, p.Name, p.Overwrite)
		})
	case "transfer.accept":
		return localAction(args, func(p struct {
			ID          string `json:"id"`
			Source      string `json:"source,omitempty"`
			Destination string `json:"destination,omitempty"`
			Overwrite   bool   `json:"overwrite,omitempty"`
		}) (protocol.TransferDescriptor, error) {
			g, err := t.generation()
			if err != nil {
				return protocol.TransferDescriptor{}, err
			}
			b := g.binding(p.ID)
			if b == nil {
				return protocol.TransferDescriptor{}, ErrTransferBinding
			}
			path := p.Destination
			if b.role == "source" {
				path = p.Source
			}
			if (p.Source == "") == (p.Destination == "") || b.role == "source" && p.Overwrite {
				return protocol.TransferDescriptor{}, ErrTransferBinding
			}
			return t.Accept(ctx, p.ID, path, p.Overwrite)
		})
	case "transfer.cancel":
		return localAction(args, func(p protocol.TransferTarget) (protocol.TransferTerminal, error) { return t.Cancel(ctx, p.ID) })
	case "transfer.status":
		return localAction(args, func(p protocol.TransferTarget) (protocol.TransferDescriptor, error) { return t.Status(ctx, p.ID) })
	default:
		return nil, errors.New("unknown action")
	}
}
func (t *Transfers) Request(ctx context.Context, target, what, destination, name string, overwrite bool) (protocol.TransferDescriptor, error) {
	g, e := t.generation()
	if e != nil {
		return protocol.TransferDescriptor{}, e
	}
	return g.create(ctx, "transfer.request", protocol.TransferRequest{Target: target, What: what, Name: name}, &transferBinding{role: "destination", path: destination, overwrite: overwrite})
}
func (t *Transfers) Accept(ctx context.Context, id, path string, overwrite bool) (protocol.TransferDescriptor, error) {
	if err := ctx.Err(); err != nil {
		return protocol.TransferDescriptor{}, err
	}
	if len(path) == 0 || len(path) > 4096 {
		return protocol.TransferDescriptor{}, ErrTransferBinding
	}
	g, e := t.generation()
	if e != nil {
		return protocol.TransferDescriptor{}, e
	}
	g.mu.Lock()
	b := g.bindings[id]
	g.mu.Unlock()
	if b == nil {
		return protocol.TransferDescriptor{}, ErrTransferBinding
	}
	b.mu.Lock()
	if b.role == "source" && overwrite {
		b.mu.Unlock()
		return protocol.TransferDescriptor{}, ErrTransferBinding
	}
	if b.path != "" && (b.path != path || b.overwrite != overwrite) {
		b.mu.Unlock()
		return protocol.TransferDescriptor{}, ErrTransferBinding
	}
	if b.ended {
		b.mu.Unlock()
		return protocol.TransferDescriptor{}, ErrTransferBinding
	}
	b.path, b.overwrite = path, overwrite
	if b.role == "destination" {
		copy := path
		b.destinationProjection.Store(&copy)
	}
	b.mu.Unlock()
	var result protocol.TransferDescriptor
	// Like creation, accept uncertainty belongs to the connection, not a caller's
	// cancellation. The single RPC is never replayed to recover its answer.
	done := make(chan error, 1)
	go func() { done <- g.wire.Call(g.ctx, "transfer.accept", protocol.TransferAccept{ID: id}, &result) }()
	select {
	case e = <-done:
		return result, e
	case <-ctx.Done():
		return protocol.TransferDescriptor{}, ctx.Err()
	}
}
func (t *Transfers) Status(ctx context.Context, id string) (protocol.TransferDescriptor, error) {
	g, e := t.generation()
	if e != nil {
		return protocol.TransferDescriptor{}, e
	}
	var r protocol.TransferDescriptor
	e = g.wire.Call(ctx, "transfer.status", protocol.TransferTarget{ID: id}, &r)
	return r, e
}
func (t *Transfers) Cancel(ctx context.Context, id string) (protocol.TransferTerminal, error) {
	g, e := t.generation()
	if e != nil {
		return protocol.TransferTerminal{}, e
	}
	var r protocol.TransferTerminal
	e = g.wire.Call(ctx, "transfer.cancel", protocol.TransferTarget{ID: id}, &r)
	return r, e
}

func (t *Transfers) handle(wire *rpc.Conn, request *rpc.Request) bool {
	if t == nil {
		return false
	}
	switch request.Method {
	case "transfer.prepare", "transfer.start", "transfer.chunk", "transfer.finish", "transfer.notice":
	default:
		return false
	}
	t.mu.Lock()
	g := t.current
	t.mu.Unlock()
	if g == nil || g.wire != wire || !g.enabled || g.ctx.Err() != nil {
		go func() { _ = wire.Error(request, protocol.UnsupportedOpen, "unsupported_transfer") }()
		return true
	}
	// No filesystem, writer, credit or callback wait on the shared RPC reader.
	go g.handle(request)
	return true
}
func (g *transferGeneration) handle(r *rpc.Request) {
	var result any
	var err error
	switch p := r.Params.(type) {
	case *protocol.TransferPrepare:
		result, err = g.prepare(*p)
	case *protocol.TransferStart:
		result, err = g.start(*p)
	case *protocol.TransferChunk:
		result, err = g.chunk(*p)
	case *protocol.TransferFinish:
		result, err = g.finish(*p)
	case *protocol.TransferNotice:
		result, err = g.notice(*p)
	default:
		err = ErrTransferBinding
	}
	if err != nil {
		_ = g.wire.Error(r, protocol.Internal, transferReason(err))
		return
	}
	_ = g.wire.Result(r, result)
}
func transferReason(err error) string {
	switch {
	case errors.Is(err, ErrTransferBinding):
		return "binding_conflict"
	case errors.Is(err, context.Canceled):
		return "generation_ended"
	default:
		return "endpoint_io_failed"
	}
}
func (g *transferGeneration) binding(id string) *transferBinding {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.bindings[id]
}
func (g *transferGeneration) prepare(p protocol.TransferPrepare) (protocol.TransferPrepared, error) {
	b := g.binding(p.ID)
	if b == nil {
		return protocol.TransferPrepared{}, ErrTransferBinding
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.ended || b.role != p.Role || b.path == "" {
		return protocol.TransferPrepared{}, ErrTransferBinding
	}
	if b.prepareAttempted {
		if b.prepareError != nil {
			return protocol.TransferPrepared{}, b.prepareError
		}
		return b.preparedResult(), nil
	}
	b.prepareAttempted = true
	defer func() {
		if !b.prepared {
			b.prepareError = ErrTransferBinding
		}
	}()
	g.mu.Lock()
	g.owner.mu.Lock()
	if g.closed || g.owner.active >= protocol.MaxActiveTransfersPerConnection {
		g.owner.mu.Unlock()
		g.mu.Unlock()
		return protocol.TransferPrepared{}, ErrTransferBinding
	}
	g.owner.pending--
	g.owner.active++
	g.active++
	b.active = true
	g.owner.mu.Unlock()
	g.mu.Unlock()
	ctx := b.ctx
	if p.Role == "source" {
		source, err := g.owner.executor.OpenTransferSource(ctx, b.path)
		if err != nil {
			return protocol.TransferPrepared{}, err
		}
		b.source = source
		b.size = source.Size()
		if b.size < 0 || b.size > protocol.MaxTransferBytes {
			return protocol.TransferPrepared{}, ErrTransferBinding
		}
	} else {
		if p.Size == nil {
			return protocol.TransferPrepared{}, ErrTransferBinding
		}
		b.size = int64(*p.Size)
		dest, err := g.owner.executor.OpenTransferDestination(ctx, b.path, b.overwrite)
		if err != nil {
			return protocol.TransferPrepared{}, err
		}
		b.destination = dest
		b.hash = sha256.New()
	}
	b.prepared = true
	return b.preparedResult(), nil
}
func (b *transferBinding) preparedResult() protocol.TransferPrepared {
	r := protocol.TransferPrepared{ID: b.id}
	if b.role == "source" {
		n := protocol.Integer(b.size)
		r.Size = &n
	} else {
		r.Ready = true
	}
	return r
}
func (g *transferGeneration) start(p protocol.TransferStart) (protocol.TransferStarted, error) {
	b := g.binding(p.ID)
	if b == nil {
		return protocol.TransferStarted{}, ErrTransferBinding
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.ended || !b.prepared || b.role != p.Role || b.size != int64(p.Size) {
		return protocol.TransferStarted{}, ErrTransferBinding
	}
	if !b.started {
		b.started = true
		if b.role == "source" {
			if p.Credit == nil || int(*p.Credit) != protocol.TransferCreditBytes {
				return protocol.TransferStarted{}, ErrTransferBinding
			}
			b.worker = make(chan struct{})
			go g.stream(b)
		}
	}
	return protocol.TransferStarted{ID: p.ID, Ready: p.Role == "destination", Started: p.Role == "source"}, nil
}
func (g *transferGeneration) stream(b *transferBinding) {
	defer close(b.worker)
	b.mu.Lock()
	source, size := b.source, b.size
	b.mu.Unlock()
	digest := sha256.New()
	buf := make([]byte, protocol.TransferChunkBytes)
	var offset int64
	for offset < size {
		n, err := io.ReadFull(source, buf[:min(int64(len(buf)), size-offset)])
		if err != nil {
			g.fail(b, "source_read_failed")
			return
		}
		digest.Write(buf[:n])
		var credit protocol.TransferCredit
		err = g.wire.Call(b.ctx, "transfer.chunk", protocol.TransferChunk{ID: b.id, Offset: protocol.Integer(offset), Data: base64.StdEncoding.EncodeToString(buf[:n])}, &credit)
		if err != nil || int64(credit.Offset) != offset+int64(n) {
			g.fail(b, "stream_lost")
			return
		}
		offset += int64(n)
	}
	if source.Verify() != nil {
		g.fail(b, "source_changed")
		return
	}
	var terminal protocol.TransferTerminal
	if g.wire.Call(b.ctx, "transfer.finish", protocol.TransferFinish{ID: b.id, Bytes: protocol.Integer(offset), SHA256: hex.EncodeToString(digest.Sum(nil))}, &terminal) != nil {
		g.fail(b, "commit_unconfirmed")
	}
}
func (g *transferGeneration) fail(b *transferBinding, reason string) {
	b.mu.Lock()
	ended := b.ended
	b.mu.Unlock()
	if ended {
		return
	}
	// Only a pre-finish source read/verification failure asserts nonpublication.
	// Transport loss uses the coordinator's existing phase/generation knowledge;
	// it is not an explicit cancel or permission to retry an uncertain commit.
	if reason == "source_changed" || reason == "source_read_failed" {
		var ignored protocol.TransferTerminal
		_ = g.wire.Call(g.ctx, "transfer.fail", protocol.TransferFailure{ID: b.id, Reason: reason}, &ignored)
	}
	g.end(b)
}
func (g *transferGeneration) chunk(p protocol.TransferChunk) (protocol.TransferCredit, error) {
	b := g.binding(p.ID)
	if b == nil {
		return protocol.TransferCredit{}, ErrTransferBinding
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.ended || !b.started || b.role != "destination" || b.offset != int64(p.Offset) {
		return protocol.TransferCredit{}, ErrTransferBinding
	}
	raw, err := base64.StdEncoding.Strict().DecodeString(p.Data)
	if err != nil || len(raw) > protocol.TransferChunkBytes || len(raw) == 0 || b.offset+int64(len(raw)) > b.size {
		return protocol.TransferCredit{}, ErrTransferBinding
	}
	n, err := b.destination.Write(raw)
	if err != nil || n != len(raw) {
		return protocol.TransferCredit{}, io.ErrShortWrite
	}
	b.hash.Write(raw)
	b.offset += int64(n)
	return protocol.TransferCredit{Offset: protocol.Integer(b.offset)}, nil
}
func (g *transferGeneration) finish(p protocol.TransferFinish) (protocol.TransferTerminal, error) {
	b := g.binding(p.ID)
	if b == nil {
		return protocol.TransferTerminal{}, ErrTransferBinding
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.ended || b.finishAttempted || !b.started || b.role != "destination" || b.offset != b.size || b.offset != int64(p.Bytes) || hex.EncodeToString(b.hash.Sum(nil)) != p.SHA256 {
		return protocol.TransferTerminal{}, ErrTransferBinding
	}
	b.finishAttempted = true
	if b.commitFinished != nil {
		defer close(b.commitFinished)
	}
	b.commitStarted.Store(true)
	published, err := b.destination.Commit()
	b.published = published
	if !published && errors.Is(err, ErrTransferCommitUnknown) {
		b.commitUnknown = true
		return protocol.TransferTerminal{ID: b.id, Outcome: "unknown", Bytes: p.Bytes, Reason: "publication_unconfirmed"}, nil
	}
	if !published {
		reason := "publication_failed"
		if errors.Is(err, errTransferNoReplaceUnsupported) {
			reason = "atomic_no_replace_unsupported"
		}
		return protocol.TransferTerminal{ID: b.id, Outcome: "failed", Bytes: protocol.Integer(b.offset), Reason: reason}, nil
	}
	r := protocol.TransferTerminal{ID: b.id, Outcome: "completed", Bytes: p.Bytes, SHA256: p.SHA256}
	if err != nil {
		r.Reason = "committed_cleanup_failed"
	}
	b.publication.Store(&r)
	return r, nil
}
func (g *transferGeneration) notice(p protocol.TransferNotice) (struct{}, error) {
	if p.Kind == "terminal" {
		if b := g.binding(p.ID); b != nil {
			g.end(b)
			b.mu.Lock()
			worker := b.worker
			b.mu.Unlock()
			if worker != nil {
				<-worker
			}
			// ACK confirms joined handling/resources only, never publication.
			<-b.retired
		}
		return struct{}{}, nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return struct{}{}, ErrTransferUnsupported
	}
	if g.bindings[p.ID] != nil {
		return struct{}{}, nil
	}
	g.owner.mu.Lock()
	defer g.owner.mu.Unlock()
	if g.owner.pending >= protocol.MaxPendingTransfersPerConnection {
		return struct{}{}, ErrTransferBinding
	}
	role := "destination"
	if p.Kind == "request" {
		role = "source"
	}
	g.owner.pending++
	ctx, cancel := context.WithCancel(g.ctx)
	g.bindings[p.ID] = &transferBinding{id: p.ID, role: role, retired: make(chan struct{}), commitFinished: make(chan struct{}), ctx: ctx, cancel: cancel}
	return struct{}{}, nil
}

// Join SDK-owned cleanup before product disposal. Custom executors must honour
// cancellation and close their owned resources.
func (t *Transfers) wait(wire *rpc.Conn) {
	if t == nil {
		return
	}
	t.mu.Lock()
	var all []*transferGeneration
	for g := range t.generations {
		if wire == nil || g.wire == wire {
			all = append(all, g)
		}
	}
	t.mu.Unlock()
	for _, g := range all {
		g.cancel()
	}
	for _, g := range all {
		<-g.finished
	}
}

// NewConnectionTransfers supports brokers that own a raw Connection and their
// own identity lifecycle instead of Peer. Nil uses the direct filesystem
// default. Register HandleConnectionRequest before hello.
func NewConnectionTransfers(executor TransferExecutor) (*Transfers, error) {
	return newTransfers(executor), nil
}

// HelloObserved negotiates the SDK-owned reserved info key without adding a
// field rejected by old daemons. The caller's identity observer and this exact
// generation's transfer binding are installed before the next incoming frame.
// generation must be the native identity lifetime, not a tool invocation.
func (t *Transfers) HelloObserved(ctx, generation context.Context, wire *Connection, identity PeerIdentity, seen func() error) error {
	if t == nil || generation == nil {
		return ErrTransferBinding
	}
	hello := identity
	hello.Protocol = 1
	hello.Info = make(map[string]any, len(identity.Info)+1)
	for key, value := range identity.Info {
		if key != protocol.TransferInfoKey {
			hello.Info[key] = value
		}
	}
	hello.Info[protocol.TransferInfoKey] = 1
	var result protocol.HelloResult
	return wire.CallObserved(ctx, "session.hello", hello, &result, func() error {
		if err := generation.Err(); err != nil {
			return err
		}
		enabled := false
		for _, c := range result.Capabilities {
			if c == protocol.TransferCapability {
				enabled = true
			}
		}
		attached := t.attach(wire, generation, enabled)
		if enabled && attached == nil {
			return ErrTransferUnsupported
		}
		if seen != nil {
			if err := seen(); err != nil {
				// Reject only this observed generation, never a newer connection
				// admitted while the product observer was running.
				if attached != nil {
					t.mu.Lock()
					if t.current == attached {
						t.current = nil
					}
					t.mu.Unlock()
					attached.cancel()
				}
				return err
			}
		}
		return nil
	})
}

// HandleConnectionRequest routes only SDK machine transfer methods. It never
// handles ordinary model delivery or issues filesystem work on the RPC reader.
func (t *Transfers) HandleConnectionRequest(wire *Connection, request *Request) bool {
	return t.handle(wire, request)
}

// CloseConnectionGeneration cancels and joins SDK-owned handles/workers before
// a raw broker disposes or replaces its native identity/executor.
func (t *Transfers) CloseConnectionGeneration(wire *Connection) { t.wait(wire) }

// ProjectDelivery captures a bounded receiver-local projection on the reader.
// Invoke its returned function OFF the reader, before normal native delivery.
// If publication was already in flight, it waits for that one fact/UNKNOWN;
// it performs no filesystem operation, retry, direct wake or model callback.
// Only the receiver gets its local destination; paths never enter wire/state.
func (t *Transfers) ProjectDelivery(wire *Connection, d *DeliveryRequest) func(context.Context, *DeliveryRequest) error {
	if t == nil || !strings.HasPrefix(d.MessageID, "transfer-notice-") {
		return nil
	}
	parsed, err := protocol.DecodeParams("transfer.notice", []byte(d.Body))
	if err != nil {
		return nil
	}
	notice := parsed.(*protocol.TransferNotice)
	if notice.Kind != "terminal" {
		return nil
	}
	t.mu.Lock()
	g := t.current
	t.mu.Unlock()
	if g == nil || g.wire != wire {
		return nil
	}
	b := g.binding(notice.ID)
	if b == nil {
		return nil
	}
	path := b.destinationProjection.Load()
	if path == nil {
		return nil
	}
	return func(ctx context.Context, out *DeliveryRequest) error {
		if b.commitStarted.Load() && b.commitFinished != nil {
			select {
			case <-b.commitFinished:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		select {
		case <-b.retired:
		case <-ctx.Done():
			return ctx.Err()
		}
		copy := *notice
		if published := b.publication.Load(); published != nil {
			copy.Outcome = published.Outcome
			copy.Bytes = &published.Bytes
			copy.SHA256 = published.SHA256
			copy.Reason = published.Reason
		}
		copy.Reason = withTransferCleanupReason(copy.Reason, b.cleanupReason)
		projection := struct {
			protocol.TransferNotice
			Destination string `json:"destination"`
		}{copy, *path}
		raw, err := json.Marshal(projection)
		if err == nil {
			out.Body = string(raw)
		}
		return err
	}
}

// Receiver-local projection only; never forward local cleanup details upstream.
func withTransferCleanupReason(reason, cleanup string) string {
	if cleanup == "" {
		return reason
	}
	if reason == "" {
		return cleanup
	}
	const limit = 2048
	for len(reason)+2+len(cleanup) > limit {
		_, n := utf8.DecodeLastRuneInString(reason)
		reason = reason[:len(reason)-n]
	}
	return reason + "; " + cleanup
}

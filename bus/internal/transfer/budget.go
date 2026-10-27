// SPDX-License-Identifier: GPL-3.0-only

// Package transfer owns bounded, generation-scoped transfer coordination. It
// never receives filesystem paths or performs endpoint filesystem operations.
package transfer

import (
	"errors"
	"sync"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

var ErrCapacity = errors.New("transfer capacity exhausted")

// Budget counts distinct transfers. A host pending reservation covers all
// three local engine copies; active reservations include their stage buffers.
// Native executor pipes, ordinary control queues and runtime RSS are separate.
type Budget struct {
	mu                    sync.Mutex
	pending, active       int
	pendingMax, activeMax int
	pendingCopies, stages int
	endpoints             map[string]endpointUsage
}

type Reservation struct {
	budget           *Budget
	mu               sync.Mutex
	active, released bool
	endpoints        []string
}

// BindPendingEndpoints charges late-resolved endpoint generations while the
// SAME pre-ID pending reservation remains admitted. No second provisional map
// or extra admission slot is created during federation binding.
func (r *Reservation) BindPendingEndpoints(keys ...string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.released || r.active {
		return ErrCapacity
	}
	b := r.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	var extra []string
	for _, key := range keys {
		found := false
		for _, old := range append(r.endpoints, extra...) {
			if old == key {
				found = true
			}
		}
		if found {
			continue
		}
		if key == "" || b.endpoints[key].pending >= protocol.MaxPendingTransfersPerConnection {
			return ErrCapacity
		}
		extra = append(extra, key)
	}
	for _, key := range extra {
		u := b.endpoints[key]
		u.pending++
		b.endpoints[key] = u
	}
	r.endpoints = append(r.endpoints, extra...)
	return nil
}

type Usage struct {
	Pending, Active int
	ControlledBytes int64
}

func HostBudget() *Budget {
	return &Budget{pendingMax: protocol.MaxPendingTransfersPerHost, activeMax: protocol.MaxActiveTransfersPerHost, pendingCopies: 3, stages: 3}
}
func HubBudget() *Budget {
	return &Budget{pendingMax: protocol.MaxPendingTransfersPerHub, activeMax: protocol.MaxActiveTransfersPerHub, pendingCopies: 1, stages: 1}
}

func (b *Budget) Pending() (*Reservation, error) {
	return b.PendingFor()
}

// PendingFor shares endpoint admission across local and federated engines on
// this host. Endpoint keys are authenticated generations, never per-call IDs.
func (b *Budget) PendingFor(keys ...string) (*Reservation, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.pending >= b.pendingMax {
		return nil, ErrCapacity
	}
	unique := make([]string, 0, len(keys))
	for _, key := range keys {
		found := false
		for _, old := range unique {
			if old == key {
				found = true
			}
		}
		if found {
			continue
		}
		if key == "" || b.endpoints[key].pending >= protocol.MaxPendingTransfersPerConnection {
			return nil, ErrCapacity
		}
		unique = append(unique, key)
	}
	if b.endpoints == nil {
		b.endpoints = map[string]endpointUsage{}
	}
	for _, key := range unique {
		u := b.endpoints[key]
		u.pending++
		b.endpoints[key] = u
	}
	b.pending++
	return &Reservation{budget: b, endpoints: unique}, nil
}

// Prepare atomically recharges one pending reservation into the combined
// preparing/accepted cap. Its old metadata must either be released or included
// in the newly reserved stage buffers; it is never another admitted transfer.
func (r *Reservation) Prepare() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.released {
		return errors.New("transfer reservation ended")
	}
	if r.active {
		return nil
	}
	b := r.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.active >= b.activeMax {
		return ErrCapacity
	}
	for _, key := range r.endpoints {
		if b.endpoints[key].active >= protocol.MaxActiveTransfersPerConnection {
			return ErrCapacity
		}
	}
	for _, key := range r.endpoints {
		u := b.endpoints[key]
		u.pending--
		u.active++
		b.endpoints[key] = u
	}
	b.pending--
	b.active++
	r.active = true
	return nil
}

// Release is idempotent for competing terminal/cancellation/generation events.
// Call it only after owned buffers/writers/handles have actually retired.
func (r *Reservation) Release() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.released {
		return
	}
	r.released = true
	b := r.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	if r.active {
		b.active--
	} else {
		b.pending--
	}
	for _, key := range r.endpoints {
		u := b.endpoints[key]
		if r.active {
			u.active--
		} else {
			u.pending--
		}
		if u.pending == 0 && u.active == 0 {
			delete(b.endpoints, key)
		} else {
			b.endpoints[key] = u
		}
	}
}

func (b *Budget) Usage() Usage {
	b.mu.Lock()
	defer b.mu.Unlock()
	return Usage{Pending: b.pending, Active: b.active, ControlledBytes: int64(b.pending*b.pendingCopies*protocol.TransferPendingCopyBytes + b.active*b.stages*protocol.TransferStageBufferBytes)}
}

// Stage accounts for every retained raw/encoded/transient backing allocation.
// A lease must be obtained BEFORE allocating/copying, and kept while any owner
// retains the backing buffer, including a pending write/decoder reference.
type Stage struct {
	mu              sync.Mutex
	bytes           int
	ingress, egress int
}

type BufferLease struct {
	once  sync.Once
	stage *Stage
	bytes int
	queue Queue
}

type Queue uint8

const (
	Scratch Queue = iota
	Ingress
	Egress
)

type StageUsage struct{ Bytes, Ingress, Egress int }

func (s *Stage) Reserve(bytes int, queue Queue) (*BufferLease, error) {
	if bytes < 0 || queue > Egress {
		return nil, errors.New("invalid transfer allocation")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if bytes > protocol.TransferStageBufferBytes-s.bytes || queue == Ingress && s.ingress >= 4 || queue == Egress && s.egress >= 4 {
		return nil, ErrCapacity
	}
	if queue == Ingress || queue == Egress {
		if bytes > protocol.MaxTransferFrameBytes {
			return nil, ErrCapacity
		}
	}
	s.bytes += bytes
	if queue == Ingress {
		s.ingress++
	}
	if queue == Egress {
		s.egress++
	}
	return &BufferLease{stage: s, bytes: bytes, queue: queue}, nil
}

func (l *BufferLease) Release() {
	l.once.Do(func() {
		s := l.stage
		s.mu.Lock()
		defer s.mu.Unlock()
		s.bytes -= l.bytes
		if l.queue == Ingress {
			s.ingress--
		}
		if l.queue == Egress {
			s.egress--
		}
	})
}

func (s *Stage) Usage() StageUsage {
	s.mu.Lock()
	defer s.mu.Unlock()
	return StageUsage{s.bytes, s.ingress, s.egress}
}

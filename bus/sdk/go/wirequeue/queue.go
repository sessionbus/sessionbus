// SPDX-License-Identifier: MIT

// Package wirequeue serializes one connection's frame writes. Request IDs are
// allocated only after priority/fairness selects a frame, so a later-selected
// control request can never overtake an already-numbered bulk request.
package wirequeue

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

var ErrClosed = errors.New("frame writer closed")
var ErrCapacity = errors.New("frame writer capacity exhausted")

// Job transfers its retained backing-buffer ownership to Queue on admission.
// Build runs once on the writer and may only encode/register a correlation; it
// must not wait on disk, credit, a response, or a connection-owner event loop.
// Request=false keeps the original reply ID inside Build; the supplied ID is0.
type Job struct {
	Stream        string
	Bulk, Request bool
	Weight        int
	Build         func(id int64) ([]byte, error)
	Complete      func(error)
	once          sync.Once
}

func (j *Job) complete(err error) {
	j.once.Do(func() {
		if j.Complete != nil {
			j.Complete(err)
		}
	})
}

type Queue struct {
	currentBulk             bool
	currentBulkBytes        int
	lastBulkStream          string
	mu                      sync.Mutex
	control, bulk           []*Job
	controlBytes, bulkBytes int
	next                    int64
	burst                   int
	closed                  error
	running                 bool
	wake                    chan struct{}
	space                   chan struct{}
}

type Usage struct {
	Control, Bulk, ControlBytes, BulkBytes int
	NextID                                 int64
	Closed                                 bool
}

func New() *Queue { return &Queue{wake: make(chan struct{}, 1), space: make(chan struct{})} }

// Submit never closes the connection on overflow. Existing control owners keep
// their own overflow policy; bulk callers receive bounded backpressure instead.
// The caller retains ownership when Submit refuses admission.
func (q *Queue) Submit(j *Job) error {
	if j == nil || j.Build == nil || j.Weight < 0 {
		return errors.New("invalid frame writer job")
	}
	if j.Bulk && j.Weight > protocol.MaxTransferFrameBytes || !j.Bulk && j.Weight > protocol.MaxFrameBytes {
		return ErrCapacity
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.submitLocked(j)
}

func (q *Queue) submitLocked(j *Job) error {
	if q.closed != nil {
		return q.closed
	}
	if j.Bulk {
		if len(q.bulk)+boolInt(q.currentBulk) >= 4 || q.bulkBytes+q.currentBulkBytes+j.Weight > 4*protocol.MaxTransferFrameBytes {
			return ErrCapacity
		}
		q.bulk = append(q.bulk, j)
		q.bulkBytes += j.Weight
	} else {
		if len(q.control) >= protocol.MaxOperations {
			return ErrCapacity
		}
		q.control = append(q.control, j)
		q.controlBytes += j.Weight
	}
	select {
	case q.wake <- struct{}{}:
	default:
	}
	return nil
}

// SubmitWait preserves ordinary callers' preexisting socket-write backpressure.
// It adds no retained frame/correlation beyond the caller's existing reservation;
// bulk admission remains nonblocking and never consumes control slots.
func (q *Queue) SubmitWait(ctx context.Context, j *Job) error {
	if j == nil || j.Build == nil || j.Weight < 0 || j.Bulk || j.Weight > protocol.MaxFrameBytes {
		return ErrCapacity
	}
	for {
		q.mu.Lock()
		err := q.submitLocked(j)
		changed := q.space
		q.mu.Unlock()
		if !errors.Is(err, ErrCapacity) {
			return err
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (q *Queue) signalSpaceLocked() { close(q.space); q.space = make(chan struct{}) }

func (q *Queue) take() (*Job, int64, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed != nil {
		return nil, 0, q.closed
	}
	var j *Job
	if len(q.control) > 0 && (q.burst < 8 || len(q.bulk) == 0) {
		j = q.control[0]
		q.control[0] = nil
		q.control = q.control[1:]
		q.controlBytes -= j.Weight
		q.burst++
	} else if len(q.bulk) > 0 {
		index := 0
		for i, job := range q.bulk {
			if job.Stream != q.lastBulkStream {
				index = i
				break
			}
		}
		j = q.bulk[index]
		copy(q.bulk[index:], q.bulk[index+1:])
		q.bulk[len(q.bulk)-1] = nil
		q.bulk = q.bulk[:len(q.bulk)-1]
		q.bulkBytes -= j.Weight
		q.currentBulkBytes = j.Weight
		q.currentBulk = true
		q.lastBulkStream = j.Stream
		q.burst = 0
	}
	if j == nil {
		return nil, 0, nil
	}
	q.signalSpaceLocked()
	var id int64
	if j.Request {
		if q.next == protocol.MaxRequestID {
			return j, 0, errors.New("request id space exhausted")
		}
		q.next++
		id = q.next
	}
	return j, id, nil
}

// Run is single-use and uses the connection's existing writer goroutine. Close
// wakes an empty queue; the connection owner still closes its fd to interrupt a
// blocked/partial write. No short socket timeout or replay is introduced.
func (q *Queue) Run(writer io.Writer) error {
	q.mu.Lock()
	if q.running {
		q.mu.Unlock()
		return errors.New("frame writer Run is single-use")
	}
	q.running = true
	q.mu.Unlock()
	for {
		j, id, err := q.take()
		if err != nil {
			if j != nil {
				q.finishSelected(j)
				j.complete(err)
			}
			q.Close(err)
			return err
		}
		if j == nil {
			<-q.wake
			continue
		}
		body, err := j.Build(id)
		if err != nil {
			q.finishSelected(j)
			j.complete(err)
			continue
		}
		limit := protocol.MaxFrameBytes
		if j.Bulk {
			limit = protocol.MaxTransferFrameBytes
		}
		if err == nil && (len(body) > limit || len(body) > j.Weight) {
			err = errors.New("frame exceeds its reserved backing bound")
		}
		if err == nil {
			err = writeFull(writer, body)
		}
		q.finishSelected(j)
		j.complete(err)
		if err != nil {
			q.Close(err)
			return err
		}
	}
}

func (q *Queue) Close(cause error) {
	if cause == nil {
		cause = ErrClosed
	}
	q.mu.Lock()
	if q.closed != nil {
		q.mu.Unlock()
		return
	}
	q.closed = cause
	q.signalSpaceLocked()
	pending := append(q.control, q.bulk...)
	q.control = nil
	q.bulk = nil
	q.controlBytes = 0
	q.bulkBytes = 0
	select {
	case q.wake <- struct{}{}:
	default:
	}
	q.mu.Unlock()
	for _, j := range pending {
		j.complete(cause)
	}
}

func (q *Queue) Usage() Usage {
	q.mu.Lock()
	defer q.mu.Unlock()
	return Usage{len(q.control), len(q.bulk) + boolInt(q.currentBulk), q.controlBytes, q.bulkBytes + q.currentBulkBytes, q.next, q.closed != nil}
}

func writeFull(w io.Writer, b []byte) error {
	for len(b) > 0 {
		n, e := w.Write(b)
		if e != nil {
			return e
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		b = b[n:]
	}
	return nil
}

func (q *Queue) finishSelected(j *Job) {
	if !j.Bulk {
		return
	}
	q.mu.Lock()
	q.currentBulkBytes = 0
	q.currentBulk = false
	q.signalSpaceLocked()
	q.mu.Unlock()
}
func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

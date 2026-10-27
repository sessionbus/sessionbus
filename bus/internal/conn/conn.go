// SPDX-License-Identifier: GPL-3.0-only

// Package conn owns one socket reader/writer. Bulk has independent bounded
// admission and never invokes the overflow-closing ordinary Send path.
package conn

import (
	"bufio"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
	"github.com/antst/sessionbus/bus/sdk/go/wirequeue"
)

const OutboxSize = 256

var ErrBulkCapacity = errors.New("bulk ingress capacity exhausted")

type Frame struct {
	Value   protocol.Frame
	Err     error
	Ordered bool
	Release func()
}
type Closed struct {
	Connection *Conn
	Cause      error
}

// Dispatched precedes any response to this request. Owners drain this dedicated
// bounded channel before consuming replies; Build never waits on an owner loop.
type Dispatched struct {
	ID    int64
	Value any
}
type Conn struct {
	bulkGate   atomic.Pointer[func(protocol.Frame) bool]
	fd         net.Conn
	inbox      chan any
	writer     *wirequeue.Queue
	dispatched chan Dispatched
	bulk       chan Frame
	bulkSlots  chan struct{}
	done       chan struct{}
	finishing  bool
}

// SetBulkGate authorizes an already admitted ID/role/generation before a
// payload can enter the retained bulk queue. Unknown IDs retain no chunk body.
// The gate must not perform I/O or block on an owner/credit callback.
func (c *Conn) SetBulkGate(gate func(protocol.Frame) bool) { c.bulkGate.Store(&gate) }

func Start(fd net.Conn, inbox chan any, group *sync.WaitGroup) *Conn {
	c := &Conn{fd: fd, inbox: inbox, writer: wirequeue.New(), dispatched: make(chan Dispatched, OutboxSize+8), bulk: make(chan Frame, 4), bulkSlots: make(chan struct{}, 4), done: make(chan struct{})}
	group.Add(2)
	go func() { defer group.Done(); c.read() }()
	go func() { defer group.Done(); _ = c.writer.Run(fd); c.Close() }()
	return c
}
func (c *Conn) Done() <-chan struct{}         { return c.done }
func (c *Conn) Bulk() <-chan Frame            { return c.bulk }
func (c *Conn) Dispatched() <-chan Dispatched { return c.dispatched }
func (c *Conn) DrainDispatched(consume func(Dispatched)) {
	for {
		select {
		case d := <-c.dispatched:
			consume(d)
		default:
			return
		}
	}
}
func (c *Conn) Post(event any) bool {
	select {
	case <-c.done:
		return false
	default:
	}
	select {
	case c.inbox <- event:
		return true
	default:
		return false
	}
}
func (c *Conn) Send(frame []byte) bool {
	if c.finishing {
		return false
	}
	select {
	case <-c.done:
		return false
	default:
	}
	err := c.writer.Submit(&wirequeue.Job{Weight: len(frame), Build: func(int64) ([]byte, error) { return frame, nil }})
	if err != nil {
		c.Close()
		return false
	}
	return true
}

// Request allocates its ID only after 8-control/1-bulk writer selection. Value
// is the owner's queued correlation, charged at admission, not another copy.
func (c *Conn) Request(bulk bool, value any, build func(int64) ([]byte, error), stream ...string) bool {
	return c.request(bulk, value, build, false, stream...)
}
func (c *Conn) request(bulk bool, value any, build func(int64) ([]byte, error), finish bool, stream ...string) bool {
	if c.finishing && !finish {
		return false
	}
	select {
	case <-c.done:
		return false
	default:
	}
	weight := protocol.MaxFrameBytes
	if bulk {
		weight = protocol.MaxTransferFrameBytes
	}
	streamID := ""
	if len(stream) > 0 {
		streamID = stream[0]
	}
	job := &wirequeue.Job{Stream: streamID, Request: true, Bulk: bulk, Weight: weight, Build: func(id int64) ([]byte, error) {
		body, err := build(id)
		if err != nil {
			return nil, err
		}
		if value != nil {
			select {
			case c.dispatched <- Dispatched{ID: id, Value: value}:
			default:
				return nil, errors.New("dispatch reservation exceeded")
			}
		}
		return body, nil
	}, Complete: func(err error) {
		if err != nil || finish {
			c.Close()
		}
	}}
	if err := c.writer.Submit(job); err != nil {
		if !bulk {
			c.Close()
		}
		return false
	}
	return true
}
func (c *Conn) Finish(frame []byte, bound time.Duration) bool {
	if c.finishing {
		return false
	}
	c.finishing = true
	_ = c.fd.SetWriteDeadline(time.Now().Add(bound))
	err := c.writer.Submit(&wirequeue.Job{Weight: len(frame), Build: func(int64) ([]byte, error) { return frame, nil }, Complete: func(error) { c.Close() }})
	if err != nil {
		c.Close()
		return false
	}
	return true
}
func (c *Conn) FinishRequest(build func(int64) ([]byte, error), bound time.Duration) bool {
	if c.finishing {
		return false
	}
	c.finishing = true
	_ = c.fd.SetWriteDeadline(time.Now().Add(bound))
	return c.request(false, nil, build, true)
}
func (c *Conn) Close()       { _ = c.fd.Close(); c.writer.Close(wirequeue.ErrClosed) }
func (c *Conn) OwnerClosed() { c.finishing = true; c.writer.Close(wirequeue.ErrClosed) }
func (c *Conn) read() {
	reader := bufio.NewReaderSize(c.fd, protocol.MaxFrameBytes)
	last := int64(0)
	for {
		body, err := reader.ReadSlice('\n')
		if err != nil {
			c.Close()
			close(c.done)
			c.inbox <- Closed{Connection: c, Cause: err}
			return
		}
		value, decodeErr := protocol.DecodeFrame(body[:len(body)-1])
		event := Frame{Value: value, Err: decodeErr, Ordered: true}
		if value.Request {
			if value.ID <= last {
				event.Err = errors.New("nonmonotonic request id")
			} else {
				last = value.ID
			}
		}
		if event.Err == nil && value.Request && value.Method == "transfer.chunk" {
			gate := c.bulkGate.Load()
			if gate == nil || !(*gate)(value) {
				event.Err = ErrBulkCapacity
				event.Value.Params = nil
				c.inbox <- event
				continue
			}
			select {
			case c.bulkSlots <- struct{}{}:
				var once sync.Once
				event.Release = func() { once.Do(func() { <-c.bulkSlots }) }
				c.bulk <- event
			default:
				event.Err = ErrBulkCapacity
				event.Value.Params = nil
				c.inbox <- event
			}
		} else {
			c.inbox <- event
		}
	}
}

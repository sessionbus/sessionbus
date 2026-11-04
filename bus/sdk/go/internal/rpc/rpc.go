// SPDX-License-Identifier: MIT

package rpc

import (
	"bufio"
	"context"
	"errors"
	"net"
	"sync"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
	"github.com/antst/sessionbus/bus/sdk/go/wirequeue"
)

var ErrClosed = errors.New("sessionbus connection closed")

type Request struct {
	release func()
	once    sync.Once
	ID      int64
	Method  string
	Params  any
}

type pending struct {
	method         string
	result         any
	done           chan error
	seen           func() error
	drain          bool
	id             int64
	reserved, bulk bool
}

type Conn struct {
	fd      net.Conn
	reader  *bufio.Reader
	client  bool
	handler func(context.Context, *Request)
	ctx     context.Context
	cancel  context.CancelFunc

	stateMu                sync.Mutex
	writer                 *wirequeue.Queue
	all                    map[*pending]bool
	normalCount, bulkCount int
	inboundBulk            int
	closed                 bool
	next, last             int64
	pending                map[int64]*pending
}

func New(fd net.Conn, client bool, handler func(context.Context, *Request)) *Conn {
	ctx, cancel := context.WithCancel(context.Background())
	c := &Conn{fd: fd, reader: bufio.NewReaderSize(fd, protocol.MaxFrameBytes), client: client, handler: handler, ctx: ctx, cancel: cancel, pending: map[int64]*pending{}}
	c.startWriter()
	go c.read()
	return c
}

func (c *Conn) Done() <-chan struct{}    { return c.ctx.Done() }
func (c *Conn) Context() context.Context { return c.ctx }

func (c *Conn) Call(ctx context.Context, method string, params, result any) error {
	return c.call(ctx, method, params, result, nil)
}

// CallObserved runs seen after a valid result is decoded and before the reader
// accepts the next frame.
func (c *Conn) CallObserved(ctx context.Context, method string, params, result any, seen func() error) error {
	return c.call(ctx, method, params, result, seen)
}

func (c *Conn) call(ctx context.Context, method string, params, result any, seen func() error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p := c.begin(method, params, result, seen)
	select {
	case err := <-p.done:
		return err
	case <-ctx.Done():
		if c.abandon(p) {
			return ctx.Err()
		}
		return <-p.done
	}
}

// Begin sends one call and returns its eventual response without tying it to a
// caller context. It lets a connection owner serialize writes without waiting
// for earlier responses.
func (c *Conn) Begin(method string, params, result any, seen func() error) <-chan error {
	p := c.begin(method, params, result, seen)
	return p.done
}

func (c *Conn) begin(method string, params, result any, seen func() error) *pending {
	raw, err := protocol.EncodeParams(method, params)
	p := &pending{method: method, result: result, done: make(chan error, 1), seen: seen, bulk: method == "transfer.chunk"}
	if err != nil {
		p.done <- err
		return p
	}
	c.stateMu.Lock()
	if c.closed {
		err = ErrClosed
	} else if p.bulk && c.bulkCount >= 4 || !p.bulk && c.normalCount >= protocol.MaxOperations {
		err = &protocol.RPCError{Code: protocol.Busy, Message: "busy"}
	} else {
		if c.all == nil {
			c.all = map[*pending]bool{}
		}
		c.all[p] = true
		p.reserved = true
		if p.bulk {
			c.bulkCount++
		} else {
			c.normalCount++
		}
	}
	c.stateMu.Unlock()
	if err != nil {
		p.done <- err
		return p
	}
	writer := c.startWriter()
	if writer == nil {
		if c.drop(p) {
			p.done <- ErrClosed
		}
		return p
	}
	job := &wirequeue.Job{Stream: protocol.TransferStreamID(params), Bulk: p.bulk, Weight: len(raw) + 128, Build: func(_ int64) ([]byte, error) {
		c.stateMu.Lock()
		if c.closed {
			c.stateMu.Unlock()
			return nil, ErrClosed
		}
		if c.next == protocol.MaxRequestID {
			c.stateMu.Unlock()
			return nil, errors.New("request id space exhausted")
		}
		// This function runs only when the writer SELECTS this job. It must not
		// assign a lower ID to a bulk frame waiting behind later priority control.
		c.next++
		p.id = c.next
		c.pending[p.id] = p
		c.stateMu.Unlock()
		return protocol.EncodeRequest(p.id, method, raw)
	}, Complete: func(err error) {
		if err != nil && c.drop(p) {
			p.done <- err
		}
	}}
	if p.bulk {
		err = writer.Submit(job)
	} else {
		err = writer.SubmitWait(c.ctx, job)
	}
	if err != nil {
		if c.drop(p) {
			if errors.Is(err, wirequeue.ErrCapacity) {
				err = &protocol.RPCError{Code: protocol.Busy, Message: "busy"}
			}
			p.done <- err
		}
	}
	return p
}

// One bounded writer goroutine replaces the caller-held socket write mutex.
// The fd owner still closes the socket to unblock a partial write.
func (c *Conn) startWriter() *wirequeue.Queue {
	c.stateMu.Lock()
	if c.writer != nil || c.closed {
		q := c.writer
		c.stateMu.Unlock()
		return q
	}
	q := wirequeue.New()
	c.writer = q
	c.stateMu.Unlock()
	go func() { err := q.Run(c.fd); c.close(err) }()
	return q
}

func (c *Conn) Result(request *Request, value any) error {
	body, err := protocol.ResultBytes(request.ID, request.Method, value)
	return c.finish(request, body, err)
}

func (c *Conn) Error(request *Request, code int, data any) error {
	body, err := protocol.ErrorBytes(request.ID, code, data)
	return c.finish(request, body, err)
}

func (c *Conn) finish(request *Request, body []byte, err error) error {
	defer request.once.Do(func() {
		if request.release != nil {
			request.release()
		}
	})
	if err != nil {
		return err
	}
	c.stateMu.Lock()
	closed := c.closed
	c.stateMu.Unlock()
	if closed {
		return ErrClosed
	}
	writer := c.startWriter()
	if writer == nil {
		return ErrClosed
	}
	done := make(chan error, 1)
	err = writer.SubmitWait(c.ctx, &wirequeue.Job{Weight: len(body), Build: func(int64) ([]byte, error) { return body, nil }, Complete: func(err error) { done <- err }})
	if err != nil {
		return err
	}
	return <-done
}

func (c *Conn) read() {
	for c.ctx.Err() == nil {
		body, err := c.reader.ReadSlice('\n')
		if err != nil {
			c.close(err)
			return
		}
		frame, err := protocol.DecodeFrame(body[:len(body)-1])
		if err != nil {
			if frame.ID > 0 && frame.Request {
				c.reject(frame, protocol.InvalidFrame)
			} else {
				c.close(err)
			}
			return
		}
		if frame.Request {
			c.receiveRequest(frame)
		} else {
			c.receiveResponse(frame)
		}
	}
}

func (c *Conn) receiveRequest(frame protocol.Frame) {
	c.stateMu.Lock()
	ordered := frame.ID > c.last
	if ordered {
		c.last = frame.ID
	}
	c.stateMu.Unlock()
	if !ordered {
		c.reject(frame, protocol.InvalidFrame)
		return
	}
	bulk := frame.Method == "transfer.chunk"
	if bulk {
		c.stateMu.Lock()
		full := c.inboundBulk >= 4
		if !full {
			c.inboundBulk++
		}
		c.stateMu.Unlock()
		if full {
			_ = c.Error(&Request{ID: frame.ID, Method: frame.Method}, protocol.Busy, nil)
			return
		}
	}
	params, err := protocol.DecodeParams(frame.Method, frame.Params)
	request := &Request{ID: frame.ID, Method: frame.Method, Params: params}
	if bulk {
		request.release = func() { c.stateMu.Lock(); c.inboundBulk--; c.stateMu.Unlock() }
	}
	if err != nil || !protocol.Allows(frame.Method, !c.client) {
		code := protocol.InvalidFrame
		if frame.Method == "session.hello" {
			code = protocol.InvalidHello
		}
		if request.release != nil {
			request.release()
		}
		c.reject(frame, code)
		return
	}
	c.handler(c.ctx, request)
}

func (c *Conn) reject(frame protocol.Frame, code int) {
	_ = c.Error(&Request{ID: frame.ID, Method: frame.Method}, code, nil)
	c.close(errors.New("invalid frame"))
}

func (c *Conn) receiveResponse(frame protocol.Frame) {
	c.stateMu.Lock()
	p, ok := c.pending[frame.ID]
	if ok {
		delete(c.pending, frame.ID)
		c.releaseLocked(p)
	}
	c.stateMu.Unlock()
	if !ok {
		c.close(errors.New("unmatched response"))
		return
	}
	err := error(frame.Error)
	if frame.Error == nil {
		if p.drain {
			_, err = protocol.DecodeResult(p.method, frame.Result)
		} else {
			err = protocol.UnmarshalResult(p.method, frame.Result, p.result)
		}
		if err != nil {
			c.close(err)
		} else if p.seen != nil {
			err = p.seen()
			if err != nil {
				c.close(err)
			}
		}
	}
	p.done <- err
}

// Keep the admitted correlation until its reply or connection closure. The
// reader and cancellation compete under stateMu; only the winner may use the
// original target/observer. A draining reply still undergoes full validation.
func (c *Conn) abandon(p *pending) bool {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if !p.reserved {
		return false
	}
	p.result, p.seen, p.drain = nil, nil, true
	return true
}

func (c *Conn) releaseLocked(p *pending) {
	if !p.reserved {
		return
	}
	p.reserved = false
	delete(c.all, p)
	if p.bulk {
		c.bulkCount--
	} else {
		c.normalCount--
	}
}

func (c *Conn) drop(p *pending) bool {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if !p.reserved {
		return false
	}
	if p.id != 0 {
		delete(c.pending, p.id)
	}
	c.releaseLocked(p)
	return true
}

func (c *Conn) Close() error { c.close(ErrClosed); return nil }

func (c *Conn) close(cause error) {
	if cause == nil {
		cause = ErrClosed
	}
	c.stateMu.Lock()
	if c.closed {
		c.stateMu.Unlock()
		return
	}
	c.closed = true
	calls := map[*pending]bool{}
	for p := range c.all {
		calls[p] = true
	}
	for _, p := range c.pending {
		calls[p] = true
	}
	for p := range calls {
		c.releaseLocked(p)
	}
	c.pending = map[int64]*pending{}
	writer := c.writer
	c.stateMu.Unlock()
	c.cancel()
	_ = c.fd.Close()
	if writer != nil {
		writer.Close(cause)
	}
	for call := range calls {
		call.done <- cause
	}
}

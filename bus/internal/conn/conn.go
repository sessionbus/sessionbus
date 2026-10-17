// SPDX-License-Identifier: GPL-3.0-only

// Package conn owns the socket reader and writer used by an Sessionbus session.
package conn

import (
	"bufio"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

const OutboxSize = 256

type Frame struct {
	Value protocol.Frame
	Err   error
}

type Closed struct {
	Connection *Conn
	Cause      error
}

type Conn struct {
	fd        net.Conn
	inbox     chan any
	outbox    chan []byte
	done      chan struct{}
	finishing bool
	started   time.Time
	lastRead  atomic.Int64
}

func Start(fd net.Conn, inbox chan any, group *sync.WaitGroup) *Conn {
	c := &Conn{fd: fd, inbox: inbox, outbox: make(chan []byte, OutboxSize), done: make(chan struct{}), started: time.Now()}
	group.Add(2)
	go func() { defer group.Done(); c.read() }()
	go func() { defer group.Done(); c.write() }()
	return c
}

func (c *Conn) Done() <-chan struct{} { return c.done }

// LastRead is when the reader last completed an inbound frame, before the
// frame waits for the owner loop; it is the start time if none was read.
func (c *Conn) LastRead() time.Time {
	return c.started.Add(time.Duration(c.lastRead.Load()))
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

// Send is called only by the connection's owner loop.
func (c *Conn) Send(frame []byte) bool {
	if c.finishing {
		return false
	}
	select {
	case <-c.done:
		return false
	default:
	}
	select {
	case c.outbox <- frame:
		return true
	default:
		c.Close()
		return false
	}
}

// Finish queues the last frame, bounds its write, then closes the outbox.
// It is called only by the connection's owner loop.
func (c *Conn) Finish(frame []byte, bound time.Duration) bool {
	if c.finishing {
		return false
	}
	select {
	case <-c.done:
		return false
	default:
	}
	c.finishing = true
	_ = c.fd.SetWriteDeadline(time.Now().Add(bound))
	select {
	case c.outbox <- frame:
		close(c.outbox)
		return true
	default:
		c.Close()
		return false
	}
}

func (c *Conn) Close() {
	_ = c.fd.Close()
}

func (c *Conn) OwnerClosed() {
	if !c.finishing {
		c.finishing = true
		close(c.outbox)
	}
}

func (c *Conn) read() {
	reader := bufio.NewReaderSize(c.fd, protocol.MaxFrameBytes)
	for {
		body, err := reader.ReadSlice('\n')
		if err != nil {
			c.Close()
			close(c.done)
			c.inbox <- Closed{Connection: c, Cause: err}
			return
		}
		c.lastRead.Store(int64(time.Since(c.started)))
		value, decodeErr := protocol.DecodeFrame(body[:len(body)-1])
		c.inbox <- Frame{Value: value, Err: decodeErr}
	}
}

func (c *Conn) write() {
	for {
		select {
		case <-c.done:
			return
		case frame, ok := <-c.outbox:
			if !ok {
				c.Close()
				return
			}
			if err := writeFull(c.fd, frame); err != nil {
				c.Close()
				return
			}
		}
	}
}

func writeFull(writer io.Writer, body []byte) error {
	for len(body) != 0 {
		written, err := writer.Write(body)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		body = body[written:]
	}
	return nil
}

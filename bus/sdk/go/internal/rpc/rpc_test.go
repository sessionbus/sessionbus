// SPDX-License-Identifier: MIT

package rpc

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestCallRoundTripIncreasingIDsAndUnmatchedResponse(t *testing.T) {
	client, peer := net.Pipe()
	c := New(client, true, nil)
	defer c.Close()
	go func() {
		for id := int64(1); id <= 2; id++ {
			frame := readFrame(t, peer)
			check(t, frame.ID == id, "id = %d, want %d", frame.ID, id)
			body, _ := protocol.ResultBytes(id, "session.list", protocol.SessionListResult{Sessions: []protocol.SessionSummary{}})
			_, _ = peer.Write(body)
			if id == 2 {
				_, _ = peer.Write(body)
			}
		}
	}()
	var result protocol.SessionListResult
	must(t, c.Call(context.Background(), "session.list", protocol.SessionListRequest{}, &result))
	c.next = protocol.MaxRequestID
	check(t, c.Call(context.Background(), "session.list", protocol.SessionListRequest{}, &protocol.SessionListResult{}) != nil, "exhausted request id succeeded")
	c.next = 1
	must(t, c.Call(context.Background(), "session.list", protocol.SessionListRequest{}, &result))
	select {
	case <-c.Done():
	case <-time.After(time.Second):
		t.Fatal("unmatched response did not close connection")
	}
	check(t, errors.Is(c.Call(context.Background(), "session.list", protocol.SessionListRequest{}, &protocol.SessionListResult{}), ErrClosed), "call after unmatched response did not fail closed")
}

func TestResponseClaimsTargetBeforeCancelOrClose(t *testing.T) {
	for _, action := range []string{"cancel", "close"} {
		t.Run(action, func(t *testing.T) {
			local, peer := net.Pipe()
			c, ctx := New(local, true, nil), context.Background()
			var cancel context.CancelFunc
			if action == "cancel" {
				ctx, cancel = context.WithCancel(ctx)
			}
			target := &pausedResult{entered: make(chan struct{}), release: make(chan struct{})}
			done := make(chan error, 1)
			go func() { done <- c.Call(ctx, "session.list", protocol.SessionListRequest{}, target) }()
			request := readFrame(t, peer)
			body, _ := protocol.ResultBytes(request.ID, "session.list", protocol.SessionListResult{Sessions: []protocol.SessionSummary{}})
			go peer.Write(body)
			<-target.entered
			if cancel != nil {
				cancel()
			} else {
				_ = c.Close()
			}
			select {
			case <-done:
				t.Fatal("call returned while its target was decoding")
			default:
			}
			close(target.release)
			must(t, <-done)
			_ = peer.Close()
		})
	}
}

func TestCloseBeforeWriteEmitsNothing(t *testing.T) {
	local, peer := net.Pipe()
	paused := &pauseWriteConn{Conn: local, entered: make(chan struct{}), release: make(chan struct{})}
	c := New(paused, true, nil)
	returned := asyncCall(c, context.Background())
	<-paused.entered
	_ = c.Close()
	close(paused.release)
	check(t, <-returned != nil, "call succeeded")
	_ = peer.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
	body, _ := bufio.NewReader(peer).ReadBytes('\n')
	check(t, len(body) == 0, "wrote %q", body)
}

func TestCloseUnblocksWriter(t *testing.T) {
	local, peer := net.Pipe()
	entered := make(chan struct{})
	blocked := &signalWriteConn{Conn: local, entered: entered}
	c := New(blocked, true, nil)
	returned := asyncCall(c, context.Background())
	<-entered
	closed := make(chan struct{})
	go func() { _ = c.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close waited for blocked Write")
	}
	check(t, <-returned != nil, "blocked call succeeded")
	check(t, blocked.closes.Load() == 1, "closes = %d", blocked.closes.Load())
	_ = peer.Close()
}

func TestRequestIDsIncreaseAndInvalidUTF8StopsLaterFrame(t *testing.T) {
	local, peer := net.Pipe()
	requests := make(chan *Request, 1)
	c := New(local, true, func(_ context.Context, request *Request) { requests <- request })
	go func() {
		body := []byte("{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"unknown\",\"method\":\"session.open\",\"params\":{\"name\":\"lane@local\",\"groups\":[],\"open\":{}}}\n")
		_, _ = peer.Write(body)
	}()
	select {
	case request := <-requests:
		check(t, request.ID == 1, "request id = %d", request.ID)
	case <-time.After(time.Second):
		t.Fatal("reader blocked on late response")
	}
	bad := []byte("{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"session.open\",\"params\":{\"name\":\"\xff\",\"groups\":[],\"open\":{}}}\n")
	good, _ := protocol.RequestBytes(3, "session.open", protocol.OpenRequest{Name: "again@local", Groups: []string{}, Open: protocol.OpenOptions{}})
	go peer.Write(append(bad, good...))
	rejected := readFrame(t, peer)
	check(t, rejected.Error != nil && rejected.Error.Code == protocol.InvalidFrame, "invalid response = %#v", rejected)
	<-c.Done()
	select {
	case <-requests:
		t.Fatal("frame after invalid input reached handler")
	default:
	}
}

func TestInvalidRequestRepliesThenCloses(t *testing.T) {
	checkRejected := func(body string, code int) {
		local, peer := net.Pipe()
		New(local, false, nil)
		go peer.Write(append([]byte(body), '\n'))
		reader := bufio.NewReader(peer)
		if code != 0 {
			want, _ := protocol.ErrorBytes(1, code, nil)
			got, err := reader.ReadBytes('\n')
			check(t, err == nil && string(got) == string(want), "response = %q, %v; want %q", got, err, want)
		}
		tail, err := reader.ReadBytes('\n')
		check(t, len(tail) == 0 && errors.Is(err, io.EOF), "after response = %q, %v; want EOF", tail, err)
	}
	checkRejected(`{"jsonrpc":"2.0","id":1,"method":"session.list","params":{"extra":true}}`, protocol.InvalidFrame)
	checkRejected(`{"jsonrpc":"2.0","id":1,"method":"session.hello","params":{"protocol":1,"product":"p","session_id":"s","name":"n","groups":[],"info":{},"launch_token":"t","supported_open_fields":[],"extra_arguments":[]}}`, protocol.InvalidHello)
	checkRejected(`{`, 0)
	for _, method := range []string{`"method":null,`, `"method":"",`, `"method":1,`, ``} {
		checkRejected(fmt.Sprintf(`{"jsonrpc":"2.0","id":1,%s"params":{}}`, method), protocol.InvalidFrame)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	local, peer := net.Pipe()
	counted := &signalWriteConn{Conn: local, entered: make(chan struct{})}
	c := New(counted, true, nil)
	returned := asyncCall(c, context.Background())
	_ = readFrame(t, peer)
	var group sync.WaitGroup
	group.Add(2)
	for range 2 {
		go func() { defer group.Done(); _ = c.Close() }()
	}
	group.Wait()
	check(t, counted.closes.Load() == 1 && <-returned != nil, "closes = %d", counted.closes.Load())
}

func TestOversizeFrameClosesAtBound(t *testing.T) {
	local, peer := net.Pipe()
	c := New(local, true, nil)
	written := make(chan error, 1)
	go func() { _, err := io.WriteString(peer, strings.Repeat("x", protocol.MaxFrameBytes+64)); written <- err }()
	select {
	case <-c.Done():
	case <-time.After(time.Second):
		t.Fatal("oversize frame was scanned past the bound")
	}
	check(t, <-written != nil, "oversize writer was not interrupted")
}

func TestBadInboundClosesPendingCalls(t *testing.T) {
	tests := []struct {
		name, body string
	}{
		{"partial frame EOF", `{`},
		{"empty frame", "\n"},
		{"result and error", `{"jsonrpc":"2.0","id":1,"result":{},"error":{"code":-32002,"message":"not_connected"}}` + "\n"},
		{"trailing empty object", `{"jsonrpc":"2.0","id":1,"result":{"sessions":[]}}{}` + "\n"},
		{"trailing null", `{"jsonrpc":"2.0","id":1,"result":{"sessions":[]}}null` + "\n"},
		{"trailing second object", `{"jsonrpc":"2.0","id":1,"result":{"sessions":[]}}{"other":true}` + "\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			local, peer := net.Pipe()
			c := New(local, true, nil)
			first, second := asyncCall(c, context.Background()), asyncCall(c, context.Background())
			_, _ = readFrame(t, peer), readFrame(t, peer)
			_, _ = io.WriteString(peer, test.body)
			if !strings.HasSuffix(test.body, "\n") {
				_ = peer.Close()
			}
			check(t, <-first != nil && <-second != nil, "malformed input left a pending call")
			_ = peer.Close()
		})
	}
}

func TestCloseCancelsAdmittedHandler(t *testing.T) {
	local, peer := net.Pipe()
	entered, release, cancelled := make(chan struct{}), make(chan struct{}), make(chan bool, 1)
	var c *Conn
	c = New(local, true, func(ctx context.Context, request *Request) {
		close(entered)
		<-release
		cancelled <- errors.Is(ctx.Err(), context.Canceled) && errors.Is(c.Result(request, protocol.OpenResult{SessionID: "late"}), ErrClosed)
	})
	go func() {
		body, _ := protocol.RequestBytes(1, "session.open", protocol.OpenRequest{Name: "lane@local", Groups: []string{}, Open: protocol.OpenOptions{}})
		_, _ = peer.Write(body)
	}()
	<-entered
	_ = c.Close()
	close(release)
	check(t, <-cancelled, "handler was not cancelled or wrote after close")
}

func TestHandlersRunInFrameOrderAndWriteCompleteFrames(t *testing.T) {
	local, peer := net.Pipe()
	release, order := make(chan struct{}), make(chan int64, 2)
	var c *Conn
	c = New(local, true, func(_ context.Context, request *Request) {
		order <- request.ID
		if request.ID == 1 {
			<-release
		}
		go func() { _ = c.Result(request, struct{}{}) }()
	})
	first, _ := protocol.RequestBytes(1, "session.superseded", struct{}{})
	second, _ := protocol.RequestBytes(2, "session.superseded", struct{}{})
	go peer.Write(append(first, second...))
	check(t, <-order == 1, "first handler was reordered")
	select {
	case id := <-order:
		t.Fatalf("later handler ran while first was active: %d", id)
	default:
	}
	close(release)
	check(t, <-order == 2, "second handler was reordered")
	reader := bufio.NewReader(peer)
	for range 2 {
		body, err := reader.ReadBytes('\n')
		check(t, err == nil && json.Valid(body), "interleaved response = %q, %v", body, err)
	}
	_ = c.Close()
}

func TestReadStopsBeforeBufferedRequestAfterClose(t *testing.T) {
	for _, test := range []struct {
		name, first string
		seen        func() error
	}{
		{"invalid result", `{"jsonrpc":"2.0","id":1,"result":{"extra":true}}`, nil},
		{"observer failure", `{"jsonrpc":"2.0","id":1,"result":{}}`, func() error { return errors.New("owner rejected acknowledgement") }},
		{"invalid request", `{"jsonrpc":"2.0","id":1,"method":"session.superseded","params":{"extra":true}}`, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			local, peer := net.Pipe()
			defer peer.Close()
			// Drain the correlated error emitted for an invalid inbound request.
			drained := make(chan struct{})
			go func() { _, _ = io.Copy(io.Discard, peer); close(drained) }()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			buffered := test.first + "\n" + `{"jsonrpc":"2.0","id":2,"method":"session.superseded","params":{}}` + "\n"
			c := &Conn{
				fd: local, reader: bufio.NewReader(strings.NewReader(buffered)), client: true,
				ctx: ctx, cancel: cancel,
				pending: map[int64]*pending{1: {method: "session.hello", result: &struct{}{}, done: make(chan error, 1), seen: test.seen}},
				handler: func(context.Context, *Request) { t.Error("buffered request dispatched after closure") },
			}
			// Run the actual reader synchronously: assertions follow reader exit,
			// not merely context cancellation, and all frames are already buffered.
			c.read()
			check(t, ctx.Err() != nil, "reader did not close the connection")
			<-drained
		})
	}
}

type pauseWriteConn struct {
	net.Conn
	entered chan struct{}
	release chan struct{}
}

func (c *pauseWriteConn) Write(body []byte) (int, error) {
	close(c.entered)
	<-c.release
	return c.Conn.Write(body)
}

type signalWriteConn struct {
	net.Conn
	entered chan struct{}
	closes  atomic.Int32
}

func (c *signalWriteConn) Write(body []byte) (int, error) {
	close(c.entered)
	return c.Conn.Write(body)
}
func (c *signalWriteConn) Close() error { c.closes.Add(1); return c.Conn.Close() }

type pausedResult struct {
	protocol.SessionListResult
	entered, release chan struct{}
}

func (r *pausedResult) UnmarshalJSON(raw []byte) error {
	close(r.entered)
	<-r.release
	return json.Unmarshal(raw, &r.SessionListResult)
}

func readFrame(t *testing.T, connection net.Conn) protocol.Frame {
	body, err := bufio.NewReader(connection).ReadBytes('\n')
	must(t, err)
	frame, err := protocol.DecodeFrame(body[:len(body)-1])
	must(t, err)
	return frame
}

func asyncCall(c *Conn, ctx context.Context) <-chan error {
	returned := make(chan error, 1)
	go func() {
		returned <- c.Call(ctx, "session.list", protocol.SessionListRequest{}, &protocol.SessionListResult{})
	}()
	return returned
}

func must(t *testing.T, err error) { check(t, err == nil, "%v", err) }
func check(t *testing.T, ok bool, format string, args ...any) {
	if !ok {
		t.Fatalf(format, args...)
	}
}

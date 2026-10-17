// SPDX-License-Identifier: GPL-3.0-only

package conn

import (
	"bufio"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestReaderPostsFramesAndOneClose(t *testing.T) {
	local, peer := net.Pipe()
	inbox := make(chan any, OutboxSize)
	var group sync.WaitGroup
	c := Start(local, inbox, &group)
	body, _ := protocol.RequestBytes(1, "session.list", protocol.SessionListRequest{})
	go func() { _, _ = peer.Write(body) }()
	event := (<-inbox).(Frame)
	if event.Err != nil || event.Value.ID != 1 || event.Value.Method != "session.list" {
		t.Fatalf("frame = %#v", event)
	}
	_ = peer.Close()
	closed := (<-inbox).(Closed)
	if closed.Connection != c {
		t.Fatalf("closed connection = %p, want %p", closed.Connection, c)
	}
	select {
	case extra := <-inbox:
		t.Fatalf("second terminal event = %#v", extra)
	case <-time.After(20 * time.Millisecond):
	}
	group.Wait()
}

func TestReaderPostBlocksUntilOwnerReceives(t *testing.T) {
	local, peer := net.Pipe()
	inbox := make(chan any, OutboxSize)
	for range OutboxSize {
		inbox <- "occupied"
	}
	var group sync.WaitGroup
	c := Start(local, inbox, &group)
	body, _ := protocol.RequestBytes(1, "session.list", protocol.SessionListRequest{})
	written := make(chan struct{})
	go func() { _, _ = peer.Write(body); close(written) }()
	<-written
	if got := len(inbox); got != OutboxSize {
		t.Fatalf("full inbox length = %d", got)
	}
	select {
	case event := <-inbox:
		if event != "occupied" {
			t.Fatalf("reader posted through a full inbox: %#v", event)
		}
	case <-time.After(20 * time.Millisecond):
		t.Fatal("owner could not receive from full inbox")
	}
	for range OutboxSize - 1 {
		if event := <-inbox; event != "occupied" {
			t.Fatalf("unexpected event while draining: %#v", event)
		}
	}
	event := (<-inbox).(Frame)
	if event.Value.ID != 1 {
		t.Fatalf("frame id = %d", event.Value.ID)
	}
	c.Close()
	if closed := (<-inbox).(Closed); closed.Connection != c {
		t.Fatalf("closed = %#v", closed)
	}
	group.Wait()
	_ = peer.Close()
}

func TestWriterPreservesFramesAndFinalCloses(t *testing.T) {
	local, peer := net.Pipe()
	inbox := make(chan any, OutboxSize)
	var group sync.WaitGroup
	c := Start(local, inbox, &group)
	first, _ := protocol.ResultBytes(1, "session.hello", struct{}{})
	last, _ := protocol.RequestBytes(1, "session.superseded", struct{}{})
	if !c.Send(first) || !c.Finish(last, time.Second) {
		t.Fatal("writer refused frames")
	}
	reader := bufio.NewReader(peer)
	for _, want := range [][]byte{first, last} {
		got, err := reader.ReadBytes('\n')
		if err != nil || string(got) != string(want) {
			t.Fatalf("frame = %q, %v; want %q", got, err, want)
		}
	}
	if tail, err := reader.ReadBytes('\n'); len(tail) != 0 || err != io.EOF {
		t.Fatalf("tail = %q, %v", tail, err)
	}
	if closed := (<-inbox).(Closed); closed.Connection != c {
		t.Fatalf("closed = %#v", closed)
	}
	group.Wait()
	_ = peer.Close()
}

func TestFinishBoundsNonReadingPeer(t *testing.T) {
	local, peer := net.Pipe()
	inbox := make(chan any, OutboxSize)
	var group sync.WaitGroup
	c := Start(local, inbox, &group)
	last, _ := protocol.RequestBytes(1, "session.superseded", struct{}{})
	if !c.Finish(last, 20*time.Millisecond) {
		t.Fatal("final frame was refused")
	}
	select {
	case <-c.Done():
	case <-time.After(time.Second):
		t.Fatal("final write did not reach its bound")
	}
	if closed := (<-inbox).(Closed); closed.Connection != c {
		t.Fatalf("closed = %#v", closed)
	}
	group.Wait()
	_ = peer.Close()
}

func TestSlowWriterClosesAtOutboxBound(t *testing.T) {
	local, peer := net.Pipe()
	inbox := make(chan any, OutboxSize)
	var group sync.WaitGroup
	c := Start(local, inbox, &group)
	frame, _ := protocol.ResultBytes(1, "session.hello", struct{}{})
	accepted := 0
	for c.Send(frame) {
		accepted++
	}
	if accepted < OutboxSize || accepted > OutboxSize+1 {
		t.Fatalf("accepted = %d", accepted)
	}
	select {
	case <-c.Done():
	case <-time.After(time.Second):
		t.Fatal("slow writer did not close")
	}
	if closed := (<-inbox).(Closed); closed.Connection != c {
		t.Fatalf("closed = %#v", closed)
	}
	group.Wait()
	_ = peer.Close()
}

func TestInvalidFrameIsPostedBeforeClose(t *testing.T) {
	local, peer := net.Pipe()
	inbox := make(chan any, OutboxSize)
	var group sync.WaitGroup
	c := Start(local, inbox, &group)
	go func() { _, _ = io.WriteString(peer, "{\n") }()
	event := (<-inbox).(Frame)
	if event.Err == nil {
		t.Fatal("invalid frame was accepted")
	}
	c.Close()
	if closed := (<-inbox).(Closed); closed.Connection != c {
		t.Fatalf("closed = %#v", closed)
	}
	group.Wait()
	_ = peer.Close()
}

func TestOversizeFrameHardCloses(t *testing.T) {
	local, peer := net.Pipe()
	inbox := make(chan any, OutboxSize)
	var group sync.WaitGroup
	c := Start(local, inbox, &group)
	written := make(chan error, 1)
	go func() { _, err := peer.Write(make([]byte, protocol.MaxFrameBytes+1)); written <- err }()
	closed := (<-inbox).(Closed)
	if closed.Connection != c || <-written == nil {
		t.Fatalf("oversize close = %#v", closed)
	}
	group.Wait()
	_ = peer.Close()
}

func TestLastReadAdvancesBeforeOwnerReceives(t *testing.T) {
	local, peer := net.Pipe()
	inbox := make(chan any)
	var group sync.WaitGroup
	c := Start(local, inbox, &group)
	t.Cleanup(func() {
		_ = local.Close()
		_ = peer.Close()
		joined := make(chan struct{})
		go func() { group.Wait(); close(joined) }()
		bound := time.After(time.Second)
		for {
			select {
			case <-inbox:
			case <-joined:
				return
			case <-bound:
				t.Error("connection goroutines did not stop")
				return
			}
		}
	})
	started := c.LastRead()
	time.Sleep(10 * time.Millisecond)
	body, _ := protocol.RequestBytes(1, "session.list", protocol.SessionListRequest{})
	written := make(chan struct{})
	go func() { _, _ = peer.Write(body); close(written) }()
	select {
	case <-written:
	case <-time.After(time.Second):
		t.Fatal("reader did not take the frame")
	}
	// Nobody has received from the unbuffered inbox yet.
	deadline := time.Now().Add(time.Second)
	for !c.LastRead().After(started) {
		if time.Now().After(deadline) {
			t.Fatal("queued frame did not advance LastRead")
		}
		time.Sleep(time.Millisecond)
	}
	if got := c.LastRead().Sub(started); got < 10*time.Millisecond {
		t.Fatalf("LastRead advanced by %s, want the frame arrival time", got)
	}
	select {
	case event := <-inbox:
		if frame, ok := event.(Frame); !ok || frame.Value.ID != 1 {
			t.Fatalf("event = %#v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("frame was not posted")
	}
	c.Close()
	select {
	case event := <-inbox:
		if closed, ok := event.(Closed); !ok || closed.Connection != c {
			t.Fatalf("closed = %#v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("close was not posted")
	}
}

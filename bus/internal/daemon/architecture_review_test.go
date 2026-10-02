// SPDX-License-Identifier: GPL-3.0-only

package daemon

import (
	"bufio"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/conn"
	"github.com/antst/sessionbus/bus/internal/structuredprocess"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func reviewSession(t *testing.T) (*Daemon, *session, net.Conn) {
	t.Helper()
	d := &Daemon{}
	d.directory = newDirectory(d, nil)
	s := newSession(d)
	local, peer := net.Pipe()
	s.wire = conn.Start(local, s.inbox, &d.group)
	t.Cleanup(func() {
		s.wire.Close()
		_ = peer.Close()
		for event := range s.inbox {
			if _, ok := event.(conn.Closed); ok {
				break
			}
		}
		s.wire.OwnerClosed()
		d.group.Wait()
	})
	return d, s, peer
}

func TestReviewAliasesDeliverOnce(t *testing.T) {
	_, socket := startDaemon(t)
	sender := connectPeer(t, socket, "sender", "sender", "shared")
	one := connectPeer(t, socket, "one", "one", "shared")
	var result protocol.MessageSendResult
	must(t, sender.call("message.send", protocol.MessageSendRequest{Targets: []string{"one", "one@local"}, Message: "once"}, &result))
	if got := len(one.deliveries); got != 1 || len(result.Deliveries) != 1 {
		t.Fatalf("deliveries = %d, receipts = %d", got, len(result.Deliveries))
	}
}

func TestReviewRejectedRunLeavesIdle(t *testing.T) {
	d, s, _ := reviewSession(t)
	item := &entry{row: row{SessionID: "lane@local"}, attachment: s, done: make(chan struct{})}
	d.directory.entries[item.row.SessionID], s.identity = item, item
	for id := int64(1); id <= maxPendingCalls; id++ {
		s.pending[id] = routedRequest{}
	}
	reply := make(chan answer, 1)
	d.directory.mu.Lock()
	code := d.directory.routeLocked(item, "turn.run", routedRequest{method: "turn.run", reply: reply})
	d.directory.mu.Unlock()
	if code != 0 {
		t.Fatal(code)
	}
	s.handleEvent(<-s.inbox)
	if got := <-reply; got.code != protocol.Busy || item.running {
		t.Fatalf("answer = %#v, running = %v", got, item.running)
	}
}

func TestReviewAbortKeepsClaimAndIDFenceThroughCleanup(t *testing.T) {
	d := &Daemon{}
	d.directory = newDirectory(d, nil)
	start := newLaunch("worker", false, true)
	start.parent = &ownership{id: "parent@local", token: "fixture"}
	defer start.timer.Stop()
	item, code := d.directory.reserveFresh(row{Name: "parent/lane@local"}, start)
	if code != 0 {
		t.Fatal(code)
	}
	defer d.group.Done()
	s := newSession(d)
	s.launch, s.identity = start, item
	if claimed, ok := d.directory.claimWorker(s, start.token, "worker"); !ok || claimed != start {
		t.Fatal("worker did not claim its launch token")
	}
	if !d.directory.reserveID(item, "pending@local") {
		t.Fatal("worker did not reserve its native ID")
	}
	s.abortLaunch(answer{code: protocol.SpawnFailed})
	if d.directory.entries[item.row.SessionID] != item || !item.claimed || start.owner != s {
		t.Fatal("abort released claim/ID fence before final cleanup")
	}
	if d.directory.connected[item.row.SessionID] != nil {
		t.Fatal("failed Open published an active connection")
	}
	d.directory.releaseLaunch(start)
	if item.claimed || d.directory.entries[item.row.SessionID] != nil || d.directory.tokens[start.token] != nil {
		t.Fatal("final cleanup retained the failed launch")
	}
}

func TestReviewRehelloSettlesOldInboundDelivery(t *testing.T) {
	d, s, _ := reviewSession(t)
	hello := &protocol.PeerHello{Protocol: 1, Product: "peer", SessionID: "old", Name: "old", Groups: []string{"shared"}}
	item, _, _, ok := d.directory.installPeer(s, hello, "local")
	if !ok {
		t.Fatal("install")
	}
	s.identity = item
	reply := make(chan answer, 1)
	s.pending[1] = routedRequest{destination: item, method: "message.deliver", reply: reply}
	hello.SessionID, hello.Name = "new", "new"
	s.peerHello(protocol.Frame{ID: 2, Method: "session.hello", Request: true}, hello)
	if got := <-reply; got.code != protocol.NotConnected {
		t.Fatalf("answer = %#v", got)
	}
}

func TestReviewRunTerminalPrecedesCloseReply(t *testing.T) {
	_, s, peer := reviewSession(t)
	target := &entry{}
	s.requests[1] = &requestState{frame: protocol.Frame{ID: 1, Method: "turn.run"}, target: target}
	s.requests[2] = &requestState{frame: protocol.Frame{ID: 2, Method: "session.close"}, target: target}
	s.owned = 2
	s.consumeReply(replyEvent{requestID: 2, answer: answer{value: struct{}{}}})
	_ = peer.SetReadDeadline(time.Now().Add(10 * time.Millisecond))
	if _, err := peer.Read(make([]byte, 1)); err == nil {
		t.Fatal("close reply was written before the run terminal")
	}
	_ = peer.SetReadDeadline(time.Time{})
	s.consumeReply(replyEvent{requestID: 1, answer: answer{value: &protocol.RunStatus{SessionID: "lane@local", RunID: "g/1", State: "done", Result: &protocol.TurnResult{Outcome: "completed", Result: "done"}}}})
	reader := bufio.NewReader(peer)
	readID := func() int64 {
		body, err := reader.ReadBytes('\n')
		must(t, err)
		frame, err := protocol.DecodeFrame(body[:len(body)-1])
		must(t, err)
		return frame.ID
	}
	if first, second := readID(), readID(); first != 1 || second != 2 {
		t.Fatalf("response order = %d, %d", first, second)
	}
}

func TestReviewInterruptedRowWriteDoesNotBlockRestart(t *testing.T) {
	path := t.TempDir()
	table, _, err := openTable(path)
	must(t, err)
	must(t, table.write(row{SessionID: "lane@local", Product: "worker", Name: "parent/lane@local", Groups: []string{"one", "two"}, CreatedAt: time.Now()}))
	must(t, os.WriteFile(filepath.Join(path, ".row-interrupted"), []byte("{"), 0o600))
	_, rows, err := openTable(path)
	must(t, err)
	if len(rows) != 1 {
		t.Fatalf("rows = %d", len(rows))
	}
}

func TestReviewBusyRunDoesNotReleaseHeldClose(t *testing.T) {
	_, s, _ := reviewSession(t)
	target := &entry{}
	s.requests[1] = &requestState{frame: protocol.Frame{ID: 1, Method: "turn.run"}, target: target}
	s.requests[2] = &requestState{frame: protocol.Frame{ID: 2, Method: "turn.run"}, target: target}
	s.requests[3] = &requestState{frame: protocol.Frame{ID: 3, Method: "session.close"}, target: target}
	s.owned = 3
	s.consumeReply(replyEvent{requestID: 3, answer: answer{value: struct{}{}}})
	s.consumeReply(replyEvent{requestID: 2, answer: answer{code: protocol.Busy}})
	if s.requests[3] == nil {
		t.Fatal("busy response to second run released close before first run terminal")
	}
}

func TestReviewAllCloseRepliesFinishAfterRun(t *testing.T) {
	_, s, _ := reviewSession(t)
	target := &entry{}
	s.requests[1] = &requestState{frame: protocol.Frame{ID: 1, Method: "turn.run"}, target: target}
	s.requests[2] = &requestState{frame: protocol.Frame{ID: 2, Method: "session.close"}, target: target}
	s.requests[3] = &requestState{frame: protocol.Frame{ID: 3, Method: "session.close"}, target: target}
	s.owned = 3
	s.consumeReply(replyEvent{requestID: 2, answer: answer{value: struct{}{}}})
	s.consumeReply(replyEvent{requestID: 3, answer: answer{code: protocol.Busy}})
	s.consumeReply(replyEvent{requestID: 1, answer: answer{value: &protocol.RunStatus{SessionID: "lane@local", RunID: "g/1", State: "done", Result: &protocol.TurnResult{Outcome: "completed", Result: "done"}}}})
	if len(s.requests) != 0 {
		t.Fatalf("%d completed close reply remains held with no pending run or helper", len(s.requests))
	}
}

func TestReviewLateChildKeepsHardStopIntent(t *testing.T) {
	_, s, _ := reviewSession(t)
	dir := t.TempDir()
	ready := filepath.Join(dir, "ready")
	path := filepath.Join(dir, "worker")
	must(t, os.WriteFile(path, []byte("#!/bin/sh\ntrap '' TERM\nprintf ready > \"$REVIEW_READY\"\nwhile :; do sleep 1; done\n"), 0o700))
	child, err := structuredprocess.Start(path, append(os.Environ(), "REVIEW_READY="+ready))
	must(t, err)
	t.Cleanup(child.Stop)
	waitFile(t, ready)
	s.launch = newLaunch("worker", false, true)
	t.Cleanup(func() { s.launch.timer.Stop() })
	s.abortLaunch(answer{code: protocol.Timeout})
	s.hardStop()
	s.handleProcess(processEvent{launch: s.launch, child: child})
	if s.stopSignal != syscall.SIGKILL {
		t.Fatalf("late child stop intent = %v", s.stopSignal)
	}
	<-child.Done()
}

func TestReviewWorkerPendingAndInboxBoundsAreSeparate(t *testing.T) {
	d, s, _ := reviewSession(t)
	item := &entry{row: row{SessionID: "lane@local"}, attachment: s, done: make(chan struct{})}
	d.directory.entries[item.row.SessionID], s.identity = item, item
	for id := int64(1); id <= maxPendingCalls; id++ {
		s.pending[id] = routedRequest{}
	}
	reply := make(chan answer, 1)
	s.handleEvent(routedRequest{destination: item, method: "turn.run", reply: reply})
	if got := <-reply; got.code != protocol.Busy {
		t.Fatalf("257th unanswered call = %#v", got)
	}
	for range conn.OutboxSize {
		s.inbox <- struct{}{}
	}
	d.directory.mu.Lock()
	code := d.directory.routeLocked(item, "message.deliver", routedRequest{reply: reply})
	d.directory.mu.Unlock()
	if code != protocol.Busy {
		t.Fatalf("full mixed-event inbox = %d", code)
	}
}

func TestReviewClaimedLaneRejectsNewControl(t *testing.T) {
	d, s, _ := reviewSession(t)
	item := &entry{row: row{SessionID: "lane@local", Groups: []string{"shared"}}, claimed: true, done: make(chan struct{})}
	d.directory.entries[item.row.SessionID], s.identity = item, item
	d.directory.mu.Lock()
	code := d.directory.routeLocked(item, "session.close", routedRequest{})
	d.directory.mu.Unlock()
	if code != protocol.Busy {
		t.Fatalf("close during provisional resume = %d", code)
	}
	item.attachment = s
	for _, method := range []string{"turn.run", "turn.interrupt", "session.close"} {
		d.directory.mu.Lock()
		code = d.directory.routeLocked(item, method, routedRequest{})
		d.directory.mu.Unlock()
		if code != protocol.Busy {
			t.Fatalf("%s after close claim = %d", method, code)
		}
	}
	start := newLaunch("", false, false)
	start.parent = &ownership{id: "parent@local", token: "fixture"}
	t.Cleanup(func() { start.timer.Stop() })
	if _, code = d.directory.reserveResume(item.row.SessionID, item.row.Groups, start); code != protocol.Busy {
		t.Fatalf("resume of claimed lane = %d", code)
	}
	d.directory.mu.Lock()
	code = d.directory.routeLocked(item, "message.deliver", routedRequest{})
	d.directory.mu.Unlock()
	if code != 0 {
		t.Fatalf("delivery to claimed attached lane = %d", code)
	}
}

func TestReviewStaleResponseSettlesNoReceipt(t *testing.T) {
	d, s, _ := reviewSession(t)
	old := &entry{row: row{SessionID: "old@local"}, peer: true, attachment: s, done: make(chan struct{})}
	d.directory.entries[old.row.SessionID], s.identity = old, old
	reply := make(chan answer, 1)
	s.pending[1] = routedRequest{destination: old, method: "message.deliver", reply: reply}
	replacement := &entry{row: row{SessionID: old.row.SessionID}, peer: true, done: make(chan struct{})}
	d.directory.mu.Lock()
	d.directory.entries[old.row.SessionID] = replacement
	d.directory.mu.Unlock()
	result, err := protocol.ResultBytes(1, "message.deliver", protocol.DeliveryReceipt{Disposition: "injected"})
	must(t, err)
	frame, err := protocol.DecodeFrame(result[:len(result)-1])
	must(t, err)
	s.receiveResponse(frame)
	got := <-reply
	if got.code != protocol.NotConnected || reason(got.code, "") != "no_receipt" {
		t.Fatalf("stale receipt = %#v", got)
	}
}

func TestReviewUnmatchedWorkerResponseClosesConnection(t *testing.T) {
	_, s, _ := reviewSession(t)
	result, err := protocol.ResultBytes(1, "message.deliver", protocol.DeliveryReceipt{Disposition: "injected"})
	must(t, err)
	frame, err := protocol.DecodeFrame(result[:len(result)-1])
	must(t, err)
	s.receiveResponse(frame)
	select {
	case <-s.wire.Done():
	case <-time.After(time.Second):
		t.Fatal("unmatched worker response did not close connection")
	}
}

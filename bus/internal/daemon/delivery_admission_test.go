// SPDX-License-Identifier: GPL-3.0-only

package daemon

import (
	"bufio"
	"net"
	"os"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestSendToClosedLaneIsUnknown(t *testing.T) {
	directory := t.TempDir()
	installFixture(t, directory, "fixture-worker")
	t.Setenv("PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))
	_, socket := startDaemon(t)
	parent := connectPeer(t, socket, "parent", "parent", "team")
	var spawned protocol.LaneSpawnResult
	must(t, parent.call("lane.spawn", protocol.LaneSpawnRequest{Name: "closed", Product: "fixture-worker", Open: &protocol.OpenOptions{}}, &spawned))
	must(t, parent.call("session.close", protocol.SessionCloseRequest{SessionID: spawned.SessionID}, &struct{}{}))
	var sent protocol.MessageSendResult
	must(t, parent.call("message.send", protocol.MessageSendRequest{Target: spawned.SessionID, Message: "must not reach a closed worker"}, &sent))
	if len(sent.Deliveries) != 1 {
		t.Fatalf("deliveries = %#v", sent)
	}
	got := sent.Deliveries[0]
	if got.Disposition != "rejected" || got.Reason != "unknown_session" || got.DeliveryID != "" {
		t.Fatalf("closed-lane receipt = %#v", got)
	}
	var listed protocol.SessionListResult
	must(t, parent.call("session.list", protocol.SessionListRequest{SessionID: spawned.SessionID}, &listed))
	if len(listed.Sessions) != 1 || listed.Sessions[0].Connected || listed.Sessions[0].Running {
		t.Fatalf("send changed offline lane = %#v", listed)
	}
}

func TestSendLostAfterDispatchRemainsUncertain(t *testing.T) {
	_, socket := startDaemon(t)
	sender := connectPeer(t, socket, "sender", "sender", "team")
	target, err := net.Dial("unix", socket)
	must(t, err)
	defer target.Close()
	must(t, target.SetDeadline(time.Now().Add(5*time.Second)))
	reader := bufio.NewReader(target)
	must(t, rawCall(target, reader, 1, "session.hello", protocol.PeerHello{Protocol: 1, Product: "fixture-peer", SessionID: "target", Name: "target", Groups: []string{"team"}, Info: map[string]any{}}, &struct{}{}))
	var sent protocol.MessageSendResult
	done := make(chan error, 1)
	go func() {
		done <- sender.call("message.send", protocol.MessageSendRequest{Target: "target", Message: "already dispatched"}, &sent)
	}()
	frame, err := readRawFrame(reader)
	must(t, err)
	if !frame.Request || frame.Method != "message.deliver" {
		t.Fatalf("target frame = %#v", frame)
	}
	value, err := protocol.DecodeParams(frame.Method, frame.Params)
	must(t, err)
	if value.(*protocol.DeliveryRequest).Body != "already dispatched" {
		t.Fatalf("delivery = %#v", value)
	}
	// Receipt is lost only after the target received the exact native-bound frame.
	must(t, target.Close())
	select {
	case err = <-done:
		must(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("lost delivery did not settle")
	}
	if len(sent.Deliveries) != 1 {
		t.Fatalf("deliveries = %#v", sent)
	}
	got := sent.Deliveries[0]
	if got.Disposition != "rejected" || got.Reason != "no_receipt" || got.DeliveryID == "" {
		t.Fatalf("admitted loss was claimed unsubmitted = %#v", got)
	}
}

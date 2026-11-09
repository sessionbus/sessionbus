// SPDX-License-Identifier: GPL-3.0-only

package federation

import (
	"bufio"
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func heldManagementLink(t *testing.T) (chan any, <-chan protocol.Frame, net.Conn) {
	t.Helper()
	left, right := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	inbox := make(chan any, 256)
	frames := make(chan protocol.Frame, 264)
	readerDone, ownerDone := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(readerDone)
		scan := bufio.NewScanner(right)
		for scan.Scan() {
			frame, err := protocol.DecodeFrame(scan.Bytes())
			if err == nil {
				frames <- frame
			}
		}
	}()
	go func() {
		defer close(ownerDone)
		_ = ServeDaemon(ctx, "alpha", left, inbox, nil, io.Discard)
	}()
	t.Cleanup(func() { cancel(); _ = right.Close(); <-ownerDone; <-readerDone })
	return inbox, frames, right
}

func managementFrame(t *testing.T, frames <-chan protocol.Frame) protocol.Frame {
	t.Helper()
	select {
	case frame := <-frames:
		return frame
	case <-time.After(time.Second):
		t.Fatal("management request not dispatched")
		return protocol.Frame{}
	}
}

// The identical fixture runs on old main. All writer jobs have actually been
// read at the far end before testing the owner pending cap.
func TestManagementPendingCapRefusesWithoutClosingAndLateReplyFreesSlot(t *testing.T) {
	inbox, frames, remote := heldManagementLink(t)
	var first chan Reply
	for i := 0; i < protocol.MaxOperations; i++ {
		reply := make(chan Reply, 1)
		if first == nil {
			first = reply
		}
		inbox <- HostsCall{Reply: reply}
		if frame := managementFrame(t, frames); frame.ID != int64(i+1) {
			t.Fatal("writer order changed", frame)
		}
	}
	for _, kind := range []string{"hosts", "roster"} {
		reply := make(chan Reply, 1)
		if kind == "hosts" {
			inbox <- HostsCall{Reply: reply}
		} else {
			inbox <- RosterCall{Reply: reply}
		}
		select {
		case r := <-reply:
			if r.Error == nil || r.Error.Code != protocol.ForwardLost {
				t.Fatal("ordinary management cap not refused", kind, r)
			}
		case <-time.After(time.Second):
			t.Fatal("dispatched pending cap bypassed", kind)
		}
	}
	body, _ := ResponseBytes(1, Reply{Result: []byte(`{"hosts":[]}`)})
	if _, err := remote.Write(body); err != nil {
		t.Fatal("ordinary refusal closed link", err)
	}
	if _, err := DecodeHosts(<-first); err != nil {
		t.Fatal("actual response lost", err)
	}
	reply := make(chan Reply, 1)
	inbox <- HostsCall{Reply: reply}
	frame := managementFrame(t, frames)
	if frame.ID != protocol.MaxOperations+1 {
		t.Fatal("refused requests allocated IDs", frame.ID)
	}
	body, _ = ResponseBytes(frame.ID, Reply{Result: []byte(`{"hosts":[]}`)})
	if _, err := remote.Write(body); err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeHosts(<-reply); err != nil {
		t.Fatal("freed slot did not work", err)
	}
}

func TestCleanupAtFullManagementCapKeepsExistingLinkLossBoundary(t *testing.T) {
	inbox, frames, remote := heldManagementLink(t)
	for range protocol.MaxOperations {
		inbox <- HostsCall{Reply: make(chan Reply, 1)}
		_ = managementFrame(t, frames)
	}
	reply := make(chan Reply, 1)
	inbox <- OwnerEndCall{Value: OwnerEnd{Host: "beta", SessionID: "parent@alpha", Lifetime: "life"}, Reply: reply}
	select {
	case value := <-reply:
		if value.Error == nil || value.Error.Code != protocol.ForwardLost {
			t.Fatal("cleanup cap invented success", value)
		}
	case <-time.After(time.Second):
		t.Fatal("cleanup overflow did not settle")
	}
	if err := remote.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	var body [1]byte
	if _, err := remote.Read(body[:]); err != io.EOF {
		t.Fatal("cleanup overflow lost existing close-through-link-loss semantics", err)
	}
}

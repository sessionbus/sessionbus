// SPDX-License-Identifier: GPL-3.0-only
package federation

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/conn"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

// Real reader credits are used here: a duplicate Release cannot create an
// extra fifth slot. No replacement callback implements the credit under test.
func bulkCreditFixture(t *testing.T) (*hostLink, func() conn.Frame, func() conn.Closed) {
	t.Helper()
	left, right := net.Pipe()
	inbox := make(chan any, 16)
	var group sync.WaitGroup
	wire := conn.Start(left, inbox, &group)
	wire.SetBulkGate(func(protocol.Frame) bool { return true })
	link := &hostLink{wire: wire, raw: right, inbox: inbox, originReplies: make(chan originReply, conn.OutboxSize+8)}
	var id int64
	send := func() conn.Frame {
		id++
		body, err := protocol.RequestBytes(id, "transfer.chunk", protocol.TransferChunk{ID: "id", Data: "YQ=="})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := right.Write(body); err != nil {
			t.Fatal(err)
		}
		select {
		case frame := <-wire.Bulk():
			return frame
		case event := <-inbox:
			return event.(conn.Frame)
		case <-time.After(time.Second):
			t.Fatal("bulk frame missing")
			return conn.Frame{}
		}
	}
	closed := func() conn.Closed {
		_ = right.Close()
		select {
		case event := <-inbox:
			value, ok := event.(conn.Closed)
			if !ok || value.Connection != wire {
				t.Fatal("wrong closure", event)
			}
			select {
			case <-wire.Done():
			default:
				t.Fatal("Closed preceded Done")
			}
			return value
		case <-time.After(time.Second):
			t.Fatal("reader did not finish")
			return conn.Closed{}
		}
	}
	t.Cleanup(func() { wire.Close(); _ = right.Close(); group.Wait() })
	return link, send, closed
}

func TestOriginRealBulkReleaseOnlyOnConsumptionAndEffectiveOnce(t *testing.T) {
	link, send, _ := bulkCreditFixture(t)
	var frames []conn.Frame
	for range 4 {
		frame := send()
		if frame.Err != nil || frame.Release == nil {
			t.Fatal(frame.Err)
		}
		frames = append(frames, frame)
	}
	first := frames[0]
	link.originAdmitted = 1
	if !link.postOriginReply(originReply{release: first.Release}) {
		t.Fatal("enqueue")
	}
	blocked := send()
	if blocked.Err != conn.ErrBulkCapacity || len(blocked.Value.Params) != 0 || blocked.Release != nil {
		t.Fatal("enqueue freed a real chunk credit", blocked)
	}
	link.consumeOriginReply(<-link.originReplies)
	if link.originAdmitted != 0 {
		t.Fatal("consumption count")
	}
	first.Release() // The real conn callback must not free a second credit.
	replacement := send()
	if replacement.Err != nil || replacement.Release == nil {
		t.Fatal("consumption failed to free one credit")
	}
	blocked = send()
	if blocked.Err != conn.ErrBulkCapacity || len(blocked.Value.Params) != 0 {
		t.Fatal("duplicate release created extra credit")
	}
	for _, frame := range frames[1:] {
		frame.Release()
	}
	replacement.Release()
}

func TestOriginShutdownOrderingAndSuccessorIsolation(t *testing.T) {
	for _, order := range []string{"enqueue-before-drain", "send-after-empty-drain", "done-before-submit"} {
		t.Run(order, func(t *testing.T) {
			link, send, closed := bulkCreditFixture(t)
			frame := send()
			event := originReply{release: frame.Release}
			link.originAdmitted = 1
			if order == "enqueue-before-drain" && !link.postOriginReply(event) {
				t.Fatal("live enqueue")
			}
			_ = closed()
			link.retireOriginReplies()
			if order == "send-after-empty-drain" {
				// Deterministically take the send arm after Done and drain. The
				// production post-enqueue guard is unconditional on that arm.
				link.originReplies <- event
				if link.originReplyLive(event) {
					t.Fatal("late send passed post-check")
				}
			} else if order == "done-before-submit" && link.postOriginReply(event) {
				t.Fatal("dead submit accepted")
			}
			// Both overlap and repeated retirement exercise the existing once
			// callback; no producer is permitted to change owner accounting.
			link.retireOriginReplies()
			frame.Release()
			if link.originAdmitted != 0 {
				t.Fatal("dead generation count")
			}
			successor, _, _, _, _ := originReleaseFixture(t)
			if successor.originAdmitted != 0 || len(successor.originReplies) != 0 {
				t.Fatal("successor inherited accounting")
			}
		})
	}
}

func TestOriginTerminalDrainReleasesActualIngressBulk(t *testing.T) {
	left, right := net.Pipe()
	inbox := make(chan any, 8)
	var group sync.WaitGroup
	wire := conn.Start(left, inbox, &group)
	wire.SetBulkGate(func(protocol.Frame) bool { return true })
	t.Cleanup(func() { wire.Close(); _ = right.Close(); group.Wait() })
	for id := int64(1); id <= 4; id++ {
		body, err := protocol.RequestBytes(id, "transfer.chunk", protocol.TransferChunk{ID: "id", Data: "YQ=="})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := right.Write(body); err != nil {
			t.Fatal(err)
		}
	}
	_ = right.Close()
	select {
	case event := <-inbox:
		if closed, ok := event.(conn.Closed); !ok || closed.Connection != wire {
			t.Fatal(event)
		}
	case <-time.After(time.Second):
		t.Fatal("reader closure missing")
	}
	select {
	case <-wire.Done():
	default:
		t.Fatal("Closed before Done")
	}
	if len(wire.Bulk()) != 4 {
		t.Fatal("actual queued ingress missing")
	}
	link := &hostLink{wire: wire, originReplies: make(chan originReply, conn.OutboxSize+8), originAdmitted: 4}
	link.retireOriginReplies()
	if len(wire.Bulk()) != 0 || link.originAdmitted != 0 {
		t.Fatal("terminal ingress not drained")
	}
}

func TestOwnerEndErrorResponseClosesOnlyThatDaemonLink(t *testing.T) {
	for _, method := range []string{ownerEndMethod, hostsMethod} {
		t.Run(method, func(t *testing.T) {
			inbox, frames, peer, done := heldManagementLink(t)
			reply := make(chan Reply, 1)
			if method == ownerEndMethod {
				inbox <- OwnerEndCall{Value: OwnerEnd{Host: "beta", SessionID: "parent@alpha", Lifetime: "life"}, Reply: reply}
			} else {
				inbox <- HostsCall{Reply: reply}
			}
			frame := managementFrame(t, frames)
			body := ErrorBytes(frame.ID, protocol.ForwardLost, nil)
			if _, err := peer.Write(body); err != nil {
				t.Fatal(err)
			}
			if value := <-reply; value.Error == nil || value.Error.Code != protocol.ForwardLost {
				t.Fatal(value)
			}
			if method == ownerEndMethod {
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Fatal("owner end was silently lost on a live link")
				}
			} else {
				inbox <- HostsCall{Reply: make(chan Reply, 1)}
				if next := managementFrame(t, frames); next.ID != frame.ID+1 {
					t.Fatal("non-owner refusal disconnected", next)
				}
			}
		})
	}
}

func TestCapacityRefusalIsAValidHubProbeAnswer(t *testing.T) {
	now := time.Now()
	live := linkLiveness{lastReply: now.Add(-silenceAfter), waiting: true}
	live.silent.Store(true)
	pending := map[int64]pendingCall{1: {method: hostsMethod, probe: &live}}
	body := ErrorBytes(1, protocol.ForwardLost, nil)
	frame, err := protocol.DecodeFrame(body)
	if err != nil {
		t.Fatal(err)
	}
	var helpers sync.WaitGroup
	last := int64(0)
	if err := daemonFrame("alpha", false, false, false, false, conn.Frame{Value: frame}, pending, nil, nil, nil, &helpers, &last, nil); err != nil {
		t.Fatal("valid capacity response became a transport error", err)
	}
	if live.silent.Load() || live.waiting || live.lastReply.Before(now) || len(pending) != 0 {
		t.Fatal("capacity refusal did not count as an answering hub")
	}
}

// Keep the real owner/registry closure join separate from the old-compatible
// response regression above: capacity refusal must retire the exact attachment.
func TestOwnerEndAtOriginCapRetiresHubAttachment(t *testing.T) {
	left, right := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	registry := make(chan registryEvent, 8)
	origin := newHostLink(ctx, "alpha", left, left, registry, io.Discard)
	origin.originAdmitted = protocol.MaxOperations
	other, _, _, _, _ := originReleaseFixture(t)
	other.host = "beta"
	state := &hubState{links: map[string]*hostLink{"alpha": origin, "beta": other}, log: io.Discard}
	joined := make(chan struct{})
	go func() { defer close(joined); event := <-registry; state.handle(ctx, registry, event) }()
	go origin.run(true)
	daemonInbox := make(chan any, 8)
	daemonDone := make(chan error, 1)
	go func() { daemonDone <- ServeDaemon(ctx, "alpha", right, daemonInbox, nil, io.Discard) }()
	t.Cleanup(func() { cancel(); origin.wire.Close(); _ = right.Close(); <-origin.done; <-joined })
	daemonInbox <- OwnerEndCall{Value: OwnerEnd{Host: "beta", SessionID: "parent@alpha", Lifetime: "life"}, Reply: make(chan Reply, 1)}
	select {
	case err := <-daemonDone:
		if err == nil {
			t.Fatal("refused end became success")
		}
	case <-time.After(time.Second):
		t.Fatal("refused owner end left daemon connected")
	}
	select {
	case event := <-other.inbox:
		call, ok := event.(lifetimeCall)
		if !ok || call.Method != hostEndMethod || call.Value.(HostEnd).Attachment != origin.attachment {
			t.Fatal("attachment retirement missing", event)
		}
	case <-time.After(time.Second):
		t.Fatal("host end not delivered")
	}
}

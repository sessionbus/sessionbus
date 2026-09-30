// SPDX-License-Identifier: GPL-3.0-only

package daemon

import (
	"testing"

	"github.com/antst/sessionbus/bus/internal/federation"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestParentTraceOwnSendIsNotCopied(t *testing.T) {
	for _, mode := range []string{"events", "content"} {
		t.Run(mode, func(t *testing.T) {
			d, parent := traceFixture(t)
			child := traceSpawn(t, parent, "child", mode, false)
			d.directory.mu.Lock()
			childName := d.directory.entries[child].row.Name
			d.directory.mu.Unlock()
			other := connectPeer(t, d.config.SocketPath, "other", "other", "team")
			for _, targets := range [][]string{{child}, {childName}, {child, "missing"}, {child, "other@local"}} {
				var result protocol.MessageSendResult
				must(t, parent.call("message.send", protocol.MessageSendRequest{Targets: targets, Message: "from parent"}, &result))
				// queued_for_next_turn is accepted too: the SDK keeps w.run set until
				// turn.ready is acked, so Worker.deliver can still return NotRunning
				// before submission for the prior seeded run; the daemon retires that
				// as completion-crossing recovery. Suppression below is by identity,
				// not receipt (see TestParentTraceOwnSendUsesIdentityNotReceipt).
				if len(result.Deliveries) != len(targets) || result.Deliveries[0].SessionID != child || (result.Deliveries[0].Disposition != "written" && result.Deliveries[0].Disposition != "injected" && result.Deliveries[0].Disposition != "queued_for_next_turn") {
					t.Fatal(result)
				}
				if len(targets) == 2 && targets[1] == "other@local" {
					traceOriginal(t, other, "parent@local", "from parent")
				}
				traceIdle(t, d)
				traceAbsent(t, parent)
			}
			d.directory.mu.Lock()
			dropped := d.traceDropped
			d.directory.mu.Unlock()
			if dropped != 0 {
				t.Fatalf("intentional omission counted as loss: %d", dropped)
			}
		})
	}
}

func TestParentTraceOwnSendUsesIdentityNotReceipt(t *testing.T) {
	d, parent := traceFixture(t)
	child := traceSpawn(t, parent, "child", "content", false)
	d.directory.mu.Lock()
	ref, ok := d.directory.snapshotTrace(d.directory.entries[child])
	d.directory.mu.Unlock()
	if !ok {
		t.Fatal("missing live trace parent")
	}
	for _, disposition := range []string{"written", "injected", "queued_for_next_turn", "no_receipt", "rejected"} {
		if !d.reserveTrace(0) {
			t.Fatal("reserve")
		}
		s := newSession(d)
		s.traceRequests = map[int64]*traceRequest{1: {
			messageID: "original", from: protocol.DeliverySource{SessionID: "parent@local"},
			refs: []federation.TraceRecipient{ref},
		}}
		s.finishTrace(protocol.Frame{ID: 1}, protocol.MessageSendResult{Deliveries: []protocol.MessageSendDelivery{{SessionID: child, Disposition: disposition}}}, 0, nil)
		traceIdle(t, d)
		traceAbsent(t, parent)
	}
}

func TestParentTraceOwnSendDoesNotSuppressOtherParent(t *testing.T) {
	for _, mode := range []string{"events", "content"} {
		t.Run(mode, func(t *testing.T) {
			daemons, parent := traceFederation(t)
			other := connectPeer(t, daemons["gamma"].config.SocketPath, "other", "other", "team")
			ownChild := traceRemoteSpawn(t, parent, "beta", "own-child", mode)
			otherChild := traceRemoteSpawn(t, other, "beta", "other-child", mode)
			var result protocol.MessageSendResult
			must(t, parent.call("message.send", protocol.MessageSendRequest{Targets: []string{ownChild, otherChild}, Message: "two parents"}, &result))
			if len(result.Deliveries) != 2 {
				t.Fatal(result)
			}
			for _, delivery := range result.Deliveries {
				if delivery.Disposition != "written" && delivery.Disposition != "injected" {
					t.Fatal(result)
				}
			}
			_, copy := traceReceive(t, other)
			if copy.MessageID != result.MessageID || copy.From.SessionID != "parent@alpha" || len(copy.MatchedChildren) != 1 || copy.MatchedChildren[0] != otherChild || len(copy.Deliveries) != 1 || copy.Deliveries[0].SessionID != otherChild {
				t.Fatal(copy)
			}
			if mode == "content" && (copy.Body == nil || *copy.Body != "two parents") || mode == "events" && copy.Body != nil {
				t.Fatal(copy)
			}
			for _, d := range daemons {
				traceIdle(t, d)
			}
			traceAbsent(t, parent)
			traceAbsent(t, other)
		})
	}
}

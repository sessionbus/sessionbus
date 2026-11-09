// SPDX-License-Identifier: GPL-3.0-only

package federation

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/conn"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestMarkRetainsQueuedAndPendingChargesAndNotifiesOnce(t *testing.T) {
	now := time.Now()
	live := linkLiveness{lastReply: now, waiting: true}
	first, second := make(chan Reply, 1), make(chan Reply, 1)
	p := &pendingCall{forwarded: true, method: "turn.wait", reply: first}
	queued := map[*pendingCall]bool{p: true}
	pending := map[int64]pendingCall{1: {forwarded: true, method: "turn.run", reply: second}, 2: {method: hostsMethod, probe: &live}}
	live.tick(now.Add(15*time.Second), hostsMethod, nil, queued, pending)
	if !live.silent.Load() || callCount(pending, queued, false) != 2 || len(queued) != 1 || len(pending) != 2 {
		t.Fatal("mark released correlation or probe counted as ordinary")
	}
	for _, reply := range []chan Reply{first, second} {
		value := <-reply
		if value.Error == nil || value.Error.Code != protocol.ForwardLost {
			t.Fatal(value)
		}
	}
	live.tick(now.Add(20*time.Second), hostsMethod, nil, queued, pending)
	installCall(conn.Dispatched{ID: 3, Value: p}, queued, pending)
	if len(queued) != 0 || pending[3].reply != nil || callCount(pending, queued, false) != 2 {
		t.Fatal("mark-before-dispatch lost abandonment or charge")
	}
	settlePending(pending)
	for _, reply := range []chan Reply{first, second} {
		select {
		case value := <-reply:
			t.Fatal("duplicate mark/close answer", value)
		default:
		}
	}
}

func TestLateRepliesDrainOnBothOwnersAndDoNotReachSuccessor(t *testing.T) {
	for _, side := range []string{"hub", "host"} {
		t.Run(side, func(t *testing.T) {
			pending := map[int64]pendingCall{1: {forwarded: true, method: "session.close", reply: nil}}
			live := linkLiveness{waiting: true}
			live.silent.Store(true)
			pending[2] = pendingCall{method: hostsMethod, probe: &live}
			next := make(chan Reply, 1)
			pending[3] = pendingCall{forwarded: true, method: "session.close", reply: next}
			var helpers sync.WaitGroup
			last := int64(0)
			consume := func(id int64, result string) {
				t.Helper()
				event := conn.Frame{Value: protocol.Frame{ID: id, Result: json.RawMessage(result)}}
				var err error
				if side == "hub" {
					err = (&hostLink{}).frame(event, pending, &last)
				} else {
					err = daemonFrame("beta", false, false, false, event, pending, nil, nil, nil, &helpers, &last, nil)
				}
				if err != nil {
					t.Fatal("late response closed owner", err)
				}
			}
			consume(1, `{}`)
			select {
			case <-next:
				t.Fatal("late result retargeted successor")
			default:
			}
			consume(2, `{"hosts":[]}`)
			if live.silent.Load() || live.waiting {
				t.Fatal("valid retained probe did not recover current attachment")
			}
			consume(3, `{}`)
			if value := <-next; value.Error != nil || string(value.Result) != `{}` {
				t.Fatal(value)
			}
			if len(pending) != 0 {
				t.Fatal("correlations not retired")
			}
		})
	}
}

func TestHealthyWaitHasNoTotalDeadlineAndReplacementStartsFresh(t *testing.T) {
	now := time.Now()
	old := linkLiveness{lastReply: now, waiting: true}
	reply := make(chan Reply, 1)
	pending := map[int64]pendingCall{1: {forwarded: true, method: "turn.wait", reply: reply}}
	// An arbitrarily old call is retained while this attachment's own probes
	// keep replying. There is no per-method wall-clock deadline.
	old.lastReply = now.Add(24 * time.Hour)
	old.tick(now.Add(24*time.Hour+14*time.Second), hostsMethod, nil, nil, pending)
	select {
	case <-reply:
		t.Fatal("healthy long wait was timed out")
	default:
	}
	old.tick(now.Add(24*time.Hour+15*time.Second), hostsMethod, nil, nil, pending)
	<-reply
	replacement := linkLiveness{lastReply: now, waiting: true}
	if replacement.silent.Load() {
		t.Fatal("replacement inherited old mark")
	}
}

func TestInvalidLateReplyIsStillAProtocolFailure(t *testing.T) {
	for _, side := range []string{"hub", "host"} {
		t.Run(side, func(t *testing.T) {
			pending := map[int64]pendingCall{1: {forwarded: true, method: "turn.wait", reply: nil}}
			event := conn.Frame{Value: protocol.Frame{ID: 1, Result: []byte(`{"invented":true}`)}}
			last := int64(0)
			var helpers sync.WaitGroup
			var err error
			if side == "hub" {
				err = (&hostLink{}).frame(event, pending, &last)
			} else {
				err = daemonFrame("beta", false, false, false, event, pending, nil, nil, nil, &helpers, &last, nil)
			}
			if err == nil || len(pending) != 1 {
				t.Fatal("abandonment disabled frame validation", err, len(pending))
			}
		})
	}
}

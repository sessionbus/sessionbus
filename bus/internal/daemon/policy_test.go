// SPDX-License-Identifier: GPL-3.0-only
package daemon

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/federation"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

type policyClock struct {
	now    time.Time
	timers []*policyAlarm
}
type policyAlarm struct {
	at      time.Time
	fired   chan time.Time
	stopped bool
}

func (c *policyClock) timer(delay time.Duration) (<-chan time.Time, func()) {
	a := &policyAlarm{at: c.now.Add(delay), fired: make(chan time.Time, 1)}
	c.timers = append(c.timers, a)
	return a.fired, func() { a.stopped = true }
}
func policySession(t *testing.T, persistent bool, delay int64) (*session, *bufio.Reader, *policyClock) {
	t.Helper()
	d, s, peer := reviewSession(t)
	clock := &policyClock{now: time.Unix(1000, 0)}
	d.config.now = func() time.Time { return clock.now }
	d.config.policyTimer = clock.timer
	parent := &ownership{id: "owner@local", token: "owner-life"}
	policy := &protocol.LanePolicy{Persistent: persistent, AutoCloseMS: delay, OwnerSessionID: parent.id}
	item := &entry{row: row{SessionID: "lane@local", Name: "owner/lane@local", Groups: []string{"team"}, Policy: policy, CreatedAt: clock.now}, parent: parent, attachment: s, done: make(chan struct{})}
	d.directory.entries[item.row.SessionID] = item
	s.identity = item
	s.committed = true
	s.launch = &launch{}
	t.Cleanup(func() {
		s.stopAutoClose()
		if s.closeTimer != nil {
			s.closeTimer.Stop()
		}
	})
	return s, bufio.NewReader(peer), clock
}
func policyFrame(t *testing.T, r *bufio.Reader) protocol.Frame {
	t.Helper()
	f, err := readRawFrame(r)
	must(t, err)
	return f
}
func policyTerminal(t *testing.T, s *session, r *bufio.Reader) {
	t.Helper()
	s.runID = "g/1"
	s.runGeneration = "g"
	s.runSequence = 1
	s.identity.running = true
	s.turnReady(protocol.Frame{ID: 1, Method: "turn.ready"}, &protocol.TurnReady{SessionID: s.identity.row.SessionID, RunID: s.runID, State: "done", Outcome: "completed"})
	f := policyFrame(t, r)
	if f.Error != nil {
		t.Fatal(f.Error)
	}
}

func TestPolicyLifetimeAndAutoCloseAreIndependent(t *testing.T) {
	for _, persistent := range []bool{false, true} {
		for _, delay := range []int64{0, 60000} {
			t.Run(fmt.Sprintf("persistent=%t/delay=%d", persistent, delay), func(t *testing.T) {
				s, r, c := policySession(t, persistent, delay)
				if s.autoClose != nil || len(c.timers) != 0 {
					t.Fatal("Open invented deadline")
				}
				policyTerminal(t, s, r)
				if (s.autoClose != nil) != (delay != 0) {
					t.Fatal("terminal deadline conflated with lifetime")
				}
				s.daemon.directory.mu.Lock()
				s.daemon.directory.endOwner(s.identity.parent)
				s.daemon.directory.mu.Unlock()
				if !persistent {
					s.handleEvent(<-s.inbox)
					if f := policyFrame(t, r); f.Method != "session.close" {
						t.Fatalf("owner exit frame %#v", f)
					}
					if s.autoClose != nil {
						t.Fatal("close left timer")
					}
					return
				}
				if s.closeCall != nil {
					t.Fatal("persistent lane closed on owner exit")
				}
				if delay == 0 {
					return
				}
				alarm := c.timers[0]
				c.now = alarm.at
				alarm.fired <- c.now
				<-s.autoClose
				s.autoClose = nil
				s.policyClose()
				if f := policyFrame(t, r); f.Method != "session.close" {
					t.Fatalf("deadline frame %#v", f)
				}
			})
		}
	}
}
func TestPolicyRefusalRestoresOriginalDeadlineAndSequence(t *testing.T) {
	s, r, c := policySession(t, true, 60000)
	policyTerminal(t, s, r)
	deadline := s.deadline
	first := c.timers[0]
	c.now = c.now.Add(20 * time.Second)
	reply := make(chan answer, 1)
	s.issue(routedRequest{destination: s.identity, method: "turn.start", params: &protocol.TurnRunRequest{SessionID: "lane", Input: "work"}, reply: reply})
	execute := policyFrame(t, r)
	var request protocol.ExecuteRequest
	must(t, json.Unmarshal(execute.Params, &request))
	if request.SessionID != "lane@local" || request.RunID != "g/2" || !first.stopped || s.autoClose != nil {
		t.Fatalf("reservation %#v", request)
	}
	s.receiveResponse(protocol.Frame{ID: execute.ID, Error: &protocol.RPCError{Code: protocol.Busy, Message: "busy"}})
	if got := <-reply; got.code != protocol.Busy {
		t.Fatalf("refusal %#v", got)
	}
	if s.runSequence != 1 || s.runID != "" || !s.deadline.Equal(deadline) || !c.timers[1].at.Equal(deadline) {
		t.Fatal("refusal reset grace or skipped run ID")
	}
	s.issue(routedRequest{destination: s.identity, method: "turn.start", params: &protocol.TurnRunRequest{SessionID: "lane", Input: "retry"}, reply: reply})
	execute = policyFrame(t, r)
	must(t, json.Unmarshal(execute.Params, &request))
	if request.RunID != "g/2" {
		t.Fatalf("retry %s", request.RunID)
	}
}
func TestPolicySeedUncertaintyDoesNotUndoAdmission(t *testing.T) {
	s, r, _ := policySession(t, true, 0)
	reply := make(chan answer, 1)
	original := protocol.DeliveryRequest{MessageID: "m", From: protocol.DeliverySource{SessionID: "peer@local", Product: "peer", Groups: []string{}}, Body: "body"}
	s.issue(routedRequest{destination: s.identity, method: "message.deliver", params: original, reply: reply})
	frame := policyFrame(t, r)
	var got protocol.DeliveryRequest
	must(t, json.Unmarshal(frame.Params, &got))
	if got.RunID == "" {
		t.Fatal("wake did not reserve run")
	}
	id := got.RunID
	got.RunID = ""
	if got.Body != original.Body || got.MessageID != original.MessageID || got.From.SessionID != original.From.SessionID {
		t.Fatal("seed changed delivery")
	}
	s.receiveResponse(protocol.Frame{ID: frame.ID, Error: &protocol.RPCError{Code: protocol.Internal, Message: "internal"}})
	if (<-reply).code != protocol.Internal {
		t.Fatal("uncertainty became receipt")
	}
	if s.runID != id || s.runSequence != 1 || !s.identity.running {
		t.Fatal("uncertain native submission rolled back accepted run")
	}
	s.turnReady(protocol.Frame{ID: 1, Method: "turn.ready"}, &protocol.TurnReady{SessionID: s.identity.row.SessionID, RunID: id, State: "unavailable", Reason: "native lost"})
	_ = policyFrame(t, r)
	if s.runID != "" || s.identity.running {
		t.Fatal("unavailable terminal did not release slot")
	}
}
func TestPolicyResumeDefaultsAndNotification(t *testing.T) {
	for _, target := range []string{"first@local", "different@local"} {
		selected, err := normalizePolicy(&protocol.LaneSpawnRequest{NotifyTarget: target}, nil, "first@local")
		if target == "first@local" {
			must(t, err)
			if !selected.Notify || selected.NotifyTarget != "" || selected.OwnerSessionID != target {
				t.Fatalf("explicit owner %#v", selected)
			}
		} else if err == nil {
			t.Fatal("different parent-owned notify target accepted")
		}
	}
	yes, no, zero := true, false, int64(0)
	initial, err := normalizePolicy(&protocol.LaneSpawnRequest{AutoCloseMS: &zero}, nil, "first@local")
	must(t, err)
	resumed, err := normalizePolicy(&protocol.LaneSpawnRequest{}, initial, "second@local")
	must(t, err)
	if resumed.Persistent || resumed.AutoCloseMS != 60000 || resumed.OwnerSessionID != "second@local" || !resumed.Notify {
		t.Fatalf("resume %#v", resumed)
	}
	promoted, err := normalizePolicy(&protocol.LaneSpawnRequest{Persistent: &yes}, initial, "second@local")
	must(t, err)
	if !promoted.Persistent || promoted.OwnerSessionID != "" || promoted.NotifyTarget != "first@local" {
		t.Fatalf("promotion %#v", promoted)
	}
	inherited, err := normalizePolicy(&protocol.LaneSpawnRequest{AutoCloseMS: &zero}, promoted, "third@local")
	must(t, err)
	if inherited.NotifyTarget != "first@local" || inherited.AutoCloseMS != 0 {
		t.Fatalf("persistent resume %#v", inherited)
	}
	if _, err = normalizePolicy(&protocol.LaneSpawnRequest{Persistent: &no}, promoted, "third@local"); err == nil {
		t.Fatal("demotion accepted")
	}
	cleared, err := normalizePolicy(&protocol.LaneSpawnRequest{Notify: &no}, promoted, "third@local")
	must(t, err)
	if cleared.Notify || cleared.NotifyTarget != "" {
		t.Fatal("disable did not clear target")
	}
}
func TestPolicySameIDTransferAndRemoteAttachmentEnd(t *testing.T) {
	d, s, _ := reviewSession(t)
	hello := &protocol.PeerHello{Protocol: 1, Product: "peer", SessionID: "owner", Name: "first", Groups: []string{"team"}}
	first, _, _, ok := d.directory.installPeer(s, hello, "local")
	if !ok {
		t.Fatal("install")
	}
	s.identity = first
	replacement := newSession(d)
	second, _, _, ok := d.directory.installPeer(replacement, hello, "local")
	if !ok || first.lifetime != nil || second.lifetime == nil || second.lifetime.ended {
		t.Fatal("same-ID transfer ended lifetime")
	}
	life := second.lifetime
	d.directory.detach(first, s)
	if life.ended {
		t.Fatal("stale detach ended replacement")
	}
	d.directory.detach(second, replacement)
	if !life.ended {
		t.Fatal("actual detach retained parent lifetime")
	}
	d.directory.mu.Lock()
	remote := d.directory.remoteOwnerLocked(federation.Caller{SessionID: "native@label@alpha", OwnerLifetime: "token", SourceAttachment: "old"})
	fresh := d.directory.remoteOwnerLocked(federation.Caller{SessionID: "native@label@alpha", OwnerLifetime: "token", SourceAttachment: "new"})
	d.directory.mu.Unlock()
	d.directory.remoteEnded(federation.LifetimeEvent{Host: &federation.HostEnd{Host: "alpha", Attachment: "old"}})
	if !remote.ended || fresh.ended {
		t.Fatal("host end crossed attachment epoch")
	}
	d.directory.remoteEnded(federation.LifetimeEvent{})
	if !fresh.ended {
		t.Fatal("destination link loss kept remote parent")
	}
}

func TestPolicyBlockingRunReadPrecedesDeferredClose(t *testing.T) {
	for _, outcome := range []string{"accepted", "rejected", "terminal-first", "lost"} {
		t.Run(outcome, func(t *testing.T) {
			s, r, _ := policySession(t, true, 0)
			runReply, closeReply := make(chan answer, 1), make(chan answer, 1)
			s.issue(routedRequest{destination: s.identity, method: "turn.run", params: &protocol.TurnRunRequest{SessionID: "lane", Input: "work"}, reply: runReply})
			execute := policyFrame(t, r)
			id := s.runID
			s.issue(routedRequest{destination: s.identity, method: "session.close", params: &protocol.SessionCloseRequest{SessionID: s.identity.row.SessionID}, reply: closeReply})
			timer := s.closeTimer
			if timer == nil || s.closeCall == nil || len(s.pending) != 1 {
				t.Fatal("close was not held behind execute admission")
			}
			refused := make(chan answer, 1)
			s.issue(routedRequest{destination: s.identity, method: "turn.status", params: &protocol.ReadRequest{SessionID: "lane"}, reply: refused})
			if (<-refused).code != protocol.Busy {
				t.Fatal("closing admitted new external reader")
			}
			if outcome == "lost" {
				s.connectionClosed()
				if (<-runReply).code != protocol.NotConnected {
					t.Fatal("lost execute did not settle")
				}
				return
			}
			if outcome == "terminal-first" {
				s.turnReady(protocol.Frame{ID: 1, Method: "turn.ready"}, &protocol.TurnReady{SessionID: s.identity.row.SessionID, RunID: id, State: "done", Outcome: "completed"})
				_ = policyFrame(t, r)
			}
			if outcome == "rejected" {
				s.receiveResponse(protocol.Frame{ID: execute.ID, Error: &protocol.RPCError{Code: protocol.Busy, Message: "busy"}})
				if (<-runReply).code != protocol.Busy {
					t.Fatal("rejected execute did not settle")
				}
			} else {
				raw, _ := json.Marshal(protocol.RunRef{SessionID: s.identity.row.SessionID, RunID: id})
				s.receiveResponse(protocol.Frame{ID: execute.ID, Result: raw})
				wait := policyFrame(t, r)
				if wait.Method != "turn.wait" {
					t.Fatalf("first frame after admission %s", wait.Method)
				}
				var request protocol.WaitRequest
				must(t, json.Unmarshal(wait.Params, &request))
				if request.RunID != id {
					t.Fatal("blocking read changed admission")
				}
			}
			closeFrame := policyFrame(t, r)
			if closeFrame.Method != "session.close" {
				t.Fatalf("close emitted %s", closeFrame.Method)
			}
			if s.closeTimer != timer {
				t.Fatal("deferred close reset original bound")
			}
		})
	}
}

func TestPolicyRealWorkerWakePointerAndGroupCollection(t *testing.T) {
	products := t.TempDir()
	installFixture(t, products, "wake-worker")
	installFixture(t, products, "fixture-worker-no-wake")
	t.Setenv("PATH", products+string(os.PathListSeparator)+os.Getenv("PATH"))
	_, socket := startDaemon(t)
	owner := connectPeer(t, socket, "owner", "owner", "team")
	collector := connectPeer(t, socket, "collector", "collector", "team")
	hidden := connectPeer(t, socket, "hidden", "hidden", "other")
	zero := int64(0)
	var lane protocol.LaneSpawnResult
	unsupported := protocol.LaneSpawnRequest{Name: "unsupported", Product: "fixture-worker-no-wake", Open: &protocol.OpenOptions{}, AutoCloseMS: &zero}
	if code := rpcCode(owner.call("lane.spawn", unsupported, &lane)); code != protocol.UnsupportedOpen {
		t.Fatalf("unsupported wake code %d", code)
	}
	must(t, owner.call("lane.spawn", protocol.LaneSpawnRequest{Name: "child", Product: "wake-worker", Open: &protocol.OpenOptions{}, ExtraGroups: []string{"team"}, AutoCloseMS: &zero}, &lane))
	if lane.Policy == nil || lane.Policy.Persistent || lane.Policy.AutoCloseMS != 0 {
		t.Fatalf("policy %#v", lane.Policy)
	}
	var sent protocol.MessageSendResult
	must(t, owner.call("message.send", protocol.MessageSendRequest{Target: lane.SessionID, Message: "hello"}, &sent))
	if len(sent.Deliveries) != 1 || sent.Deliveries[0].Disposition != "written" {
		t.Fatalf("wake receipt %#v", sent)
	}
	var status protocol.RunStatus
	must(t, collector.call("turn.wait", protocol.WaitRequest{SessionID: unqualify(lane.SessionID)}, &status))
	if status.SessionID != lane.SessionID || status.Result == nil || status.Result.Result != "hello" {
		t.Fatalf("collected %#v", status)
	}
	pointer := <-owner.deliveries
	wantPointer := fmt.Sprintf("Lane %s run %s is %s. Collect with the public wait action (session_id=%s, run_id=%s), then use the public ack action after collecting.", status.SessionID, status.RunID, status.State, status.SessionID, status.RunID)
	if pointer.From.SessionID != lane.SessionID || pointer.Body != wantPointer || strings.Contains(pointer.Body, "turn.wait") || strings.Contains(pointer.Body, "turn.ack") || strings.Contains(pointer.Body, "hello") {
		t.Fatalf("completion pointer %#v", pointer)
	}
	if code := rpcCode(hidden.call("turn.status", protocol.ReadRequest{SessionID: lane.SessionID}, &protocol.RunStatus{})); code != protocol.UnknownSession {
		t.Fatalf("hidden read %d", code)
	}
	var again protocol.RunStatus
	must(t, collector.call("turn.status", protocol.ReadRequest{SessionID: lane.SessionID}, &again))
	if again.RunID != status.RunID {
		t.Fatal("read consumed cursor")
	}
	must(t, collector.call("turn.ack", protocol.RunRef{SessionID: unqualify(lane.SessionID), RunID: status.RunID}, &struct{}{}))
	if code := rpcCode(collector.call("turn.status", protocol.ReadRequest{SessionID: lane.SessionID}, &again)); code != protocol.UnknownSession {
		t.Fatalf("ack left record %d", code)
	}
	must(t, collector.call("session.close", protocol.SessionCloseRequest{SessionID: lane.SessionID}, &struct{}{}))
	must(t, collector.call("lane.spawn", protocol.LaneSpawnRequest{ResumeSessionID: lane.SessionID, AutoCloseMS: &zero}, &lane))
	if lane.Policy.OwnerSessionID != "collector@local" {
		t.Fatalf("resume %#v", lane.Policy)
	}
	if code := rpcCode(collector.call("turn.status", protocol.ReadRequest{SessionID: lane.SessionID, RunID: status.RunID}, &again)); code != protocol.UnknownSession {
		t.Fatalf("resume resurrected old output %d", code)
	}
	must(t, collector.call("session.close", protocol.SessionCloseRequest{SessionID: lane.SessionID, Forget: true}, &struct{}{}))
}

func TestPolicyOwnerLossBeforePublicationCannotCreateOwnedOrphan(t *testing.T) {
	for _, persistent := range []bool{false, true} {
		t.Run(fmt.Sprint(persistent), func(t *testing.T) {
			d := &Daemon{}
			d.directory = newDirectory(d, nil)
			life := &ownership{id: "parent@local", token: "life"}
			start := newLaunch("fixture", false, true)
			start.parent = life
			defer start.timer.Stop()
			item, code := d.directory.reserveFresh(row{Name: "parent/leaf@local", Policy: &protocol.LanePolicy{Persistent: persistent}}, start)
			if code != 0 {
				t.Fatal(code)
			}
			defer d.group.Done()
			if !d.directory.reserveID(item, "native@local") {
				t.Fatal("reserve ID")
			}
			d.directory.mu.Lock()
			d.directory.endOwner(life)
			d.directory.mu.Unlock()
			published := d.directory.publish(start, newSession(d), time.Now())
			if published != persistent {
				t.Fatalf("publish after owner loss = %t, persistent=%t", published, persistent)
			}
		})
	}
}

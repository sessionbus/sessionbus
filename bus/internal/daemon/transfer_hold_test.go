// SPDX-License-Identifier: GPL-3.0-only

package daemon

import (
	"fmt"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestAdditionalTransferCanHoldPastCancelledDeadline(t *testing.T) {
	for _, persistent := range []bool{false, true} {
		t.Run(fmt.Sprintf("persistent=%t", persistent), func(t *testing.T) {
			s, r, clock := policySession(t, persistent, 60000)
			policyTerminal(t, s, r)
			oldDeadline, oldAlarm := s.deadline, clock.timers[0]
			clock.now = clock.now.Add(30 * time.Second)
			first := transferHoldEvent{entry: s.identity, id: "first"}
			if !s.transferHold(first) || !oldAlarm.stopped || s.autoClose != nil {
				t.Fatal("first accepted transfer did not cancel retirement timer")
			}

			clock.now = oldDeadline.Add(time.Second)
			if !s.transferHold(first) || len(s.transferHolds) != 1 || len(clock.timers) != 1 {
				t.Fatal("duplicate hold changed hold count or retirement timer")
			}
			if !s.transferHold(transferHoldEvent{entry: s.identity, id: "second"}) {
				t.Fatal("second transfer refused after cancelled deadline while first hold is active")
			}
			if len(s.transferHolds) != 2 || s.autoClose != nil || len(clock.timers) != 1 || s.runID != "" || s.identity.running {
				t.Fatal("additional hold invented a timer or native Run")
			}

			first.terminal = true
			clock.now = clock.now.Add(time.Second)
			if !s.transferHold(first) || len(s.transferHolds) != 1 || s.autoClose != nil || len(clock.timers) != 1 {
				t.Fatal("first terminal armed retirement while second transfer is active")
			}
			if s.transferHold(first) || len(s.transferHolds) != 1 {
				t.Fatal("duplicate terminal changed remaining hold")
			}

			clock.now = clock.now.Add(time.Second)
			if !s.transferHold(transferHoldEvent{entry: s.identity, id: "second", terminal: true}) {
				t.Fatal("last transfer terminal refused")
			}
			newDeadline := clock.now.Add(time.Minute)
			if len(s.transferHolds) != 0 || s.autoClose == nil || len(clock.timers) != 2 ||
				!s.deadline.Equal(newDeadline) || !clock.timers[1].at.Equal(newDeadline) || clock.timers[1].stopped {
				t.Fatal("last terminal did not arm fresh grace from its terminal time")
			}
			clock.now = newDeadline
			if s.transferHold(transferHoldEvent{entry: s.identity, id: "too-late"}) || len(s.transferHolds) != 0 || len(clock.timers) != 2 {
				t.Fatal("hold with no existing transfer overtook due retirement")
			}
		})
	}
}

func TestAcceptedTransferOwnsGraceWithoutInventingRun(t *testing.T) {
	s, _, clock := policySession(t, false, 60000)
	hold := transferHoldEvent{entry: s.identity, id: "one"}
	if !s.transferHold(hold) || s.autoClose != nil || s.runID != "" || s.identity.running {
		t.Fatal("hold inventedRun or grace")
	}
	clock.now = clock.now.Add(time.Second)
	if !s.transferHold(transferHoldEvent{entry: s.identity, id: "one", terminal: true}) {
		t.Fatal("lost original hold")
	}
	deadline := clock.now.Add(time.Minute)
	if !s.deadline.Equal(deadline) {
		t.Fatal(s.deadline, deadline)
	}
	// A duplicate terminal/status-like event must never extend grace.
	clock.now = clock.now.Add(time.Second)
	if s.transferHold(transferHoldEvent{entry: s.identity, id: "one", terminal: true}) || !s.deadline.Equal(deadline) {
		t.Fatal("duplicate terminal rearmed")
	}
}

func TestTransferAndNativeTerminalsChooseOneLaterAnchor(t *testing.T) {
	s, r, clock := policySession(t, true, 60000)
	for _, id := range []string{"first", "second"} {
		if !s.transferHold(transferHoldEvent{entry: s.identity, id: id}) {
			t.Fatal(id)
		}
	}
	policyTerminal(t, s, r)
	if s.autoClose != nil {
		t.Fatal("native terminal ignored active transfers")
	}
	clock.now = clock.now.Add(2 * time.Second)
	_ = s.transferHold(transferHoldEvent{entry: s.identity, id: "first", terminal: true})
	if s.autoClose != nil {
		t.Fatal("first of two terminals armed")
	}
	clock.now = clock.now.Add(2 * time.Second)
	_ = s.transferHold(transferHoldEvent{entry: s.identity, id: "second", terminal: true})
	if !s.deadline.Equal(clock.now.Add(time.Minute)) {
		t.Fatal("last held terminal not anchor", s.deadline)
	}
	// A later accepted transfer finishing during another Run cannot arm early.
	if !s.transferHold(transferHoldEvent{entry: s.identity, id: "third"}) {
		t.Fatal("later valid hold refused")
	}
	s.runID = "g/2"
	clock.now = clock.now.Add(2 * time.Second)
	_ = s.transferHold(transferHoldEvent{entry: s.identity, id: "third", terminal: true})
	if s.autoClose != nil {
		t.Fatal("transfer terminal armed during activeRun")
	}
}

func TestTransferHoldCannotAdoptReplacementOrOvertakeDueClose(t *testing.T) {
	s, r, clock := policySession(t, false, 60000)
	policyTerminal(t, s, r)
	clock.now = s.deadline
	if s.transferHold(transferHoldEvent{entry: s.identity, id: "too-late"}) {
		t.Fatal("accept overtook due close")
	}
	clock.now = clock.now.Add(-time.Second)
	old := s.identity
	replacement := &entry{row: old.row, attachment: s}
	s.daemon.directory.mu.Lock()
	s.daemon.directory.entries[old.row.SessionID] = replacement
	s.daemon.directory.mu.Unlock()
	if s.transferHold(transferHoldEvent{entry: old, id: "stale"}) {
		t.Fatal("sameID adopted oldgeneration hold")
	}
}

func TestZeroGraceAndClosingOwnerDoNotGainTransferTimer(t *testing.T) {
	s, _, _ := policySession(t, true, 0)
	if !s.transferHold(transferHoldEvent{entry: s.identity, id: "zero"}) {
		t.Fatal("zero-grace hold")
	}
	_ = s.transferHold(transferHoldEvent{entry: s.identity, id: "zero", terminal: true})
	if s.autoClose != nil {
		t.Fatal("zero grace armed")
	}
	s.closeCall = &routedRequest{}
	if s.transferHold(transferHoldEvent{entry: s.identity, id: "closing"}) {
		t.Fatal("held closing owner")
	}
}

func TestLastTransferTerminalCanGrantGraceAfterUnavailableRun(t *testing.T) {
	s, r, clock := policySession(t, true, 60000)
	s.runID = "g/1"
	s.runGeneration = "g"
	s.runSequence = 1
	s.identity.running = true
	if !s.transferHold(transferHoldEvent{entry: s.identity, id: "owned"}) {
		t.Fatal("hold refused")
	}
	clock.now = clock.now.Add(time.Second)
	s.transferHold(transferHoldEvent{entry: s.identity, id: "owned", terminal: true})
	if s.autoClose != nil || !s.transferGracePending {
		t.Fatal("armed before original terminal")
	}
	clock.now = clock.now.Add(time.Second)
	s.turnReady(protocol.Frame{ID: 1, Method: "turn.ready", Request: true}, &protocol.TurnReady{SessionID: s.identity.row.SessionID, RunID: s.runID, State: "unavailable", Reason: "fixture native refusal"})
	if frame := policyFrame(t, r); frame.Error != nil {
		t.Fatal(frame.Error)
	}
	if !s.deadline.Equal(clock.now.Add(time.Minute)) || s.transferGracePending {
		t.Fatal("lost last-transfer terminal grace", s.deadline)
	}
}

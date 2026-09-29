// SPDX-License-Identifier: GPL-3.0-only

package daemon

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/antst/sessionbus/bus/internal/commslog"
	"github.com/antst/sessionbus/bus/internal/federation"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

const defaultAutoCloseMS int64 = 60000

// ownership is a caller lifetime, not a transport request or worker claimant.
// All fields are protected by directory.mu. Same-ID replacement keeps this token.
type ownership struct {
	caller                      federation.Caller
	id, token, host, attachment string
	ended                       bool
	destinations                map[string]bool
}
type ownerEndEvent struct{ owner *ownership }

func normalizePolicy(input *protocol.LaneSpawnRequest, previous *protocol.LanePolicy, ownerID string) (*protocol.LanePolicy, error) {
	value := protocol.LanePolicy{AutoCloseMS: defaultAutoCloseMS, Notify: true, OwnerSessionID: ownerID}
	if previous != nil {
		value = *previous
		value.AutoCloseMS = defaultAutoCloseMS
		value.OwnerSessionID = ownerID
	}
	if input.Persistent != nil {
		if value.Persistent && !*input.Persistent {
			return nil, errors.New("persistent lifetime cannot be demoted")
		}
		value.Persistent = *input.Persistent
	}
	if input.AutoCloseMS != nil {
		value.AutoCloseMS = *input.AutoCloseMS
	}
	if value.Persistent {
		value.OwnerSessionID = ""
		if previous == nil {
			value.Notify = false
		}
		if previous != nil && !previous.Persistent && previous.Notify {
			value.NotifyTarget = previous.OwnerSessionID
		}
		if input.NotifyTarget != "" {
			value.NotifyTarget, value.Notify = input.NotifyTarget, true
		}
		if input.Notify != nil {
			value.Notify = *input.Notify
		}
		if !value.Notify {
			value.NotifyTarget = ""
		}
		if value.Notify && value.NotifyTarget == "" {
			return nil, errors.New("persistent notification requires a target")
		}
	} else {
		if input.NotifyTarget != "" && input.NotifyTarget != ownerID {
			return nil, errors.New("parent-owned lane notifies its owner")
		}
		value.Notify, value.NotifyTarget = true, ""
		if input.Notify != nil {
			value.Notify = *input.Notify
		}
	}
	if input.Notify != nil && !*input.Notify && input.NotifyTarget != "" {
		return nil, errors.New("disabled notification cannot have a target")
	}
	if _, err := protocol.EncodeResult("lane.spawn", protocol.LaneSpawnResult{SessionID: "policy", Policy: &value}); err != nil {
		return nil, err
	}
	return &value, nil
}

func (d *directory) callerOwner(s *session) *ownership {
	d.mu.Lock()
	defer d.mu.Unlock()
	if s.caller != nil {
		return d.remoteOwnerLocked(*s.caller)
	}
	if s.identity == nil || s.identity.attachment != s {
		return nil
	}
	return s.identity.lifetime
}

func (d *directory) endOwner(owner *ownership) {
	if owner == nil || owner.ended {
		return
	}
	owner.ended = true
	for _, item := range d.entries {
		if item.parent == owner {
			item.traceMode, item.traceVersion = "", ""
		}
		if item.parent == owner && item.row.Policy != nil && !item.row.Policy.Persistent && item.attachment != nil {
			if !item.attachment.wire.Post(ownerEndEvent{owner}) {
				item.attachment.wire.Close()
			}
		}
	}
	d.forwardOwnerEnd(owner)
}

func (s *session) stopAutoClose() {
	if s.stopAuto != nil {
		s.stopAuto()
		s.stopAuto = nil
	}
	s.autoClose = nil
}
func (s *session) setDeadline(at time.Time) {
	s.stopAutoClose()
	s.deadline = at
	if at.IsZero() {
		return
	}
	s.autoClose, s.stopAuto = s.daemon.config.policyTimer(at.Sub(s.daemon.config.now()))
}
func (s *session) reserveRun() string {
	s.runFromTrace = false
	s.runFromCompletion = false
	s.previousDeadline = s.deadline
	s.setDeadline(time.Time{})
	if s.runGeneration == "" {
		s.runGeneration = randomID("run")
	}
	s.runSequence++
	s.runID = s.runGeneration + "/" + strconv.FormatUint(s.runSequence, 10)
	return s.runID
}
func (s *session) refuseRun(id string) {
	if s.runID != id {
		return
	}
	s.runID = ""
	s.runFromTrace = false
	s.runFromCompletion = false
	s.runSequence-- // Only a refused, not-yet-admitted reservation can be reused.
	s.daemon.directory.finishRun(s.identity, s)
	s.setDeadline(s.previousDeadline)
}
func (s *session) policyClose() {
	if s.stopping || s.closeCall != nil || !s.committed {
		return
	}
	s.stopAutoClose()
	s.beginClose(routedRequest{destination: s.identity, method: "session.close", params: &protocol.SessionCloseRequest{SessionID: s.identity.row.SessionID}, reply: make(chan answer, 1)})
}
func (s *session) ownerEnded(owner *ownership) {
	if s.identity.parent != owner || s.identity.row.Policy.Persistent {
		return
	}
	s.daemon.directory.mu.Lock()
	ended := owner.ended
	s.daemon.directory.mu.Unlock()
	if ended {
		s.policyClose()
	}
}
func (s *session) turnReady(frame protocol.Frame, value *protocol.TurnReady) {
	if s.launch == nil || value.SessionID != s.identity.row.SessionID || s.runID == "" || value.RunID != s.runID {
		s.reject(frame, protocol.InvalidFrame)
		return
	}
	s.logRun(commslog.TurnReady, value.RunID, commslog.Settled, value.Outcome)
	s.runID = ""
	fromTrace, fromCompletion := s.runFromTrace, s.runFromCompletion
	s.runFromTrace = false
	s.runFromCompletion = false
	s.daemon.directory.finishRun(s.identity, s)
	policy := s.identity.row.Policy
	if value.State == "done" && policy.AutoCloseMS > 0 && s.closeCall == nil {
		s.setDeadline(s.daemon.config.now().Add(time.Duration(policy.AutoCloseMS) * time.Millisecond))
	}
	// Ack precedes any subsequent worker command; the SDK publishes its cursor
	// in CallObserved before dispatching those frames.
	s.result(frame, struct{}{})
	s.startDeferredDelivery()
	if policy.Notify && !fromCompletion && s.closeCall == nil {
		target := policy.NotifyTarget
		if !policy.Persistent {
			target = policy.OwnerSessionID
		}
		if target != "" {
			body := fmt.Sprintf("Lane %s run %s is %s. Collect with the public wait action (session_id=%s, run_id=%s), then use the public ack action after collecting.", value.SessionID, value.RunID, value.State, value.SessionID, value.RunID)
			// A trace-triggered Run keeps its normal completion pointer, but the
			// pointer is not a new trace source. Remote legs never emit copies.
			wait := s.daemon.directory.forwardLocalTrace(s.federationCaller(), s.identity, "message.send", &protocol.MessageSendRequest{Target: target, Message: body}, "", !fromTrace, nil, true)
			s.owned++
			go func() { _, _ = wait(s.identity.done); s.inbox <- replyEvent{} }()
		}
	}
}

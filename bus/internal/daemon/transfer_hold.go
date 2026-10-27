// SPDX-License-Identifier: GPL-3.0-only

package daemon

import "time"

// These bounded control events run on the existing connection owner. Their
// entry pointer is the admitted generation, not merely its reusable session ID.
type transferHoldEvent struct {
	key      string
	entry    *entry
	id       string
	terminal bool
	reply    chan bool
}

func (s *session) transferHold(e transferHoldEvent) bool {
	if e.key != "" && !s.daemon.directory.transferCurrent(e.entry, s, e.key) {
		return false
	}
	if e.entry != s.identity || s.identity == nil || !s.daemon.directory.current(s.identity, s) {
		return false
	}
	if e.terminal {
		return s.endTransferHold(e.id)
	}
	if s.stopping || s.closeCall != nil || s.launch != nil && !s.committed {
		return false
	}
	if s.launch == nil {
		return true
	} // Ordinary peers have no lane grace to hold.
	if s.transferHolds[e.id] {
		return true
	}
	now := s.daemon.config.now()
	if !s.deadline.IsZero() && !now.Before(s.deadline) {
		return false
	} // Already-due close wins.
	if s.transferHolds == nil {
		s.transferHolds = map[string]bool{}
	}
	s.transferHolds[e.id] = true
	s.stopAutoClose()
	return true
}

func (s *session) endTransferHold(id string) bool {
	if !s.transferHolds[id] {
		return false
	}
	delete(s.transferHolds, id)
	if len(s.transferHolds) > 0 {
		return true
	}
	s.lastTransferTerminal = s.daemon.config.now()
	if s.runID != "" {
		s.transferGracePending = true
	}
	if s.stopping || s.closeCall != nil || s.runID != "" {
		return true
	}
	policy := s.identity.row.Policy
	if policy == nil || policy.AutoCloseMS == 0 {
		return true
	}
	anchor := s.lastTransferTerminal
	if s.lastRunTerminal.After(anchor) {
		anchor = s.lastRunTerminal
	}
	s.transferGracePending = false
	s.setDeadline(anchor.Add(time.Duration(policy.AutoCloseMS) * time.Millisecond))
	return true
}

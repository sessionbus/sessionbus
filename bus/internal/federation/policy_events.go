// SPDX-License-Identifier: GPL-3.0-only

package federation

import "time"

type policyRosterObservation struct {
	selected time.Time
	reply    Reply
}

func (s *hubState) policyRosterFlag(host string) {
	if link := s.links[host]; link != nil {
		link.liveness.observeRoster.Store(s.relations != nil && s.relations.charges[host] != 0)
	}
}

func (s *hubState) completePolicy(event policyCompletion) {
	admission := event.admission
	if s.relations == nil {
		return
	}
	reply, once := s.relations.complete(admission, event.reply, s.links[admission.destination.host] == admission.destination)
	if !once {
		return
	}
	s.policyRosterFlag(admission.destination.host)
	if event.deliver {
		// This is the already-existing one-result channel. No origin waiter
		// lifetime controls authoritative verification or reservation release.
		admission.reply <- reply
	}
}

func (s *hubState) observePolicyRoster(link *hostLink, event policyRosterObservation) {
	if s.relations == nil || s.links[link.host] != link || event.reply.Error != nil {
		return
	}
	decoded, err := decodeRosterReply(event.reply.Result)
	if err != nil || len(decoded.Hosts) != 1 || decoded.Hosts[0].Host != link.host {
		return
	}
	s.relations.prune(link.host, event.selected, decoded.Hosts[0])
	s.policyRosterFlag(link.host)
}

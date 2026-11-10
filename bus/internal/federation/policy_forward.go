// SPDX-License-Identifier: GPL-3.0-only

package federation

import (
	"context"
	"strings"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

type policyForward struct {
	admission *policyAdmission
	reply     *policyReplyScope
	refusal   *Reply
}

// preparePolicyForward runs in the registry before dispatch. It retains only
// the policy and the minimal metadata belonging to an admitted correlation.
func (s *hubState) preparePolicyForward(ctx context.Context, inbox chan<- registryEvent, origin, destination *hostLink, value *Forward, answer chan Reply) policyForward {
	policy := s.policy
	tracked := policy.tracks(origin.host, destination.host)
	restricted := policy.groupsRestricted(origin.host, destination.host)
	// A forget can retire a retained grant after its original rule was removed.
	retainedForget := value.Request.Method == "session.close" && s.relations != nil && s.relations.charges[destination.host] != 0
	if !tracked && !retainedForget {
		return policyForward{}
	}
	if !restricted && !retainedForget && value.Request.Method != "lane.spawn" && value.Request.Method != "lane.describe" {
		return policyForward{}
	}
	params, err := protocol.DecodeParams(value.Request.Method, value.Request.Params)
	if err != nil {
		return refusedPolicyForward(errorReply(protocol.InvalidFrame, nil))
	}
	var admission *policyAdmission
	relation, traceCopy, ownership := false, false, false
	var targets []string
	switch input := params.(type) {
	case *protocol.LaneSpawnRequest:
		ownership = true
		if !policy.laneAllowed(origin.host, destination.host) {
			return refusedPolicyForward(policyRefused())
		}
		if restricted && input.ResumeSessionID != "" && s.relations != nil {
			if lane := s.relations.entries[value.From.SessionID]; lane != nil && !lane.oneWay && !s.relations.targeted(value.From.SessionID, input.ResumeSessionID) {
				// Slash-containing leaves can collide with a nested lane's
				// derived group. It proves access only to recorded counterparts.
				return refusedPolicyForward(policyRefused())
			}
		}
		relation = true // Only verified private groups reach owner creation.
		if restricted {
			ownPrivate, _, ok := s.relations.verified(value.From)
			if !ok {
				return refusedPolicyForward(policyRefused())
			}
			for _, group := range input.ExtraGroups {
				if strings.HasPrefix(group, "session:") {
					if group != ownPrivate {
						return refusedPolicyForward(policyRefused())
					}
				} else if !policy.groupAllowed(origin.host, destination.host, group) {
					return refusedPolicyForward(policyRefused())
				}
			}
		}
		if tracked {
			kind := policyFresh
			if input.ResumeSessionID != "" {
				kind = policyResume

			}
			admission = &policyAdmission{kind: kind, creator: value.From.SessionID, private: value.From.PrivateGroup, name: input.Name, id: input.ResumeSessionID}
		}
	case *protocol.LaneDescribeRequest:
		if !policy.laneAllowed(origin.host, destination.host) {
			return refusedPolicyForward(policyRefused())
		}
	case *protocol.TraceConfigureRequest:
		ownership = true
		if restricted && !s.relations.child(value.From.SessionID, input.SessionID) {
			return refusedPolicyForward(policyRefused())
		}
		relation = true
	case *protocol.MessageSendRequest:
		if input.Group != "" {
			if restricted && !policy.groupAllowed(origin.host, destination.host, input.Group) {
				return refusedPolicyForward(policyRefused())
			}
		} else if input.Target != "" {
			targets = []string{input.Target}
		} else {
			targets = input.Targets
		}
		if restricted && value.Request.TraceCopy != nil {
			traceCopy = s.relations.traceParent(origin.host, input.Target, value.From.PrivateGroup)
			if !traceCopy {
				return refusedPolicyForward(policyRefused())
			}
			relation = true
		}
	case *protocol.SessionListRequest:
		if input.SessionID != "" {
			targets = []string{input.SessionID}
		}
	case *protocol.TurnRunRequest:
		targets = []string{input.SessionID}
	case *protocol.ReadRequest:
		targets = []string{input.SessionID}
	case *protocol.WaitRequest:
		targets = []string{input.SessionID}
	case *protocol.RunRef:
		targets = []string{input.SessionID}
	case *protocol.SessionTarget:
		targets = []string{input.SessionID}
	case *protocol.SessionCloseRequest:
		targets = []string{input.SessionID}
		if input.Forget && s.relations != nil {
			if entry := s.relations.entries[input.SessionID]; entry != nil {
				admission = &policyAdmission{kind: policyForget, id: input.SessionID, entry: entry}
			}
		}
	}
	if restricted {
		if len(targets) != 0 && !traceCopy {
			counterparts := 0
			for _, target := range targets {
				if s.relations.targeted(value.From.SessionID, target) {
					counterparts++
				}
			}

			relation = counterparts == len(targets)
		}
		from, allowed, err := s.policyCaller(policy, origin.host, destination.host, value.From, relation, traceCopy)
		if err != nil {
			return refusedPolicyForward(errorReply(protocol.Internal, nil))
		}
		if !allowed {
			if value.Request.Method == "session.list" && len(targets) == 0 {
				raw, _ := protocol.EncodeResult("session.list", protocol.SessionListResult{Sessions: []protocol.SessionSummary{}})
				return refusedPolicyForward(Reply{Result: raw})
			}
			return refusedPolicyForward(policyRefused())
		}
		if ownership {
			// Ownership creation must not borrow a lane's parent's private
			// group, even when another recorded relation could authorize it.
			from.Groups = appendPolicyGroups(policy.namedGroups(origin.host, destination.host, from.Groups), []string{from.PrivateGroup})
		}
		value.From = from
	}
	// Only a private-only, canonical peer proof can reconstruct access. Named
	// visibility or an existing entry never creates an additional grant.
	if admission != nil && admission.kind == policyResume && restricted &&
		(s.relations == nil || s.relations.entries[admission.id] == nil) &&
		len(value.From.Groups) == 1 && value.From.Groups[0] == value.From.PrivateGroup &&
		value.From.PrivateGroup == "session:"+value.From.SessionID {
		admission.kind = policyResumeAccess
	}
	if admission != nil {
		if s.relations == nil {
			s.relations = newPolicyRelations()
		}
		admission.ctx, admission.registry = ctx, inbox
		admission.destination, admission.reply = destination, answer
		if !s.relations.admit(admission) {
			return refusedPolicyForward(policyRefused())
		}
		s.policyRosterFlag(destination.host)
	}
	result := policyForward{admission: admission}
	if restricted && value.Request.Method == "session.list" {
		result.reply = &policyReplyScope{policy: policy, source: origin.host, target: destination.host}
	}
	return result
}

func refusedPolicyForward(reply Reply) policyForward { return policyForward{refusal: &reply} }

// Projection at each reply hop preserves legacy callers.
func policyRefused() Reply { return errorReply(protocol.PolicyDenied, nil) }

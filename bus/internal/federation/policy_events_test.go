// SPDX-License-Identifier: GPL-3.0-only

package federation

import (
	"context"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestPolicyAuthoritativeCompletionPrecedesReplyAndMarkDoesNotRetire(t *testing.T) {
	for _, mode := range []string{"valid", "foreign", "duplicate", "marked", "link-ended"} {
		t.Run(mode, func(t *testing.T) {
			registry := make(chan registryEvent, 2)
			link := &hostLink{host: "beta"}
			s := hubState{links: map[string]*hostLink{"beta": link}, relations: newPolicyRelations()}
			reply := make(chan Reply, 1)
			admission := &policyAdmission{ctx: context.Background(), registry: registry, kind: policyFresh, destination: link, creator: "parent@alpha", private: "session:parent@alpha", name: "child", reply: reply}
			if !s.relations.admit(admission) {
				t.Fatal("admission refused")
			}
			call := pendingCall{forwarded: true, policy: admission, reply: reply}
			if mode == "duplicate" {
				s.relations.entries["child@beta"] = &policyRelation{lane: "child@beta", creator: "other@alpha"}
				s.relations.charges[policyPair{"alpha", "beta"}]++
			}
			if mode == "marked" {
				notifyCall(&call, errorReply(protocol.ForwardLost, nil))
				if first := <-reply; first.Error == nil || s.relations.charges[policyPair{"alpha", "beta"}] != 1 || !s.relations.pending[admission] {
					t.Fatal("mark was converted into authoritative retirement")
				}
			}
			actual := relationResult("child@beta")
			if mode == "foreign" {
				actual = relationResult("child@gamma")
			}
			if mode == "link-ended" {
				actual = errorReply(protocol.ForwardLost, nil)
				delete(s.links, "beta")
			}
			settleCall(&call, actual)
			select {
			case value := <-reply:
				t.Fatal("raw success escaped before registry verification", value)
			default:
			}
			event := <-registry
			s.handle(context.Background(), registry, event)
			if s.relations.pending[admission] {
				t.Fatal("authoritative completion did not retire its token")
			}
			if mode == "marked" {
				select {
				case value := <-reply:
					t.Fatal("late completion produced a second public outcome", value)
				default:
				}
			} else {
				value := <-reply
				if (value.Error == nil) != (mode == "valid") {
					t.Fatal("foreign/colliding/ended attempt became public success", mode, value)
				}
			}
			if mode == "valid" || mode == "marked" {
				if !s.relations.counterpart("parent@alpha", "child@beta") || s.relations.charges[policyPair{"alpha", "beta"}] != 1 {
					t.Fatal("valid late identity grant was not recorded")
				}
			} else if mode == "duplicate" {
				if s.relations.entries["child@beta"].creator != "other@alpha" || s.relations.charges[policyPair{"alpha", "beta"}] != 1 {
					t.Fatal("collision changed the previous grant")
				}
			} else if len(s.relations.entries) != 0 || s.relations.charges[policyPair{"alpha", "beta"}] != 0 {
				t.Fatal("failed admission retained a grant or charge")
			}
			s.handle(context.Background(), registry, event)
			if len(reply) != 0 {
				t.Fatal("duplicate completion answered twice")
			}
		})
	}
}

func TestPolicyRosterRejectsStaleAttachmentAndIncompleteData(t *testing.T) {
	old, current := &hostLink{host: "beta"}, &hostLink{host: "beta"}
	when := time.Now()
	s := hubState{links: map[string]*hostLink{"beta": current}, relations: newPolicyRelations()}
	s.relations.entries["child@beta"] = &policyRelation{lane: "child@beta", creator: "parent@alpha", recorded: when}
	s.relations.charges[policyPair{"alpha", "beta"}] = 1
	complete := Reply{Result: []byte(`{"hosts":[{"host":"beta","products":[],"sessions":[]}]}`)}
	s.observePolicyRoster(old, policyRosterObservation{selected: when.Add(time.Second), reply: complete})
	s.observePolicyRoster(current, policyRosterObservation{selected: when.Add(-time.Second), reply: complete})
	s.observePolicyRoster(current, policyRosterObservation{selected: when.Add(time.Second), reply: Reply{Result: []byte(`{"hosts":[]}`)}})
	if s.relations.entries["child@beta"] == nil {
		t.Fatal("stale/incomplete roster pruned a current relation")
	}
	s.observePolicyRoster(current, policyRosterObservation{selected: when.Add(time.Second), reply: complete})
	if s.relations.entries["child@beta"] != nil || s.relations.charges[policyPair{"alpha", "beta"}] != 0 || current.liveness.observeRoster.Load() {
		t.Fatal("fresh complete current roster did not retire observation/entry")
	}
}

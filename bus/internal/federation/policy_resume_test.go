// SPDX-License-Identifier: GPL-3.0-only
package federation

import (
	"context"
	"testing"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestPolicyPrivateResumeOneWayAdmissionAndBoundaries(t *testing.T) {
	origin, target := &hostLink{host: "alpha"}, &hostLink{host: "beta"}
	inbox := make(chan registryEvent, 4)
	for _, mode := range []string{"private", "named", "existing", "lanes-only", "none", "foreign", "duplicate", "failed", "marked", "ended"} {
		t.Run(mode, func(t *testing.T) {
			s := hubState{links: map[string]*hostLink{"alpha": origin, "beta": target}, policy: testPolicy(t, `{"hosts":{"alpha":{"groups":["team"]}}}`)}
			caller := policyTestCaller()
			caller.Groups = []string{caller.PrivateGroup}
			if mode == "named" {
				caller.Groups = append(caller.Groups, "team")
			}
			if mode == "none" {
				s.policy = nil
			}
			if mode == "lanes-only" {
				s.policy = testPolicy(t, `{"hosts":{"alpha":{"lanes_to":["beta"]}}}`)
			}
			if mode == "existing" {
				s.relations = newPolicyRelations()
				s.relations.entries["child@beta"] = &policyRelation{lane: "child@beta", creator: "another@alpha", private: "session:another@alpha/child", parentPrivate: "session:another@alpha"}
				s.relations.charges["beta"] = 1
			}
			answer := make(chan Reply, 1)
			value := Forward{From: caller, Request: policyTestRequest(t, "lane.spawn", protocol.LaneSpawnRequest{ResumeSessionID: "child@beta"})}
			decision := s.preparePolicyForward(context.Background(), inbox, origin, target, &value, answer)
			if decision.refusal != nil {
				t.Fatal("independent or eligible resume refused", decision.refusal)
			}
			if mode == "none" {
				if s.relations != nil || decision.admission != nil {
					t.Fatal("unused policy did work")
				}
				return
			}
			a := decision.admission
			if a == nil {
				t.Fatal("missing admission")
			}
			proof := mode != "named" && mode != "lanes-only" && mode != "existing"
			if (a.kind == policyResumeAccess) != proof {
				t.Fatal("wrong proof eligibility", a.kind)
			}
			if proof && s.relations.charges["beta"] != 1 {
				t.Fatal("proof dispatched before charged slot")
			}
			if mode == "duplicate" {
				s.relations.entries["child@beta"] = &policyRelation{lane: "child@beta", creator: "other@alpha"}
				s.relations.charges["beta"]++
			}
			result := relationResult("child@beta")
			if mode == "foreign" {
				result = relationResult("child@gamma")
			}
			if mode == "failed" {
				result = errorReply(protocol.ForwardLost, nil)
			}
			if mode == "ended" {
				delete(s.links, "beta")
			}
			call := pendingCall{policy: a, reply: answer}
			if mode == "marked" {
				notifyCall(&call, errorReply(protocol.ForwardLost, nil))
				<-answer
			}
			settleCall(&call, result)
			select {
			case <-answer:
				t.Fatal("unverified authoritative success escaped")
			default:
			}
			s.completePolicy(*(<-inbox).completion)
			failed := mode == "foreign" || mode == "duplicate" || mode == "failed" || mode == "ended"
			if mode != "marked" {
				reply := <-answer
				if (reply.Error != nil) != failed {
					t.Fatal("wrong caller-visible outcome", reply)
				}
			} else if len(answer) != 0 {
				t.Fatal("late success answered twice")
			}
			if failed {
				if mode != "duplicate" && s.relations.charges["beta"] != 0 {
					t.Fatal("failed proof leaked slot")
				}
				return
			}
			if !proof {
				if mode != "existing" && len(s.relations.entries) != 0 {
					t.Fatal("named or lanes-only granted access")
				}
				return
			}
			entry := s.relations.entries["child@beta"]
			if entry == nil || !entry.oneWay || entry.private != "" || !s.relations.targeted(caller.SessionID, entry.lane) || s.relations.targeted(entry.lane, caller.SessionID) || s.relations.counterpart(caller.SessionID, entry.lane) {
				t.Fatal("directed access became symmetric or invented private group", entry)
			}
			if s.relations.traceParent("beta", caller.SessionID, caller.PrivateGroup) || len(s.relations.transferSnapshot(caller.SessionID, "beta")) != 0 || snapshotCounterpart([]*policyRelation{entry}, caller.SessionID, entry.lane) {
				t.Fatal("one-way relation escaped reverse trace/transfer exclusions")
			}
			if _, _, ok := s.relations.verified(Caller{SessionID: entry.lane, PrivateGroup: caller.PrivateGroup + "/child"}); ok {
				t.Fatal("guessed derived identity verified")
			}
			if _, again := s.relations.complete(a, result, true); again || s.relations.charges["beta"] != 1 {
				t.Fatal("duplicate completion changed charge")
			}
		})
	}
}

func TestPolicyOneWayAllExplicitTargetsAndNoEnumeration(t *testing.T) {
	hub, address, secrets := testHub(t)
	if !hub.ReloadPolicy(testPolicy(t, `{"hosts":{"alpha":{"groups":[]}}}`)) {
		t.Fatal("reload")
	}
	seen := make(chan IncomingCall, 32)
	beta := connectTestDaemon(t, address, "beta", secrets["beta"], func(call IncomingCall) (Wait, error) {
		seen <- call
		if call.Request.Method == "lane.spawn" {
			return immediate(relationResult("child@beta")), nil
		}
		return immediate(errorReply(protocol.Busy, nil)), nil
	})
	_ = callHosts(t, beta)
	alpha := connectTestDaemon(t, address, "alpha", secrets["alpha"], func(IncomingCall) (Wait, error) { return immediate(emptyReply()), nil })
	_ = callHosts(t, alpha)
	caller := policyTestCaller()
	caller.Groups = []string{caller.PrivateGroup}
	if r := policyTestForward(t, alpha, caller, policyTestRequest(t, "lane.spawn", protocol.LaneSpawnRequest{ResumeSessionID: "child@beta"})); r.Error != nil {
		t.Fatal(r.Error)
	}
	policyTestSeen(t, seen)
	methods := []struct {
		method string
		params any
	}{
		{"turn.run", protocol.TurnRunRequest{SessionID: "child@beta", Input: "continue"}},
		{"turn.start", protocol.TurnRunRequest{SessionID: "child@beta", Input: "continue"}},
		{"turn.status", protocol.ReadRequest{SessionID: "child@beta"}},
		{"turn.wait", protocol.WaitRequest{SessionID: "child@beta"}},
		{"turn.ack", protocol.RunRef{SessionID: "child@beta", RunID: "run-1"}},
		{"turn.interrupt", protocol.SessionTarget{SessionID: "child@beta"}},
		{"session.close", protocol.SessionCloseRequest{SessionID: "child@beta"}},
		{"session.list", protocol.SessionListRequest{SessionID: "child@beta"}},
		{"trace.configure", protocol.TraceConfigureRequest{SessionID: "child@beta", Mode: "off"}},
		{"message.send", protocol.MessageSendRequest{Target: "child@beta", Message: "continue"}},
		{"message.send", protocol.MessageSendRequest{Targets: []string{"child@beta"}, Message: "continue"}},
	}
	for _, test := range methods {
		reply := policyTestForward(t, alpha, caller, policyTestRequest(t, test.method, test.params))
		if reply.Error == nil || reply.Error.Code != protocol.Busy {
			t.Fatal("targeted directed call did not reach destination", test.method, reply)
		}
		forwarded := policyTestSeen(t, seen)
		if forwarded.From.PrivateGroup != caller.PrivateGroup {
			t.Fatal("directed relation randomized private identity")
		}
	}
	mixed := policyTestRequest(t, "message.send", protocol.MessageSendRequest{Targets: []string{"child@beta", "other@beta"}, Message: "no shared named group"})
	if r := policyTestForward(t, alpha, caller, mixed); r.Error == nil || r.Error.Code != protocol.UnknownSession {
		t.Fatal("mixed targets borrowed relation", r)
	}
	policyTestNotSeen(t, seen)
	broad := policyTestForward(t, alpha, caller, policyTestRequest(t, "session.list", protocol.SessionListRequest{Host: "beta"}))
	if broad.Error != nil {
		t.Fatal(broad.Error)
	}
	decoded, err := protocol.DecodeResult("session.list", broad.Result)
	if err != nil || len(decoded.(*protocol.SessionListResult).Sessions) != 0 {
		t.Fatal("broad list enumerated private child", broad, err)
	}
	policyTestNotSeen(t, seen)
}

func TestPolicyOneWayUsesSameLiveAndInflightCap(t *testing.T) {
	r := newPolicyRelations()
	link := &hostLink{host: "beta"}
	for range policyRelationsPerHost - 1 {
		if !r.admit(&policyAdmission{kind: policyFresh, destination: link}) {
			t.Fatal("early cap")
		}
	}
	a := &policyAdmission{kind: policyResumeAccess, destination: link, creator: "access@alpha", private: "session:access@alpha", id: "lane@beta"}
	if !r.admit(a) || r.charges["beta"] != policyRelationsPerHost {
		t.Fatal("resume not charged with inflight spawns")
	}
	if r.admit(&policyAdmission{kind: policyResumeAccess, destination: link}) || r.admit(&policyAdmission{kind: policyFresh, destination: link}) {
		t.Fatal("live/inflight shared cap bypass")
	}
	if got, once := r.complete(a, errorReply(protocol.ForwardLost, nil), true); !once || got.Error == nil || r.charges["beta"] != policyRelationsPerHost-1 {
		t.Fatal("failed proof charge not released")
	}
	if _, again := r.complete(a, errorReply(protocol.ForwardLost, nil), true); again || r.charges["beta"] != policyRelationsPerHost-1 {
		t.Fatal("failed proof released twice")
	}
	if !r.admit(&policyAdmission{kind: policyResumeAccess, destination: link}) {
		t.Fatal("released slot not reusable")
	}
}

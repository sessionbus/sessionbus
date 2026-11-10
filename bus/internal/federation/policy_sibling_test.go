// SPDX-License-Identifier: GPL-3.0-only
package federation

import (
	"context"
	"slices"
	"testing"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestPolicyRecordedLaneCannotResumeSiblingThroughParentGroup(t *testing.T) {
	hub, address, secrets := testHub(t)
	if !hub.ReloadPolicy(testPolicy(t, `{"hosts":{"alpha":{"groups":[]},"beta":{"groups":[]}}}`)) {
		t.Fatal("reload")
	}
	seen := make(chan IncomingCall, 4)
	beta := connectTestDaemon(t, address, "beta", secrets["beta"], func(call IncomingCall) (Wait, error) {
		seen <- call
		return immediate(relationResult("left@beta")), nil
	})
	parent := policyTestCaller()
	parent.Groups = []string{parent.PrivateGroup}
	alpha := connectTestDaemon(t, address, "alpha", secrets["alpha"], func(call IncomingCall) (Wait, error) {
		seen <- call
		// This is the destination's shared-group resume rule, using the
		// creation group of the parent's unrelated offline victim lane.
		if slices.Contains(call.From.Groups, parent.PrivateGroup) {
			return immediate(relationResult("victim@alpha")), nil
		}
		return immediate(errorReply(protocol.UnknownSession, nil)), nil
	})
	_ = callHosts(t, alpha)
	_ = callHosts(t, beta)
	spawn := policyTestRequest(t, "lane.spawn", protocol.LaneSpawnRequest{Host: "beta", Name: "left", Product: "fixture-worker", Open: &protocol.OpenOptions{}})
	if reply := policyTestForward(t, alpha, parent, spawn); reply.Error != nil {
		t.Fatal("parent spawn", reply.Error)
	}
	policyTestSeen(t, seen)
	child := Caller{SessionID: "left@beta", Product: "peer", Name: "Parent/left@beta", OwnerLifetime: "left-life", PrivateGroup: parent.PrivateGroup + "/left", Groups: []string{parent.PrivateGroup, parent.PrivateGroup + "/left"}}
	resume := policyTestRequest(t, "lane.spawn", protocol.LaneSpawnRequest{ResumeSessionID: "victim@alpha"})
	reply := policyTestForward(t, beta, child, resume)
	if reply.Error == nil || reply.Error.Code != protocol.UnknownSession {
		t.Fatal("recorded child borrowed parent group for unrelated resume", reply)
	}
	policyTestNotSeen(t, seen)
}

func TestPolicyOwnershipExtraCannotReintroduceParentPrivate(t *testing.T) {
	parent := "session:parent@alpha"
	child := Caller{SessionID: "left@beta", PrivateGroup: parent + "/left", Groups: []string{parent, parent + "/left"}}
	s := hubState{policy: testPolicy(t, `{"hosts":{"beta":{"groups":[]}}}`), relations: newPolicyRelations()}
	s.relations.entries[child.SessionID] = &policyRelation{lane: child.SessionID, creator: "parent@alpha", parentPrivate: parent, private: child.PrivateGroup}
	request := policyTestRequest(t, "lane.spawn", protocol.LaneSpawnRequest{Host: "gamma", Name: "grandchild", Product: "fixture-worker", Open: &protocol.OpenOptions{}, ExtraGroups: []string{parent}})
	value := Forward{From: child, Request: request}
	decision := s.preparePolicyForward(context.Background(), make(chan registryEvent, 1), &hostLink{host: "beta"}, &hostLink{host: "gamma"}, &value, make(chan Reply, 1))
	if decision.refusal == nil || decision.refusal.Error == nil || decision.admission != nil {
		t.Fatal("ownership extra_groups reintroduced borrowed parent group", value.From, decision)
	}
}

func TestPolicyDerivedGroupCollisionCannotResumeUnrelatedLane(t *testing.T) {
	for _, named := range []bool{false, true} {
		t.Run(map[bool]string{false: "private-only", true: "named"}[named], func(t *testing.T) {
			hub, address, secrets := testHub(t)
			if !hub.ReloadPolicy(testPolicy(t, `{"hosts":{"alpha":{"groups":["team"]},"beta":{"groups":["team"]},"gamma":{"groups":["team"]}}}`)) {
				t.Fatal("reload")
			}
			seen := make(chan IncomingCall, 8)
			parent := policyTestCaller()
			parent.Groups = []string{parent.PrivateGroup}
			admit := func(host string) func(IncomingCall) (Wait, error) {
				return func(call IncomingCall) (Wait, error) {
					seen <- call
					input, _ := protocol.DecodeParams(call.Request.Method, call.Request.Params)
					if trace, ok := input.(*protocol.TraceConfigureRequest); ok {
						raw, _ := protocol.EncodeResult("trace.configure", protocol.TraceConfigureResult{SessionID: trace.SessionID, Mode: trace.Mode})
						return immediate(Reply{Result: raw}), nil
					}
					spawn := input.(*protocol.LaneSpawnRequest)
					if spawn.ResumeSessionID != "" {
						if spawn.ResumeSessionID == "grand@gamma" && slices.Contains(call.From.Groups, parent.PrivateGroup+"/left") {
							return immediate(relationResult(spawn.ResumeSessionID)), nil
						}
						// A legal local slash leaf 'left/grand' has this same
						// creation group as the cross-host nested lane's OWN group.
						if slices.Contains(call.From.Groups, parent.PrivateGroup+"/left/grand") || named && slices.Contains(call.From.Groups, "team") {
							return immediate(relationResult("victim@alpha")), nil
						}
						return immediate(errorReply(protocol.UnknownSession, nil)), nil
					}
					return immediate(relationResult(spawn.Name + "@" + host)), nil
				}
			}
			alpha := connectTestDaemon(t, address, "alpha", secrets["alpha"], admit("alpha"))
			beta := connectTestDaemon(t, address, "beta", secrets["beta"], admit("beta"))
			gamma := connectTestDaemon(t, address, "gamma", secrets["gamma"], admit("gamma"))
			for _, daemon := range []*testDaemon{alpha, beta, gamma} {
				_ = callHosts(t, daemon)
			}
			spawn := policyTestRequest(t, "lane.spawn", protocol.LaneSpawnRequest{Host: "beta", Name: "left", Product: "fixture-worker", Open: &protocol.OpenOptions{}})
			if reply := policyTestForward(t, alpha, parent, spawn); reply.Error != nil {
				t.Fatal("parent spawn", reply.Error)
			}
			policyTestSeen(t, seen)
			left := Caller{SessionID: "left@beta", Product: "peer", Name: "Parent/left@beta", OwnerLifetime: "left-life", PrivateGroup: parent.PrivateGroup + "/left", Groups: []string{parent.PrivateGroup, parent.PrivateGroup + "/left"}}
			spawn = policyTestRequest(t, "lane.spawn", protocol.LaneSpawnRequest{Host: "gamma", Name: "grand", Product: "fixture-worker", Open: &protocol.OpenOptions{}})
			if reply := policyTestForward(t, beta, left, spawn); reply.Error != nil {
				t.Fatal("fresh nested spawn", reply.Error)
			}
			policyTestSeen(t, seen)
			grand := Caller{SessionID: "grand@gamma", Product: "peer", PrivateGroup: left.PrivateGroup + "/grand", Groups: []string{left.PrivateGroup, left.PrivateGroup + "/grand"}}
			if named {
				grand.Groups = append(grand.Groups, "team")
			}
			resume := policyTestRequest(t, "lane.spawn", protocol.LaneSpawnRequest{ResumeSessionID: "victim@alpha"})
			if reply := policyTestForward(t, gamma, grand, resume); reply.Error == nil || reply.Error.Code == protocol.ForwardLost {
				t.Fatal("derived collision admitted unrelated resume", reply)
			}
			policyTestNotSeen(t, seen)
			resume = policyTestRequest(t, "lane.spawn", protocol.LaneSpawnRequest{ResumeSessionID: "grand@gamma"})
			if reply := policyTestForward(t, beta, left, resume); reply.Error != nil {
				t.Fatal("recorded lane lost own-child resume", reply.Error)
			}
			call := policyTestSeen(t, seen)
			if !slices.Equal(call.From.Groups, []string{left.PrivateGroup}) {
				t.Fatal("own-child ownership borrowed parent private", call.From)
			}
			trace := policyTestRequest(t, "trace.configure", protocol.TraceConfigureRequest{SessionID: "grand@gamma", Mode: "events"})
			if reply := policyTestForward(t, beta, left, trace); reply.Error != nil {
				t.Fatal("recorded lane lost own-child trace", reply.Error)
			}
			call = policyTestSeen(t, seen)
			if !slices.Equal(call.From.Groups, []string{left.PrivateGroup}) {
				t.Fatal("own-child trace borrowed parent private", call.From)
			}
		})
	}
}

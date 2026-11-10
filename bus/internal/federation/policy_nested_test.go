// SPDX-License-Identifier: GPL-3.0-only

package federation

import (
	"slices"
	"testing"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestPolicyActualNestedImmediateHostsAndVerifiedPrivateExtras(t *testing.T) {
	hub, address, secrets := testHub(t)
	if !hub.ReloadPolicy(testPolicy(t, `{"hosts":{"alpha":{"groups":[],"lanes_to":["beta"]},"beta":{"groups":[],"lanes_on":["alpha"],"lanes_to":["gamma"]},"gamma":{"groups":[],"lanes_on":["beta"]}}}`)) {
		t.Fatal("reload failed")
	}
	seen := make(chan IncomingCall, 16)
	connect := func(host string) *testDaemon {
		return connectTestDaemon(t, address, host, secrets[host], func(call IncomingCall) (Wait, error) {
			seen <- call
			if call.Request.Method == "lane.spawn" {
				params, _ := protocol.DecodeParams("lane.spawn", call.Request.Params)
				return immediate(relationResult(params.(*protocol.LaneSpawnRequest).Name + "@" + host)), nil
			}
			if call.Request.Method == "message.send" {
				return immediate(Reply{Result: []byte(`{"message_id":"policy-test-message","deliveries":[]}`)}), nil
			}
			return immediate(emptyReply()), nil
		})
	}
	beta, gamma, alpha := connect("beta"), connect("gamma"), connect("alpha")
	for _, daemon := range []*testDaemon{alpha, beta, gamma} {
		_ = callHosts(t, daemon)
	}
	root := policyTestCaller()
	spawn := policyTestRequest(t, "lane.spawn", protocol.LaneSpawnRequest{Host: "beta", Name: "child", Product: "fixture-worker", Open: &protocol.OpenOptions{}})
	if reply := policyTestForward(t, alpha, root, spawn); reply.Error != nil {
		t.Fatal("first hop failed", reply.Error)
	}
	policyTestSeen(t, seen)
	child := Caller{SessionID: "child@beta", Product: "peer", Name: "Parent/child@beta", PrivateGroup: root.PrivateGroup + "/child", Groups: []string{root.PrivateGroup, root.PrivateGroup + "/child", "session:victim@gamma"}}
	spawn = policyTestRequest(t, "lane.spawn", protocol.LaneSpawnRequest{Host: "gamma", Name: "grandchild", Product: "fixture-worker", Open: &protocol.OpenOptions{}, ExtraGroups: []string{child.PrivateGroup}})
	if reply := policyTestForward(t, beta, child, spawn); reply.Error != nil {
		t.Fatal("valid nested immediate-host hop/private extra refused", reply.Error)
	}
	call := policyTestSeen(t, seen)
	if call.From.PrivateGroup != child.PrivateGroup || !slices.Equal(call.From.Groups, []string{child.PrivateGroup}) {
		t.Fatal("nested identity lost its verified groups or gained a forged one", call.From)
	}
	grandchild := Caller{SessionID: "grandchild@gamma", Product: "peer", PrivateGroup: child.PrivateGroup + "/grandchild", Groups: []string{child.PrivateGroup, child.PrivateGroup + "/grandchild"}}
	for _, test := range []struct {
		origin *testDaemon
		caller Caller
		target string
	}{{gamma, grandchild, child.SessionID}, {beta, child, root.SessionID}, {alpha, root, child.SessionID}, {beta, child, grandchild.SessionID}} {
		request := policyTestRequest(t, "turn.interrupt", protocol.SessionTarget{SessionID: test.target})
		if reply := policyTestForward(t, test.origin, test.caller, request); reply.Error != nil {
			t.Fatal("recorded direct counterpart lost its relation", test.caller.SessionID, test.target, reply.Error)
		}
		policyTestSeen(t, seen)
	}
	// The grandparent does not acquire a three-host ancestry grant.
	request := policyTestRequest(t, "turn.interrupt", protocol.SessionTarget{SessionID: grandchild.SessionID})
	if reply := policyTestForward(t, alpha, root, request); reply.Error == nil || reply.Error.Code != protocol.UnknownSession {
		t.Fatal("unrecorded grandparent relation escaped the immediate-host rule", reply)
	}
	// A local colleague lane cannot borrow the verified private group of a
	// different ID, nor smuggle it into a child's creation groups.
	unverified := child
	unverified.SessionID, unverified.Name = "local-lane@beta", "Local@beta"
	if reply := policyTestForward(t, beta, unverified, spawn); reply.Error == nil || reply.Error.Code == protocol.ForwardLost {
		t.Fatal("unverifiable local lane created ownership", reply)
	}
	forged := policyTestRequest(t, "lane.spawn", protocol.LaneSpawnRequest{Host: "gamma", Name: "forged", Product: "fixture-worker", Open: &protocol.OpenOptions{}, ExtraGroups: []string{"session:victim@gamma"}})
	if reply := policyTestForward(t, beta, child, forged); reply.Error == nil || reply.Error.Code == protocol.ForwardLost {
		t.Fatal("spawn extra_groups created an unverified private grant", reply)
	}
	policyTestNotSeen(t, seen)
	borrowed := policyTestRequest(t, "lane.spawn", protocol.LaneSpawnRequest{Host: "gamma", Name: "borrowed", Product: "fixture-worker", Open: &protocol.OpenOptions{}, ExtraGroups: []string{root.PrivateGroup}})
	if reply := policyTestForward(t, beta, child, borrowed); reply.Error == nil || reply.Error.Code == protocol.ForwardLost {
		t.Fatal("ownership extra_groups borrowed the parent's private group", reply)
	}
	policyTestNotSeen(t, seen)
}

func TestPolicyActualTraceConfigureAndTraceCopyUseRecordedChild(t *testing.T) {
	hub, address, secrets := testHub(t)
	if !hub.ReloadPolicy(testPolicy(t, `{"hosts":{"alpha":{"groups":[]}}}`)) {
		t.Fatal("reload failed")
	}
	seen := make(chan IncomingCall, 12)
	admit := func(call IncomingCall) (Wait, error) {
		seen <- call
		switch call.Request.Method {
		case "lane.spawn":
			return immediate(relationResult("child@beta")), nil
		case "trace.configure":
			params, _ := protocol.DecodeParams("trace.configure", call.Request.Params)
			input := params.(*protocol.TraceConfigureRequest)
			body, _ := protocol.EncodeResult("trace.configure", protocol.TraceConfigureResult{SessionID: input.SessionID, Mode: input.Mode})
			return immediate(Reply{Result: body}), nil
		case "message.send":
			return immediate(Reply{Result: []byte(`{"message_id":"policy-test-message","deliveries":[]}`)}), nil
		}
		return immediate(emptyReply()), nil
	}
	beta := connectTestDaemon(t, address, "beta", secrets["beta"], admit)
	alpha := connectTestDaemon(t, address, "alpha", secrets["alpha"], admit)
	_ = callHosts(t, beta)
	_ = callHosts(t, alpha)
	parent := policyTestCaller()
	spawn := policyTestRequest(t, "lane.spawn", protocol.LaneSpawnRequest{Host: "beta", Name: "child", Product: "fixture-worker", Open: &protocol.OpenOptions{}})
	if reply := policyTestForward(t, alpha, parent, spawn); reply.Error != nil {
		t.Fatal(reply.Error)
	}
	policyTestSeen(t, seen)
	for _, mode := range []string{"off", "events", "content"} {
		request := policyTestRequest(t, "trace.configure", protocol.TraceConfigureRequest{SessionID: "child@beta", Mode: mode})
		if reply := policyTestForward(t, alpha, parent, request); reply.Error != nil {
			t.Fatal("creator trace mode was refused", mode, reply.Error)
		}
		if call := policyTestSeen(t, seen); call.From.PrivateGroup != parent.PrivateGroup {
			t.Fatal("trace owner was randomized")
		}
		request = policyTestRequest(t, "trace.configure", protocol.TraceConfigureRequest{SessionID: "unrelated@beta", Mode: mode})
		if reply := policyTestForward(t, alpha, parent, request); reply.Error == nil || reply.Error.Code == protocol.ForwardLost {
			t.Fatal("unrecorded trace target created ownership", mode, reply)
		}
	}
	trace := Caller{SessionID: "trace@beta", Product: "sessionbus", PrivateGroup: parent.PrivateGroup, Groups: []string{parent.PrivateGroup, "session:victim@alpha"}}
	request := policyTestRequest(t, "message.send", protocol.MessageSendRequest{Target: parent.SessionID, Message: "fixed trace fixture"})
	request.TraceCopy = &TraceDestination{SessionID: parent.SessionID, Lifetime: parent.OwnerLifetime}
	if reply := policyTestForward(t, beta, trace, request); reply.Error != nil {
		t.Fatal("source-host recorded-parent trace copy failed", reply.Error)
	}
	if call := policyTestSeen(t, seen); len(call.From.Groups) != 1 || call.From.Groups[0] != parent.PrivateGroup {
		t.Fatal("trace copy carried unverified private groups", call.From)
	}
	request.TraceCopy = nil
	if reply := policyTestForward(t, beta, trace, request); reply.Error == nil || reply.Error.Code != protocol.UnknownSession {
		t.Fatal("synthetic source acquired a general private grant", reply)
	}
	policyTestNotSeen(t, seen)
}

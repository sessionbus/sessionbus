// SPDX-License-Identifier: GPL-3.0-only

package federation

import (
	"bytes"
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/roster"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func policyTestCaller() Caller {
	return Caller{SessionID: "parent@alpha", Name: "Parent@alpha", Product: "peer", OwnerLifetime: "owner-generation", PrivateGroup: "session:parent@alpha", Groups: []string{"team", "other", "session:parent@alpha", "session:victim@gamma"}}
}

func policyTestRequest(t *testing.T, method string, params any) PublicRequest {
	t.Helper()
	raw, err := protocol.EncodeParams(method, params)
	if err != nil {
		t.Fatal(err)
	}
	request := PublicRequest{Method: method, Params: raw}
	if method == "message.send" {
		request.MessageID = "policy-test-message"
	}
	return request
}

func policyTestForward(t *testing.T, daemon *testDaemon, caller Caller, request PublicRequest) Reply {
	t.Helper()
	reply := make(chan Reply, 1)
	daemon.inbox <- OutgoingCall{Value: Forward{From: caller, Request: request}, Reply: reply}
	select {
	case result := <-reply:
		return result
	case <-time.After(2 * time.Second):
		t.Fatal("policy call did not settle")
		return Reply{}
	}
}

func policyTestSeen(t *testing.T, seen <-chan IncomingCall) IncomingCall {
	t.Helper()
	select {
	case call := <-seen:
		return call
	case <-time.After(time.Second):
		t.Fatal("allowed call did not reach destination")
		return IncomingCall{}
	}
}

func policyTestNotSeen(t *testing.T, seen <-chan IncomingCall) {
	t.Helper()
	select {
	case call := <-seen:
		t.Fatal("refused call reached destination", call.Request.Method)
	default:
	}
}

func TestPolicyActualForwardUnusedRulesPreserveEnvelopeAndBytes(t *testing.T) {
	for _, rawPolicy := range []string{"", `{"hosts":{"gamma":{"groups":[]}}}`, `{"hosts":{"alpha":{}}}`, `{"hosts":{"alpha":{"lanes_to":["beta"]}}}`} {
		t.Run(rawPolicy, func(t *testing.T) {
			hub, address, secrets := testHub(t)
			if rawPolicy != "" && !hub.ReloadPolicy(testPolicy(t, rawPolicy)) {
				t.Fatal("reload failed")
			}
			seen := make(chan IncomingCall, 2)
			beta := connectTestDaemon(t, address, "beta", secrets["beta"], func(call IncomingCall) (Wait, error) {
				seen <- call
				return immediate(emptyReply()), nil
			})
			_ = callHosts(t, beta)
			alpha := connectTestDaemon(t, address, "alpha", secrets["alpha"], func(IncomingCall) (Wait, error) { return immediate(emptyReply()), nil })
			_ = callHosts(t, alpha)
			caller := policyTestCaller()
			request := policyTestRequest(t, "turn.interrupt", protocol.SessionTarget{SessionID: "child@beta"})
			if reply := policyTestForward(t, alpha, caller, request); reply.Error != nil {
				t.Fatal(reply.Error)
			}
			call := policyTestSeen(t, seen)
			if call.From.SourceAttachment == "" {
				t.Fatal("baseline authenticated attachment was lost")
			}
			call.From.SourceAttachment = ""
			if !reflect.DeepEqual(call.From, caller) || !bytes.Equal(call.Request.Params, request.Params) {
				t.Fatal("unused/group-independent rules changed the ordinary envelope", call.From)
			}
		})
	}
}

func TestPolicyActualForwardClipsNamedAndUnverifiedPrivateGroups(t *testing.T) {
	hub, address, secrets := testHub(t)
	if !hub.ReloadPolicy(testPolicy(t, `{"hosts":{"alpha":{"groups":["team"]},"beta":{"groups":["team"]}}}`)) {
		t.Fatal("reload failed")
	}
	seen := make(chan IncomingCall, 8)
	beta := connectTestDaemon(t, address, "beta", secrets["beta"], func(call IncomingCall) (Wait, error) {
		seen <- call
		if call.Request.Method == "session.list" {
			decoded, err := protocol.DecodeParams("session.list", call.Request.Params)
			if err != nil {
				t.Fatal(err)
			}
			if decoded.(*protocol.SessionListRequest).SessionID == "" {
				raw, err := protocol.EncodeResult("session.list", protocol.SessionListResult{Sessions: []protocol.SessionSummary{}, Hosts: []protocol.HostProducts{{Host: "beta", Products: []string{"fixture"}}}})
				if err != nil {
					t.Fatal(err)
				}
				return immediate(Reply{Result: raw}), nil
			}
		}
		if call.Request.Method == "message.send" {
			return immediate(Reply{Result: []byte(`{"message_id":"policy-test-message","deliveries":[]}`)}), nil
		}
		return immediate(emptyReply()), nil
	})
	_ = callHosts(t, beta)
	alpha := connectTestDaemon(t, address, "alpha", secrets["alpha"], func(IncomingCall) (Wait, error) { return immediate(emptyReply()), nil })
	_ = callHosts(t, alpha)
	caller := policyTestCaller()
	before, _ := json.Marshal(caller)
	request := policyTestRequest(t, "turn.interrupt", protocol.SessionTarget{SessionID: "child@beta"})
	var previous string
	for range 2 {
		if reply := policyTestForward(t, alpha, caller, request); reply.Error != nil {
			t.Fatal(reply.Error)
		}
		call := policyTestSeen(t, seen)
		if !validCaller(call.From, "alpha") || len(call.From.Groups) != 2 || !slices.Contains(call.From.Groups, "team") || call.From.PrivateGroup == caller.PrivateGroup || call.From.PrivateGroup == previous || !strings.HasPrefix(call.From.PrivateGroup, "session:parent@alpha#") {
			t.Fatal("policy left an unverified grant or reused the private nonce", call.From)
		}
		previous = call.From.PrivateGroup
		if call.From.SessionID != caller.SessionID || call.From.Product != caller.Product || call.From.OwnerLifetime != caller.OwnerLifetime || !bytes.Equal(call.Request.Params, request.Params) {
			t.Fatal("group filtering altered ordinary identity or input")
		}
	}
	after, _ := json.Marshal(caller)
	if !bytes.Equal(before, after) {
		t.Fatal("policy mutated the source caller")
	}
	caller.Groups = []string{"other", caller.PrivateGroup}
	if reply := policyTestForward(t, alpha, caller, request); reply.Error == nil || reply.Error.Code != protocol.UnknownSession {
		t.Fatal("hidden control was not unknown", reply)
	}
	listed := policyTestForward(t, alpha, caller, policyTestRequest(t, "session.list", protocol.SessionListRequest{Host: "beta"}))
	decoded, err := protocol.DecodeResult("session.list", listed.Result)
	if listed.Error != nil || err != nil || len(decoded.(*protocol.SessionListResult).Sessions) != 0 || len(decoded.(*protocol.SessionListResult).Hosts) != 1 {
		t.Fatal("metadata-only list lost destination inventory or exposed sessions", listed, err)
	}
	forwardedList := policyTestSeen(t, seen)
	if len(forwardedList.From.Groups) != 1 || forwardedList.From.PrivateGroup == caller.PrivateGroup || forwardedList.From.Groups[0] != forwardedList.From.PrivateGroup {
		t.Fatal("metadata-only list borrowed shared private visibility", forwardedList.From)
	}
	if hosts := decoded.(*protocol.SessionListResult).Hosts; hosts[0].Host != "beta" || !slices.Equal(hosts[0].Products, []string{"fixture"}) {
		t.Fatal("hub changed actual destination inventory", hosts)
	}
	caller = policyTestCaller()
	group := policyTestRequest(t, "message.send", protocol.MessageSendRequest{Group: "other", Host: "beta", Message: "refuse before dispatch"})
	if reply := policyTestForward(t, alpha, caller, group); reply.Error == nil || reply.Error.Code == protocol.ForwardLost {
		t.Fatal("named-group refusal became success or transport loss", reply)
	}
	policyTestNotSeen(t, seen)
	group = policyTestRequest(t, "message.send", protocol.MessageSendRequest{Group: "team", Host: "beta", Message: "allowed"})
	if reply := policyTestForward(t, alpha, caller, group); reply.Error != nil {
		t.Fatal(reply.Error)
	}
	policyTestSeen(t, seen)
	if len(callHosts(t, alpha)) != 1 {
		t.Fatal("policy refusal ended the destination link")
	}
}

func TestPolicyActualFreshGrantCollisionForeignAndRevokedCreator(t *testing.T) {
	hub, address, secrets := testHub(t)
	if !hub.ReloadPolicy(testPolicy(t, `{"hosts":{"alpha":{"groups":["team"]}}}`)) {
		t.Fatal("reload failed")
	}
	seen := make(chan IncomingCall, 12)
	beta := connectTestDaemon(t, address, "beta", secrets["beta"], func(call IncomingCall) (Wait, error) {
		seen <- call
		if call.Request.Method == "lane.spawn" {
			input, _ := protocol.DecodeParams("lane.spawn", call.Request.Params)
			if input.(*protocol.LaneSpawnRequest).Name == "foreign" {
				return immediate(relationResult("child@gamma")), nil
			}
			return immediate(relationResult("child@beta")), nil
		}
		return immediate(emptyReply()), nil
	})
	_ = callHosts(t, beta)
	alpha := connectTestDaemon(t, address, "alpha", secrets["alpha"], func(IncomingCall) (Wait, error) { return immediate(emptyReply()), nil })
	_ = callHosts(t, alpha)
	caller := policyTestCaller()
	spawn := func(name string) PublicRequest {
		return policyTestRequest(t, "lane.spawn", protocol.LaneSpawnRequest{Host: "beta", Product: "fixture-worker", Name: name, Open: &protocol.OpenOptions{}})
	}
	if reply := policyTestForward(t, alpha, caller, spawn("child")); reply.Error != nil {
		t.Fatal("first grant failed", reply.Error)
	}
	call := policyTestSeen(t, seen)
	if call.From.PrivateGroup != caller.PrivateGroup || slices.Contains(call.From.Groups, "session:victim@gamma") {
		t.Fatal("owner creation used randomized or unverified private identity", call.From)
	}
	attacker := caller
	attacker.SessionID, attacker.Name, attacker.PrivateGroup = "attacker@alpha", "Attacker@alpha", "session:attacker@alpha"
	attacker.Groups = []string{attacker.PrivateGroup, "team"}
	if reply := policyTestForward(t, alpha, attacker, spawn("collision")); reply.Error == nil || reply.Error.Code != protocol.ForwardLost {
		t.Fatal("colliding authoritative success escaped verification", reply)
	}
	policyTestSeen(t, seen)
	if reply := policyTestForward(t, alpha, caller, spawn("foreign")); reply.Error == nil || reply.Error.Code != protocol.ForwardLost {
		t.Fatal("foreign authoritative ID escaped verification", reply)
	}
	policyTestSeen(t, seen)
	if !hub.ReloadPolicy(testPolicy(t, `{"hosts":{"alpha":{"groups":[],"lanes_to":[]}}}`)) {
		t.Fatal("revocation reload failed")
	}
	control := policyTestRequest(t, "turn.interrupt", protocol.SessionTarget{SessionID: "child@beta"})
	if reply := policyTestForward(t, alpha, caller, control); reply.Error != nil {
		t.Fatal("recorded creator lost known-ID collection after group/lane revocation", reply.Error)
	}
	if call = policyTestSeen(t, seen); call.From.PrivateGroup != caller.PrivateGroup {
		t.Fatal("creator control lost relation identity")
	}
	if reply := policyTestForward(t, alpha, attacker, control); reply.Error == nil || reply.Error.Code != protocol.UnknownSession {
		t.Fatal("revoked noncreator retained private control", reply)
	}
	if reply := policyTestForward(t, alpha, caller, spawn("blocked")); reply.Error == nil || reply.Error.Code == protocol.ForwardLost {
		t.Fatal("new spawn bypassed lane revocation or became transport loss", reply)
	}
	policyTestNotSeen(t, seen)
	if len(callHosts(t, alpha)) != 1 {
		t.Fatal("refusal closed existing ownership")
	}
}

func TestPolicyActualOperatorRosterDropsForbiddenWholeRows(t *testing.T) {
	hub, address, secrets := testHub(t)
	if !hub.ReloadPolicy(testPolicy(t, `{"hosts":{"alpha":{"groups":["team"]}}}`)) {
		t.Fatal("reload failed")
	}
	rows := roster.Host{Host: "beta", Products: []string{"peer"}, Sessions: []roster.Row{
		{SessionID: "visible@beta", Kind: "peer", Product: "peer", Groups: []string{"team", "other", "session:visible@beta"}},
		{SessionID: "hidden@beta", Kind: "peer", Name: "PRIVATE-NAME", Product: "private-product", Groups: []string{"other", "session:hidden@beta"}},
		{SessionID: "private-child@beta", Kind: "lane", Product: "peer", Groups: []string{"session:parent@alpha", "session:parent@alpha/child"}},
	}}
	fixture := RosterReply(rows)
	if _, err := decodeRosterReply(fixture.Result); fixture.Error != nil || err != nil {
		t.Fatal("invalid raw roster fixture", fixture.Error, err)
	}
	beta := connectTestDaemon(t, address, "beta", secrets["beta"], func(call IncomingCall) (Wait, error) {
		if IsRoster(call) {
			return immediate(fixture), nil
		}
		return immediate(emptyReply()), nil
	})
	_ = callHosts(t, beta)
	alpha := connectTestDaemon(t, address, "alpha", secrets["alpha"], func(IncomingCall) (Wait, error) { return immediate(emptyReply()), nil })
	_ = callHosts(t, alpha)
	reply := make(chan Reply, 1)
	alpha.inbox <- RosterCall{Reply: reply}
	select {
	case value := <-reply:
		decoded, err := decodeRosterReply(value.Result)
		if value.Error != nil || err != nil || len(decoded.Hosts) != 1 || len(decoded.Hosts[0].Sessions) != 1 || decoded.Hosts[0].Sessions[0].SessionID != "visible@beta" || slices.Contains(decoded.Hosts[0].Sessions[0].Groups, "other") || bytes.Contains(value.Result, []byte("PRIVATE-NAME")) || bytes.Contains(value.Result, []byte("private-child")) {
			t.Fatal("operator roster leaked a forbidden identity or group", string(value.Result), value.Error, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("filtered roster did not return")
	}
}

func TestPolicyActualBothLaneDirectionsGateFreshResumeAndDescribe(t *testing.T) {
	for _, test := range []struct {
		source, destination, policy string
	}{
		{"alpha", "beta", `{"hosts":{"alpha":{"lanes_to":[]}}}`},
		{"alpha", "beta", `{"hosts":{"beta":{"lanes_on":[]}}}`},
		{"beta", "alpha", `{"hosts":{"beta":{"lanes_to":[]}}}`},
		{"beta", "alpha", `{"hosts":{"alpha":{"lanes_on":[]}}}`},
	} {
		t.Run(test.source+"-"+test.destination+test.policy, func(t *testing.T) {
			hub, address, secrets := testHub(t)
			if !hub.ReloadPolicy(testPolicy(t, test.policy)) {
				t.Fatal("reload failed")
			}
			seen := make(chan IncomingCall, 4)
			destination := connectTestDaemon(t, address, test.destination, secrets[test.destination], func(call IncomingCall) (Wait, error) {
				seen <- call
				return immediate(emptyReply()), nil
			})
			_ = callHosts(t, destination)
			origin := connectTestDaemon(t, address, test.source, secrets[test.source], func(IncomingCall) (Wait, error) { return immediate(emptyReply()), nil })
			_ = callHosts(t, origin)
			caller := Caller{SessionID: "parent@" + test.source, Product: "peer", PrivateGroup: "session:parent@" + test.source, Groups: []string{"session:parent@" + test.source}}
			requests := []PublicRequest{
				policyTestRequest(t, "lane.spawn", protocol.LaneSpawnRequest{Host: test.destination, Product: "fixture-worker", Name: "child", Open: &protocol.OpenOptions{}}),
				policyTestRequest(t, "lane.spawn", protocol.LaneSpawnRequest{ResumeSessionID: "child@" + test.destination}),
				policyTestRequest(t, "lane.describe", protocol.LaneDescribeRequest{Host: test.destination, Product: "fixture-worker"}),
			}
			for _, request := range requests {
				if reply := policyTestForward(t, origin, caller, request); reply.Error == nil || reply.Error.Code == protocol.ForwardLost {
					t.Fatal("lane rule admitted an operation or mislabeled transport loss", request.Method, reply)
				}
			}
			policyTestNotSeen(t, seen)
			control := policyTestRequest(t, "turn.interrupt", protocol.SessionTarget{SessionID: "child@" + test.destination})
			if reply := policyTestForward(t, origin, caller, control); reply.Error != nil {
				t.Fatal("lane-only rules cut existing-lane controls", reply.Error)
			}
			if call := policyTestSeen(t, seen); call.From.PrivateGroup != caller.PrivateGroup {
				t.Fatal("lane-only rule enabled group rewriting")
			}
		})
	}
}

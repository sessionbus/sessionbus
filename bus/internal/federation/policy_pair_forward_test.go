// SPDX-License-Identifier: GPL-3.0-only
package federation

import (
	"fmt"
	"testing"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

// Only existing public fixture APIs: this identical regression runs at 7f54.
func TestPolicyActualSourcePairIsolationAndBusy(t *testing.T) {
	hub, address, secrets := testHub(t)
	if !hub.ReloadPolicy(testPolicy(t, `{"hosts":{"alpha":{},"beta":{},"gamma":{}}}`)) {
		t.Fatal("policy")
	}
	beta := connectTestDaemon(t, address, "beta", secrets["beta"], func(call IncomingCall) (Wait, error) {
		params, err := protocol.DecodeParams("lane.spawn", call.Request.Params)
		if err != nil {
			t.Error(err)
			return immediate(errorReply(protocol.Internal, "fixture")), nil
		}
		return immediate(relationResult(params.(*protocol.LaneSpawnRequest).Name + "@beta")), nil
	})
	_ = callHosts(t, beta)
	alpha := connectTestDaemon(t, address, "alpha", secrets["alpha"], func(IncomingCall) (Wait, error) { return immediate(emptyReply()), nil })
	_ = callHosts(t, alpha)
	gamma := connectTestDaemon(t, address, "gamma", secrets["gamma"], func(IncomingCall) (Wait, error) { return immediate(emptyReply()), nil })
	_ = callHosts(t, gamma)
	request := func(name string) PublicRequest {
		return policyTestRequest(t, "lane.spawn", protocol.LaneSpawnRequest{Host: "beta", Name: name, Product: "worker", Open: &protocol.OpenOptions{}})
	}
	caller := policyTestCaller()
	caller.Policy = true
	for index := 0; index < 256; index++ {
		if reply := policyTestForward(t, alpha, caller, request(fmt.Sprintf("child%d", index))); reply.Error != nil {
			t.Fatal("below cap", index, reply.Error)
		}
	}
	if reply := policyTestForward(t, alpha, caller, request("excess")); reply.Error == nil || reply.Error.Code != protocol.Busy || len(reply.Error.Data) != 0 {
		t.Error("pair cap must be Busy", reply.Error)
	}
	caller.Name = "Parent@gamma"
	caller.SessionID = "parent@gamma"
	caller.PrivateGroup = "session:parent@gamma"
	caller.Groups = []string{caller.PrivateGroup}
	if reply := policyTestForward(t, gamma, caller, request("independent")); reply.Error != nil {
		t.Error("another source was denied by destination-wide charge", reply.Error)
	}
	if len(callHosts(t, gamma)) != 2 {
		t.Fatal("refusal closed the link")
	}
}

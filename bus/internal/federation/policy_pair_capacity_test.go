// SPDX-License-Identifier: GPL-3.0-only
package federation

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/roster"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestPolicyPairCapacityIsolationAndExactLifecycle(t *testing.T) {
	r := newPolicyRelations()
	beta, alpha := &hostLink{host: "beta"}, &hostLink{host: "alpha"}
	for index := 0; index < 256; index++ {
		a := &policyAdmission{kind: policyFresh, destination: beta, creator: "parent@alpha", private: "session:parent@alpha", name: fmt.Sprintf("lane%d", index)}
		if !r.admit(a) {
			t.Fatal("early pair refusal", index)
		}
		if index != 255 {
			if value, once := r.complete(a, relationResult(fmt.Sprintf("lane%d@beta", index)), true); !once || value.Error != nil {
				t.Fatal(value)
			}
		}
	}
	if r.admit(&policyAdmission{kind: policyFresh, destination: beta, creator: "other@alpha"}) {
		t.Fatal("same source bypassed pair cap")
	}
	for _, a := range []*policyAdmission{
		{kind: policyFresh, destination: beta, creator: "other@gamma", private: "session:other@gamma", name: "gamma"},
		{kind: policyFresh, destination: alpha, creator: "parent@beta", private: "session:parent@beta", name: "reverse"},
	} {
		if !r.admit(a) {
			t.Fatal("independent pair was denied", a.creator, a.destination.host)
		}
		if value, once := r.complete(a, errorReply(protocol.ForwardLost, nil), false); !once || value.Error == nil {
			t.Fatal(value)
		}
		if _, once := r.complete(a, relationResult("bad@beta"), true); once {
			t.Fatal("duplicate failure released twice")
		}
	}
	for a := range r.pending {
		if value, once := r.complete(a, relationResult("late@beta"), true); !once || value.Error != nil {
			t.Fatal("late actual identity lost", value)
		}
	}
	old := r.entries["lane0@beta"]
	forget := &policyAdmission{kind: policyForget, destination: beta, id: old.lane, entry: old}
	r.admit(forget)
	r.remove(old)
	replacement := &policyAdmission{kind: policyFresh, destination: beta, creator: "new@alpha", private: "session:new@alpha", name: "lane0"}
	if !r.admit(replacement) {
		t.Fatal("forget did not free pair slot")
	}
	r.complete(replacement, relationResult(old.lane), true)
	r.complete(forget, emptyReply(), true)
	if r.entries[old.lane] == old || r.entries[old.lane] == nil || r.charges[policyPair{"alpha", "beta"}] != 256 {
		t.Fatal("stale forget affected successor")
	}
	r.prune("beta", time.Now().Add(time.Second), roster.Host{Host: "beta", Sessions: []roster.Row{}})
	if r.chargedDestination("beta") || len(r.entries) != 0 {
		t.Fatal("authoritative empty roster retained pair charges")
	}
	for _, a := range []*policyAdmission{{kind: policyFresh, destination: beta}, {kind: policyResumeAccess, destination: beta, creator: "invalid"}} {
		if r.admit(a) {
			t.Fatal("invalid creator charged a default pair")
		}
	}
}

func TestPolicyPairCapReportsBusyAndOtherSourceStillDispatches(t *testing.T) {
	beta := &hostLink{host: "beta"}
	s := hubState{policy: testPolicy(t, `{"hosts":{"alpha":{},"beta":{},"gamma":{}}}`), relations: newPolicyRelations()}
	for range 256 {
		if !s.relations.admit(&policyAdmission{kind: policyFresh, destination: beta, creator: "parent@alpha"}) {
			t.Fatal("early refusal")
		}
	}
	for _, host := range []string{"alpha", "gamma"} {
		caller := policyTestCaller()
		caller.SessionID = "parent@" + host
		caller.PrivateGroup = "session:" + caller.SessionID
		caller.Groups = []string{caller.PrivateGroup}
		value := Forward{From: caller, Request: policyTestRequest(t, "lane.spawn", protocol.LaneSpawnRequest{Host: "beta", Name: "child", Product: "worker", Open: &protocol.OpenOptions{}})}
		result := s.preparePolicyForward(context.Background(), make(chan registryEvent, 1), &hostLink{host: host}, beta, &value, make(chan Reply, 1))
		if host == "alpha" {
			if result.refusal == nil || result.refusal.Error == nil || result.refusal.Error.Code != protocol.Busy || len(result.refusal.Error.Data) != 0 {
				t.Fatal("cap is not strict Busy", result)
			}
		} else if result.refusal != nil || result.admission == nil {
			t.Fatal("other source lost admission", result)
		}
	}
}

func TestPolicyKnownHostValidationCoversAllDimensions(t *testing.T) {
	known := map[string]string{"alpha": "secret-not-to-log", "offline": "also-private"}
	for _, body := range []string{`{"hosts":{"ghost":{}}}`, `{"hosts":{"alpha":{"lanes_on":["ghost"]}}}`, `{"hosts":{"alpha":{"lanes_to":["ghost"]}}}`} {
		p := testPolicy(t, body)
		if err := p.ValidateHosts(known); err == nil || err.Error() != `unknown federation policy host "ghost"` {
			t.Fatal("unknown name was accepted or secret leaked", err)
		}
	}
	if err := testPolicy(t, `{"hosts":{"alpha":{"groups":[],"lanes_on":["offline"],"lanes_to":["offline"]}}}`).ValidateHosts(known); err != nil {
		t.Fatal("offline configured host rejected", err)
	}
	if err := (*Policy)(nil).ValidateHosts(known); err != nil {
		t.Fatal(err)
	}
}

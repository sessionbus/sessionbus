// SPDX-License-Identifier: GPL-3.0-only
package federation

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestPolicyProductInventoryUsesUnsharedPrivateIdentityWithoutGrant(t *testing.T) {
	state := &hubState{policy: testPolicy(t, `{"hosts":{"alpha":{"groups":[],"lanes_to":["beta"]}}}`)}
	origin, destination := &hostLink{host: "alpha"}, &hostLink{host: "beta"}
	caller := policyTestCaller()
	before, _ := json.Marshal(caller)
	last := ""
	for range 2 {
		value := Forward{From: caller, Request: policyTestRequest(t, "session.list", protocol.SessionListRequest{Host: "beta"})}
		result := state.preparePolicyForward(context.Background(), nil, origin, destination, &value, make(chan Reply, 1))
		if result.refusal != nil || result.admission != nil || state.relations != nil {
			t.Fatal("inventory was refused or acquired a grant", result)
		}
		if !validCaller(value.From, "alpha") || len(value.From.Groups) != 1 || value.From.Groups[0] != value.From.PrivateGroup || !strings.HasPrefix(value.From.PrivateGroup, caller.PrivateGroup+"#") || value.From.PrivateGroup == last {
			t.Fatal("inventory reused a visible or invalid private identity", value.From)
		}
		if value.From.OwnerLifetime != caller.OwnerLifetime || value.From.SessionID != caller.SessionID {
			t.Fatal("authenticated caller changed", value.From)
		}
		last = value.From.PrivateGroup
	}
	after, _ := json.Marshal(caller)
	if string(before) != string(after) {
		t.Fatal("caller's shared identity mutated")
	}
}

type policyEntropyFailure struct{}

func (policyEntropyFailure) Read([]byte) (int, error) {
	return 0, errors.New("fixture entropy failure")
}

func TestPolicyMetadataNonceFailureNeverFallsBackToCallerIdentity(t *testing.T) {
	if os.Getenv("SESSIONBUS_TEST_POLICY_ENTROPY_CHILD") == "1" {
		rand.Reader = policyEntropyFailure{}
		state := &hubState{policy: testPolicy(t, `{"hosts":{"alpha":{"groups":[]}}}`)}
		caller := policyTestCaller()
		value := Forward{From: caller, Request: policyTestRequest(t, "session.list", protocol.SessionListRequest{Host: "beta"})}
		result := state.preparePolicyForward(context.Background(), nil, &hostLink{host: "alpha"}, &hostLink{host: "beta"}, &value, make(chan Reply, 1))
		// On toolchains where rand.Read returns an error rather than terminating
		// the process, it must remain a refusal, with no borrowed caller or grant.
		if result.refusal == nil || result.refusal.Error == nil || result.refusal.Error.Code != protocol.Internal || state.relations != nil || !reflect.DeepEqual(value.From, caller) {
			t.Fatal("entropy failure did not stop forwarding")
		}
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestPolicyMetadataNonceFailureNeverFallsBackToCallerIdentity$")
	cmd.Env = append(os.Environ(), "SESSIONBUS_TEST_POLICY_ENTROPY_CHILD=1")
	out, err := cmd.CombinedOutput()
	// Current Go rand.Read terminates on CSPRNG failure. That is a process STOP,
	// not a successful inventory or a proof of an observed live host failure.
	if err != nil && (!strings.Contains(string(out), "crypto/rand") || !strings.Contains(string(out), "fixture entropy failure")) {
		t.Fatalf("unexpected entropy control: %s %v", out, err)
	}
}

func TestPolicyCutOffInventoryAndTargetedListsKeepTheirRefusals(t *testing.T) {
	for _, targeted := range []bool{false, true} {
		state := &hubState{policy: testPolicy(t, `{"hosts":{"alpha":{"groups":[],"lanes_to":[]}}}`)}
		params := protocol.SessionListRequest{Host: "beta"}
		if targeted {
			params = protocol.SessionListRequest{SessionID: "target@beta"}
		}
		value := Forward{From: policyTestCaller(), Request: policyTestRequest(t, "session.list", params)}
		before := value.From
		result := state.preparePolicyForward(context.Background(), nil, &hostLink{host: "alpha"}, &hostLink{host: "beta"}, &value, make(chan Reply, 1))
		if result.refusal == nil || result.admission != nil || state.relations != nil || !reflect.DeepEqual(value.From, before) {
			t.Fatal("cut-off list was dispatched, mutated or acquired a grant")
		}
		if targeted {
			if result.refusal.Error == nil || result.refusal.Error.Code != protocol.PolicyDenied {
				t.Fatal("targeted list changed refusal", result.refusal)
			}
		} else {
			decoded, err := protocol.DecodeResult("session.list", result.refusal.Result)
			if err != nil || len(decoded.(*protocol.SessionListResult).Hosts) != 0 || len(decoded.(*protocol.SessionListResult).Sessions) != 0 {
				t.Fatal("cut-off list exposed inventory", decoded, err)
			}
		}
	}
}

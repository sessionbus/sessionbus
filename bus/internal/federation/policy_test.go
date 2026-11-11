// SPDX-License-Identifier: GPL-3.0-only

package federation

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/roster"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func testPolicy(t *testing.T, raw string) *Policy {
	t.Helper()
	policy, err := decodePolicy([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	return policy
}

func TestPolicyDimensionsAreIndependentAndExact(t *testing.T) {
	policy := testPolicy(t, `{"hosts":{"alpha":{"groups":["team","literal*"]},"beta":{"lanes_on":[],"lanes_to":["alpha"]},"gamma":{}}}`)
	if !policy.groupAllowed("alpha", "beta", "team") || policy.groupAllowed("alpha", "beta", "other") || policy.groupAllowed("alpha", "beta", "literal-other") || !policy.groupAllowed("alpha", "beta", "literal*") {
		t.Fatal("named policy is not exact")
	}
	if policy.laneAllowed("alpha", "beta") || !policy.laneAllowed("beta", "alpha") || policy.laneAllowed("beta", "gamma") {
		t.Fatal("lane directions were combined or defaulted incorrectly")
	}
	if policy.groupsRestricted("beta", "gamma") || !policy.groupAllowed("beta", "gamma", "arbitrary") || policy.tracks("absent", "other") || !policy.tracks("alpha", "other") {
		t.Fatal("missing dimensions became deny-all or group restrictions")
	}
	var omitted *Policy
	if omitted.tracks("alpha", "beta") || omitted.groupsRestricted("alpha", "beta") || !omitted.laneAllowed("alpha", "beta") {
		t.Fatal("omitted policy enabled enforcement")
	}
}

func TestPolicyFileStrictValidationAndMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "policy.json")
	write := func(raw string, mode os.FileMode) {
		t.Helper()
		if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, mode); err != nil {
			t.Fatal(err)
		}
	}
	write(`{"hosts":{"alpha":{"groups":[]}}}`, 0o600)
	if p, err := LoadPolicy(path); err != nil || p.groupAllowed("alpha", "beta", "team") || !p.laneAllowed("alpha", "beta") {
		t.Fatal("empty groups changed lane defaults", p, err)
	}
	for _, raw := range []string{`null`, `[]`, `{"hosts":null}`, `{"Hosts":{}}`, `{"hosts":{"local":{}}}`, `{"hosts":{},"hosts":{}}`, `{"hosts":{"alpha":{},"alpha":{}}}`, `{"hosts":{"alpha":{"groups":[],"groups":["team"]}}}`, `{"hosts":{"alpha":{"groups":null}}}`, `{"hosts":{"alpha":{"groups":["session:spoof"]}}}`, `{"hosts":{"alpha":{"groups":["team","team"]}}}`, `{"hosts":{"alpha":{"lanes_on":["bad host"]}}}`, `{"hosts":{"alpha":{"unknown":[]}}}`, `{"hosts":{}} {}`} {
		write(raw, 0o600)
		if _, err := LoadPolicy(path); err == nil {
			t.Errorf("accepted malformed/ambiguous policy %s", raw)
		}
	}
	write(`{}`, 0o644)
	if _, err := LoadPolicy(path); err == nil {
		t.Fatal("accepted readable policy")
	}
	write(strings.Repeat(" ", protocol.MaxFrameBytes+1), 0o600)
	if _, err := LoadPolicy(path); err == nil {
		t.Fatal("accepted an oversized policy")
	}
	if _, err := LoadPolicy(path + ".missing"); err == nil {
		t.Fatal("missing explicit policy became unrestricted")
	}
}

func relationResult(id string) Reply {
	raw, _ := protocol.EncodeResult("lane.spawn", protocol.LaneSpawnResult{SessionID: id})
	return Reply{Result: raw}
}

func TestPolicyRelationCompletionCapCollisionAndGeneration(t *testing.T) {
	r := newPolicyRelations()
	link := &hostLink{host: "beta"}
	first := &policyAdmission{kind: policyFresh, destination: link, creator: "parent@alpha", private: "session:parent@alpha", name: "child"}
	if !r.admit(first) {
		t.Fatal("first reservation refused")
	}
	value, once := r.complete(first, relationResult("child@beta"), true)
	if !once || value.Error != nil || !r.counterpart("parent@alpha", "child@beta") || r.charges[policyPair{"alpha", "beta"}] != 1 {
		t.Fatal("success did not install its exact grant")
	}
	old := r.entries["child@beta"]
	duplicate := &policyAdmission{kind: policyFresh, destination: link, creator: "attacker@alpha", private: "session:attacker@alpha", name: "other"}
	r.admit(duplicate)
	if value, _ := r.complete(duplicate, relationResult("child@beta"), true); value.Error == nil || value.Error.Code != protocol.ForwardLost || r.entries["child@beta"] != old {
		t.Fatal("collision exposed success or replaced a live grant")
	}
	foreign := &policyAdmission{kind: policyFresh, destination: link, creator: "parent@alpha"}
	r.admit(foreign)
	if value, _ := r.complete(foreign, relationResult("child@gamma"), true); value.Error == nil || r.entries["child@gamma"] != nil {
		t.Fatal("foreign identity acquired a grant")
	}
	forget := &policyAdmission{kind: policyForget, destination: link, id: old.lane, entry: old}
	r.admit(forget)
	r.remove(old)
	replacement := &policyAdmission{kind: policyFresh, destination: link, creator: "new@alpha", private: "session:new@alpha", name: "child"}
	r.admit(replacement)
	r.complete(replacement, relationResult("child@beta"), true)
	r.complete(forget, emptyReply(), true)
	if r.entries["child@beta"] == nil || r.entries["child@beta"].creator != "new@alpha" || r.entries["child@beta"] == old {
		t.Fatal("late forget removed the successor generation")
	}
	if _, repeated := r.complete(forget, emptyReply(), true); repeated {
		t.Fatal("duplicate completion was settled twice")
	}
	for range policyRelationsPerPair - 1 {
		if !r.admit(&policyAdmission{kind: policyFresh, destination: link, creator: "parent@alpha"}) {
			t.Fatal("pending cap refused before counting live+in-flight")
		}
	}
	if r.admit(&policyAdmission{kind: policyFresh, destination: link, creator: "parent@alpha"}) || r.charges[policyPair{"alpha", "beta"}] != policyRelationsPerPair {
		t.Fatal("in-flight spawns bypassed the relation cap")
	}
}

func TestPolicyRelationVerificationAndOnlyFreshCompleteRosterPrunes(t *testing.T) {
	r := newPolicyRelations()
	when := time.Now()
	entry := &policyRelation{lane: "child@beta", creator: "parent@alpha", parentPrivate: "session:parent@alpha", private: "session:parent@alpha/child", recorded: when}
	r.entries[entry.lane], r.charges[policyPair{"alpha", "beta"}] = entry, 1
	for _, caller := range []Caller{{SessionID: "parent@alpha", PrivateGroup: "session:parent@alpha"}, {SessionID: "child@beta", PrivateGroup: entry.private}} {
		if _, _, ok := r.verified(caller); !ok {
			t.Fatal("verified parent or lane refused", caller)
		}
	}
	if _, _, ok := r.verified(Caller{SessionID: "parent@alpha/child@beta", PrivateGroup: "session:parent@alpha/child@beta"}); ok {
		t.Fatal("custom-ID collision was accepted as own private")
	}
	empty := roster.Host{Host: "beta", Sessions: []roster.Row{}}
	r.prune("beta", when.Add(-time.Second), empty)
	r.prune("beta", when.Add(time.Second), roster.Host{Host: "beta", Sessions: []roster.Row{}, Error: "host_roster_unavailable"})
	r.prune("beta", when.Add(time.Second), roster.Host{Host: "beta"})
	r.prune("beta", when.Add(time.Second), roster.Host{Host: "beta", Sessions: []roster.Row{{SessionID: entry.lane, Connected: false}}})
	if r.entries[entry.lane] == nil {
		t.Fatal("stale/incomplete/error or offline-retained row was treated as absence")
	}
	r.prune("beta", when.Add(time.Second), empty)
	if r.entries[entry.lane] != nil || r.charges[policyPair{"alpha", "beta"}] != 0 {
		t.Fatal("fresh complete absence did not retire the grant")
	}
}

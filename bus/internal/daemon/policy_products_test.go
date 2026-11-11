// SPDX-License-Identifier: GPL-3.0-only
package daemon

import (
	"encoding/json"
	"slices"
	"testing"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestPolicyLaneOnlyListPreservesProductsWithoutSessionVisibility(t *testing.T) {
	for _, direction := range [][2]string{{"alpha", "beta"}, {"beta", "alpha"}} {
		for _, shape := range []string{"lanes-only", "cut-off", "named-allowed"} {
			t.Run(direction[0]+"-"+shape, func(t *testing.T) {
				hub, _, peers := policyDaemonFixture(t, "fixture-worker")
				source, destination := direction[0], direction[1]
				rule := map[string]any{"groups": []string{}, "lanes_to": []string{}}
				if shape == "lanes-only" {
					rule["lanes_to"] = []string{destination}
				} else if shape == "named-allowed" {
					rule["groups"] = []string{"team"}
				}
				raw, err := json.Marshal(map[string]any{"hosts": map[string]any{source: rule}})
				must(t, err)
				if !hub.ReloadPolicy(loadFixturePolicy(t, string(raw))) {
					t.Fatal("policy reload failed")
				}
				var listed protocol.SessionListResult
				must(t, peers[source].call("session.list", protocol.SessionListRequest{Host: destination}, &listed))
				if listed.SelfInfo == nil || listed.SelfInfo.SessionID != "parent@"+source {
					t.Fatal("directed list changed authenticated caller", listed.SelfInfo)
				}
				published := false
				for _, host := range listed.Hosts {
					if host.Host == destination && slices.Equal(host.Products, []string{"fixture-worker"}) {
						published = true
					}
				}
				if published != (shape != "cut-off") {
					t.Fatal("published products differ from allowed inventory", shape, listed.Hosts)
				}
				if shape == "named-allowed" {
					if len(listed.Sessions) != 1 || listed.Sessions[0].SessionID != "parent@"+destination {
						t.Fatal("allowed named visibility changed", listed.Sessions)
					}
				} else if len(listed.Sessions) != 0 {
					t.Fatal("metadata-only or cut-off list exposed sessions", listed.Sessions)
				}
			})
		}
	}
}

// SPDX-License-Identifier: GPL-3.0-only

package federation

import (
	"reflect"
	"testing"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestLegacyAvailabilityOmitsOnlyMarkedHosts(t *testing.T) {
	original := Reply{Result: []byte(`{"sessions":[], "hosts":[{"host":"alpha","products":[]},{"host":"beta","products":[],"availability":"not_responding"},{"host":"gamma","products":["peer"]}],"self_info":{"session_id":"sender@alpha","product":"peer","groups":["team"]}}`)}
	before := string(original.Result)
	for _, optedIn := range []bool{false, true} {
		value := ProjectAvailability("session.list", original, optedIn)
		decoded, err := protocol.DecodeResult("session.list", value.Result)
		if err != nil {
			t.Fatal(err)
		}
		listed := decoded.(*protocol.SessionListResult)
		want := []protocol.HostProducts{{Host: "alpha", Products: []string{}}, {Host: "gamma", Products: []string{"peer"}}}
		if optedIn {
			want = []protocol.HostProducts{{Host: "alpha", Products: []string{}}, {Host: "beta", Products: []string{}, Availability: "not_responding"}, {Host: "gamma", Products: []string{"peer"}}}
			if string(value.Result) != before {
				t.Error("opted-in result bytes changed")
			}
		}
		if !reflect.DeepEqual(listed.Hosts, want) || len(listed.Sessions) != 0 || listed.SelfInfo == nil || listed.SelfInfo.SessionID != "sender@alpha" {
			t.Errorf("legacy host membership or confirmed identity changed: %#v", listed)
		}
		if string(original.Result) != before {
			t.Fatal("projection mutated original result")
		}
	}
	for _, raw := range []string{`{"hosts":[{"host":"alpha","products":[]}], "sessions":[]}`, `{"sessions":[]}`} {
		plain := Reply{Result: []byte(raw)}
		if got := ProjectAvailability("session.list", plain, false); string(got.Result) != raw {
			t.Fatal("legacy raw bytes changed", string(got.Result))
		}
	}
}

// SPDX-License-Identifier: GPL-3.0-only

package federation

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestAvailabilityIsProjectedOnEveryNegotiatedHop(t *testing.T) {
	for _, optIn := range []bool{false, true} {
		for _, oldOrigin := range []bool{false, true} {
			for _, oldTarget := range []bool{false, true} {
				t.Run(fmt.Sprintf("optin%v/originOld%v/targetOld%v", optIn, oldOrigin, oldTarget), func(t *testing.T) {
					_, address, secrets := testHub(t)
					seen := make(chan Caller, 1)
					configuration, err := ClientTLS("beta", secrets["beta"])
					if err != nil {
						t.Fatal(err)
					}
					if oldTarget {
						configuration.NextProtos = []string{RosterALPN}
					}
					beta := connectTestDaemonTLS(t, address, "beta", configuration, func(call IncomingCall) (Wait, error) {
						seen <- call.From
						// A valid extended result must still be stripped BEFORE an old
						// wire parser, even if a callback accidentally supplied it.
						return immediate(Reply{Result: []byte(`{"sessions":[],"hosts":[{"host":"beta","products":[],"availability":"not_responding"}]}`)}), nil
					})
					_ = callHosts(t, beta)
					configuration, err = ClientTLS("alpha", secrets["alpha"])
					if err != nil {
						t.Fatal(err)
					}
					if oldOrigin {
						configuration.NextProtos = []string{RosterALPN}
					}
					alpha := connectTestDaemonTLS(t, address, "alpha", configuration, func(IncomingCall) (Wait, error) { return immediate(emptyReply()), nil })
					_ = callHosts(t, alpha)
					from := Caller{Availability: optIn, SessionID: "sender@alpha", Product: "peer", PrivateGroup: "session:sender@alpha", Groups: []string{"team", "session:sender@alpha"}}
					params, _ := protocol.EncodeParams("session.list", protocol.SessionListRequest{Host: "beta"})
					reply := make(chan Reply, 1)
					alpha.inbox <- OutgoingCall{Value: Forward{From: from, Request: PublicRequest{Method: "session.list", Params: params}}, Reply: reply}
					want := optIn && !oldOrigin && !oldTarget
					select {
					case caller := <-seen:
						if caller.Availability != want || caller.SessionID != from.SessionID || caller.PrivateGroup != from.PrivateGroup || len(caller.Groups) != 2 {
							t.Fatal("old-hop caller stripping changed identity or capability", caller)
						}
					case <-time.After(time.Second):
						t.Fatal("mixed-version request failed before destination")
					}
					select {
					case value := <-reply:
						if value.Error != nil || strings.Contains(string(value.Result), `"availability"`) != want {
							t.Fatal("old-hop reply projection failed", string(value.Result), value.Error)
						}
					case <-time.After(time.Second):
						t.Fatal("mixed-version reply did not return")
					}
				})
			}
		}
	}
}

// SPDX-License-Identifier: GPL-3.0-only

package federation

import (
	"bytes"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/roster"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestPolicyActualReplyAndRosterKeepAdmissionSnapshotAcrossReload(t *testing.T) {
	for _, operator := range []bool{false, true} {
		t.Run(map[bool]string{false: "public-list", true: "operator-roster"}[operator], func(t *testing.T) {
			hub, address, secrets := testHub(t)
			if !hub.ReloadPolicy(testPolicy(t, `{"hosts":{"alpha":{"groups":["team"]}}}`)) {
				t.Fatal("initial reload failed")
			}
			entered, release := make(chan struct{}), make(chan struct{}, 1)
			defer close(release)
			var calls atomic.Int32
			original := []byte(`{"sessions":[{"session_id":"visible@beta","kind":"peer","product":"peer","groups":["team","other","session:visible@beta"],"connected":true,"running":false}]}`)
			rosterReply := RosterReply(roster.Host{Host: "beta", Products: []string{"peer"}, Sessions: []roster.Row{
				{SessionID: "visible@beta", Kind: "peer", Product: "peer", Groups: []string{"team", "other", "session:visible@beta"}},
				{SessionID: "other@beta", Kind: "peer", Product: "peer", Groups: []string{"other", "session:other@beta"}},
			}})
			if _, err := decodeRosterReply(rosterReply.Result); err != nil {
				t.Fatal("invalid operator fixture", err)
			}
			unchanged := append([]byte(nil), original...)
			beta := connectTestDaemon(t, address, "beta", secrets["beta"], func(call IncomingCall) (Wait, error) {
				result := Reply{Result: original}
				if operator {
					result = rosterReply
				}
				if calls.Add(1) == 1 {
					close(entered)
					return func(done <-chan struct{}) (Reply, bool) {
						select {
						case <-release:
							return result, true
						case <-done:
							return Reply{}, false
						}
					}, nil
				}
				return immediate(result), nil
			})
			_ = callHosts(t, beta)
			alpha := connectTestDaemon(t, address, "alpha", secrets["alpha"], func(IncomingCall) (Wait, error) { return immediate(emptyReply()), nil })
			_ = callHosts(t, alpha)
			start := func() chan Reply {
				answer := make(chan Reply, 1)
				if operator {
					alpha.inbox <- RosterCall{Reply: answer}
				} else {
					request := policyTestRequest(t, "session.list", protocol.SessionListRequest{Host: "beta"})
					alpha.inbox <- OutgoingCall{Value: Forward{From: policyTestCaller(), Request: request}, Reply: answer}
				}
				return answer
			}
			assert := func(answer <-chan Reply, allowed, removed string, count int) {
				t.Helper()
				select {
				case reply := <-answer:
					if reply.Error != nil {
						t.Fatal(reply.Error)
					}
					var groups []string
					if operator {
						result, err := decodeRosterReply(reply.Result)
						if err != nil || len(result.Hosts) != 1 || len(result.Hosts[0].Sessions) != count {
							t.Fatal("wrong roster snapshot membership", string(reply.Result), err)
						}
						groups = result.Hosts[0].Sessions[0].Groups
					} else {
						result, err := protocol.DecodeResult("session.list", reply.Result)
						if err != nil || len(result.(*protocol.SessionListResult).Sessions) != 1 {
							t.Fatal("invalid public-list result", string(reply.Result), err)
						}
						groups = result.(*protocol.SessionListResult).Sessions[0].Groups
					}
					if !slices.Contains(groups, allowed) || slices.Contains(groups, removed) {
						t.Fatal("reply consulted a later policy instead of its admission", groups)
					}
				case <-time.After(time.Second):
					t.Fatal("admitted snapshot result did not settle")
				}
			}
			answer := start()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("initial call was not dispatched")
			}
			if !hub.ReloadPolicy(testPolicy(t, `{"hosts":{"alpha":{"groups":["other"]}}}`)) {
				t.Fatal("replacement reload failed")
			}
			// Signal once without closing so deferred failure cleanup remains safe.
			release <- struct{}{}
			assert(answer, "team", "other", 1)
			count := 1
			if operator {
				count = 2
			}
			assert(start(), "other", "team", count)
			if !bytes.Equal(original, unchanged) {
				t.Fatal("per-recipient filtering mutated the shared original result")
			}
		})
	}
}

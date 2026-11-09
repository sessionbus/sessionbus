// SPDX-License-Identifier: GPL-3.0-only

package federation

import (
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

// Two origins exercise the HUB destination cap rather than the separate
// per-origin cap already covered by TestOutgoingForwardCapacityRejectsExcess.
func TestDestinationForwardCapRefusesWithoutEndingAdmittedWork(t *testing.T) {
	_, address, secrets := testHub(t)
	release := make(chan struct{})
	defer close(release)
	admitted := make(chan struct{}, protocol.MaxOperations)
	beta := connectTestDaemon(t, address, "beta", secrets["beta"], func(IncomingCall) (Wait, error) {
		admitted <- struct{}{}
		return func(done <-chan struct{}) (Reply, bool) {
			select {
			case <-release:
				return Reply{Result: []byte(`{}`)}, true
			case <-done:
				return Reply{}, false
			}
		}, nil
	})
	_ = callHosts(t, beta)
	origins := []*testDaemon{
		connectTestDaemon(t, address, "alpha", secrets["alpha"], func(IncomingCall) (Wait, error) { return immediate(Reply{}), nil }),
		connectTestDaemon(t, address, "gamma", secrets["gamma"], func(IncomingCall) (Wait, error) { return immediate(Reply{}), nil }),
	}
	for _, origin := range origins {
		_ = callHosts(t, origin)
	}
	params, _ := protocol.EncodeParams("turn.interrupt", protocol.SessionTarget{SessionID: "lane@beta"})
	call := func(origin int) chan Reply {
		host := []string{"alpha", "gamma"}[origin]
		id := "sender@" + host
		caller := Caller{SessionID: id, Product: "peer", PrivateGroup: "session:" + id, Groups: []string{"session:" + id}}
		reply := make(chan Reply, 1)
		origins[origin].inbox <- OutgoingCall{Value: Forward{From: caller, Request: PublicRequest{Method: "turn.interrupt", Params: params}}, Reply: reply}
		return reply
	}
	var first chan Reply
	for i := 0; i < protocol.MaxOperations; i++ {
		result := call(i % 2)
		if first == nil {
			first = result
		}
		select {
		case <-admitted:
		case <-time.After(time.Second):
			t.Fatal("initial call did not reach target")
		}
	}
	excess := call(0)
	select {
	case result := <-excess:
		if result.Error == nil || result.Error.Code != protocol.ForwardLost {
			t.Fatalf("excess result: %#v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("destination cap did not refuse excess")
	}
	select {
	case result := <-first:
		t.Fatalf("destination cap ended admitted work: %#v", result)
	case <-time.After(25 * time.Millisecond):
	}
	if hosts := callHosts(t, origins[0]); len(hosts) != 2 {
		t.Fatalf("destination cap removed a host: %v", hosts)
	}
	select {
	case <-beta.closed:
		t.Fatal("destination cap closed link")
	default:
	}
}

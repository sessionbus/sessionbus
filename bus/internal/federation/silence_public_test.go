// SPDX-License-Identifier: GPL-3.0-only

package federation

import (
	"sync"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/roster"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

// Only preexisting APIs are used: the identical fixture executes on old main,
// where the public waiter does not end. No artificial transport close is used.
func TestPublicSilenceWaitIsUnknownWhileOwnershipLinkStaysConnected(t *testing.T) {
	_, address, secrets := testHub(t)
	release := make(chan struct{})
	var released sync.Once
	defer released.Do(func() { close(release) })
	probes := make(chan struct{}, 2)
	admitted := make(chan struct{}, 1)
	beta := connectTestDaemon(t, address, "beta", secrets["beta"], func(call IncomingCall) (Wait, error) {
		if IsRoster(call) {
			probes <- struct{}{}
		} else {
			admitted <- struct{}{}
		}
		return func(done <-chan struct{}) (Reply, bool) {
			select {
			case <-release:
				if IsRoster(call) {
					return RosterReply(roster.Host{Host: "beta", Products: []string{}, Sessions: []roster.Row{}}), true
				}
				return Reply{Result: []byte(`{}`)}, true
			case <-done:
				return Reply{}, false
			}
		}, nil
	})
	_ = callHosts(t, beta)
	alpha := connectTestDaemon(t, address, "alpha", secrets["alpha"], func(call IncomingCall) (Wait, error) {
		if IsRoster(call) {
			return immediate(RosterReply(roster.Host{Host: "alpha", Products: []string{}, Sessions: []roster.Row{}})), nil
		}
		return immediate(emptyReply()), nil
	})
	_ = callHosts(t, alpha)
	caller := Caller{SessionID: "sender@alpha", Product: "peer", PrivateGroup: "session:sender@alpha", Groups: []string{"session:sender@alpha"}}
	params, _ := protocol.EncodeParams("turn.interrupt", protocol.SessionTarget{SessionID: "lane@beta"})
	reply := make(chan Reply, 1)
	alpha.inbox <- OutgoingCall{Value: Forward{From: caller, Request: PublicRequest{Method: "turn.interrupt", Params: params}}, Reply: reply}
	select {
	case <-admitted:
	case <-time.After(time.Second):
		t.Fatal("original control did not reach destination")
	}
	select {
	case value := <-reply:
		if value.Error == nil || value.Error.Code != protocol.ForwardLost {
			t.Fatal("silence invented a native result", value)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("silent destination left public waiter unbounded")
	}
	if hosts := callHosts(t, alpha); len(hosts) != 1 || hosts[0] != "beta" {
		t.Fatal("mark ended host ownership", hosts)
	}
	select {
	case <-beta.closed:
		t.Fatal("mark closed destination")
	default:
	}
	refused := make(chan Reply, 1)
	alpha.inbox <- OutgoingCall{Value: Forward{From: caller, Request: PublicRequest{Method: "turn.interrupt", Params: params}}, Reply: refused}
	select {
	case value := <-refused:
		if value.Error == nil || value.Error.Code != protocol.ForwardLost {
			t.Fatal("marked destination accepted new work", value)
		}
	case <-time.After(time.Second):
		t.Fatal("marked destination did not refuse immediately")
	}
	released.Do(func() { close(release) })
	// Seeing the next own probe means the hub has consumed the first retained
	// reply and cleared its one-outstanding guard. No uncertain call is replayed.
	for range 2 {
		select {
		case <-probes:
		case <-time.After(6 * time.Second):
			t.Fatal("current attachment did not recover from valid probe reply")
		}
	}
	fresh := make(chan Reply, 1)
	alpha.inbox <- OutgoingCall{Value: Forward{From: caller, Request: PublicRequest{Method: "turn.interrupt", Params: params}}, Reply: fresh}
	select {
	case value := <-fresh:
		if value.Error != nil || string(value.Result) != `{}` {
			t.Fatal("same-attachment valid call failed after recovery", value)
		}
	case <-time.After(time.Second):
		t.Fatal("fresh control after recovery did not settle")
	}
}

// SPDX-License-Identifier: GPL-3.0-only
package federation

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/transfer"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestDEV1BotHubReplyDoesNotBlockItsFullOwnerInbox(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h := &hubTransfers{ctx: ctx}
	link := &hostLink{inbox: make(chan any, 1), done: make(chan struct{})}
	link.inbox <- struct{}{}
	var released atomic.Int32
	frame := connTransferFrame{release: func() { released.Add(1) }}
	done := make(chan struct{})
	go func() { defer close(done); h.reply(link, frame, nil, transfer.ErrState) }()
	blocked := false
	select {
	case <-done:
	case <-time.After(25 * time.Millisecond):
		blocked = true
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reply did not end after cancellation")
	}
	if blocked {
		t.Error("hub reply waited on its own full owner inbox until context cancellation")
	}
	if released.Load() != 1 {
		t.Errorf("release count %d", released.Load())
	}
}

func TestOutgoingForwardCapacityRejectsExcessAndKeepsBothLinks(t *testing.T) {
	_, address, secrets := testHub(t)
	release := make(chan struct{})
	defer close(release)
	admitted := make(chan struct{}, 256)
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
	alpha := connectTestDaemon(t, address, "alpha", secrets["alpha"], func(IncomingCall) (Wait, error) { return immediate(Reply{}), nil })
	_ = callHosts(t, alpha)
	params, _ := protocol.EncodeParams("turn.run", protocol.TurnRunRequest{SessionID: "lane@beta", Input: "go"})
	caller := Caller{SessionID: "sender@alpha", Name: "sender@alpha", Product: "peer", PrivateGroup: "session:sender@alpha", Groups: []string{"session:sender@alpha"}}
	replies := make([]chan Reply, 256)
	for i := range replies {
		replies[i] = make(chan Reply, 1)
		alpha.inbox <- OutgoingCall{Value: Forward{From: caller, Request: PublicRequest{Method: "turn.run", Params: params}}, Reply: replies[i]}
		select {
		case <-admitted:
		case <-time.After(time.Second):
			t.Fatal("held call not admitted")
		}
	}
	excess := make(chan Reply, 1)
	alpha.inbox <- OutgoingCall{Value: Forward{From: caller, Request: PublicRequest{Method: "turn.run", Params: params}}, Reply: excess}
	select {
	case r := <-excess:
		if r.Error == nil || r.Error.Code != protocol.ForwardLost {
			t.Fatal("excess call not refused", r)
		}
	case <-time.After(time.Second):
		t.Fatal("excess refusal missing")
	}
	select {
	case r := <-replies[0]:
		t.Error("already admitted work was lost at source capacity", r)
	case <-time.After(25 * time.Millisecond):
	}
	// New origin admission must not turn destination overload into a link close.
	// An ordinary source hosts request is ordered after the excess input.
	if hosts := callHosts(t, alpha); len(hosts) != 1 || hosts[0] != "beta" {
		t.Fatal("excess forwarding disconnected destination", hosts)
	}
	select {
	case <-beta.closed:
		t.Fatal("destination closed at source capacity")
	default:
	}
	select {
	case <-alpha.closed:
		t.Fatal("origin closed at source capacity")
	default:
	}
}

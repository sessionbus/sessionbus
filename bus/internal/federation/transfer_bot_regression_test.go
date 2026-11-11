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
	prepareReplyFixture(t, link)
	link.originAdmitted = 1
	link.inbox <- struct{}{}
	var released atomic.Int32
	frame := connTransferFrame{release: func() { released.Add(1) }}
	// This reply belongs to the live wire, not the coordinator context. A
	// synchronous call must queue it without using the full shared inbox.
	cancel()
	select {
	case <-link.wire.Done():
		t.Fatal("reply fixture wire ended with coordinator cancellation")
	default:
	}
	if !h.reply(link, frame, nil, transfer.ErrState) {
		t.Fatal("live-wire reply was refused with a full shared inbox")
	}
	if len(link.inbox) != 1 || len(link.originReplies) != 1 || link.originAdmitted != 1 {
		t.Fatal("reply changed the shared inbox or released its reservation")
	}
	if released.Load() != 0 {
		t.Fatal("enqueue released an unconsumed response")
	}
	select {
	case event := <-link.originReplies:
		link.consumeOriginReply(event)
	default:
		t.Fatal("dedicated reply missing")
	}
	if released.Load() != 1 {
		t.Errorf("release count %d", released.Load())
	}
	if len(link.inbox) != 1 || link.originAdmitted != 0 {
		t.Fatal("consumption changed the shared inbox or retained the reservation")
	}
}

func TestOutgoingForwardCapacityRejectsExcessAndKeepsBothLinks(t *testing.T) {
	_, address, secrets := testHub(t)
	release := make(chan struct{})
	defer close(release)
	admitted := make(chan struct{}, 256)
	beta := connectTestDaemon(t, address, "beta", secrets["beta"], func(call IncomingCall) (Wait, error) {
		if IsRoster(call) {
			return immediate(errorReply(protocol.ForwardLost, nil)), nil
		}
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
	alpha := connectTestDaemon(t, address, "alpha", secrets["alpha"], func(call IncomingCall) (Wait, error) {
		if IsRoster(call) {
			return immediate(errorReply(protocol.ForwardLost, nil)), nil
		}
		return immediate(Reply{}), nil
	})
	_ = callHosts(t, alpha)
	// Exercise both real roster legs before admitting any held forward.
	for _, probe := range []struct {
		source, destination *testDaemon
		host                string
	}{{alpha, beta, "beta"}, {beta, alpha, "alpha"}} {
		answer := make(chan Reply, 1)
		probe.source.inbox <- RosterCall{Reply: answer}
		select {
		case reply := <-answer:
			remote, err := decodeRosterReply(reply.Result)
			if reply.Error != nil || err != nil || len(remote.Hosts) != 1 || remote.Hosts[0].Host != probe.host || remote.Hosts[0].Error != "host_roster_unavailable" || len(remote.Hosts[0].Sessions) != 0 {
				t.Fatal("invalid incomplete capacity fixture roster", probe.host, reply, err)
			}
		case <-admitted:
			t.Fatal("fixture roster was admitted as a held forward", probe.host)
		case <-time.After(time.Second):
			t.Fatal("capacity fixture roster did not return", probe.host)
		}
		if hosts := callHosts(t, probe.source); len(hosts) != 1 || hosts[0] != probe.host {
			select {
			case <-probe.destination.closed:
				t.Fatal("fixture roster closed its destination before any forward", probe.host)
			case <-time.After(time.Second):
				t.Fatal("fixture destination disappeared without observed close", probe.host)
			}
		}
	}
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
	// The source's ordinary cap is full. Query from the other endpoint rather
	// than requiring an ordinary hosts request to bypass pending accounting.
	if hosts := callHosts(t, beta); len(hosts) != 1 || hosts[0] != "alpha" {
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

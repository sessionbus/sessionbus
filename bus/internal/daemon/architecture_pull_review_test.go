// SPDX-License-Identifier: GPL-3.0-only

package daemon

import (
	"testing"

	"github.com/antst/sessionbus/bus/internal/federation"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func pullReviewSender(d *Daemon) *session {
	s := newSession(d)
	s.identity = &entry{row: row{SessionID: "sender@alpha", Name: "sender@alpha", Product: "peer", Groups: []string{"team", "session:sender@alpha"}}, peer: true, done: make(chan struct{})}
	s.forwarded = make(chan federation.Reply, 1)
	return s
}

func TestReviewHostScopedGroupConsumesWholeResult(t *testing.T) {
	d := &Daemon{host: "alpha"}
	d.directory = newDirectory(d, nil)
	d.federation = &federationLink{inbox: make(chan any, 256), cancel: func() {}}
	s := pullReviewSender(d)
	input := &protocol.MessageSendRequest{Group: "team", Host: "beta", Message: "hello"}
	raw, _ := protocol.EncodeParams("message.send", input)
	if !s.sendFederated(protocol.Frame{ID: 1, Method: "message.send", Params: raw, Request: true}, input, "message-fixture") {
		t.Fatal("host-scoped group was not forwarded")
	}
	call := (<-d.federation.inbox).(federation.OutgoingCall)
	result, _ := protocol.EncodeResult("message.send", protocol.MessageSendResult{MessageID: "mail", Deliveries: []protocol.MessageSendDelivery{}})
	call.Reply <- federation.Reply{Result: result}
	event := (<-s.inbox).(replyEvent)
	defer func() {
		if failure := recover(); failure != nil {
			t.Fatalf("whole remote send result entered per-delivery consumer: %v", failure)
		}
	}()
	s.consumeReply(event)
}

func TestReviewMixedMulticastDeduplicatesBeforeDelivery(t *testing.T) {
	d, target, _ := reviewSession(t)
	d.host = "alpha"
	d.federation = &federationLink{inbox: make(chan any, 256), cancel: func() {}}
	item := &entry{row: row{SessionID: "recipient-id@alpha", Name: "recipient@alpha", Groups: []string{"team"}}, peer: true, attachment: target, done: make(chan struct{})}
	d.directory.mu.Lock()
	d.directory.entries[item.row.SessionID] = item
	d.directory.addConnected(item)
	d.directory.mu.Unlock()
	s := pullReviewSender(d)
	input := &protocol.MessageSendRequest{Targets: []string{"recipient@alpha", "recipient-id@alpha", "away@beta"}, Message: "once"}
	raw, _ := protocol.EncodeParams("message.send", input)
	s.sendFederated(protocol.Frame{ID: 1, Method: "message.send", Params: raw, Request: true}, input, "message-fixture")
	count := len(target.inbox)
	var deliveredID string
	for len(target.inbox) != 0 {
		call := (<-target.inbox).(routedRequest)
		deliveredID = call.params.(protocol.DeliveryRequest).MessageID
		call.reply <- answer{value: &protocol.DeliveryReceipt{Disposition: "injected"}}
	}
	remote := (<-d.federation.inbox).(federation.OutgoingCall)
	result, _ := protocol.EncodeResult("message.send", protocol.MessageSendResult{MessageID: "remote-mail", Deliveries: []protocol.MessageSendDelivery{}})
	remote.Reply <- federation.Reply{Result: result}
	event := (<-s.inbox).(replyEvent)
	s.consumeReply(event)
	response := <-s.forwarded
	value, err := protocol.DecodeResult("message.send", response.Result)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("same local recipient received %d copies before receipt deduplication", count)
	}
	if returnedID := value.(*protocol.MessageSendResult).MessageID; returnedID != deliveredID {
		t.Errorf("caller message_id %q differs from delivered message_id %q", returnedID, deliveredID)
	}
}

func TestReviewListAdmissionFailureCannotFallBackToLocal(t *testing.T) {
	d := &Daemon{host: "alpha"}
	d.directory = newDirectory(d, nil)
	d.federation = &federationLink{inbox: make(chan any, 1), cancel: func() {}}
	d.federation.inbox <- federation.HostsCall{Reply: make(chan federation.Reply, 1)}
	s := pullReviewSender(d)
	s.list(protocol.Frame{ID: 1, Method: "session.list", Request: true}, &protocol.SessionListRequest{})
	value := <-s.forwarded
	if value.Error == nil || value.Error.Code != protocol.ForwardLost {
		t.Fatalf("full active federation inbox produced local success: %#v", value)
	}
}

func TestReviewEmptyHostGroupReturnsSuccessfulEmptyArray(t *testing.T) {
	d := &Daemon{host: "alpha"}
	d.directory = newDirectory(d, nil)
	d.federation = &federationLink{inbox: make(chan any, 256), cancel: func() {}}
	s := pullReviewSender(d)
	input := &protocol.MessageSendRequest{Group: "team", Host: "beta", Message: "hello"}
	raw, _ := protocol.EncodeParams("message.send", input)
	s.sendFederated(protocol.Frame{ID: 1, Method: "message.send", Params: raw, Request: true}, input, "message-fixture")
	call := (<-d.federation.inbox).(federation.OutgoingCall)
	result, _ := protocol.EncodeResult("message.send", protocol.MessageSendResult{MessageID: call.Value.Request.MessageID, Deliveries: []protocol.MessageSendDelivery{}})
	call.Reply <- federation.Reply{Result: result}
	s.consumeReply((<-s.inbox).(replyEvent))
	reply := <-s.forwarded
	if reply.Error != nil {
		t.Fatalf("empty successful group became error: %#v", reply.Error)
	}
	value, err := protocol.DecodeResult("message.send", reply.Result)
	if err != nil || len(value.(*protocol.MessageSendResult).Deliveries) != 0 {
		t.Fatalf("invalid empty result: %s (%v)", reply.Result, err)
	}
}

func TestReviewUnknownHostGroupRemainsUnknownHost(t *testing.T) {
	d := &Daemon{host: "alpha"}
	d.directory = newDirectory(d, nil)
	s := pullReviewSender(d)
	input := &protocol.MessageSendRequest{Group: "team", Host: "beta", Message: "hello"}
	raw, _ := protocol.EncodeParams("message.send", input)
	s.sendFederated(protocol.Frame{ID: 1, Method: "message.send", Params: raw, Request: true}, input, "message-fixture")
	s.consumeReply((<-s.inbox).(replyEvent))
	reply := <-s.forwarded
	if reply.Error == nil || reply.Error.Code != protocol.UnknownHost {
		t.Fatalf("known absence became ambiguous transport loss: %#v", reply.Error)
	}
}

// SPDX-License-Identifier: GPL-3.0-only
package daemon

import (
	"testing"

	"github.com/antst/sessionbus/bus/internal/federation"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func groupWritten(t *testing.T, host string) federation.Reply {
	t.Helper()
	raw, err := protocol.EncodeResult("message.send", protocol.MessageSendResult{MessageID: "mail", Deliveries: []protocol.MessageSendDelivery{{Target: "recipient@" + host, SessionID: "recipient@" + host, DeliveryID: "delivery-" + host, Disposition: "written"}}})
	if err != nil {
		t.Fatal(err)
	}
	return federation.Reply{Result: raw}
}

func TestGroupSendKeepsWrittenLegsAndLabelsFailedKnownHost(t *testing.T) {
	d := &Daemon{host: "alpha"}
	d.directory = newDirectory(d, nil)
	d.federation = &federationLink{inbox: make(chan any, 2), cancel: func() {}}
	s := pullReviewSender(d)
	hosts := make(chan federation.Reply, 1)
	hosts <- federation.Reply{Result: []byte(`{"hosts":["beta","gamma"]}`)}
	localCollected := false
	finished := make(chan answer, 1)
	go func() {
		finished <- s.collectGroupSend(s.federationCaller(), &protocol.MessageSendRequest{Group: "team", Message: "hello"}, "mail", func(<-chan struct{}) (federation.Reply, bool) {
			localCollected = true
			return groupWritten(t, "alpha"), true
		}, hosts, s.identity.done)
	}()
	for range 2 {
		call := (<-d.federation.inbox).(federation.OutgoingCall)
		params, err := protocol.DecodeParams("message.send", call.Value.Request.Params)
		if err != nil {
			t.Fatal(err)
		}
		if params.(*protocol.MessageSendRequest).Host == "beta" {
			call.Reply <- federation.Reply{Error: &protocol.RPCError{Code: protocol.ForwardLost, Message: "forward_lost"}}
		} else {
			call.Reply <- groupWritten(t, "gamma")
		}
	}
	result := <-finished
	if result.code != 0 || !localCollected {
		t.Fatal("group result discarded a confirmed leg", result.code, localCollected)
	}
	value := result.value.(*protocol.MessageSendResult)
	if len(value.Deliveries) != 3 {
		t.Fatal(value)
	}
	written, rejected := 0, 0
	for _, delivery := range value.Deliveries {
		if delivery.Disposition == "written" {
			written++
		} else {
			rejected++
			if delivery.Target != "team@beta" || delivery.Reason != "no_receipt" || delivery.SessionID != "" || delivery.DeliveryID != "" {
				t.Fatal(delivery)
			}
		}
	}
	if written != 2 || rejected != 1 {
		t.Fatal(value)
	}
	if _, err := protocol.EncodeResult("message.send", value); err != nil {
		t.Fatal("strict result", err)
	}
}

func TestGroupEnumerationFailureCollectsAlreadyStartedLocalDelivery(t *testing.T) {
	d := &Daemon{host: "alpha"}
	d.directory = newDirectory(d, nil)
	s := pullReviewSender(d)
	hosts := make(chan federation.Reply, 1)
	hosts <- federation.Reply{Error: &protocol.RPCError{Code: protocol.ForwardLost, Message: "forward_lost"}}
	collected := 0
	value := s.collectGroupSend(s.federationCaller(), &protocol.MessageSendRequest{Group: "team", Message: "hello"}, "mail", func(<-chan struct{}) (federation.Reply, bool) { collected++; return groupWritten(t, "alpha"), true }, hosts, s.identity.done)
	if value.code != 0 || collected != 1 {
		t.Fatal("local receipt was abandoned", value.code, collected)
	}
	result := value.value.(*protocol.MessageSendResult)
	if len(result.Deliveries) != 2 {
		t.Fatal(result)
	}
	written, rejected := 0, 0
	for _, delivery := range result.Deliveries {
		switch delivery.Disposition {
		case "written":
			written++
			if delivery.Target != "recipient@alpha" || delivery.SessionID != "recipient@alpha" || delivery.DeliveryID != "delivery-alpha" || delivery.Reason != "" {
				t.Fatal("enumeration lost the exact local receipt", delivery)
			}
		case "rejected":
			rejected++
			if delivery.Target != "team" || delivery.Reason != "no_receipt" || delivery.SessionID != "" || delivery.DeliveryID != "" {
				t.Fatal("enumeration invented remote identity", delivery)
			}
		default:
			t.Fatal("unexpected enumeration delivery disposition", delivery)
		}
	}
	if written != 1 || rejected != 1 {
		t.Fatal("enumeration did not retain one local receipt and one refusal", result)
	}
	if _, err := protocol.EncodeResult("message.send", result); err != nil {
		t.Fatal(err)
	}
}

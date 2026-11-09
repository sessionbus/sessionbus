// SPDX-License-Identifier: GPL-3.0-only

package daemon

import (
	"strings"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/federation"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func silenceListed(host string) federation.Reply {
	raw, _ := protocol.EncodeResult("session.list", protocol.SessionListResult{
		Sessions: []protocol.SessionSummary{{SessionID: "peer@" + host, Kind: "peer", Product: "peer", Groups: []string{"team"}, Connected: true}},
		Hosts:    []protocol.HostProducts{{Host: host, Products: []string{"peer"}}},
	})
	return federation.Reply{Result: raw}
}

// Only existing APIs: this identical fixture fails on old main because one
// silent host discarded the already-confirmed local and other-host results.
func TestSilentListLegPreservesConfirmedRows(t *testing.T) {
	d := &Daemon{host: "alpha"}
	d.directory = newDirectory(d, nil)
	d.federation = &federationLink{inbox: make(chan any, 2), cancel: func() {}}
	s := pullReviewSender(d)
	hosts := make(chan federation.Reply, 1)
	hosts <- federation.Reply{Result: []byte(`{"hosts":["beta","gamma"]}`)}
	done := make(chan answer, 1)
	go func() {
		done <- s.collectFederatedList(s.federationCaller(), func(<-chan struct{}) (federation.Reply, bool) { return silenceListed("alpha"), true }, hosts, s.identity.done)
	}()
	for range 2 {
		call := (<-d.federation.inbox).(federation.OutgoingCall)
		params, err := protocol.DecodeParams("session.list", call.Value.Request.Params)
		if err != nil {
			t.Fatal(err)
		}
		if params.(*protocol.SessionListRequest).Host == "beta" {
			call.Reply <- federation.Reply{Error: &protocol.RPCError{Code: protocol.ForwardLost, Message: "forward_lost"}}
		} else {
			call.Reply <- silenceListed("gamma")
		}
	}
	value := <-done
	if value.code != 0 {
		t.Fatal("silent leg discarded confirmed rows", value.code)
	}
	listed := value.value.(*protocol.SessionListResult)
	if len(listed.Sessions) != 2 || listed.Sessions[0].SessionID != "peer@alpha" || listed.Sessions[1].SessionID != "peer@gamma" || len(listed.Hosts) != 2 || listed.SelfInfo.SessionID != "sender@alpha" {
		t.Fatal("partial result changed confirmed identities", listed)
	}
}

func TestAvailabilityListRequestSnapshotSurvivesRehello(t *testing.T) {
	for _, initial := range []bool{false, true} {
		t.Run(map[bool]string{false: "old-to-new", true: "new-to-old"}[initial], func(t *testing.T) {
			d := &Daemon{host: "alpha"}
			d.directory = newDirectory(d, nil)
			d.federation = &federationLink{inbox: make(chan any, 1), cancel: func() {}}
			s := pullReviewSender(d)
			s.supportsAvailability = initial
			params, _ := protocol.EncodeParams("session.list", protocol.SessionListRequest{Host: "beta"})
			s.forward(protocol.Frame{ID: 1, Method: "session.list", Params: params, Request: true}, "list@beta")
			call := (<-d.federation.inbox).(federation.OutgoingCall)
			if call.Value.From.Availability != initial {
				t.Fatal("forward did not capture hello capability")
			}
			s.supportsAvailability = !initial // A later hello cannot change this request.
			call.Reply <- federation.Reply{Result: []byte(`{"sessions":[],"hosts":[{"host":"beta","products":[],"availability":"not_responding"}]}`)}
			s.consumeReply((<-s.inbox).(replyEvent))
			value := <-s.forwarded
			if value.Error != nil || strings.Contains(string(value.Result), `"availability"`) != initial {
				t.Fatal("reply used current hello instead of admission snapshot", value)
			}
		})
	}
}

func TestSilentHubListKeepsLocalRowsWithoutInventingRemoteNames(t *testing.T) {
	d := &Daemon{host: "alpha"}
	d.directory = newDirectory(d, nil)
	s := pullReviewSender(d)
	hosts := make(chan federation.Reply, 1)
	hosts <- federation.Reply{Error: &protocol.RPCError{Code: protocol.ForwardLost, Message: "forward_lost"}}
	value := s.collectFederatedList(s.federationCaller(), func(<-chan struct{}) (federation.Reply, bool) { return silenceListed("alpha"), true }, hosts, s.identity.done)
	if value.code != 0 {
		t.Fatal(value.code)
	}
	listed := value.value.(*protocol.SessionListResult)
	if len(listed.Hosts) != 1 || listed.Hosts[0].Host != "alpha" || len(listed.Sessions) != 1 || listed.SelfInfo.SessionID != "sender@alpha" {
		t.Fatal("invented remote inventory or lost local result", listed)
	}
}

func TestAvailabilityAggregateIncludesOnlyKnownSilentHost(t *testing.T) {
	for _, marked := range []bool{false, true} {
		t.Run(map[bool]string{false: "generic-refusal", true: "successful-marked-stub"}[marked], func(t *testing.T) {
			d := &Daemon{host: "alpha"}
			d.directory = newDirectory(d, nil)
			d.federation = &federationLink{inbox: make(chan any, 1), cancel: func() {}}
			s := pullReviewSender(d)
			s.supportsAvailability = true
			caller := s.federationCaller()
			hosts := make(chan federation.Reply, 1)
			hosts <- federation.Reply{Result: []byte(`{"hosts":["beta"]}`)}
			done := make(chan answer, 1)
			go func() {
				done <- s.collectFederatedList(caller, func(<-chan struct{}) (federation.Reply, bool) { return silenceListed("alpha"), true }, hosts, s.identity.done)
			}()
			call := (<-d.federation.inbox).(federation.OutgoingCall)
			if marked {
				call.Reply <- federation.Reply{Result: []byte(`{"sessions":[],"hosts":[{"host":"beta","products":[],"availability":"not_responding"}]}`)}
			} else {
				call.Reply <- federation.Reply{Error: &protocol.RPCError{Code: protocol.ForwardLost, Message: "forward_lost"}}
			}
			value := <-done
			if value.code != 0 {
				t.Fatal(value.code)
			}
			listed := value.value.(*protocol.SessionListResult)
			if len(listed.Sessions) != 1 || len(listed.Hosts) != 1+boolCount(marked) || listed.Hosts[0].Host != "alpha" || listed.Hosts[0].Availability != "" {
				t.Fatal("partial availability lost confirmed rows or invented products", listed)
			}
			if marked && (listed.Hosts[1].Host != "beta" || listed.Hosts[1].Availability != "not_responding" || len(listed.Hosts[1].Products) != 0) {
				t.Fatal("actual marked result not preserved", listed)
			}
		})
	}
}

func boolCount(value bool) int {
	if value {
		return 1
	}
	return 0
}

func TestLaterListAdmissionRefusalPreservesLocalAndEarlierHealthyLeg(t *testing.T) {
	d := &Daemon{host: "alpha"}
	d.directory = newDirectory(d, nil)
	d.federation = &federationLink{inbox: make(chan any, 1), cancel: func() { t.Error("ordinary list overflow closed link") }}
	s := pullReviewSender(d)
	defer close(s.identity.done)
	s.supportsAvailability = true
	hosts := make(chan federation.Reply, 1)
	hosts <- federation.Reply{Result: []byte(`{"hosts":["beta","gamma","delta"]}`)}
	postsFinished := make(chan struct{})
	done := make(chan answer, 1)
	go func() {
		done <- s.collectFederatedList(s.federationCaller(), func(<-chan struct{}) (federation.Reply, bool) {
			close(postsFinished)
			return silenceListed("alpha"), true
		}, hosts, s.identity.done)
	}()
	select {
	case <-postsFinished:
	case value := <-done:
		t.Fatal("later admission refusal discarded local and admitted healthy work", value.code)
	case <-time.After(time.Second):
		t.Fatal("post loop did not reach one-shot collection")
	}
	call := (<-d.federation.inbox).(federation.OutgoingCall)
	if string(call.Value.Request.Params) != `{"host":"beta"}` {
		t.Fatal("unexpected or retried leg", string(call.Value.Request.Params))
	}
	call.Reply <- silenceListed("beta")
	value := <-done
	if value.code != 0 {
		t.Fatal(value.code)
	}
	listed := value.value.(*protocol.SessionListResult)
	if len(listed.Sessions) != 2 || len(listed.Hosts) != 2 || listed.SelfInfo.SessionID != "sender@alpha" || listed.Hosts[0].Host != "alpha" || listed.Hosts[1].Host != "beta" || listed.Hosts[0].Availability != "" || listed.Hosts[1].Availability != "" || len(d.federation.inbox) != 0 {
		t.Fatal("partial admission lost confirmed rows or invented host status", listed)
	}
}

func TestNonForwardLostListErrorRemainsAnError(t *testing.T) {
	d := &Daemon{host: "alpha"}
	d.directory = newDirectory(d, nil)
	d.federation = &federationLink{inbox: make(chan any, 1), cancel: func() {}}
	s := pullReviewSender(d)
	hosts := make(chan federation.Reply, 1)
	hosts <- federation.Reply{Result: []byte(`{"hosts":["beta"]}`)}
	done := make(chan answer, 1)
	go func() {
		done <- s.collectFederatedList(s.federationCaller(), func(<-chan struct{}) (federation.Reply, bool) { return silenceListed("alpha"), true }, hosts, s.identity.done)
	}()
	call := (<-d.federation.inbox).(federation.OutgoingCall)
	call.Reply <- federation.Reply{Error: &protocol.RPCError{Code: protocol.UnknownSession, Message: "unknown_session"}}
	if value := <-done; value.code != protocol.ForwardLost || value.value != nil {
		t.Fatal("non-forward-lost RPC error was turned into partial success", value)
	}
}

func TestSilentSendLegPreservesWrittenAndUnknownDispositions(t *testing.T) {
	written := make(chan federation.Reply, 1)
	raw, err := protocol.EncodeResult("message.send", protocol.MessageSendResult{MessageID: "mail", Deliveries: []protocol.MessageSendDelivery{{Target: "peer@alpha", SessionID: "peer@alpha", DeliveryID: "delivery-alpha", Disposition: "written"}}})
	if _, decodeErr := protocol.DecodeResult("message.send", raw); err != nil || decodeErr != nil {
		t.Fatalf("invalid confirmed-leg fixture: %v/%v", err, decodeErr)
	}
	written <- federation.Reply{Result: raw}
	silent := make(chan federation.Reply, 1)
	silent <- federation.Reply{Error: &protocol.RPCError{Code: protocol.ForwardLost, Message: "forward_lost"}}
	value := collectMessageSend("mail", []messageLeg{{labels: []string{"peer@alpha"}, reply: written}, {labels: []string{"peer@beta"}, reply: silent}}, []string{"peer@alpha", "peer@beta"}, make(chan struct{}), protocol.NotConnected)
	if value.code != 0 {
		t.Fatal(value)
	}
	listed := value.value.(*protocol.MessageSendResult)
	if len(listed.Deliveries) != 2 || listed.Deliveries[0].Disposition != "written" || listed.Deliveries[1].Disposition != "rejected" || listed.Deliveries[1].Reason != "no_receipt" {
		t.Fatal("lost partial success or invented nonconsumption", listed)
	}
}

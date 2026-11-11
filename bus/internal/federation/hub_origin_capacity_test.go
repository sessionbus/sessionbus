// SPDX-License-Identifier: GPL-3.0-only
package federation

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/conn"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func originCapacityRequests(t *testing.T) []protocol.Frame {
	t.Helper()
	caller := policyTestCaller()
	values := []struct {
		method string
		params any
	}{
		{hostsMethod, struct{}{}}, {rosterMethod, struct{}{}},
		{ownerEndMethod, OwnerEnd{Host: "beta", SessionID: caller.SessionID, Lifetime: "life"}},
		{forwardMethod, Forward{From: caller, Request: policyTestRequest(t, "turn.wait", protocol.WaitRequest{SessionID: "lane@beta"})}},
		{TransferCreateMethod, TransferCreate{From: caller, Generation: "generation", Target: "receiver@beta", Kind: "offer", Name: "file"}},
		{TransferAnnounceMethod, protocol.TransferTarget{ID: "id"}},
		{TransferEndMethod, TransferBindingTarget{Binding: "binding"}},
		{"transfer.accept", protocol.TransferAccept{ID: "id"}},
		{"transfer.status", protocol.TransferTarget{ID: "id"}},
		{"transfer.cancel", protocol.TransferTarget{ID: "id"}},
		{"transfer.fail", protocol.TransferFailure{ID: "id", Reason: "source_read_failed"}},
		{"transfer.chunk", protocol.TransferChunk{ID: "id", Data: "YQ=="}},
		{"transfer.finish", protocol.TransferFinish{ID: "id", Bytes: 1, SHA256: strings.Repeat("a", 64)}},
	}
	var result []protocol.Frame
	for _, value := range values {
		raw, err := json.Marshal(value.params)
		if err != nil {
			t.Fatal(err)
		}
		if isTransferFrame(value.method) {
			if _, err := decodeTransferParams(value.method, raw); err != nil {
				t.Fatal(value.method, err)
			}
		}
		result = append(result, protocol.Frame{Request: true, Method: value.method, Params: raw})
	}
	return result
}

func TestOriginCapacityAllFamiliesAndConsumptionOnlyRefill(t *testing.T) {
	link, _, registry, responses, _ := originReleaseFixture(t)
	link.fd = policyTransferFD(t, "alpha")
	link.transfers.Store(newHubTransfers(context.Background()))
	last := int64(0)
	for id := int64(1); id <= protocol.MaxOperations; id++ {
		frame := protocol.Frame{Request: true, ID: id, Method: hostsMethod, Params: []byte(`{}`)}
		if err := link.frame(conn.Frame{Value: frame, Ordered: true}, nil, &last); err != nil {
			t.Fatal(err)
		}
		if event := <-registry; event.kind != registryHosts {
			t.Fatal(event)
		}
	}
	if link.originAdmitted != 256 {
		t.Fatal("admission count", link.originAdmitted)
	}
	// Enqueue does not release a slot, including after the producing helper ends.
	if !link.postOriginReply(originReply{id: 1, value: emptyReply()}) {
		t.Fatal("enqueue")
	}
	for repeat := 0; repeat < 2; repeat++ {
		for _, frame := range originCapacityRequests(t) {
			last++
			frame.ID = last
			if err := link.frame(conn.Frame{Value: frame, Ordered: true}, nil, &last); err != nil {
				t.Fatal(frame.Method, err)
			}
			want := protocol.ForwardLost
			if isTransferFrame(frame.Method) {
				want = protocol.Busy
			}
			select {
			case reply := <-responses:
				if reply.ID != frame.ID || reply.Error == nil || reply.Error.Code != want || len(reply.Error.Data) != 0 {
					t.Fatal(frame.Method, reply)
				}
			case <-time.After(time.Second):
				t.Fatal("refusal missing", frame.Method)
			}
			if link.originAdmitted != 256 || len(registry) != 0 || len(link.originReplies) != 1 {
				t.Fatal("refusal dispatched, charged or freed capacity", frame.Method)
			}
		}
	}
	link.consumeOriginReply(<-link.originReplies)
	if link.originAdmitted != 255 {
		t.Fatal("consumption did not free exactly one slot")
	}
	last++
	if err := link.frame(conn.Frame{Ordered: true, Value: protocol.Frame{ID: last, Request: true, Method: hostsMethod, Params: []byte(`{}`)}}, nil, &last); err != nil {
		t.Fatal(err)
	}
	if event := <-registry; event.kind != registryHosts || link.originAdmitted != 256 {
		t.Fatal("freed slot unusable", event)
	}
	select {
	case <-link.wire.Done():
		t.Fatal("per-call refusal closed origin")
	default:
	}
}

func TestOriginMalformedBeforeCapacityKeepsValidation(t *testing.T) {
	link, _, registry, responses, _ := originReleaseFixture(t)
	link.fd = policyTransferFD(t, "alpha")
	link.transfers.Store(newHubTransfers(context.Background()))
	link.originAdmitted = protocol.MaxOperations
	last := int64(0)
	for _, method := range []string{TransferCreateMethod, "transfer.status", "transfer.chunk", "transfer.finish"} {
		last++
		if err := link.frame(conn.Frame{Ordered: true, Value: protocol.Frame{ID: last, Request: true, Method: method, Params: []byte(`{}`)}}, nil, &last); err != nil {
			t.Fatal(err)
		}
		frame := managementFrame(t, responses)
		if frame.Error == nil || frame.Error.Code != protocol.Internal || string(frame.Error.Data) != `"transfer_unavailable"` {
			t.Fatal(method, frame)
		}
	}
	for _, from := range []Caller{func() Caller {
		c := policyTestCaller()
		c.SessionID = "parent@beta"
		c.PrivateGroup = "session:parent@beta"
		c.Groups = []string{c.PrivateGroup}
		return c
	}(), func() Caller { c := policyTestCaller(); c.SourceAttachment = "attachment"; return c }()} {
		last++
		raw, _ := json.Marshal(TransferCreate{From: from, Generation: "generation", Target: "receiver@beta", Kind: "offer", Name: "file"})
		if err := link.frame(conn.Frame{Ordered: true, Value: protocol.Frame{ID: last, Request: true, Method: TransferCreateMethod, Params: raw}}, nil, &last); err != nil {
			t.Fatal(err)
		}
		if frame := managementFrame(t, responses); frame.Error == nil || frame.Error.Code != protocol.Internal {
			t.Fatal(frame)
		}
	}
	for _, event := range []conn.Frame{
		{Value: protocol.Frame{ID: 100, Request: true, Method: "transfer.bind", Params: []byte(`{}`)}},
		{Value: protocol.Frame{ID: 101, Request: true, Method: hostsMethod, Params: []byte(`{"bad":true}`)}},
		{Value: protocol.Frame{ID: 1, Request: true, Method: hostsMethod, Params: []byte(`{}`)}},
	} {
		if err := link.frame(event, nil, &last); err == nil {
			t.Fatal("cap masked fatal method/params/order", event.Value)
		}
	}
	link.fd = nil
	if err := link.frame(conn.Frame{Value: protocol.Frame{ID: 200, Request: true, Method: "transfer.status", Params: []byte(`{"id":"id"}`)}}, nil, &last); err == nil {
		t.Fatal("cap masked missing ALPN")
	}
	if link.originAdmitted != 256 || len(registry) != 0 || len(link.originReplies) != 0 {
		t.Fatal("invalid request changed admission")
	}
}

func TestOriginEveryProducerReservesUntilConsumption(t *testing.T) {
	for _, request := range originCapacityRequests(t) {
		t.Run(request.Method, func(t *testing.T) {
			link, _, registry, _, _ := originReleaseFixture(t)
			link.fd = policyTransferFD(t, "alpha")
			link.transfers.Store(newHubTransfers(context.Background()))
			request.ID = 1
			last := int64(0)
			if err := link.frame(conn.Frame{Value: request}, nil, &last); err != nil {
				t.Fatal(err)
			}
			if link.originAdmitted != 1 {
				t.Fatal("producer did not reserve", request.Method)
			}
			if request.Method == TransferCreateMethod || !isTransferFrame(request.Method) {
				select {
				case <-registry:
				case <-time.After(time.Second):
					t.Fatal("registry admission missing")
				}
				if !link.postOriginReply(originReply{id: request.ID, value: emptyReply()}) {
					t.Fatal("registry reply enqueue")
				}
			}
			select {
			case reply := <-link.originReplies:
				if link.originAdmitted != 1 {
					t.Fatal("producer/helper return freed reservation")
				}
				link.consumeOriginReply(reply)
				if link.originAdmitted != 0 {
					t.Fatal("consumption did not retire reservation")
				}
			case <-time.After(time.Second):
				t.Fatal("actual transfer reply missing")
			}
		})
	}
}

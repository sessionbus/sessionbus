// SPDX-License-Identifier: GPL-3.0-only

package federation

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/transfer"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

// The callback fixture executes the actual hub bind/adopt/drop path. Its TLS
// handshake establishes capabilities; fixed callback controls use the existing
// owner inbox rather than native products or filesystem operations.
func policyTransferFD(t *testing.T, host string) *tls.Conn {
	t.Helper()
	secret := testSecret(host[0])
	serverConfig, err := ServerTLS(map[string]string{host: secret})
	if err != nil {
		t.Fatal(err)
	}
	clientConfig, err := ClientTLS(host, secret)
	if err != nil {
		t.Fatal(err)
	}
	left, right := net.Pipe()
	_ = left.SetDeadline(time.Now().Add(time.Second))
	_ = right.SetDeadline(time.Now().Add(time.Second))
	client, server := tls.Client(left, clientConfig), tls.Server(right, serverConfig)
	done := make(chan error, 1)
	go func() { done <- server.Handshake() }()
	if err := client.Handshake(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	_ = left.Close()
	_ = right.Close()
	return server
}

type policyTransferCallbacks struct {
	before func(string)
	reply  chan<- originReply
}

func policyTransferLink(t *testing.T, ctx context.Context, host string, identity protocol.DeliverySource, methods chan<- string, controls ...policyTransferCallbacks) *hostLink {
	t.Helper()
	var hooks policyTransferCallbacks
	if len(controls) != 0 {
		hooks = controls[0]
	}
	link := &hostLink{host: host, attachment: host + "-attachment", fd: policyTransferFD(t, host), inbox: make(chan any, 8), done: make(chan struct{})}
	joined := make(chan struct{})
	var sequence atomic.Int32
	go func() {
		defer close(joined)
		for {
			select {
			case raw := <-link.inbox:
				if result, ok := raw.(originReply); ok && hooks.reply != nil {
					if result.release != nil {
						result.release()
					}
					hooks.reply <- result
					continue
				}
				call, ok := raw.(TransferCall)
				if !ok {
					t.Error("unexpected callback fixture event")
					continue
				}
				methods <- call.Method
				if hooks.before != nil {
					hooks.before(call.Method)
				}
				encoded, _ := json.Marshal(call.Params)
				if _, err := decodeTransferParams(call.Method, encoded); err != nil {
					t.Error("invalid callback request", call.Method, err)
					call.Reply <- errorReply(protocol.Internal, nil)
					continue
				}
				var result any = struct{}{}
				switch call.Method {
				case TransferBindMethod:
					result = TransferBound{Binding: fmt.Sprintf("%s-binding-%d", host, sequence.Add(1)), Generation: host + "-generation", Identity: identity}
				case TransferHoldMethod:
					result = TransferHeld{Held: true}
				}
				reply := transferReply(result, nil)
				if !validateTransferResult(call.Method, reply.Result) {
					t.Error("invalid callback result", call.Method, string(reply.Result))
				}
				call.Reply <- reply
			case <-ctx.Done():
				return
			}
		}
	}()
	t.Cleanup(func() {
		select {
		case <-joined:
		case <-time.After(time.Second):
			t.Error("callback fixture did not join")
		}
	})
	return link
}

func TestPolicyTransferActualBoundIdentityAndUnadoptedCleanup(t *testing.T) {
	for _, mode := range []string{"allowed-named", "denied-private", "wrong-source-id", "relation-snapshot"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			methods := make(chan string, 32)
			source := protocol.DeliverySource{SessionID: "parent@alpha", Product: "peer", Groups: []string{"other", "session:parent@alpha"}}
			target := protocol.DeliverySource{SessionID: "child@beta", Product: "peer", Groups: []string{"other", "session:parent@alpha"}}
			if mode == "allowed-named" {
				source.Groups, target.Groups = []string{"team"}, []string{"team"}
			}
			if mode == "wrong-source-id" {
				source.SessionID = "different@alpha"
				source.Groups, target.Groups = []string{"team"}, []string{"team"}
			}
			origin := policyTransferLink(t, ctx, "alpha", source, methods)
			destination := policyTransferLink(t, ctx, "beta", target, methods)
			// Cleanup callbacks join after their shared context is cancelled.
			defer cancel()
			h := newHubTransfers(ctx)
			policy := testPolicy(t, `{"hosts":{"alpha":{"groups":["team"]}}}`)
			check := &policyTransfer{policy: policy, sourceHost: "alpha", targetHost: "beta", sourceID: "parent@alpha"}
			if mode == "relation-snapshot" {
				r := newPolicyRelations()
				entry := &policyRelation{lane: target.SessionID, creator: source.SessionID, private: "session:parent@alpha/child"}
				r.entries[entry.lane] = entry
				check.relations = r.transferSnapshot(source.SessionID, "beta")
				delete(r.entries, entry.lane) // An admitted immutable reference survives later removal.
			}
			from := policyTestCaller() // Its asserted team must not override actual endpoint groups.
			input := TransferCreate{From: from, Generation: "source-generation", Target: "alias@beta", Kind: "offer", Name: "file"}
			reservation, err := h.reserveCreate(origin, destination, input)
			if err != nil || h.budget.Usage().Pending != 1 {
				t.Fatal("same pending reservation was not admitted first", err, h.budget.Usage())
			}
			value, err := h.createReserved(origin, destination, input, reservation, check)
			allowed := mode == "allowed-named" || mode == "relation-snapshot"
			if allowed {
				if err != nil || value.Transfer.ID == "" || h.budget.Usage().Pending != 1 {
					t.Fatal("captured actual identity/relation admission failed", err, h.budget.Usage())
				}
				if _, err := policyTransferAction(t, h, origin, TransferAnnounceMethod, protocol.TransferTarget{ID: value.Transfer.ID}); err != nil {
					t.Fatal("ordinary creation announcement failed", err)
				}
				if _, err := policyTransferAction(t, h, origin, "transfer.cancel", protocol.TransferTarget{ID: value.Transfer.ID}); err != nil {
					t.Fatal("ordinary terminal cleanup failed", err)
				}
			} else if err != transfer.ErrRole {
				t.Fatal("caller assertion/private overlap/wrong source admitted binding", err)
			}
			deadline := time.Now().Add(time.Second)
			for h.budget.Usage().Pending != 0 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if usage := h.budget.Usage(); usage.Pending != 0 || usage.Active != 0 {
				t.Fatal("terminal/refusal leaked the existing reservation", usage)
			}
			h.mu.Lock()
			left := len(h.bindings)
			h.mu.Unlock()
			if left != 0 {
				t.Fatal("obtained bindings were not retired", left)
			}
			counts := map[string]int{}
			for len(methods) != 0 {
				counts[<-methods]++
			}
			if counts[TransferBindMethod] != 2 || counts[TransferBindReadyMethod] != 2 || !allowed && counts[TransferDropMethod] != 2 || counts[TransferPrepareMethod] != 0 || counts["transfer.chunk"] != 0 || counts["transfer.finish"] != 0 {
				t.Fatal("policy did not check both actual binds before preparation/bytes", counts)
			}
		})
	}
}

func policyTransferAction(t *testing.T, h *hubTransfers, origin *hostLink, method string, value any) (any, error) {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	params, err := decodeTransferParams(method, raw)
	if err != nil {
		t.Fatal("invalid hub-action fixture", method, err)
	}
	return h.action(origin, method, params)
}

func TestPolicyRegistryTransferBudgetPrecedesBindFanout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	origin := &hostLink{host: "alpha", attachment: "origin", fd: policyTransferFD(t, "alpha"), inbox: make(chan any, 1), done: make(chan struct{})}
	destination := &hostLink{host: "beta", fd: policyTransferFD(t, "beta"), inbox: make(chan any, 1), done: make(chan struct{})}
	h := newHubTransfers(ctx)
	var reservations []*transfer.Reservation
	for range protocol.MaxPendingTransfersPerConnection {
		r, err := h.budget.PendingFor("origin:generation")
		if err != nil {
			t.Fatal(err)
		}
		reservations = append(reservations, r)
		defer r.Release()
	}
	state := hubState{policy: testPolicy(t, `{"hosts":{"alpha":{"groups":[]}}}`), transfers: h, relations: newPolicyRelations(), links: map[string]*hostLink{"alpha": origin, "beta": destination}}
	input := TransferCreate{From: policyTestCaller(), Generation: "generation", Target: "child@beta", Kind: "offer", Name: "file"}
	raw, _ := json.Marshal(input)
	if _, err := decodeTransferParams(TransferCreateMethod, raw); err != nil {
		t.Fatal("invalid registry create fixture", err)
	}
	var released atomic.Int32
	state.handle(ctx, nil, registryEvent{kind: registryTransferCreate, link: origin, transfer: &connTransferFrame{value: protocol.Frame{ID: 1, Method: TransferCreateMethod, Params: raw}, release: func() { released.Add(1) }}})
	select {
	case event := <-origin.inbox:
		reply, ok := event.(originReply)
		if !ok || reply.value.Error == nil {
			t.Fatal("full existing budget dispatched a bind", event)
		}
		reply.release()
	default:
		t.Fatal("budget refusal did not settle immediately")
	}
	if len(origin.inbox) != 0 || len(destination.inbox) != 0 || h.budget.Usage().Pending != len(reservations) || released.Load() != 1 {
		t.Fatal("budget refusal fanned out or changed existing charges", h.budget.Usage(), released.Load())
	}
}

func TestPolicyRegistryTransferCapturesRelationBeforeBindAndReload(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	started, release := make(chan struct{}), make(chan struct{})
	replies, methods := make(chan originReply, 1), make(chan string, 32)
	from := protocol.DeliverySource{SessionID: "parent@alpha", Product: "peer", Groups: []string{"session:parent@alpha"}}
	to := protocol.DeliverySource{SessionID: "child@beta", Product: "peer", Groups: []string{"session:parent@alpha/child"}}
	origin := policyTransferLink(t, ctx, "alpha", from, methods, policyTransferCallbacks{reply: replies, before: func(method string) {
		if method == TransferBindMethod {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
			}
		}
	}})
	destination := policyTransferLink(t, ctx, "beta", to, methods)
	defer cancel()
	h := newHubTransfers(ctx)
	relations := newPolicyRelations()
	entry := &policyRelation{lane: to.SessionID, creator: from.SessionID, parentPrivate: "session:parent@alpha", private: "session:parent@alpha/child"}
	relations.entries[entry.lane], relations.charges["beta"] = entry, 1
	state := hubState{policy: testPolicy(t, `{"hosts":{"alpha":{"groups":[]}}}`), transfers: h, relations: relations, links: map[string]*hostLink{"alpha": origin, "beta": destination}}
	input := TransferCreate{From: policyTestCaller(), Generation: "generation", Target: "alias@beta", Kind: "offer", Name: "file"}
	raw, _ := json.Marshal(input)
	if _, err := decodeTransferParams(TransferCreateMethod, raw); err != nil {
		t.Fatal("invalid registry create fixture", err)
	}
	state.handle(ctx, nil, registryEvent{kind: registryTransferCreate, link: origin, transfer: &connTransferFrame{value: protocol.Frame{ID: 1, Method: TransferCreateMethod, Params: raw}}})
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("admitted source bind did not start")
	}
	if h.budget.Usage().Pending != 1 {
		t.Fatal("bind ran before existing pending charge", h.budget.Usage())
	}
	relations.remove(entry)
	ack := make(chan bool, 1)
	state.handle(ctx, nil, registryEvent{kind: registryPolicy, policy: testPolicy(t, `{"hosts":{"alpha":{"groups":[],"lanes_to":[]},"beta":{"groups":[]}}}`), ack: ack})
	<-ack
	close(release)
	select {
	case reply := <-replies:
		if reply.value.Error != nil || !validateTransferResult(TransferCreateMethod, reply.value.Result) {
			t.Fatal("reload/removal changed the admitted relation snapshot", reply.value.Error)
		}
		var created TransferCreated
		if err := json.Unmarshal(reply.value.Result, &created); err != nil {
			t.Fatal(err)
		}
		if _, err := policyTransferAction(t, h, origin, TransferAnnounceMethod, protocol.TransferTarget{ID: created.Transfer.ID}); err != nil {
			t.Fatal(err)
		}
		if _, err := policyTransferAction(t, h, origin, "transfer.cancel", protocol.TransferTarget{ID: created.Transfer.ID}); err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("admitted registry transfer did not settle")
	}
	until := time.Now().Add(time.Second)
	for h.budget.Usage().Pending != 0 && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	if usage := h.budget.Usage(); usage.Pending != 0 || usage.Active != 0 {
		t.Fatal("admitted snapshot did not release through normal cleanup", usage)
	}
}

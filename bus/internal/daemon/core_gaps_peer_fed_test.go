// SPDX-License-Identifier: GPL-3.0-only

package daemon

import (
	"context"
	"errors"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func peerFedGapProducts(t *testing.T) {
	t.Helper()
	products := t.TempDir()
	installFixture(t, products, "fixture-worker")
	t.Setenv("PATH", products+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func peerFedGapRow(t *testing.T, caller *peerClient, id string) protocol.SessionSummary {
	t.Helper()
	var listed protocol.SessionListResult
	must(t, caller.call("session.list", protocol.SessionListRequest{SessionID: id}, &listed))
	if len(listed.Sessions) != 1 {
		t.Fatalf("list %s = %#v", id, listed.Sessions)
	}
	return listed.Sessions[0]
}

func peerFedGapError(t *testing.T, err error) *protocol.RPCError {
	t.Helper()
	var rpcErr *protocol.RPCError
	if !errors.As(err, &rpcErr) {
		t.Fatalf("error = %v, want a correlated RPC error", err)
	}
	return rpcErr
}

// PROTOCOL 155-157, 570: a same-ID re-hello updates info in place, and every
// re-hello is a complete assertion, so another caller lists only the new info.
func TestPeerFedGapSameIDRehelloInfoVisibleToAnotherCaller(t *testing.T) {
	_, socket := startDaemon(t)
	subject := connectPeer(t, socket, "subject", "subject", "team")
	observer := connectPeer(t, socket, "observer", "observer", "team")
	before := peerFedGapRow(t, observer, "subject")
	if !reflect.DeepEqual(before.Info, map[string]any{"ready": true}) {
		t.Fatalf("initial info = %#v", before.Info)
	}
	info := map[string]any{"revision": float64(2), "state": "busy", "nested": map[string]any{"cwd": "/work"}}
	must(t, subject.peer.Rehello(context.Background(), "subject", info))
	after := peerFedGapRow(t, observer, "subject")
	if !reflect.DeepEqual(after.Info, info) {
		t.Fatalf("observed info = %#v, want %#v", after.Info, info)
	}
	if after.SessionID != "subject@local" || after.Name != "subject@local" || !slices.Equal(after.Groups, before.Groups) || !after.Connected {
		t.Fatalf("same-ID re-hello changed identity: before %#v, after %#v", before, after)
	}
	select {
	case <-subject.superseded:
		t.Fatal("same-ID re-hello superseded its own connection")
	default:
	}
}

// PROTOCOL 211-212, 724: listing an existing session outside the caller's
// visibility is indistinguishable from listing a missing one.
func TestPeerFedGapListInvisibleExistingSessionIsUnknownSession(t *testing.T) {
	peerFedGapProducts(t)
	_, socket := startDaemon(t)
	caller := connectPeer(t, socket, "caller", "caller", "team")
	hidden := connectPeer(t, socket, "hidden", "hidden", "private")
	var lane protocol.LaneSpawnResult
	must(t, hidden.call("lane.spawn", protocol.LaneSpawnRequest{Name: "child", Product: "fixture-worker", Open: &protocol.OpenOptions{}}, &lane))
	// Both sessions exist and are listed for a session sharing their groups.
	peerFedGapRow(t, hidden, "hidden")
	peerFedGapRow(t, hidden, lane.SessionID)
	missing := peerFedGapError(t, caller.call("session.list", protocol.SessionListRequest{SessionID: "missing"}, &protocol.SessionListResult{}))
	if missing.Code != protocol.UnknownSession || missing.Message != "unknown_session" {
		t.Fatalf("missing session = %#v", missing)
	}
	for _, id := range []string{"hidden", "hidden@local", lane.SessionID, unqualify(lane.SessionID)} {
		got := peerFedGapError(t, caller.call("session.list", protocol.SessionListRequest{SessionID: id}, &protocol.SessionListResult{}))
		if !reflect.DeepEqual(got, missing) {
			t.Fatalf("invisible %s = %#v, want %#v", id, got, missing)
		}
	}
}

// PROTOCOL 223-229: an invisible label is rejected on its own receipt while
// every visible label still delivers, with receipts in label order.
func TestPeerFedGapSendMixedInvisibleTargetKeepsVisibleDeliveriesInLabelOrder(t *testing.T) {
	_, socket := startDaemon(t)
	sender := connectPeer(t, socket, "sender", "sender", "team")
	first := connectPeer(t, socket, "first", "first", "team")
	second := connectPeer(t, socket, "second", "second", "team")
	hidden := connectPeer(t, socket, "hidden", "hidden", "private")
	// Label order deliberately differs from session-ID order.
	labels := []string{"second", "hidden", "first", "missing"}
	var sent protocol.MessageSendResult
	must(t, sender.call("message.send", protocol.MessageSendRequest{Targets: labels, Message: "mixed"}, &sent))
	if len(sent.Deliveries) != len(labels) {
		t.Fatalf("receipts = %#v", sent.Deliveries)
	}
	for index, label := range labels {
		if sent.Deliveries[index].Target != label {
			t.Fatalf("receipt %d = %#v, want label %q", index, sent.Deliveries[index], label)
		}
	}
	for _, index := range []int{0, 2} {
		receipt := sent.Deliveries[index]
		if receipt.Disposition != "injected" || receipt.SessionID != labels[index]+"@local" || receipt.DeliveryID == "" || receipt.Reason != "" {
			t.Fatalf("visible receipt = %#v", receipt)
		}
	}
	invisible, missing := sent.Deliveries[1], sent.Deliveries[3]
	invisible.Target, missing.Target = "", ""
	if invisible.Disposition != "rejected" || invisible.Reason != "unknown_session" || invisible != missing {
		t.Fatalf("invisible receipt = %#v, missing receipt = %#v", sent.Deliveries[1], sent.Deliveries[3])
	}
	for _, peer := range []*peerClient{second, first} {
		select {
		case delivery := <-peer.deliveries:
			if delivery.Body != "mixed" || delivery.MessageID != sent.MessageID || delivery.From.SessionID != "sender@local" {
				t.Fatalf("delivery = %#v", delivery)
			}
		case <-time.After(time.Second):
			t.Fatal("visible target did not receive the delivery")
		}
		select {
		case extra := <-peer.deliveries:
			t.Fatalf("duplicate delivery = %#v", extra)
		default:
		}
	}
	select {
	case leaked := <-hidden.deliveries:
		t.Fatalf("invisible target received %#v", leaked)
	default:
	}
}

// PROTOCOL 279-287: a describe for another connected host is forwarded one hop
// and returns the destination worker's hello declaration.
func TestPeerFedGapFederatedDescribeReturnsRemoteDeclaration(t *testing.T) {
	peerFedGapProducts(t)
	secrets := reconnectSecrets()
	_, address := reconnectHub(t, "127.0.0.1:0", secrets)
	_, alphaSocket := reconnectDaemon(t, "alpha", address, secrets["alpha"])
	beta, betaSocket := reconnectDaemon(t, "beta", address, secrets["beta"])
	caller := connectPeer(t, alphaSocket, "caller", "caller", "team")
	_ = connectPeer(t, betaSocket, "remote", "remote", "team")
	awaitRemotePeer(t, caller, "beta", "remote@beta")

	var described protocol.LaneDescribeResult
	must(t, caller.call("lane.describe", protocol.LaneDescribeRequest{Product: "fixture-worker", Host: "beta"}, &described))
	want := protocol.LaneDescribeResult{SupportsMessageRun: true, Product: "fixture-worker", Version: "test",
		SupportedOpenFields: []string{"cwd", "permission_mode", "model", "reasoning_effort", "arguments"}, ExtraArguments: []protocol.ExtraArgument{}}
	if !reflect.DeepEqual(described, want) {
		t.Fatalf("remote description = %#v, want %#v", described, want)
	}
	if code := rpcCode(caller.call("lane.describe", protocol.LaneDescribeRequest{Product: "fixture-worker", Host: "gamma"}, &described)); code != protocol.UnknownHost {
		t.Fatalf("unconnected host describe code = %d", code)
	}
	// The probe depends on the destination: once beta leaves, the identical
	// request is unknown_host rather than a local fallback.
	must(t, beta.Close())
	deadline := time.Now().Add(5 * time.Second)
	for {
		code := rpcCode(caller.call("session.list", protocol.SessionListRequest{Host: "beta"}, &protocol.SessionListResult{}))
		if code == protocol.UnknownHost {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("closed host still routed: code %d", code)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if code := rpcCode(caller.call("lane.describe", protocol.LaneDescribeRequest{Product: "fixture-worker", Host: "beta"}, &described)); code != protocol.UnknownHost {
		t.Fatalf("departed host describe code = %d", code)
	}
}

// PROTOCOL 737: when the hub ends while a forwarded request awaits its
// response, the caller gets forward_lost and the request is never retried.
func TestPeerFedGapHubLossDuringForwardedRunIsForwardLostWithoutRetry(t *testing.T) {
	peerFedGapProducts(t)
	secrets := reconnectSecrets()
	hub, address := reconnectHub(t, "127.0.0.1:0", secrets)
	_, alphaSocket := reconnectDaemon(t, "alpha", address, secrets["alpha"])
	_, betaSocket := reconnectDaemon(t, "beta", address, secrets["beta"])
	caller := connectPeer(t, alphaSocket, "caller", "caller", "team")
	observer := connectPeer(t, betaSocket, "observer", "observer", "team")
	awaitRemotePeer(t, caller, "beta", "observer@beta")
	// Persistence keeps the lane through the owner loss implied by hub loss.
	persistent, notify, never := true, false, int64(0)
	var lane protocol.LaneSpawnResult
	must(t, caller.call("lane.spawn", protocol.LaneSpawnRequest{Host: "beta", Name: "held", Product: "fixture-worker", Open: &protocol.OpenOptions{},
		ExtraGroups: []string{"team"}, Persistent: &persistent, AutoCloseMS: &never, Notify: &notify}, &lane))

	lost := make(chan error, 1)
	go func() {
		lost <- caller.call("turn.run", protocol.TurnRunRequest{SessionID: lane.SessionID, Input: "block"}, nil)
	}()
	// The fixture holds the forwarded run until interrupted, so no response
	// can precede the transport loss.
	waitRunning(t, observer, lane.SessionID)
	hub.Close()
	select {
	case err := <-lost:
		rpcErr := peerFedGapError(t, err)
		if rpcErr.Code != protocol.ForwardLost || rpcErr.Message != "forward_lost" || len(rpcErr.Data) != 0 {
			t.Fatalf("lost forward = %#v", rpcErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("forwarded run was not settled after hub loss")
	}

	// The destination applied the lost request once; settle it locally there.
	must(t, observer.call("turn.interrupt", protocol.SessionTarget{SessionID: lane.SessionID}, &struct{}{}))
	var held protocol.RunStatus
	must(t, observer.call("turn.wait", protocol.WaitRequest{SessionID: lane.SessionID}, &held))
	generation, first := strings.CutSuffix(held.RunID, "/1")
	if !first || held.State != "done" || held.Result == nil || held.Result.Outcome != "interrupted" {
		t.Fatalf("lost run on destination = %#v", held)
	}
	// Restored federation does not replay the lost request: the next explicit
	// run is the second admission of this worker generation.
	_, _ = reconnectHub(t, address, secrets)
	awaitRemotePeer(t, caller, "beta", "observer@beta")
	var next protocol.RunStatus
	must(t, caller.call("turn.run", protocol.TurnRunRequest{SessionID: lane.SessionID, Input: "after recovery"}, &next))
	if next.RunID != generation+"/2" || next.State != "done" || next.Result == nil || next.Result.Result != "after recovery" {
		t.Fatalf("run after recovery = %#v, want run %s/2", next, generation)
	}
}

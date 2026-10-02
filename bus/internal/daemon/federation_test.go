// SPDX-License-Identifier: GPL-3.0-only

package daemon

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/federation"
	sessionkit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
	"github.com/antst/sessionbus/bus/sdk/go/testsocket"
)

func TestFederatedAdmissionKeepsRunInterruptOrder(t *testing.T) {
	d, target, _ := reviewSession(t)
	d.host = "beta"
	item := &entry{row: row{SessionID: "lane@beta", CreatedAt: time.Now(), Groups: []string{"team"}}, attachment: target, done: make(chan struct{})}
	d.directory.mu.Lock()
	d.directory.entries[item.row.SessionID] = item
	d.directory.addConnected(item)
	d.directory.mu.Unlock()
	from := federation.Caller{Groups: []string{"team"}}
	for _, request := range []federation.PublicRequest{
		{Method: "turn.run", Params: []byte(`{"session_id":"lane@beta","input":"go"}`)},
		{Method: "turn.interrupt", Params: []byte(`{"session_id":"lane@beta"}`)},
	} {
		if _, err := d.directory.admitFederation(federation.IncomingCall{From: from, Request: request}); err != nil {
			t.Fatal(err)
		}
	}
	first := (<-target.inbox).(routedRequest)
	second := (<-target.inbox).(routedRequest)
	if first.method != "turn.run" || second.method != "turn.interrupt" {
		t.Fatalf("admission order = %s, %s", first.method, second.method)
	}
}

func TestPullHubListsAndDeliversWithoutReplicatedRows(t *testing.T) {
	secret := func(value byte) string {
		bytes := make([]byte, federation.SecretBytes)
		bytes[len(bytes)-1] = value
		return base64.StdEncoding.EncodeToString(bytes)
	}
	secrets := map[string]string{"alpha": secret(1), "beta": secret(2)}
	products := t.TempDir()
	installFixture(t, products, "fixture-worker")
	t.Setenv("PATH", products+string(os.PathListSeparator)+os.Getenv("PATH"))
	serverTLS, err := federation.ServerTLS(secrets)
	must(t, err)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	must(t, err)
	hub := federation.StartHub(listener, serverTLS, io.Discard)
	t.Cleanup(hub.Close)
	start := func(host string) (*Daemon, string) {
		directory := t.TempDir()
		socket := filepath.Join(testsocket.Directory(t), host+".sock")
		d, startErr := Start(Config{SocketPath: socket, TablePath: filepath.Join(directory, "rows"), Host: host, Products: []string{host + "-product"}, HubAddress: listener.Addr().String(), HubSecret: secrets[host]})
		must(t, startErr)
		t.Cleanup(func() { _ = d.Close() })
		return d, socket
	}
	alpha, alphaSocket := start("alpha")
	_, betaSocket := start("beta")
	sender := connectPeer(t, alphaSocket, "sender", "sender", "team")
	receiver := connectPeer(t, betaSocket, "receiver-id", "receiver", "team")
	hidden := connectPeer(t, betaSocket, "hidden-id", "hidden", "other")
	// Local peer readiness does not await the asynchronous hub supervisor.
	// Establish remote routing before asserting the complete aggregate roster.
	deadline := time.Now().Add(5 * time.Second)
	for {
		var remote protocol.SessionListResult
		err := sender.call("session.list", protocol.SessionListRequest{Host: "beta"}, &remote)
		if err == nil {
			break
		}
		if rpcCode(err) != protocol.UnknownHost || time.Now().After(deadline) {
			t.Fatalf("remote host did not become ready: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
	var listed protocol.SessionListResult
	checkSelf := func() {
		t.Helper()
		if listed.SelfInfo == nil || listed.SelfInfo.SessionID != "sender@alpha" || listed.SelfInfo.Name != "sender@alpha" || listed.SelfInfo.Product != "fixture-client" || !slices.Contains(listed.SelfInfo.Groups, "team") {
			t.Fatalf("originating caller = %#v", listed.SelfInfo)
		}
	}
	must(t, sender.call("session.list", protocol.SessionListRequest{}, &listed))
	checkSelf()
	if len(listed.Sessions) != 2 || listed.Sessions[0].SessionID != "receiver-id@beta" || listed.Sessions[1].SessionID != "sender@alpha" || len(listed.Hosts) != 2 {
		t.Fatalf("aggregate list = %#v", listed)
	}
	listed = protocol.SessionListResult{}
	must(t, sender.call("session.list", protocol.SessionListRequest{Host: "beta"}, &listed))
	checkSelf()
	if len(listed.Sessions) != 1 || listed.Sessions[0].SessionID != "receiver-id@beta" {
		t.Fatalf("direct host list = %#v", listed)
	}
	listed = protocol.SessionListResult{}
	must(t, sender.call("session.list", protocol.SessionListRequest{SessionID: "receiver-id@beta"}, &listed))
	checkSelf()
	var sent protocol.MessageSendResult
	must(t, sender.call("message.send", protocol.MessageSendRequest{Target: "receiver@beta", Message: "hello"}, &sent))
	if len(sent.Deliveries) != 1 || sent.Deliveries[0].Disposition != "injected" {
		t.Fatalf("send = %#v", sent)
	}
	select {
	case delivery := <-receiver.deliveries:
		if delivery.From.SessionID != "sender@alpha" || delivery.From.Product != "fixture-client" {
			t.Fatalf("source = %#v", delivery.From)
		}
	case <-time.After(time.Second):
		t.Fatal("remote delivery missing")
	}
	sent = protocol.MessageSendResult{}
	must(t, sender.call("message.send", protocol.MessageSendRequest{Target: "hidden@beta", Message: "hello"}, &sent))
	if len(sent.Deliveries) != 1 || sent.Deliveries[0].Reason != "unknown_session" {
		t.Fatalf("hidden send = %#v", sent)
	}
	select {
	case delivery := <-hidden.deliveries:
		t.Fatalf("hidden peer received %#v", delivery)
	default:
	}
	sent = protocol.MessageSendResult{}
	must(t, sender.call("message.send", protocol.MessageSendRequest{Group: "team", Message: "group"}, &sent))
	if len(sent.Deliveries) != 1 || sent.Deliveries[0].SessionID != "receiver-id@beta" {
		t.Fatalf("group send = %#v", sent)
	}
	select {
	case delivery := <-receiver.deliveries:
		if delivery.Body != "group" {
			t.Fatalf("group body = %q", delivery.Body)
		}
	case <-time.After(time.Second):
		t.Fatal("group delivery missing")
	}
	if code := rpcCode(sender.call("session.list", protocol.SessionListRequest{Host: "gamma"}, &listed)); code != protocol.UnknownHost {
		t.Fatalf("unknown host code = %d", code)
	}
	var spawned protocol.LaneSpawnResult
	must(t, sender.call("lane.spawn", protocol.LaneSpawnRequest{Name: "child", Product: "fixture-worker", Host: "beta", Open: &protocol.OpenOptions{}}, &spawned))
	listed = protocol.SessionListResult{}
	must(t, sender.call("session.list", protocol.SessionListRequest{SessionID: spawned.SessionID}, &listed))
	if len(listed.Sessions) != 1 || listed.Sessions[0].Name != "sender/child@beta" || !slices.Equal(listed.Sessions[0].Groups, []string{"session:sender@alpha", "session:sender@alpha/child"}) {
		t.Fatalf("remote child = %#v", listed)
	}
	var ref protocol.RunRef
	must(t, sender.call("turn.start", protocol.TurnRunRequest{SessionID: spawned.SessionID, Input: "remote answer"}, &ref))
	var status protocol.RunStatus
	must(t, sender.call("turn.wait", protocol.WaitRequest{SessionID: ref.SessionID, RunID: ref.RunID}, &status))
	if ref.SessionID != spawned.SessionID || status.SessionID != ref.SessionID || status.Result == nil || status.Result.Result != "remote answer" {
		t.Fatalf("remote cursor %#v %#v", ref, status)
	}
	pointer := <-sender.deliveries
	if pointer.From.SessionID != spawned.SessionID || !strings.Contains(pointer.Body, ref.RunID) || strings.Contains(pointer.Body, "remote answer") {
		t.Fatalf("remote pointer %#v", pointer)
	}
	must(t, sender.call("turn.ack", ref, &struct{}{}))
	must(t, sender.call("session.close", protocol.SessionCloseRequest{SessionID: spawned.SessionID, Forget: true}, &struct{}{}))

	// Remove both native names, then exercise the same authenticated hub path.
	must(t, sender.peer.Rehello(context.Background(), "", map[string]any{}))
	must(t, receiver.peer.Rehello(context.Background(), "", map[string]any{}))
	var rawList map[string]any
	must(t, sender.call("session.list", protocol.SessionListRequest{}, &rawList))
	for _, row := range rawList["sessions"].([]any) {
		if _, present := row.(map[string]any)["name"]; present {
			t.Fatalf("federated absent name: %#v", row)
		}
	}
	for _, input := range []protocol.MessageSendRequest{{Target: "receiver-id@beta", Message: "unnamed direct"}, {Group: "team", Message: "unnamed group"}} {
		must(t, sender.call("message.send", input, &sent))
		if len(sent.Deliveries) != 1 || sent.Deliveries[0].SessionID != "receiver-id@beta" {
			t.Fatalf("unnamed federation delivery: %#v", sent)
		}
		source := (<-receiver.deliveries).From
		raw, marshalErr := json.Marshal(source)
		must(t, marshalErr)
		var fields map[string]any
		must(t, json.Unmarshal(raw, &fields))
		if _, present := fields["name"]; present || source.SessionID != "sender@alpha" {
			t.Fatalf("unnamed federation source: %s", raw)
		}
	}
	must(t, sender.call("message.send", protocol.MessageSendRequest{Target: "receiver@beta", Message: "old name"}, &sent))
	if sent.Deliveries[0].Reason != "unknown_session" {
		t.Fatalf("removed remote name matched: %#v", sent)
	}
	t.Setenv("SESSIONBUS_SOCKET", betaSocket)
	captured := make(chan sessionkit.DeliveryRequest, 1)
	written, connectErr := sessionkit.ConnectPeer(sessionkit.PeerIdentity{Product: "fixture-client", SessionID: "written-id", Groups: []string{"team"}, Info: map[string]any{}}, func(_ context.Context, _ sessionkit.PeerIdentity, request sessionkit.DeliveryRequest) (sessionkit.DeliveryReceipt, error) {
		captured <- request
		if request.Body == "uncertain" {
			return sessionkit.DeliveryReceipt{}, &sessionkit.ProtocolError{Code: protocol.Internal, Message: "internal", Data: json.RawMessage(`"post-submission transport loss; consumption unknown"`)}
		}
		return sessionkit.DeliveryReceipt{Disposition: "written"}, nil
	})
	must(t, connectErr)
	t.Cleanup(func() { written.Shutdown(); <-written.Closed() })
	<-written.Ready()
	for _, body := range []string{"written", "uncertain"} {
		sent = protocol.MessageSendResult{}
		must(t, sender.call("message.send", protocol.MessageSendRequest{Target: "written-id@beta", Message: body}, &sent))
		receipt := sent.Deliveries[0]
		want, reason := "written", ""
		if body == "uncertain" {
			want, reason = "rejected", "no_receipt"
		}
		if receipt.Disposition != want || receipt.Reason != reason || receipt.SessionID != "written-id@beta" || receipt.DeliveryID == "" {
			t.Fatalf("federated receipt: %#v", receipt)
		}
		request := <-captured
		if request.From.SessionID != "sender@alpha" || request.From.Name != "" || request.MessageID != sent.MessageID || request.Body != body {
			t.Fatalf("federated captured frame: %#v", request)
		}
	}
	// Same-ID replacement preserves remote ownership; actual owner loss ends it.
	zero, yes := int64(0), true
	spawn := func(owner *peerClient, name string, persistent bool) string {
		var result protocol.LaneSpawnResult
		must(t, owner.call("lane.spawn", protocol.LaneSpawnRequest{Host: "beta", Name: name, Product: "fixture-worker", Open: &protocol.OpenOptions{}, ExtraGroups: []string{"team"}, Persistent: &persistent, AutoCloseMS: &zero}, &result))
		return result.SessionID
	}
	owned, persistent := spawn(sender, "owned", false), spawn(sender, "persistent", yes)
	replacement := connectPeer(t, alphaSocket, "sender", "replacement", "team")
	<-sender.superseded
	remoteConnected(t, receiver, owned, true)
	replacement.peer.Shutdown()
	remoteConnected(t, receiver, owned, false)
	remoteConnected(t, receiver, persistent, true)
	newOwner := connectPeer(t, alphaSocket, "new-owner", "new-owner", "team")
	owned, persistent = spawn(newOwner, "host-owned", false), spawn(newOwner, "host-persistent", yes)
	must(t, alpha.Close())
	remoteConnected(t, receiver, owned, false)
	remoteConnected(t, receiver, persistent, true)

}

func TestForwardWaiterKeepsOriginalIdentityLifetime(t *testing.T) {
	s := newSession(&Daemon{})
	old, replacement := &entry{done: make(chan struct{})}, &entry{done: make(chan struct{})}
	s.identity = old
	s.awaitRemote(1, 0, "turn.run", make(chan federation.Reply, 1), old.done)
	s.identity = replacement
	close(old.done)
	select {
	case event := <-s.inbox:
		s.consumeReply(event.(replyEvent))
	case <-time.After(time.Second):
		t.Fatal("forward waiter followed the replacement identity")
	}
}

func TestForwardedDeliveryKeepsCapturedProduct(t *testing.T) {
	d, target, _ := reviewSession(t)
	d.host = "beta"
	item := &entry{row: row{SessionID: "receiver@beta", CreatedAt: time.Now(), Groups: []string{"team"}}, attachment: target, done: make(chan struct{})}
	d.directory.mu.Lock()
	d.directory.entries[item.row.SessionID] = item
	d.directory.addConnected(item)
	d.directory.mu.Unlock()
	from := federation.Caller{SessionID: "sender@alpha", Name: "old name@alpha", Product: "old-product", Groups: []string{"team"}}
	wait, err := d.directory.admitFederation(federation.IncomingCall{From: from, Request: federation.PublicRequest{Method: "message.send", Params: []byte(`{"target":"receiver@beta","message":"hello"}`)}})
	if err != nil {
		t.Fatal(err)
	}
	from.Name, from.Product = "new name@alpha", "new-product"
	routed := (<-target.inbox).(routedRequest)
	delivery := routed.params.(protocol.DeliveryRequest)
	if delivery.From.Name != "old name@alpha" || delivery.From.Product != "old-product" {
		t.Fatalf("delivery source = %#v", delivery.From)
	}
	routed.reply <- answer{value: &protocol.DeliveryReceipt{Disposition: "injected"}}
	if reply, ok := wait(make(chan struct{})); !ok || reply.Error != nil {
		t.Fatalf("delivery reply = %#v, %v", reply, ok)
	}
}

func TestAggregateListLosesWholeResultWithCapturedHost(t *testing.T) {
	d := &Daemon{host: "alpha"}
	d.directory = newDirectory(d, nil)
	d.federation = &federationLink{inbox: make(chan any, 1)}
	s := newSession(d)
	s.identity = &entry{row: row{SessionID: "sender@alpha", Name: "sender@alpha", Product: "peer", Groups: []string{"team", "session:sender@alpha"}, CreatedAt: time.Now()}, done: make(chan struct{})}
	hosts := make(chan federation.Reply, 1)
	hosts <- federation.Reply{Result: []byte(`{"hosts":["beta"]}`)}
	go func() {
		call := (<-d.federation.inbox).(federation.OutgoingCall)
		call.Reply <- federation.Reply{Error: &protocol.RPCError{Code: protocol.UnknownHost, Message: "unknown_host"}}
	}()
	local := func(<-chan struct{}) (federation.Reply, bool) {
		return rpcReply("session.list", answer{value: &protocol.SessionListResult{Sessions: []protocol.SessionSummary{}}}), true
	}
	result := s.collectFederatedList(s.federationCaller(), local, hosts, s.identity.done)
	if result.code != protocol.ForwardLost || result.value != nil {
		t.Fatalf("aggregate result = %#v", result)
	}
}

func TestRemoteCloseWaitsForRunTerminal(t *testing.T) {
	_, s, _ := reviewSession(t)
	runFrame := protocol.Frame{ID: 1, Method: "turn.run", Request: true}
	closeFrame := protocol.Frame{ID: 2, Method: "session.close", Request: true}
	run := &requestState{frame: runFrame, targetID: "lane@beta"}
	closed := &requestState{frame: closeFrame, targetID: "lane@beta"}
	s.requests[1], s.requests[2], s.owned = run, closed, 2
	closeRaw, _ := protocol.EncodeResult("session.close", struct{}{})
	closeReply := federation.Reply{Result: closeRaw}
	s.consumeReply(replyEvent{requestID: 2, answer: answer{value: &struct{}{}, remote: &closeReply}})
	if closed.held == nil || s.requests[2] == nil {
		t.Fatal("remote close completed before run terminal")
	}
	runRaw, _ := protocol.EncodeResult("turn.run", protocol.RunStatus{SessionID: "lane@beta", RunID: "g/1", State: "done", Result: &protocol.TurnResult{Outcome: "completed", Result: "done"}})
	runReply := federation.Reply{Result: runRaw}
	s.consumeReply(replyEvent{requestID: 1, answer: answer{value: &protocol.RunStatus{SessionID: "lane@beta", RunID: "g/1", State: "done", Result: &protocol.TurnResult{Outcome: "completed", Result: "done"}}, remote: &runReply}})
	if len(s.requests) != 0 {
		t.Fatalf("requests after terminal = %#v", s.requests)
	}
}

func remoteConnected(t *testing.T, peer *peerClient, id string, want bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		var result protocol.SessionListResult
		must(t, peer.call("session.list", protocol.SessionListRequest{SessionID: id}, &result))
		if len(result.Sessions) == 1 && result.Sessions[0].Connected == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("session %s connected != %t: %#v", id, want, result)
		}
		time.Sleep(time.Millisecond)
	}
}

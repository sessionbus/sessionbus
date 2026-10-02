// SPDX-License-Identifier: GPL-3.0-only

package daemon

import (
	"bufio"
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
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
	"github.com/antst/sessionbus/bus/sdk/go/testsocket"
)

func traceFixture(t *testing.T) (*Daemon, *peerClient) {
	t.Helper()
	products := t.TempDir()
	installFixture(t, products, "fixture-worker")
	installFixture(t, products, "wake-worker")
	t.Setenv("PATH", products+string(os.PathListSeparator)+os.Getenv("PATH"))
	d, socket := startDaemon(t)
	return d, connectPeer(t, socket, "parent", "parent", "team")
}

func traceSpawn(t *testing.T, parent *peerClient, name, mode string, persistent bool) string {
	t.Helper()
	var result protocol.LaneSpawnResult
	notify := false
	must(t, parent.call("lane.spawn", protocol.LaneSpawnRequest{Name: name, Product: "fixture-worker", Open: &protocol.OpenOptions{}, Trace: mode, Persistent: &persistent, Notify: &notify, ExtraGroups: []string{"team"}}, &result))
	return result.SessionID
}

func traceConfigure(t *testing.T, parent *peerClient, id, mode string) {
	t.Helper()
	var result protocol.TraceConfigureResult
	must(t, parent.call("trace.configure", protocol.TraceConfigureRequest{SessionID: id, Mode: mode}, &result))
	if result.SessionID != id || result.Mode != mode {
		t.Fatalf("configure = %+v", result)
	}
}

// Use the ordinary daemon-owned send path with the actual opened child identity,
// just as the existing completion-pointer tests do; no identity is fabricated.
func traceChildSend(t *testing.T, d *Daemon, id string, request protocol.MessageSendRequest) protocol.MessageSendResult {
	t.Helper()
	d.directory.mu.Lock()
	item := d.directory.entries[id]
	owner := item.attachment
	d.directory.mu.Unlock()
	wait := d.directory.forwardLocal(owner.federationCaller(), item, "message.send", &request, "")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	reply, ok := wait(ctx.Done())
	if !ok || reply.Error != nil {
		t.Fatalf("send: %+v, %v", reply, ok)
	}
	var result protocol.MessageSendResult
	must(t, protocol.UnmarshalResult("message.send", reply.Result, &result))
	return result
}

func traceReceive(t *testing.T, parent *peerClient) (protocol.DeliveryRequest, traceEnvelope) {
	t.Helper()
	select {
	case received := <-parent.deliveries:
		var body traceEnvelope
		must(t, json.Unmarshal([]byte(received.Body), &body))
		if body.Kind != "sessionbus.trace" || received.MessageID == body.MessageID || received.From.Product != "sessionbus" {
			t.Fatalf("trace attribution = %+v / %+v", received, body)
		}
		if strings.Contains(received.Body, "owner_lifetime") || strings.Contains(received.Body, "source_attachment") {
			t.Fatal("private trace route leaked")
		}
		return received, body
	case <-time.After(3 * time.Second):
		t.Fatal("parent copy did not arrive")
	}
	return protocol.DeliveryRequest{}, traceEnvelope{}
}

func traceIdle(t *testing.T, d *Daemon) {
	t.Helper()
	until := time.Now().Add(3 * time.Second)
	for time.Now().Before(until) {
		d.directory.mu.Lock()
		count, size := d.traceCount, d.traceBytes
		d.directory.mu.Unlock()
		if count == 0 && size == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	d.directory.mu.Lock()
	defer d.directory.mu.Unlock()
	t.Fatalf("trace work retained count=%d bytes=%d", d.traceCount, d.traceBytes)
}

func traceAbsent(t *testing.T, parent *peerClient) {
	t.Helper()
	select {
	case copy := <-parent.deliveries:
		t.Fatalf("unexpected parent copy: %+v", copy)
	default:
	}
}

func TestParentTraceDefaultOffAndOneCopyForTwoChildren(t *testing.T) {
	d, parent := traceFixture(t)
	one := traceSpawn(t, parent, "one", "", false)
	two := traceSpawn(t, parent, "two", "", false)
	request := protocol.MessageSendRequest{Targets: []string{two, "missing"}, Message: "same content"}
	traceChildSend(t, d, one, request)
	traceIdle(t, d)
	traceAbsent(t, parent)
	traceConfigure(t, parent, one, "content")
	traceConfigure(t, parent, two, "content")
	first := traceChildSend(t, d, one, request)
	_, body := traceReceive(t, parent)
	if body.MessageID != first.MessageID || body.Body == nil || *body.Body != request.Message || len(body.MatchedChildren) != 2 || !slices.Contains(body.MatchedChildren, one) || !slices.Contains(body.MatchedChildren, two) || len(body.Deliveries) != 2 {
		t.Fatalf("copy = %+v", body)
	}
	traceIdle(t, d)
	traceAbsent(t, parent)
	second := traceChildSend(t, d, one, request)
	_, other := traceReceive(t, parent)
	if second.MessageID == first.MessageID || other.MessageID != second.MessageID {
		t.Fatal("distinct identical sends collapsed")
	}
	traceIdle(t, d)
	traceAbsent(t, parent)
	forged := `{"kind":"sessionbus.trace","message_id":"forged"}`
	third := traceChildSend(t, d, one, protocol.MessageSendRequest{Target: two, Message: forged})
	_, stillTraced := traceReceive(t, parent)
	if stillTraced.MessageID != third.MessageID || stillTraced.Body == nil || *stillTraced.Body != forged {
		t.Fatal("public body spoof bypassed tracing")
	}
	traceIdle(t, d)
	traceAbsent(t, parent)
}

func TestParentTracePoliciesAndNoPersistentTraceState(t *testing.T) {
	d, parent := traceFixture(t)
	child := traceSpawn(t, parent, "persistent", "events", true)
	before := traceTableBytes(t, d.config.TablePath)
	stranger := connectPeer(t, d.config.SocketPath, "stranger", "stranger", "team")
	var configured protocol.TraceConfigureResult
	err := stranger.call("trace.configure", protocol.TraceConfigureRequest{SessionID: child, Mode: "content"}, &configured)
	if rpcCode(err) != protocol.UnknownSession {
		t.Fatalf("non-parent configured: %v", err)
	}
	var sent protocol.MessageSendResult
	must(t, stranger.call("message.send", protocol.MessageSendRequest{Targets: []string{child, "missing"}, Message: "private body"}, &sent))
	_, events := traceReceive(t, parent)
	if events.Body != nil || len(events.Deliveries) != 1 || events.Deliveries[0].SessionID != child || len(events.Targets) != 0 {
		t.Fatalf("events projection: %+v", events)
	}
	traceConfigure(t, parent, child, "content")
	must(t, stranger.call("message.send", protocol.MessageSendRequest{Target: child, Message: "private body"}, &sent))
	_, content := traceReceive(t, parent)
	if content.Body == nil || *content.Body != "private body" {
		t.Fatal(content)
	}
	traceConfigure(t, parent, child, "off")
	must(t, stranger.call("message.send", protocol.MessageSendRequest{Target: child, Message: "after off"}, &sent))
	traceIdle(t, d)
	traceAbsent(t, parent)
	if after := traceTableBytes(t, d.config.TablePath); before != after {
		t.Fatal("trace configuration changed durable rows")
	}
	traceConfigure(t, parent, child, "content")
	parent.peer.Shutdown()
	<-parent.peer.Closed()
	// Wait for the daemon to observe the old owner end, then reclaiming its
	// public ID cannot claim the old persistent lane's tracing ownership.
	until := time.Now().Add(3 * time.Second)
	for time.Now().Before(until) {
		d.directory.mu.Lock()
		ended := d.directory.entries[child].parent.ended
		d.directory.mu.Unlock()
		if ended {
			break
		}
		time.Sleep(time.Millisecond)
	}
	replacement := connectPeer(t, d.config.SocketPath, "parent", "parent", "team")
	err = replacement.call("trace.configure", protocol.TraceConfigureRequest{SessionID: child, Mode: "content"}, &configured)
	if rpcCode(err) != protocol.UnknownSession {
		t.Fatalf("new lifetime reclaimed policy: %v", err)
	}
}

func traceTableBytes(t *testing.T, path string) string {
	t.Helper()
	var content strings.Builder
	must(t, filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			raw, readErr := os.ReadFile(path)
			if readErr != nil {
				return readErr
			}
			content.Write(raw)
		}
		return nil
	}))
	return content.String()
}

func TestParentTraceCopyDoesNotTraceItsParentAgain(t *testing.T) {
	d, grandparent := traceFixture(t)
	parentID := traceSpawn(t, grandparent, "parent-lane", "content", false)
	// A normal peer lets this test inspect received copies while exercising the
	// exact lane parent/trace relationship at the daemon's delivery boundary.
	parent := connectPeer(t, d.config.SocketPath, "inspect-parent", "inspect-parent", "team")
	d.directory.mu.Lock()
	parentItem := d.directory.entries["inspect-parent@local"]
	parentItem.peer = false
	parentItem.row.CreatedAt = time.Now()
	parentItem.parent = d.directory.entries[parentID].parent
	parentItem.row.Policy = &protocol.LanePolicy{}
	parentItem.row.Groups = []string{"session:parent@local", "session:parent@local/inspect-parent", "team"}
	parentItem.traceMode, parentItem.traceVersion = "content", randomID("policy")
	d.directory.mu.Unlock()
	child := traceSpawn(t, parent, "child", "content", false)
	sender := connectPeer(t, d.config.SocketPath, "sender", "sender", "team")
	var result protocol.MessageSendResult
	must(t, sender.call("message.send", protocol.MessageSendRequest{Target: child, Message: "only parent gets this copy"}, &result))
	traceReceive(t, parent)
	traceIdle(t, d)
	traceAbsent(t, grandparent)
	traceAbsent(t, parent)
}

func TestParentTraceCopyLifetimeGateAndPressure(t *testing.T) {
	d, parent := traceFixture(t)
	child := traceSpawn(t, parent, "child", "content", false)
	target := connectPeer(t, d.config.SocketPath, "receiver", "receiver", "team")
	d.directory.mu.Lock()
	item := d.directory.entries["parent@local"]
	old := item.lifetime.token
	item.lifetime.token = "replacement-lifetime"
	d.directory.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	caller := federation.Caller{SessionID: "trace@local", Product: "sessionbus", PrivateGroup: privateGroup(item), Groups: []string{privateGroup(item)}}
	wait := d.directory.forwardLocalTrace(caller, nil, "message.send", &protocol.MessageSendRequest{Target: "parent@local", Message: "stale copy"}, "", false, &federation.TraceDestination{SessionID: "parent@local", Lifetime: old}, false)
	reply, ok := wait(ctx.Done())
	if !ok || reply.Error != nil {
		t.Fatalf("stale copy result %+v", reply)
	}
	var stale protocol.MessageSendResult
	must(t, protocol.UnmarshalResult("message.send", reply.Result, &stale))
	if len(stale.Deliveries) != 1 || stale.Deliveries[0].Reason != "not_submitted" {
		t.Fatal(stale)
	}
	traceAbsent(t, parent)
	d.directory.mu.Lock()
	d.traceBytes = maxTraceBytes
	d.directory.mu.Unlock()
	result := traceChildSend(t, d, child, protocol.MessageSendRequest{Target: "receiver", Message: "ordinary delivery survives trace pressure"})
	if len(result.Deliveries) != 1 || (result.Deliveries[0].Disposition != "written" && result.Deliveries[0].Disposition != "injected") {
		t.Fatal(result)
	}
	select {
	case <-target.deliveries:
	case <-time.After(time.Second):
		t.Fatal("ordinary message lost")
	}
	d.directory.mu.Lock()
	d.traceBytes = 0
	lost := d.traceDropped
	d.directory.mu.Unlock()
	if lost == 0 {
		t.Fatal("trace overflow unaccounted")
	}
	traceIdle(t, d)
	traceAbsent(t, parent)
}

func TestParentTraceDifferentParentsReceiveOwnProjection(t *testing.T) {
	d, oneParent := traceFixture(t)
	twoParent := connectPeer(t, d.config.SocketPath, "other-parent", "other-parent", "team")
	one := traceSpawn(t, oneParent, "one", "content", false)
	two := traceSpawn(t, twoParent, "two", "events", false)
	result := traceChildSend(t, d, one, protocol.MessageSendRequest{Targets: []string{two, "missing"}, Message: "one original"})
	_, first := traceReceive(t, oneParent)
	_, second := traceReceive(t, twoParent)
	if first.MessageID != result.MessageID || second.MessageID != result.MessageID || first.Body == nil || second.Body != nil || len(first.Deliveries) != 2 || len(second.Deliveries) != 1 {
		t.Fatalf("projections: %+v / %+v", first, second)
	}
	traceIdle(t, d)
	traceAbsent(t, oneParent)
	traceAbsent(t, twoParent)
}

func TestParentTraceOfflineTargetIsUnknownWithoutCopy(t *testing.T) {
	d, parent := traceFixture(t)
	child := traceSpawn(t, parent, "offline", "content", true)
	must(t, parent.call("session.close", protocol.SessionCloseRequest{SessionID: child}, &struct{}{}))
	traceConfigure(t, parent, child, "content")
	sender := connectPeer(t, d.config.SocketPath, "sender", "sender", "team")
	var result protocol.MessageSendResult
	must(t, sender.call("message.send", protocol.MessageSendRequest{Target: "parent/offline", Message: "cannot dispatch"}, &result))
	if len(result.Deliveries) != 1 || result.Deliveries[0].Disposition != "rejected" || result.Deliveries[0].Reason != "unknown_session" || result.Deliveries[0].SessionID != "" || result.Deliveries[0].DeliveryID != "" {
		t.Fatalf("archived target receipt: %+v", result)
	}
	traceIdle(t, d)
	traceAbsent(t, parent)
}

func TestParentTraceLocalPolicyChangeWhileOriginalIsPending(t *testing.T) {
	d, parent := traceFixture(t)
	child := traceSpawn(t, parent, "child", "events", false)
	target, err := net.Dial("unix", d.config.SocketPath)
	must(t, err)
	defer target.Close()
	must(t, target.SetDeadline(time.Now().Add(5*time.Second)))
	reader := bufio.NewReader(target)
	must(t, rawCall(target, reader, 1, "session.hello", protocol.PeerHello{Protocol: 1, Product: "fixture-peer", SessionID: "held", Groups: []string{"team"}, Info: map[string]any{}}, &struct{}{}))
	d.directory.mu.Lock()
	item := d.directory.entries[child]
	worker := item.attachment
	d.directory.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	wait := d.directory.forwardLocal(worker.federationCaller(), item, "message.send", &protocol.MessageSendRequest{Target: "held", Message: "must not upgrade this admitted message"}, "")
	done := make(chan federation.Reply, 1)
	go func() { value, _ := wait(ctx.Done()); done <- value }()
	frame, err := readRawFrame(reader)
	must(t, err)
	if frame.Method != "message.deliver" {
		t.Fatal(frame)
	}
	traceAbsent(t, parent) // No pre-delivery copy.
	traceConfigure(t, parent, child, "content")
	response, err := protocol.ResultBytes(frame.ID, frame.Method, protocol.DeliveryReceipt{Disposition: "written"})
	must(t, err)
	_, err = target.Write(response)
	must(t, err)
	select {
	case value := <-done:
		if value.Error != nil {
			t.Fatal(value.Error)
		}
	case <-ctx.Done():
		t.Fatal("send did not settle")
	}
	traceIdle(t, d)
	traceAbsent(t, parent) // A version change drops locally pending snapshots.
}

func TestParentTraceHeldParentDoesNotHoldOriginalAndShutdownJoins(t *testing.T) {
	d, parent := traceFixture(t)
	child := traceSpawn(t, parent, "child", "content", false)
	held, err := net.Dial("unix", d.config.SocketPath)
	must(t, err)
	defer held.Close()
	must(t, held.SetDeadline(time.Now().Add(5*time.Second)))
	reader := bufio.NewReader(held)
	must(t, rawCall(held, reader, 1, "session.hello", protocol.PeerHello{Protocol: 1, Product: "fixture-peer", SessionID: "held-parent", Groups: []string{"team"}, Info: map[string]any{}}, &struct{}{}))
	d.directory.mu.Lock()
	d.directory.entries[child].parent = d.directory.entries["held-parent@local"].lifetime
	d.directory.mu.Unlock()
	sender := connectPeer(t, d.config.SocketPath, "sender", "sender", "team")
	var result protocol.MessageSendResult
	// This returns before the parent reads or acknowledges its copy.
	must(t, sender.call("message.send", protocol.MessageSendRequest{Target: child, Message: "original completes"}, &result))
	if len(result.Deliveries) != 1 || (result.Deliveries[0].Disposition != "written" && result.Deliveries[0].Disposition != "injected") {
		t.Fatal(result)
	}
	frame, err := readRawFrame(reader)
	must(t, err)
	if frame.Method != "message.deliver" {
		t.Fatal(frame)
	}
	done := make(chan error, 1)
	go func() { done <- d.Close() }()
	select {
	case err := <-done:
		must(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown did not join held trace work")
	}
	traceIdle(t, d)
}

func TestParentTraceIdleRunKeepsNormalCompletionPointer(t *testing.T) {
	d, grandparent := traceFixture(t)
	var parent protocol.LaneSpawnResult
	must(t, grandparent.call("lane.spawn", protocol.LaneSpawnRequest{Name: "run-parent", Product: "wake-worker", Open: &protocol.OpenOptions{}, Trace: "content", ExtraGroups: []string{"team"}}, &parent))
	child := traceSpawn(t, grandparent, "child", "content", false)
	d.directory.mu.Lock()
	d.directory.entries[child].parent = d.directory.entries[parent.SessionID].lifetime
	d.directory.mu.Unlock()
	sender := connectPeer(t, d.config.SocketPath, "sender", "sender", "team")
	var sent protocol.MessageSendResult
	must(t, sender.call("message.send", protocol.MessageSendRequest{Target: child, Message: "starts ordinary parent run"}, &sent))
	select {
	case pointer := <-grandparent.deliveries:
		if !strings.HasPrefix(pointer.Body, "Lane "+parent.SessionID+" run ") {
			t.Fatalf("not normal completion pointer: %s", pointer.Body)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("parent did not run/notify normally")
	}
	var completed protocol.RunStatus
	must(t, grandparent.call("turn.wait", protocol.WaitRequest{SessionID: parent.SessionID}, &completed))
	if completed.Result == nil {
		t.Fatal(completed)
	}
	var body traceEnvelope
	must(t, json.Unmarshal([]byte(completed.Result.Result), &body))
	if body.MessageID != sent.MessageID || body.Body == nil || *body.Body != "starts ordinary parent run" {
		t.Fatal(body)
	}
	must(t, grandparent.call("turn.ack", protocol.RunRef{SessionID: parent.SessionID, RunID: completed.RunID}, &struct{}{}))
	traceIdle(t, d)
	traceAbsent(t, grandparent) // No trace of the copy or its completion pointer.
}

func TestParentTraceRefusedWakeDoesNotMarkNextExplicitRun(t *testing.T) {
	s, reader, _ := policySession(t, true, 0)
	reply := make(chan answer, 1)
	s.issue(routedRequest{destination: s.identity, method: "message.deliver", traceCopy: true,
		params: protocol.DeliveryRequest{MessageID: "copy", From: protocol.DeliverySource{SessionID: "daemon@local", Product: "sessionbus", Groups: []string{}}, Body: "trace"}, reply: reply})
	delivery := policyFrame(t, reader)
	if !s.runFromTrace {
		t.Fatal("trace wake lost its marker")
	}
	s.receiveResponse(protocol.Frame{ID: delivery.ID, Error: &protocol.RPCError{Code: protocol.Busy, Message: "busy"}})
	if got := <-reply; got.code != protocol.Busy || s.runID != "" || s.runFromTrace {
		t.Fatalf("refused wake retained run attribution: reply=%+v id=%s trace=%v", got, s.runID, s.runFromTrace)
	}
	s.issue(routedRequest{destination: s.identity, method: "turn.start", params: &protocol.TurnRunRequest{SessionID: "lane", Input: "explicit"}, reply: reply})
	if execute := policyFrame(t, reader); execute.Method != "turn.execute" || s.runFromTrace {
		t.Fatalf("explicit run inherited trace marker: %+v", execute)
	}
}

func traceFederation(t *testing.T) (map[string]*Daemon, *peerClient) {
	t.Helper()
	products := t.TempDir()
	installFixture(t, products, "fixture-worker")
	t.Setenv("PATH", products+string(os.PathListSeparator)+os.Getenv("PATH"))
	secrets := map[string]string{}
	for index, host := range []string{"alpha", "beta", "gamma"} {
		key := make([]byte, federation.SecretBytes)
		key[0] = byte(index + 1)
		secrets[host] = base64.StdEncoding.EncodeToString(key)
	}
	configuration, err := federation.ServerTLS(secrets)
	must(t, err)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	must(t, err)
	hub := federation.StartHub(listener, configuration, io.Discard)
	t.Cleanup(hub.Close)
	daemons := map[string]*Daemon{}
	for _, host := range []string{"alpha", "beta", "gamma"} {
		dir := testsocket.Directory(t)
		d, err := Start(Config{Host: host, SocketPath: filepath.Join(dir, "bus.sock"), TablePath: filepath.Join(dir, "rows"), HubAddress: listener.Addr().String(), HubSecret: secrets[host]})
		must(t, err)
		t.Cleanup(func() { _ = d.Close() })
		daemons[host] = d
	}
	parent := connectPeer(t, daemons["alpha"].config.SocketPath, "parent", "parent", "team")
	// Await actual federation registration rather than racing the supervisor.
	until := time.Now().Add(5 * time.Second)
	for {
		var listed protocol.SessionListResult
		err := parent.call("session.list", protocol.SessionListRequest{Host: "gamma"}, &listed)
		var beta protocol.SessionListResult
		err2 := parent.call("session.list", protocol.SessionListRequest{Host: "beta"}, &beta)
		if err == nil && err2 == nil {
			break
		}
		if time.Now().After(until) {
			t.Fatalf("federation unavailable: %v / %v", err, err2)
		}
		time.Sleep(10 * time.Millisecond)
	}
	return daemons, parent
}

func traceRemoteSpawn(t *testing.T, parent *peerClient, host, name, mode string) string {
	t.Helper()
	var result protocol.LaneSpawnResult
	notify := false
	must(t, parent.call("lane.spawn", protocol.LaneSpawnRequest{Name: name, Product: "fixture-worker", Host: host, Open: &protocol.OpenOptions{}, Trace: mode, Notify: &notify, ExtraGroups: []string{"team"}}, &result))
	return result.SessionID
}

func TestParentTraceFederatedSiblingSendHasOneOriginCopy(t *testing.T) {
	daemons, parent := traceFederation(t)
	one := traceRemoteSpawn(t, parent, "alpha", "one", "content")
	two := traceRemoteSpawn(t, parent, "beta", "two", "content")
	before := traceTableBytes(t, daemons["beta"].config.TablePath)
	traceConfigure(t, parent, two, "events")
	traceConfigure(t, parent, two, "content")
	if before != traceTableBytes(t, daemons["beta"].config.TablePath) {
		t.Fatal("remote trace configuration persisted")
	}
	result := traceChildSend(t, daemons["alpha"], one, protocol.MessageSendRequest{Target: two, Message: "cross-host siblings"})
	_, body := traceReceive(t, parent)
	if body.MessageID != result.MessageID || len(body.MatchedChildren) != 2 || !slices.Contains(body.MatchedChildren, one) || !slices.Contains(body.MatchedChildren, two) || body.Body == nil || *body.Body != "cross-host siblings" || len(body.Deliveries) != 1 || body.Deliveries[0].DeliveryID != result.Deliveries[0].DeliveryID {
		t.Fatalf("cross-host copy: %+v", body)
	}
	for _, d := range daemons {
		traceIdle(t, d)
	}
	traceAbsent(t, parent)
}

func TestParentTraceThirdHostSenderRemoteChildAndRemoteParent(t *testing.T) {
	daemons, parent := traceFederation(t)
	child := traceRemoteSpawn(t, parent, "beta", "child", "content")
	sender := connectPeer(t, daemons["gamma"].config.SocketPath, "sender", "sender", "team")
	var result protocol.MessageSendResult
	must(t, sender.call("message.send", protocol.MessageSendRequest{Target: child, Message: "third host source"}, &result))
	_, body := traceReceive(t, parent)
	if body.MessageID != result.MessageID || body.From.SessionID != "sender@gamma" || body.Body == nil || *body.Body != "third host source" || !slices.Equal(body.MatchedChildren, []string{child}) {
		t.Fatalf("third-host copy: %+v", body)
	}
	for _, d := range daemons {
		traceIdle(t, d)
	}
	traceAbsent(t, parent)
	// Neither the public caller result nor the parent body contains live routing
	// credentials, despite the target's remote-parent metadata crossing the hub.
	raw, err := json.Marshal(result)
	must(t, err)
	if strings.Contains(string(raw), "trace") || strings.Contains(string(raw), "owner_lifetime") {
		t.Fatalf("internal reply leaked: %s", raw)
	}
}

func TestParentTraceFederatedGroupFanoutHasOneParentCopy(t *testing.T) {
	daemons, parent := traceFederation(t)
	children := make([]string, 0, 3)
	for _, host := range []string{"alpha", "beta", "gamma"} {
		var child protocol.LaneSpawnResult
		notify := false
		must(t, parent.call("lane.spawn", protocol.LaneSpawnRequest{
			Name: "group-" + host, Product: "fixture-worker", Host: host,
			Open: &protocol.OpenOptions{}, Trace: "content", Notify: &notify,
			ExtraGroups: []string{"trace-siblings"},
		}, &child))
		children = append(children, child.SessionID)
	}
	sender := connectPeer(t, daemons["gamma"].config.SocketPath, "sender", "sender", "trace-siblings")
	var result protocol.MessageSendResult
	must(t, sender.call("message.send", protocol.MessageSendRequest{Group: "trace-siblings", Message: "one group send"}, &result))
	_, body := traceReceive(t, parent)
	slices.Sort(children)
	if len(result.Deliveries) != 3 || len(body.Deliveries) != 3 || body.MessageID != result.MessageID || !slices.Equal(body.MatchedChildren, children) {
		t.Fatalf("group projection = %+v, original = %+v", body, result)
	}
	for _, delivery := range result.Deliveries {
		if !slices.Contains(body.Deliveries, delivery) {
			t.Fatalf("missing exact group receipt: %+v", delivery)
		}
	}
	for _, d := range daemons {
		traceIdle(t, d)
	}
	traceAbsent(t, parent)
}

func TestParentTraceRemoteEndedParentCannotReceiveLateCopy(t *testing.T) {
	daemons, parent := traceFederation(t)
	child := traceRemoteSpawn(t, parent, "beta", "child", "content")
	beta := daemons["beta"]
	held, err := net.Dial("unix", beta.config.SocketPath)
	must(t, err)
	defer held.Close()
	must(t, held.SetDeadline(time.Now().Add(5*time.Second)))
	reader := bufio.NewReader(held)
	must(t, rawCall(held, reader, 1, "session.hello", protocol.PeerHello{Protocol: 1, Product: "fixture-peer", SessionID: "held", Groups: []string{"team"}, Info: map[string]any{}}, &struct{}{}))
	beta.directory.mu.Lock()
	item := beta.directory.entries["held@beta"]
	item.peer, item.row.CreatedAt = false, time.Now()
	item.row.Policy = &protocol.LanePolicy{Persistent: true}
	item.parent = beta.directory.entries[child].parent
	item.traceMode, item.traceVersion = "content", randomID("policy")
	oldParent := item.parent
	beta.directory.mu.Unlock()
	sender := connectPeer(t, daemons["gamma"].config.SocketPath, "sender", "sender", "team")
	done := make(chan error, 1)
	var result protocol.MessageSendResult
	go func() {
		done <- sender.call("message.send", protocol.MessageSendRequest{Target: "held@beta", Message: "late copy must not reach replacement"}, &result)
	}()
	frame, err := readRawFrame(reader)
	must(t, err)
	if frame.Method != "message.deliver" {
		t.Fatal(frame)
	}
	parent.peer.Shutdown()
	<-parent.peer.Closed()
	until := time.Now().Add(3 * time.Second)
	for {
		beta.directory.mu.Lock()
		ended := oldParent.ended
		beta.directory.mu.Unlock()
		if ended {
			break
		}
		if time.Now().After(until) {
			t.Fatal("remote parent end did not arrive")
		}
		time.Sleep(time.Millisecond)
	}
	replacement := connectPeer(t, daemons["alpha"].config.SocketPath, "parent", "parent", "team")
	response, err := protocol.ResultBytes(frame.ID, frame.Method, protocol.DeliveryReceipt{Disposition: "written"})
	must(t, err)
	_, err = held.Write(response)
	must(t, err)
	select {
	case err := <-done:
		must(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("original result did not settle")
	}
	if len(result.Deliveries) != 1 || result.Deliveries[0].Disposition != "written" {
		t.Fatal(result)
	}
	for _, d := range daemons {
		traceIdle(t, d)
	}
	traceAbsent(t, replacement)
}

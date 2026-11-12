// SPDX-License-Identifier: MIT

package sessionkit

import (
	"context"
	"errors"
	"net"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/sdk/go/internal/rpc"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestPeerDesiredIdentityAndDeepSnapshots(t *testing.T) {
	fixtures := loadCallerFixtures(t)
	requests, server, client := peerPipe(t)
	connections := make(chan net.Conn, 1)
	connections <- client
	usePeerDialer(t, func(string, string) (net.Conn, error) {
		select {
		case fd := <-connections:
			return fd, nil
		default:
			return nil, errors.New("offline")
		}
	})
	identity := fixtures.Sequences.CrossedRehello.Identity
	peer, err := ConnectPeer(identity, acceptDelivery)
	if err != nil {
		t.Fatal(err)
	}
	identity.Info["nested"].(map[string]any)["value"] = "mutated"
	firstHello := <-requests
	assertPeerHello(t, firstHello, "initial", "initial")
	first := fixtures.Sequences.CrossedRehello.First
	if err = peer.Rehello(context.Background(), first.Name, first.Info); !isCode(err, protocol.NotConnected) {
		t.Fatalf("rehello before ready = %v", err)
	}
	first.Info["nested"].(map[string]any)["value"] = "mutated"
	mustRPC(t, server.Result(firstHello, struct{}{}))
	readyHello := <-requests
	assertPeerHello(t, readyHello, "first", "first")
	mustRPC(t, server.Result(readyHello, struct{}{}))
	await(t, peer.Ready(), "peer ready")

	fixtures = loadCallerFixtures(t)
	first, second := fixtures.Sequences.CrossedRehello.First, fixtures.Sequences.CrossedRehello.Second
	first.Name = "crossed first"
	firstDone := make(chan error, 1)
	go func() { firstDone <- peer.Rehello(context.Background(), first.Name, first.Info) }()
	firstRequest := <-requests
	first.Info["nested"].(map[string]any)["value"] = "mutated"
	secondDone := make(chan error, 1)
	go func() { secondDone <- peer.Rehello(context.Background(), second.Name, second.Info) }()
	secondRequest := <-requests
	second.Info["nested"].(map[string]any)["value"] = "mutated"
	mustRPC(t, server.Result(secondRequest, struct{}{}))
	mustRPC(t, <-secondDone)
	mustRPC(t, server.Result(firstRequest, struct{}{}))
	finalRequest := <-requests
	assertPeerHello(t, finalRequest, "second", "second")
	mustRPC(t, server.Result(finalRequest, struct{}{}))
	mustRPC(t, <-firstDone)
	peer.Shutdown()
	await(t, peer.Closed(), "peer closed")
	if peer.Err() != nil {
		t.Fatalf("shutdown terminal error = %v", peer.Err())
	}
}

func TestPeerRehelloCancellationKeepsLateAckOwned(t *testing.T) {
	requests, server, client := peerPipe(t)
	connections := make(chan net.Conn, 1)
	connections <- client
	usePeerDialer(t, func(string, string) (net.Conn, error) { return <-connections, nil })
	admitted := make(chan PeerIdentity, 2)
	peer, err := ConnectPeer(PeerIdentity{Product: "fixture-client", SessionID: "peer", Name: "initial", Groups: []string{}, Info: map[string]any{}}, func(_ context.Context, identity PeerIdentity, _ DeliveryRequest) (DeliveryReceipt, error) {
		admitted <- identity
		return DeliveryReceipt{Disposition: "injected"}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	mustRPC(t, server.Result(<-requests, struct{}{}))
	await(t, peer.Ready(), "peer ready")
	if err = peer.Rehello(context.Background(), "initial", map[string]any{}); err != nil {
		t.Fatalf("unchanged rehello = %v", err)
	}
	called := make(chan error, 1)
	go func() { _, callErr := peer.Caller.List(context.Background(), SessionListRequest{}); called <- callErr }()
	request := <-requests
	if request.Method != "session.list" {
		t.Fatalf("unchanged rehello sent %q", request.Method)
	}
	mustRPC(t, server.Result(request, SessionListResult{Sessions: []SessionSummary{}, Hosts: []HostProducts{}}))
	mustRPC(t, <-called)

	ctx, cancel := context.WithCancel(context.Background())
	renamed := make(chan error, 1)
	go func() { renamed <- peer.Rehello(ctx, "renamed", map[string]any{"revision": 2}) }()
	hello := <-requests
	var receipt DeliveryReceipt
	mustRPC(t, server.Call(context.Background(), "message.deliver", DeliveryRequest{MessageID: "before", From: DeliverySource{SessionID: "sender@local", Name: "sender@local", Product: "fixture", Groups: []string{}}, Body: "body"}, &receipt))
	if identity := <-admitted; identity.Name != "initial" || len(identity.Info) != 0 {
		t.Fatalf("pre-ack admitted identity = %#v", identity)
	}
	cancel()
	if err = <-renamed; !errors.Is(err, context.Canceled) {
		t.Fatalf("rehello cancellation = %v", err)
	}
	mustRPC(t, server.Result(hello, struct{}{}))
	mustRPC(t, server.Call(context.Background(), "message.deliver", DeliveryRequest{MessageID: "after", From: DeliverySource{SessionID: "sender@local", Name: "sender@local", Product: "fixture", Groups: []string{}}, Body: "body"}, &receipt))
	if identity := <-admitted; identity.Name != "renamed" || identity.Info["revision"] != float64(2) {
		t.Fatalf("late ack identity = %#v", identity)
	}
	peer.Shutdown()
	await(t, peer.Closed(), "peer closed")
}

func TestPeerReconnectsButSupersededStops(t *testing.T) {
	var attempts atomic.Int32
	connections := make(chan net.Conn, 2)
	usePeerDialer(t, func(string, string) (net.Conn, error) {
		attempts.Add(1)
		select {
		case fd := <-connections:
			return fd, nil
		default:
			return nil, errors.New("offline")
		}
	})
	requests1, server1, client1 := peerPipe(t)
	connections <- client1
	peer, err := ConnectPeer(PeerIdentity{Product: "fixture-client", SessionID: "peer", Name: "peer", Groups: []string{}, Info: map[string]any{}}, acceptDelivery)
	if err != nil {
		t.Fatal(err)
	}
	hello := <-requests1
	mustRPC(t, server1.Result(hello, struct{}{}))
	await(t, peer.Ready(), "peer ready")
	started := RunRef{SessionID: "lane@local", RunID: "g/1"}
	starting := make(chan error, 1)
	go func() {
		_, callErr := peer.Caller.Start(context.Background(), TurnRunRequest{SessionID: started.SessionID, Input: "block"})
		starting <- callErr
	}()
	request := <-requests1
	if request.Method != "turn.start" {
		t.Fatalf("method %s", request.Method)
	}
	mustRPC(t, server1.Result(request, started))
	mustRPC(t, <-starting)
	mustRPC(t, server1.Close())
	awaitConnection(t, peer, false)
	if _, err = peer.Call(context.Background(), "session.list", SessionListRequest{}); !isCode(err, protocol.NotConnected) {
		t.Fatalf("disconnected call = %v", err)
	}
	requests2, server2, client2 := peerPipe(t)
	connections <- client2
	reconnected := <-requests2
	assertPeerHello(t, reconnected, "peer", "")
	mustRPC(t, server2.Result(reconnected, struct{}{}))
	awaitConnection(t, peer, true)
	callDone := make(chan error, 1)
	go func() {
		_, callErr := peer.Call(context.Background(), "session.list", SessionListRequest{})
		callDone <- callErr
	}()
	listed := <-requests2
	mustRPC(t, server2.Result(listed, SessionListResult{Sessions: []SessionSummary{}}))
	mustRPC(t, <-callDone)
	attemptsBeforeSupersession := attempts.Load()
	superseded := make(chan error, 1)
	go func() {
		superseded <- server2.Call(context.Background(), "session.superseded", struct{}{}, &struct{}{})
	}()
	mustRPC(t, <-superseded)
	await(t, peer.Closed(), "superseded peer closed")
	if !isCode(peer.Err(), protocol.Superseded) {
		t.Fatalf("superseded terminal error = %v", peer.Err())
	}
	if attempts.Load() != attemptsBeforeSupersession {
		t.Fatal("superseded peer retried")
	}
}

func TestPeerRejectedHelloIsTerminal(t *testing.T) {
	var attempts atomic.Int32
	requests, server, client := peerPipe(t)
	connections := make(chan net.Conn, 1)
	connections <- client
	usePeerDialer(t, func(string, string) (net.Conn, error) {
		attempts.Add(1)
		select {
		case fd := <-connections:
			return fd, nil
		default:
			return nil, errors.New("offline")
		}
	})
	peer, err := ConnectPeer(PeerIdentity{Product: "fixture-client", SessionID: "peer", Name: "peer title", Groups: []string{}, Info: map[string]any{}}, acceptDelivery)
	if err != nil {
		t.Fatal(err)
	}
	hello := <-requests
	mustRPC(t, server.Error(hello, protocol.InvalidHello, nil))
	await(t, peer.Closed(), "rejected peer closed")
	if !isCode(peer.Err(), protocol.InvalidHello) {
		t.Fatalf("rejected hello error = %v", peer.Err())
	}
	select {
	case <-peer.Ready():
		t.Fatal("rejected peer became ready")
	default:
	}
	if attempts.Load() != 1 {
		t.Fatalf("rejected hello retried %d times", attempts.Load())
	}
}

func TestPeerContextCancelsInitialDial(t *testing.T) {
	oldDial, oldInterval := dialPeer, peerReconnectInterval
	entered, returned := make(chan struct{}), make(chan struct{})
	dialPeer = func(ctx context.Context, _, _ string) (net.Conn, error) {
		close(entered)
		<-ctx.Done()
		close(returned)
		return nil, ctx.Err()
	}
	peerReconnectInterval = time.Millisecond
	t.Setenv("SESSIONBUS_SOCKET", "fixture.sock")
	t.Cleanup(func() { dialPeer, peerReconnectInterval = oldDial, oldInterval })

	ctx, cancel := context.WithCancel(context.Background())
	peer, err := ConnectPeerContext(ctx, PeerIdentity{Product: "fixture-client", SessionID: "peer", Name: "peer", Groups: []string{}, Info: map[string]any{}}, acceptDelivery)
	if err != nil {
		t.Fatal(err)
	}
	await(t, entered, "initial dial")
	cancel()
	await(t, returned, "canceled dial")
	await(t, peer.Closed(), "peer closed")
	select {
	case <-peer.Ready():
		t.Fatal("canceled initial dial became ready")
	default:
	}
	if peer.Err() != nil {
		t.Fatalf("canceled peer terminal error = %v", peer.Err())
	}
}

func TestPeerReplaceSettlesOldRunAndReconnectsAsNewIdentity(t *testing.T) {
	connections := make(chan net.Conn, 2)
	usePeerDialer(t, func(string, string) (net.Conn, error) {
		select {
		case fd := <-connections:
			return fd, nil
		default:
			return nil, errors.New("offline")
		}
	})
	requests1, server1, client1 := peerPipe(t)
	paused := &pauseNthWriteConn{Conn: client1, at: 3, entered: make(chan struct{}), release: make(chan struct{})}
	connections <- paused
	peer, err := ConnectPeer(Identity{Product: "fixture-client", SessionID: "old", Name: "old", Groups: []string{"old"}, Info: map[string]any{}}, acceptDelivery)
	if err != nil {
		t.Fatal(err)
	}
	hello := <-requests1
	mustRPC(t, server1.Result(hello, struct{}{}))
	await(t, peer.Ready(), "peer ready")
	started := RunRef{SessionID: "lane@local", RunID: "g/1"}
	starting := make(chan error, 1)
	go func() {
		_, callErr := peer.Caller.Start(context.Background(), TurnRunRequest{SessionID: started.SessionID, Input: "block"})
		starting <- callErr
	}()
	run := <-requests1
	mustRPC(t, server1.Result(run, started))
	mustRPC(t, <-starting)
	replaced := make(chan error, 1)
	next := Identity{Product: "fixture-client", SessionID: "new", Name: "new", Groups: []string{"new"}, Info: map[string]any{}}
	go func() { replaced <- peer.Replace(context.Background(), next) }()
	<-paused.entered
	status, err := peer.Caller.Wait(WaitRequest{SessionID: started.SessionID, RunID: started.RunID})
	if !isCode(err, protocol.NotConnected) || status.State != "" {
		t.Fatalf("old run = %#v, %v", status, err)
	}
	if _, err = peer.Call(context.Background(), "session.list", SessionListRequest{}); !isCode(err, protocol.NotConnected) {
		t.Fatalf("call during replacement = %v", err)
	}
	if err = peer.Rehello(context.Background(), "retitled", map[string]any{"revision": 2}); !isCode(err, protocol.NotConnected) {
		t.Fatalf("crossed rehello = %v", err)
	}
	close(paused.release)
	replacement := <-requests1
	if _, err = peer.Call(context.Background(), "session.list", SessionListRequest{}); !isCode(err, protocol.NotConnected) {
		t.Fatalf("call before replacement ack = %v", err)
	}
	select {
	case request := <-requests1:
		t.Fatalf("request escaped replacement gap: %s", request.Method)
	default:
	}
	mustRPC(t, server1.Result(replacement, struct{}{}))
	retitled := <-requests1
	assertPeerHello(t, retitled, "retitled", "")
	if got := retitled.Params.(*protocol.PeerHello).SessionID; got != "new" {
		t.Fatalf("retitled identity = %q", got)
	}
	mustRPC(t, server1.Result(retitled, struct{}{}))
	mustRPC(t, <-replaced)
	peer.mu.Lock()
	currentID, currentName := peer.identity.SessionID, peer.identity.Name
	peer.mu.Unlock()
	if currentID != "new" || currentName != "retitled" {
		t.Fatalf("identity = %q/%q", currentID, currentName)
	}
	requests2, server2, client2 := peerPipe(t)
	connections <- client2
	mustRPC(t, server1.Close())
	reconnected := <-requests2
	if got := reconnected.Params.(*protocol.PeerHello).SessionID; got != "new" {
		t.Fatalf("reconnect identity = %q", got)
	}
	assertPeerHello(t, reconnected, "retitled", "")
	mustRPC(t, server2.Result(reconnected, struct{}{}))
	peer.Shutdown()
	await(t, peer.Closed(), "peer closed")
}

func TestPeerDeliveryKeepsAdmissionIdentityAcrossReplace(t *testing.T) {
	requests, server, client := peerPipe(t)
	connections := make(chan net.Conn, 1)
	connections <- client
	usePeerDialer(t, func(string, string) (net.Conn, error) { return <-connections, nil })
	admitted := make(chan PeerIdentity, 1)
	peer, err := ConnectPeer(Identity{Product: "fixture-client", SessionID: "old", Name: "old", Groups: []string{"old"}, Info: map[string]any{}}, func(ctx context.Context, identity PeerIdentity, _ DeliveryRequest) (DeliveryReceipt, error) {
		admitted <- identity
		<-ctx.Done()
		return DeliveryReceipt{Disposition: "rejected", Reason: "closing"}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	hello := <-requests
	mustRPC(t, server.Result(hello, struct{}{}))
	await(t, peer.Ready(), "peer ready")
	delivered := make(chan DeliveryReceipt, 1)
	go func() {
		var receipt DeliveryReceipt
		_ = server.Call(context.Background(), "message.deliver", DeliveryRequest{MessageID: "message", From: DeliverySource{SessionID: "sender@local", Name: "sender@local", Product: "fixture", Groups: []string{}}, Body: "body"}, &receipt)
		delivered <- receipt
	}()
	identity := <-admitted
	replaced := make(chan error, 1)
	go func() {
		replaced <- peer.Replace(context.Background(), Identity{Product: "fixture-client", SessionID: "new", Name: "new", Groups: []string{"new"}, Info: map[string]any{}})
	}()
	replacement := <-requests
	receipt := <-delivered
	if identity.SessionID != "old" || receipt.Reason != "closing" {
		t.Fatalf("delivery identity/receipt = %q/%#v", identity.SessionID, receipt)
	}
	mustRPC(t, server.Result(replacement, struct{}{}))
	mustRPC(t, <-replaced)
	peer.Shutdown()
	await(t, peer.Closed(), "peer closed")
}

func TestPeerInstallsIdentityBeforeReadingDeliveryAfterHello(t *testing.T) {
	old := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(old)
	requests, server, client := peerPipe(t)
	connections := make(chan net.Conn, 1)
	connections <- client
	usePeerDialer(t, func(string, string) (net.Conn, error) { return <-connections, nil })
	called := make(chan struct{}, 1)
	peer, err := ConnectPeer(Identity{Product: "fixture-client", SessionID: "peer", Name: "peer", Groups: []string{}, Info: map[string]any{}}, func(context.Context, PeerIdentity, DeliveryRequest) (DeliveryReceipt, error) {
		called <- struct{}{}
		return DeliveryReceipt{Disposition: "injected"}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	mustRPC(t, server.Result(<-requests, struct{}{}))
	var receipt DeliveryReceipt
	mustRPC(t, server.Call(context.Background(), "message.deliver", DeliveryRequest{MessageID: "m", From: DeliverySource{SessionID: "s@local", Name: "s@local", Product: "fixture", Groups: []string{}}, Body: "body"}, &receipt))
	select {
	case <-called:
	default:
		t.Fatalf("delivery after hello was rejected: %#v", receipt)
	}
	peer.Shutdown()
}

func TestPeerShutdownOwnsWireDuringReplacementHello(t *testing.T) {
	requests, server, client := peerPipe(t)
	connections := make(chan net.Conn, 1)
	connections <- client
	usePeerDialer(t, func(string, string) (net.Conn, error) { return <-connections, nil })
	peer, err := ConnectPeer(Identity{Product: "fixture-client", SessionID: "old", Name: "old", Groups: []string{}, Info: map[string]any{}}, acceptDelivery)
	if err != nil {
		t.Fatal(err)
	}
	mustRPC(t, server.Result(<-requests, struct{}{}))
	await(t, peer.Ready(), "peer ready")
	replaced := make(chan error, 1)
	go func() {
		replaced <- peer.Replace(context.Background(), Identity{Product: "fixture-client", SessionID: "new", Name: "new", Groups: []string{}, Info: map[string]any{}})
	}()
	<-requests
	peer.Shutdown()
	await(t, peer.Closed(), "peer closed during replacement")
	select {
	case <-replaced:
	case <-time.After(time.Second):
		t.Fatal("replacement stayed blocked after shutdown")
	}
}

func TestPeerDeliveryGetsDeepIdentitySnapshot(t *testing.T) {
	requests, server, client := peerPipe(t)
	connections := make(chan net.Conn, 1)
	connections <- client
	usePeerDialer(t, func(string, string) (net.Conn, error) { return <-connections, nil })
	peer, err := ConnectPeer(Identity{Product: "fixture-client", SessionID: "peer", Name: "peer", Groups: []string{"original"}, Info: map[string]any{"nested": map[string]any{"value": "original"}}}, func(_ context.Context, identity PeerIdentity, _ DeliveryRequest) (DeliveryReceipt, error) {
		identity.Groups[0] = "mutated"
		identity.Info["nested"].(map[string]any)["value"] = "mutated"
		return DeliveryReceipt{Disposition: "injected"}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	mustRPC(t, server.Result(<-requests, struct{}{}))
	await(t, peer.Ready(), "peer ready")
	var receipt DeliveryReceipt
	mustRPC(t, server.Call(context.Background(), "message.deliver", DeliveryRequest{MessageID: "m", From: DeliverySource{SessionID: "s@local", Name: "s@local", Product: "fixture", Groups: []string{}}, Body: "body"}, &receipt))
	peer.mu.Lock()
	group := peer.identity.Groups[0]
	nested := peer.identity.Info["nested"].(map[string]any)["value"]
	peer.mu.Unlock()
	if group != "original" || nested != "original" {
		t.Fatalf("delivery callback mutated stored identity: %q/%v", group, nested)
	}
	peer.Shutdown()
}

type pauseNthWriteConn struct {
	net.Conn
	at, writes       int
	entered, release chan struct{}
}

func (c *pauseNthWriteConn) Write(body []byte) (int, error) {
	c.writes++
	if c.writes == c.at {
		close(c.entered)
		<-c.release
	}
	return c.Conn.Write(body)
}

type crossedRehelloFixture struct {
	Identity      PeerIdentity
	First, Second rehelloFixture
}

type rehelloFixture struct {
	Name string
	Info map[string]any
}

func usePeerDialer(t *testing.T, dial func(string, string) (net.Conn, error)) {
	t.Helper()
	oldDial, oldInterval := dialPeer, peerReconnectInterval
	dialPeer, peerReconnectInterval = func(_ context.Context, network, address string) (net.Conn, error) {
		return dial(network, address)
	}, time.Millisecond
	t.Setenv("SESSIONBUS_SOCKET", "fixture.sock")
	t.Cleanup(func() { dialPeer, peerReconnectInterval = oldDial, oldInterval })
}

func peerPipe(t *testing.T) (chan *rpc.Request, *rpc.Conn, net.Conn) {
	t.Helper()
	client, server := net.Pipe()
	requests := make(chan *rpc.Request, 8)
	wire := rpc.New(server, false, func(_ context.Context, request *rpc.Request) { requests <- request })
	t.Cleanup(func() { _ = wire.Close(); _ = client.Close() })
	return requests, wire, client
}

func assertPeerHello(t *testing.T, request *rpc.Request, name, nested string) {
	t.Helper()
	hello := request.Params.(*protocol.PeerHello)
	if hello.Info["sessionbus_unknown_transfer"] != float64(1) {
		t.Error("standard SDK hello did not advertise unknown-transfer decoding")
	}
	if hello.Name != name || nested != "" && hello.Info["nested"].(map[string]any)["value"] != nested {
		t.Fatalf("hello = %#v", hello)
	}
}

func acceptDelivery(context.Context, PeerIdentity, DeliveryRequest) (DeliveryReceipt, error) {
	return DeliveryReceipt{Disposition: "injected"}, nil
}

func await(t *testing.T, signal <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatal(name + " timed out")
	}
}

func awaitConnection(t *testing.T, peer *Peer, connected bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for (peer.connection() != nil) != connected {
		if time.Now().After(deadline) {
			t.Fatal("peer connection timed out")
		}
		runtime.Gosched()
	}
}

func mustRPC(t *testing.T, err error) {
	t.Helper()
	if err != nil && !errors.Is(err, rpc.ErrClosed) {
		t.Fatal(err)
	}
}

func TestUnnamedPeerWrittenDeliveryKeepsCapturedIdentity(t *testing.T) {
	requests, server, client := peerPipe(t)
	usePeerDialer(t, func(string, string) (net.Conn, error) { return client, nil })
	entered := make(chan PeerIdentity, 1)
	frames := make(chan DeliveryRequest, 1)
	release := make(chan struct{})
	initial := PeerIdentity{Product: "native-product", SessionID: "old", Groups: []string{"shared"}, Info: map[string]any{}}
	peer, err := ConnectPeer(initial, func(_ context.Context, identity PeerIdentity, request DeliveryRequest) (DeliveryReceipt, error) {
		entered <- identity
		frames <- request
		<-release
		return DeliveryReceipt{Disposition: "written"}, nil
	})
	mustRPC(t, err)
	defer peer.Shutdown()
	hello := <-requests
	if hello.Params.(*protocol.PeerHello).Name != "" {
		t.Fatal("unnamed hello acquired a name")
	}
	mustRPC(t, server.Result(hello, struct{}{}))
	await(t, peer.Ready(), "ready")
	for _, name := range []string{"first title", ""} {
		changed := make(chan error, 1)
		go func() { changed <- peer.Rehello(context.Background(), name, map[string]any{}) }()
		hello = <-requests
		if got := hello.Params.(*protocol.PeerHello); got.SessionID != "old" || got.Name != name {
			t.Fatalf("rehello = %#v", got)
		}
		mustRPC(t, server.Result(hello, struct{}{}))
		mustRPC(t, <-changed)
	}
	request := DeliveryRequest{MessageID: "captured", From: DeliverySource{SessionID: "source@local", Product: "native-product", Groups: []string{"shared"}}, Body: "complete frame"}
	var receipt DeliveryReceipt
	done := async(server, "message.deliver", request, &receipt)
	captured, frame := <-entered, <-frames
	changed := make(chan error, 1)
	go func() {
		changed <- peer.Replace(context.Background(), PeerIdentity{Product: "native-product", SessionID: "new", Name: "replacement", Groups: []string{"shared"}, Info: map[string]any{}})
	}()
	mustRPC(t, server.Result(<-requests, struct{}{}))
	mustRPC(t, <-changed)
	close(release)
	mustRPC(t, <-done)
	if captured.SessionID != "old" || captured.Name != "" || frame.MessageID != request.MessageID || frame.From.SessionID != request.From.SessionID || frame.From.Name != "" || frame.Body != request.Body || receipt.Disposition != "written" {
		t.Fatalf("captured delivery: %#v %#v %#v", captured, frame, receipt)
	}
	peer.Shutdown()
	await(t, peer.Closed(), "closed")
}

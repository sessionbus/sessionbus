// SPDX-License-Identifier: GPL-3.0-only

package federation

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"io"
	"net"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

type testDaemon struct {
	inbox  chan any
	cancel context.CancelFunc
	done   chan error
	closed chan struct{}
}

func testSecret(fill byte) string {
	return base64.StdEncoding.EncodeToString([]byte(string(make([]byte, SecretBytes-1)) + string(fill)))
}

func testHub(t *testing.T) (*Hub, string, map[string]string) {
	t.Helper()
	secrets := map[string]string{"alpha": testSecret('a'), "beta": testSecret('b'), "gamma": testSecret('c')}
	configuration, err := ServerTLS(secrets)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	hub := StartHub(listener, configuration, io.Discard)
	t.Cleanup(hub.Close)
	return hub, listener.Addr().String(), secrets
}

func connectTestDaemon(t *testing.T, address, host, secret string, admit func(IncomingCall) (Wait, error), lifetime ...func(LifetimeEvent)) *testDaemon {
	t.Helper()
	configuration, err := ClientTLS(host, secret)
	if err != nil {
		t.Fatal(err)
	}
	return connectTestDaemonTLS(t, address, host, configuration, admit, lifetime...)
}

func connectTestDaemonTLS(t *testing.T, address, host string, configuration *tls.Config, admit func(IncomingCall) (Wait, error), lifetime ...func(LifetimeEvent)) *testDaemon {
	t.Helper()
	raw, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	fd := tls.Client(raw, configuration)
	if err = fd.Handshake(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	value := &testDaemon{inbox: make(chan any, 256), cancel: cancel, done: make(chan error, 1), closed: make(chan struct{})}
	go func() {
		defer close(value.closed)
		value.done <- ServeDaemon(ctx, host, fd, value.inbox, admit, io.Discard, lifetime...)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-value.closed:
		case <-time.After(time.Second):
			t.Fatal("daemon link did not stop")
		}
	})
	return value
}

func connectLegacyTestDaemon(t *testing.T, address, host, secret string, admit func(IncomingCall) (Wait, error)) *testDaemon {
	t.Helper()
	configuration, err := ClientTLS(host, secret)
	if err != nil {
		t.Fatal(err)
	}
	configuration.NextProtos = []string{RosterALPN}
	return connectTestDaemonTLS(t, address, host, configuration, admit)
}

func immediate(reply Reply) Wait {
	return func(<-chan struct{}) (Reply, bool) { return reply, true }
}

func callHosts(t *testing.T, daemon *testDaemon) []string {
	t.Helper()
	reply := make(chan Reply, 1)
	daemon.inbox <- HostsCall{Reply: reply}
	select {
	case value := <-reply:
		hosts, err := DecodeHosts(value)
		if err != nil {
			t.Fatal(err)
		}
		return hosts
	case <-time.After(time.Second):
		t.Fatal("hosts call did not return")
		return nil
	}
}

func TestHostsResultIsStrict(t *testing.T) {
	for _, raw := range []string{`{}`, `{"hosts":["local"]}`, `{"hosts":["bad host"]}`, `{"hosts":["beta","beta"]}`} {
		if _, err := DecodeHosts(Reply{Result: []byte(raw)}); err == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
}

func TestPullRosterForwardAndCapturedCaller(t *testing.T) {
	_, address, secrets := testHub(t)
	seen := make(chan Caller, 2)
	beta := connectTestDaemon(t, address, "beta", secrets["beta"], func(call IncomingCall) (Wait, error) {
		seen <- call.From
		if call.Request.Method == "session.list" {
			return immediate(Reply{Result: []byte(`{"sessions":[]}`)}), nil
		}
		errorRaw := []byte(`{"message":"unknown_session","code":-32001}`)
		return immediate(Reply{Error: &protocol.RPCError{Code: protocol.UnknownSession, Message: "unknown_session"}, ErrorRaw: errorRaw}), nil
	})
	_ = beta
	alpha := connectTestDaemon(t, address, "alpha", secrets["alpha"], func(IncomingCall) (Wait, error) {
		t.Fatal("unexpected call to alpha")
		return nil, nil
	})
	if hosts := callHosts(t, alpha); len(hosts) != 1 || hosts[0] != "beta" {
		t.Fatalf("hosts = %#v", hosts)
	}
	caller := Caller{SessionID: "sender@alpha", Name: "Sender@alpha", Product: "old-product", PrivateGroup: "session:sender@alpha", Groups: []string{"shared", "session:sender@alpha"}}
	params, _ := protocol.EncodeParams("session.list", protocol.SessionListRequest{Host: "beta"})
	reply := make(chan Reply, 1)
	alpha.inbox <- OutgoingCall{Value: Forward{From: caller, Request: PublicRequest{Method: "session.list", Params: params}}, Reply: reply}
	caller.Product = "new-product"
	listed := <-reply
	if string(listed.Result) != `{"sessions":[]}` || (<-seen).Product != "old-product" {
		t.Fatalf("result/caller = %s / %#v", listed.Result, caller)
	}
	params, _ = protocol.EncodeParams("turn.interrupt", protocol.SessionTarget{SessionID: "missing@beta"})
	select {
	case err := <-beta.done:
		t.Fatalf("beta stopped before second call: %v", err)
	default:
	}
	reply = make(chan Reply, 1)
	alpha.inbox <- OutgoingCall{Value: Forward{From: caller, Request: PublicRequest{Method: "turn.interrupt", Params: params}}, Reply: reply}
	failed := <-reply
	if string(failed.ErrorRaw) != `{"message":"unknown_session","code":-32001}` {
		t.Fatalf("error = %s", failed.ErrorRaw)
	}
}

func TestDuplicateKeepsIncumbentAndDestinationClosesOn257th(t *testing.T) {
	_, address, secrets := testHub(t)
	release := make(chan struct{})
	admitted := make(chan struct{})
	beta := connectTestDaemon(t, address, "beta", secrets["beta"], func(IncomingCall) (Wait, error) {
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
	// Client TLS completion does not imply hub registration. Complete an
	// authenticated request on each incumbent before racing a duplicate.
	_ = callHosts(t, beta)
	alpha := connectTestDaemon(t, address, "alpha", secrets["alpha"], func(IncomingCall) (Wait, error) { return immediate(Reply{}), nil })
	if hosts := callHosts(t, alpha); len(hosts) != 1 || hosts[0] != "beta" {
		t.Fatalf("registered destination roster = %#v", hosts)
	}
	duplicate := connectTestDaemon(t, address, "alpha", secrets["alpha"], func(IncomingCall) (Wait, error) { return immediate(Reply{}), nil })
	select {
	case <-duplicate.closed:
	case <-time.After(time.Second):
		t.Fatal("duplicate host remained connected")
	}
	if hosts := callHosts(t, beta); len(hosts) != 1 || hosts[0] != "alpha" {
		t.Fatalf("incumbent roster = %#v", hosts)
	}
	params, _ := protocol.EncodeParams("turn.run", protocol.TurnRunRequest{SessionID: "lane@beta", Input: "go"})
	gamma := connectTestDaemon(t, address, "gamma", secrets["gamma"], func(IncomingCall) (Wait, error) { return immediate(Reply{}), nil })
	_ = callHosts(t, gamma)
	replies := make([]chan Reply, 257)
	caller := Caller{SessionID: "sender@alpha", Name: "sender@alpha", Product: "peer", PrivateGroup: "session:sender@alpha", Groups: []string{"session:sender@alpha"}}
	for index := range replies {
		replies[index] = make(chan Reply, 1)
		origin, from := alpha, caller
		if index == 256 {
			// A distinct authenticated origin isolates the hub destination cap from
			// the source daemon's independent 256-operation admission bound.
			origin = gamma
			from = Caller{SessionID: "sender@gamma", Name: "sender@gamma", Product: "peer", PrivateGroup: "session:sender@gamma", Groups: []string{"session:sender@gamma"}}
		}
		origin.inbox <- OutgoingCall{Value: Forward{From: from, Request: PublicRequest{Method: "turn.run", Params: params}}, Reply: replies[index]}
		// Fill the destination pending table, not the independent bounded
		// origin outbox. The last request must overflow before beta dispatch.
		if index < 256 {
			select {
			case <-admitted:
			case <-time.After(time.Second):
				t.Fatal("destination did not admit held call")
			}
		}
	}
	for index, reply := range replies {
		select {
		case value := <-reply:
			if value.Error == nil || value.Error.Code != protocol.ForwardLost {
				t.Fatalf("reply %d = %#v", index, value)
			}
		case <-time.After(time.Second):
			t.Fatalf("reply %d did not settle", index)
		}
	}
	close(release)
	select {
	case <-beta.closed:
	case <-time.After(time.Second):
		t.Fatal("overflow did not close destination")
	}
	if hosts := callHosts(t, alpha); len(hosts) != 1 || hosts[0] != "gamma" {
		t.Fatalf("closed destination remained in roster: %#v", hosts)
	}
}

func TestRetiredSocketCannotEnterAuthentication(t *testing.T) {
	local, peer := net.Pipe()
	defer peer.Close()
	state := hubState{sockets: map[net.Conn]bool{local: true}, log: io.Discard}
	state.handle(context.Background(), make(chan registryEvent, 1), registryEvent{kind: registryRetire, fd: local})
	if state.sockets[local] || len(state.links) != 0 {
		t.Fatal("retired socket entered host admission")
	}
	_ = local.Close()
}

func TestRegistryCloseJoinsPreHandshakeSocket(t *testing.T) {
	registry := NewRegistry(io.Discard)
	local, peer := net.Pipe()
	if !registry.Accept(local, &tls.Config{}) {
		t.Fatal("pre-handshake socket was not accepted")
	}
	registry.Close()
	_ = peer.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := peer.Read(make([]byte, 1)); err == nil {
		t.Fatal("pre-handshake socket remained open")
	} else if failure, ok := err.(net.Error); ok && failure.Timeout() {
		t.Fatal("pre-handshake socket reached a deadline instead of closing")
	}
	_ = peer.Close()
}

func TestOriginLossDoesNotRetargetReplyAfterReconnect(t *testing.T) {
	_, address, secrets := testHub(t)
	entered, release := make(chan struct{}), make(chan struct{})
	beta := connectTestDaemon(t, address, "beta", secrets["beta"], func(call IncomingCall) (Wait, error) {
		if call.Request.Method == "turn.run" {
			close(entered)
			return func(done <-chan struct{}) (Reply, bool) {
				select {
				case <-release:
					raw, _ := protocol.EncodeResult("turn.run", protocol.RunStatus{SessionID: "lane@beta", RunID: "g/1", State: "done", Result: &protocol.TurnResult{Outcome: "completed", Result: "done"}})
					return Reply{Result: raw}, true
				case <-done:
					return Reply{}, false
				}
			}, nil
		}
		return immediate(Reply{Result: []byte(`{"sessions":[]}`)}), nil
	})
	_ = beta
	alpha := connectTestDaemon(t, address, "alpha", secrets["alpha"], func(IncomingCall) (Wait, error) { return nil, nil })
	caller := Caller{SessionID: "sender@alpha", Name: "sender@alpha", Product: "peer", PrivateGroup: "session:sender@alpha", Groups: []string{"session:sender@alpha"}}
	params, _ := protocol.EncodeParams("turn.run", protocol.TurnRunRequest{SessionID: "lane@beta", Input: "go"})
	reply := make(chan Reply, 1)
	alpha.inbox <- OutgoingCall{Value: Forward{From: caller, Request: PublicRequest{Method: "turn.run", Params: params}}, Reply: reply}
	<-entered
	alpha.cancel()
	select {
	case <-alpha.closed:
	case <-time.After(time.Second):
		t.Fatal("origin did not close")
	}
	close(release)
	deadline := time.Now().Add(time.Second)
	for len(callHosts(t, beta)) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("old origin attachment remained")
		}
		time.Sleep(time.Millisecond)
	}
	replacement := connectTestDaemon(t, address, "alpha", secrets["alpha"], func(IncomingCall) (Wait, error) { return nil, nil })
	params, _ = protocol.EncodeParams("session.list", protocol.SessionListRequest{Host: "beta"})
	next := make(chan Reply, 1)
	replacement.inbox <- OutgoingCall{Value: Forward{From: caller, Request: PublicRequest{Method: "session.list", Params: params}}, Reply: next}
	select {
	case value := <-next:
		if string(value.Result) != `{"sessions":[]}` {
			t.Fatalf("replacement result = %#v", value)
		}
	case <-time.After(time.Second):
		t.Fatal("replacement inherited the old pending call")
	}
}

func TestForwardOptionalNameIsAbsentNeverEmpty(t *testing.T) {
	for _, name := range []string{"", `,"name":"sender@alpha"`, `,"name":""`, `,"name":null`, `,"name":"@alpha"`, `,"name":"sender@beta"`} {
		raw := []byte(`{"from":{"session_id":"sender@alpha","product":"peer","private_group":"session:sender@alpha","groups":["session:sender@alpha"]` + name + `},"request":{"method":"session.list","params":{"host":"beta"}}}`)
		value, host, err := decodeForward(raw, "alpha")
		valid := name == "" || name == `,"name":"sender@alpha"`
		if (err == nil) != valid {
			t.Fatalf("name %s: %v", name, err)
		}
		if valid && (host != "beta" || name == "" && value.From.Name != "") {
			t.Fatalf("forward: %#v, %s", value, host)
		}
	}
}

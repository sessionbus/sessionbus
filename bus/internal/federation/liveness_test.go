// SPDX-License-Identifier: GPL-3.0-only

package federation

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/json"
	"net"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/roster"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

type syncLog struct {
	mu   sync.Mutex
	body bytes.Buffer
}

func (w *syncLog) Write(value []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.body.Write(value)
}

func (w *syncLog) silentLines(host string) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return strings.Count(w.body.String(), "federation host \""+host+"\" silent for ")
}

// livenessHub shrinks the link timings before any link starts. The restore is
// registered first, so it runs after the hub cleanup has joined every link.
func livenessHub(t *testing.T, stall, timeout time.Duration) (string, map[string]string, *syncLog) {
	t.Helper()
	stallBefore, timeoutBefore, checkBefore := linkStallAfter, linkProbeTimeout, linkCheckInterval
	t.Cleanup(func() { linkStallAfter, linkProbeTimeout, linkCheckInterval = stallBefore, timeoutBefore, checkBefore })
	linkStallAfter, linkProbeTimeout, linkCheckInterval = stall, timeout, 20*time.Millisecond
	secrets := map[string]string{"alpha": testSecret('a'), "beta": testSecret('b')}
	configuration, err := ServerTLS(secrets)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	log := &syncLog{}
	hub := StartHub(listener, configuration, log)
	t.Cleanup(hub.Close)
	return listener.Addr().String(), secrets, log
}

// rawPeer is a host that answers nothing unless the test writes for it.
// Probes (federation.roster requests) are routed apart from other frames.
type rawPeer struct {
	fd      *tls.Conn
	frames  chan protocol.Frame
	probes  chan protocol.Frame
	rosters atomic.Int64
	paused  atomic.Bool
	resumed chan struct{}
	stop    chan struct{}
	closed  chan struct{}
	write   sync.Mutex
	nextID  int64
}

func dialRawPeer(t *testing.T, address, host, secret string, alpn bool) *rawPeer {
	t.Helper()
	configuration, err := ClientTLS(host, secret)
	if err != nil {
		t.Fatal(err)
	}
	if !alpn {
		configuration.NextProtos = nil
	}
	raw, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	fd := tls.Client(raw, configuration)
	_ = fd.SetDeadline(time.Now().Add(time.Second))
	if err = fd.Handshake(); err != nil {
		_ = raw.Close()
		t.Fatal(err)
	}
	_ = fd.SetDeadline(time.Time{})
	peer := &rawPeer{fd: fd, frames: make(chan protocol.Frame, 1024), probes: make(chan protocol.Frame, 64), resumed: make(chan struct{}), stop: make(chan struct{}), closed: make(chan struct{})}
	go peer.read()
	t.Cleanup(func() {
		close(peer.stop)
		_ = raw.Close()
		select {
		case <-peer.closed:
		case <-time.After(time.Second):
			t.Error("raw peer reader did not stop")
		}
	})
	return peer
}

func (p *rawPeer) read() {
	defer close(p.closed)
	reader := bufio.NewReaderSize(p.fd, protocol.MaxFrameBytes)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			return
		}
		frame, err := protocol.DecodeFrame(line[:len(line)-1])
		if err != nil {
			return
		}
		target := p.frames
		if frame.Request && frame.Method == rosterMethod {
			p.rosters.Add(1)
			target = p.probes
		}
		select {
		case target <- frame:
		case <-p.stop:
			return
		}
		if p.paused.Load() {
			select {
			case <-p.resumed:
			case <-p.stop:
				return
			}
		}
	}
}

// pause stops reading after at most the frame being read, like a stopped process.
func (p *rawPeer) pause()  { p.paused.Store(true) }
func (p *rawPeer) resume() { p.paused.Store(false); close(p.resumed) }

func (p *rawPeer) expect(t *testing.T, method string, within time.Duration) protocol.Frame {
	t.Helper()
	deadline := time.After(within)
	for {
		select {
		case frame := <-p.frames:
			if frame.Request && frame.Method == method {
				return frame
			}
		case <-deadline:
			t.Fatalf("no %s request within %s", method, within)
			return protocol.Frame{}
		}
	}
}

func (p *rawPeer) probe(t *testing.T, within time.Duration) protocol.Frame {
	t.Helper()
	select {
	case frame := <-p.probes:
		return frame
	case <-time.After(within):
		t.Fatalf("no roster probe within %s", within)
		return protocol.Frame{}
	}
}

// probesDuring counts roster requests that arrive while the test waits.
func (p *rawPeer) probesDuring(during time.Duration) int64 {
	before := p.rosters.Load()
	time.Sleep(during)
	return p.rosters.Load() - before
}

func (p *rawPeer) send(t *testing.T, body []byte) {
	t.Helper()
	p.write.Lock()
	defer p.write.Unlock()
	_ = p.fd.SetWriteDeadline(time.Now().Add(time.Second))
	if _, err := p.fd.Write(body); err != nil {
		t.Fatal(err)
	}
}

func (p *rawPeer) requestHosts(t *testing.T) {
	t.Helper()
	p.nextID++
	body, err := requestBytes(p.nextID, hostsMethod, struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	p.send(t, body)
}

func (p *rawPeer) answer(t *testing.T, id int64, reply Reply) {
	t.Helper()
	body, err := ResponseBytes(id, reply)
	if err != nil {
		t.Fatal(err)
	}
	p.send(t, body)
}

func (p *rawPeer) waitClosed(t *testing.T, within time.Duration) {
	t.Helper()
	select {
	case <-p.closed:
	case <-time.After(within):
		t.Fatalf("hub did not close the raw peer within %s", within)
	}
}

func hostRoster(host string) Reply {
	return RosterReply(roster.Host{Host: host, Products: []string{}, Sessions: []roster.Row{}})
}

var emptyList = Reply{Result: []byte(`{"sessions":[]}`)}

func livenessAlpha(t *testing.T, address string, secrets map[string]string) *testDaemon {
	t.Helper()
	return connectTestDaemon(t, address, "alpha", secrets["alpha"], func(call IncomingCall) (Wait, error) {
		if IsRoster(call) {
			return immediate(hostRoster("alpha")), nil
		}
		return immediate(emptyList), nil
	})
}

func listed(t *testing.T, alpha *testDaemon) bool {
	t.Helper()
	return slices.Contains(callHosts(t, alpha), "beta")
}

func waitListed(t *testing.T, alpha *testDaemon, want bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for listed(t, alpha) != want {
		if time.Now().After(deadline) {
			t.Fatalf("beta listed = %t, want %t", !want, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func forwardFrom(t *testing.T, daemon *testDaemon, method string, value any) chan Reply {
	t.Helper()
	params, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	caller := Caller{SessionID: "sender@alpha", Name: "sender@alpha", Product: "peer", PrivateGroup: "session:sender@alpha", Groups: []string{"session:sender@alpha"}}
	reply := make(chan Reply, 1)
	daemon.inbox <- OutgoingCall{Value: Forward{From: caller, Request: PublicRequest{Method: method, Params: params}}, Reply: reply}
	return reply
}

func listBeta(t *testing.T, alpha *testDaemon) chan Reply {
	t.Helper()
	return forwardFrom(t, alpha, "session.list", protocol.SessionListRequest{Host: "beta"})
}

func forwardLost(t *testing.T, reply chan Reply, within time.Duration) {
	t.Helper()
	select {
	case value := <-reply:
		if value.Error == nil || value.Error.Code != protocol.ForwardLost {
			t.Fatalf("reply = %#v, want forward_lost", value)
		}
	case <-time.After(within):
		t.Fatalf("forward did not settle within %s", within)
	}
}

func unsettled(t *testing.T, reply chan Reply) {
	t.Helper()
	select {
	case value := <-reply:
		t.Fatalf("forward settled early: %#v", value)
	default:
	}
}

func TestHubClosesSilentRosterHostLink(t *testing.T) {
	const stall, timeout = 150 * time.Millisecond, 250 * time.Millisecond
	t.Run("probe", func(t *testing.T) {
		address, secrets, log := livenessHub(t, stall, timeout)
		beta := dialRawPeer(t, address, "beta", secrets["beta"], true)
		alpha := livenessAlpha(t, address, secrets)
		waitListed(t, alpha, true)
		started := time.Now()
		reply := listBeta(t, alpha)
		beta.expect(t, forwardMethod, time.Second)
		beta.probe(t, 3*time.Second)
		forwardLost(t, reply, 3*time.Second)
		if elapsed := time.Since(started); elapsed < stall+timeout {
			t.Fatalf("link closed after %s, before stall + timeout", elapsed)
		}
		waitListed(t, alpha, false)
		beta.waitClosed(t, 3*time.Second)
		select {
		case <-alpha.closed:
			t.Fatal("healthy alpha was disconnected")
		default:
		}
		if lines := log.silentLines("beta"); lines != 1 {
			t.Fatalf("silent log lines = %d", lines)
		}
	})
	// Failure control: the same silent host and demand, with the trigger beyond
	// the test duration, stays listed. The close above comes from the probe.
	t.Run("probe disabled", func(t *testing.T) {
		address, secrets, log := livenessHub(t, time.Hour, timeout)
		beta := dialRawPeer(t, address, "beta", secrets["beta"], true)
		alpha := livenessAlpha(t, address, secrets)
		waitListed(t, alpha, true)
		reply := listBeta(t, alpha)
		beta.expect(t, forwardMethod, time.Second)
		if probes := beta.probesDuring(4 * (stall + timeout)); probes != 0 {
			t.Fatalf("probes = %d", probes)
		}
		unsettled(t, reply)
		if !listed(t, alpha) || log.silentLines("beta") != 0 {
			t.Fatal("silent host without a probe was closed")
		}
	})
}

func TestHubDoesNotProbeIdleLink(t *testing.T) {
	const stall, timeout = 150 * time.Millisecond, 250 * time.Millisecond
	address, secrets, log := livenessHub(t, stall, timeout)
	beta := dialRawPeer(t, address, "beta", secrets["beta"], true)
	alpha := livenessAlpha(t, address, secrets)
	waitListed(t, alpha, true)
	if probes := beta.probesDuring(5 * (stall + timeout)); probes != 0 {
		t.Fatalf("idle link probes = %d", probes)
	}
	if !listed(t, alpha) || log.silentLines("beta") != 0 {
		t.Fatal("idle host was closed")
	}
}

func TestHubDoesNotProbeForLongCalls(t *testing.T) {
	const stall, timeout = 150 * time.Millisecond, 250 * time.Millisecond
	for _, call := range []struct {
		method string
		value  any
	}{
		{"turn.run", protocol.TurnRunRequest{SessionID: "lane@beta", Input: "go"}},
		{"turn.wait", protocol.WaitRequest{SessionID: "lane@beta"}},
		{"lane.spawn", protocol.LaneSpawnRequest{Host: "beta", Name: "child", Product: "worker", Open: &protocol.OpenOptions{}}},
		{"session.close", protocol.SessionCloseRequest{SessionID: "lane@beta"}},
	} {
		t.Run(call.method, func(t *testing.T) {
			address, secrets, log := livenessHub(t, stall, timeout)
			beta := dialRawPeer(t, address, "beta", secrets["beta"], true)
			alpha := livenessAlpha(t, address, secrets)
			waitListed(t, alpha, true)
			reply := forwardFrom(t, alpha, call.method, call.value)
			frame := beta.expect(t, forwardMethod, time.Second)
			var forwarded Forward
			if err := json.Unmarshal(frame.Params, &forwarded); err != nil || forwarded.Request.Method != call.method {
				t.Fatalf("forwarded %s, %v", frame.Params, err)
			}
			if probes := beta.probesDuring(3 * (stall + timeout)); probes != 0 {
				t.Fatalf("probes for held %s = %d", call.method, probes)
			}
			unsettled(t, reply)
			if !listed(t, alpha) || log.silentLines("beta") != 0 {
				t.Fatalf("host holding %s was closed", call.method)
			}
		})
	}
}

func TestHubDoesNotProbeWhileHostSends(t *testing.T) {
	const stall, timeout = 150 * time.Millisecond, 250 * time.Millisecond
	address, secrets, log := livenessHub(t, stall, timeout)
	beta := dialRawPeer(t, address, "beta", secrets["beta"], true)
	alpha := livenessAlpha(t, address, secrets)
	waitListed(t, alpha, true)
	reply := listBeta(t, alpha)
	beta.expect(t, forwardMethod, time.Second)
	for started := time.Now(); time.Since(started) < 3*(stall+timeout); {
		beta.requestHosts(t)
		time.Sleep(stall / 3)
	}
	if probes := beta.rosters.Load(); probes != 0 {
		t.Fatalf("probes while the host sends = %d", probes)
	}
	unsettled(t, reply)
	beta.probe(t, 3*time.Second)
	if log.silentLines("beta") != 0 {
		t.Fatal("host sending frames was closed")
	}
}

func TestHubRenewsProbeDeadlineOnTrafficThenClosesWhenSilent(t *testing.T) {
	const stall, timeout = 150 * time.Millisecond, 400 * time.Millisecond
	address, secrets, log := livenessHub(t, stall, timeout)
	beta := dialRawPeer(t, address, "beta", secrets["beta"], true)
	alpha := livenessAlpha(t, address, secrets)
	waitListed(t, alpha, true)
	reply := listBeta(t, alpha)
	beta.expect(t, forwardMethod, time.Second)
	beta.probe(t, 3*time.Second)
	probed := time.Now()
	for time.Since(probed) < 3*timeout {
		beta.requestHosts(t)
		time.Sleep(timeout / 4)
	}
	unsettled(t, reply)
	if !listed(t, alpha) || log.silentLines("beta") != 0 {
		t.Fatal("host sending frames was closed")
	}
	stopped := time.Now()
	forwardLost(t, reply, 3*time.Second)
	if elapsed := time.Since(stopped); elapsed < timeout/2 {
		t.Fatalf("closed %s after traffic stopped", elapsed)
	}
	waitListed(t, alpha, false)
	beta.waitClosed(t, 3*time.Second)
	if probes := beta.rosters.Load(); probes != 1 {
		t.Fatalf("probes = %d, want the one outstanding probe", probes)
	}
	if lines := log.silentLines("beta"); lines != 1 {
		t.Fatalf("silent log lines = %d", lines)
	}
}

func TestHubAcceptsLateProbeReplyAndProbesAgain(t *testing.T) {
	const stall, timeout = 150 * time.Millisecond, 600 * time.Millisecond
	address, secrets, log := livenessHub(t, stall, timeout)
	beta := dialRawPeer(t, address, "beta", secrets["beta"], true)
	alpha := livenessAlpha(t, address, secrets)
	waitListed(t, alpha, true)
	reply := listBeta(t, alpha)
	forward := beta.expect(t, forwardMethod, time.Second)
	first := beta.probe(t, 3*time.Second)
	probed := time.Now()
	time.Sleep(timeout / 2)
	beta.requestHosts(t)
	// After the un-renewed probe deadline, before the renewed one.
	time.Sleep(time.Until(probed.Add(timeout + timeout/6)))
	beta.answer(t, first.ID, hostRoster("beta"))
	beta.answer(t, forward.ID, emptyList)
	select {
	case value := <-reply:
		if string(value.Result) != `{"sessions":[]}` {
			t.Fatalf("reply after late probe answer = %#v", value)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("forward did not complete")
	}
	if probes := beta.probesDuring(stall + timeout); probes != 0 {
		t.Fatalf("probes without demand = %d", probes)
	}
	next := listBeta(t, alpha)
	forward = beta.expect(t, forwardMethod, time.Second)
	second := beta.probe(t, 3*time.Second)
	if second.ID == first.ID {
		t.Fatalf("second probe reused id %d", second.ID)
	}
	beta.answer(t, second.ID, hostRoster("beta"))
	beta.answer(t, forward.ID, emptyList)
	select {
	case value := <-next:
		if value.Error != nil {
			t.Fatalf("second forward = %#v", value)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("second forward did not complete")
	}
	if !listed(t, alpha) || log.silentLines("beta") != 0 {
		t.Fatal("host answering probes was closed")
	}
}

func TestHubKeepsSlowHealthyHost(t *testing.T) {
	const stall, timeout = 150 * time.Millisecond, 250 * time.Millisecond
	address, secrets, log := livenessHub(t, stall, timeout)
	var probes atomic.Int64
	beta := connectTestDaemon(t, address, "beta", secrets["beta"], func(call IncomingCall) (Wait, error) {
		if IsRoster(call) {
			probes.Add(1)
			return immediate(hostRoster("beta")), nil
		}
		return func(done <-chan struct{}) (Reply, bool) {
			select {
			case <-time.After(3 * (stall + timeout)):
				return emptyList, true
			case <-done:
				return Reply{}, false
			}
		}, nil
	})
	_ = beta
	alpha := livenessAlpha(t, address, secrets)
	waitListed(t, alpha, true)
	reply := listBeta(t, alpha)
	select {
	case value := <-reply:
		if string(value.Result) != `{"sessions":[]}` {
			t.Fatalf("slow reply = %#v", value)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("slow forward did not complete")
	}
	if probes.Load() == 0 {
		t.Fatal("slow short call was never probed")
	}
	if !listed(t, alpha) || log.silentLines("beta") != 0 {
		t.Fatal("slow healthy host was closed")
	}
}

func TestHubProbeDoesNotUseForwardCapacity(t *testing.T) {
	// Filling takes many round trips under -race; the stall leaves it room.
	t.Run("healthy saturation", func(t *testing.T) {
		address, secrets, log := livenessHub(t, 500*time.Millisecond, 3*time.Second)
		beta := dialRawPeer(t, address, "beta", secrets["beta"], true)
		alpha := livenessAlpha(t, address, secrets)
		waitListed(t, alpha, true)
		replies := make([]chan Reply, 0, maxPendingForwardedPerHost)
		for range maxPendingForwardedPerHost - 1 {
			replies = append(replies, listBeta(t, alpha))
			beta.expect(t, forwardMethod, 2*time.Second)
		}
		probe := beta.probe(t, 3*time.Second)
		// The probe is outstanding and unanswered: the 256th user forward
		// must still reach beta.
		replies = append(replies, listBeta(t, alpha))
		beta.expect(t, forwardMethod, 2*time.Second)
		beta.answer(t, probe.ID, hostRoster("beta"))
		beta.answer(t, beta.probe(t, 3*time.Second).ID, hostRoster("beta"))
		for _, reply := range replies {
			unsettled(t, reply)
		}
		if !listed(t, alpha) || log.silentLines("beta") != 0 {
			t.Fatal("saturated healthy host was closed")
		}
	})
	t.Run("silent saturation", func(t *testing.T) {
		address, secrets, log := livenessHub(t, 2*time.Second, 500*time.Millisecond)
		beta := dialRawPeer(t, address, "beta", secrets["beta"], true)
		alpha := livenessAlpha(t, address, secrets)
		waitListed(t, alpha, true)
		replies := make([]chan Reply, 0, maxPendingForwardedPerHost)
		for range maxPendingForwardedPerHost {
			replies = append(replies, listBeta(t, alpha))
			beta.expect(t, forwardMethod, 2*time.Second)
		}
		if beta.rosters.Load() != 0 {
			t.Fatal("probe arrived before saturation was established")
		}
		beta.pause()
		for _, reply := range replies {
			forwardLost(t, reply, 5*time.Second)
		}
		waitListed(t, alpha, false)
		beta.resume()
		beta.probe(t, time.Second)
		beta.waitClosed(t, 3*time.Second)
		if lines := log.silentLines("beta"); lines != 1 {
			t.Fatalf("silent log lines = %d", lines)
		}
	})
}

func TestHubDoesNotProbeLegacyHost(t *testing.T) {
	const stall, timeout = 150 * time.Millisecond, 250 * time.Millisecond
	address, secrets, log := livenessHub(t, stall, timeout)
	beta := dialRawPeer(t, address, "beta", secrets["beta"], false)
	if protocol := beta.fd.ConnectionState().NegotiatedProtocol; protocol != "" {
		t.Fatalf("legacy client negotiated %q", protocol)
	}
	alpha := livenessAlpha(t, address, secrets)
	waitListed(t, alpha, true)
	reply := listBeta(t, alpha)
	beta.expect(t, forwardMethod, time.Second)
	if probes := beta.probesDuring(3 * (stall + timeout)); probes != 0 {
		t.Fatalf("legacy host probes = %d", probes)
	}
	unsettled(t, reply)
	if !listed(t, alpha) || log.silentLines("beta") != 0 {
		t.Fatal("legacy host was closed")
	}
}

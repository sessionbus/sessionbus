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
	"strings"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/commslog"
	"github.com/antst/sessionbus/bus/internal/federation"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
	"github.com/antst/sessionbus/bus/sdk/go/testsocket"
)

func TestCommunicationLogMessageAttribution(t *testing.T) {
	for _, mode := range []commslog.Mode{commslog.Off, commslog.Metadata, commslog.Content} {
		t.Run(string(mode), func(t *testing.T) {
			dir := testsocket.Directory(t)
			path := filepath.Join(t.TempDir(), "comms", "sessionbus.jsonl")
			d, err := Start(Config{Host: "test", SocketPath: filepath.Join(dir, "bus.sock"), TablePath: filepath.Join(dir, "rows"), CommsLog: commslog.Options{Mode: mode, Path: path, MaxFileBytes: 1 << 20, MaxFiles: 2, QueueBytes: 1 << 20}})
			must(t, err)
			t.Cleanup(func() { _ = d.Close() })
			sender := connectPeer(t, d.config.SocketPath, "sender", "Sender", "shared")
			one := connectPeer(t, d.config.SocketPath, "one", "First", "shared")
			two := connectPeer(t, d.config.SocketPath, "two", "Second", "shared")
			var result protocol.MessageSendResult
			body := "one body with newline\nsecond line"
			must(t, sender.call("message.send", protocol.MessageSendRequest{Targets: []string{"First", "Second", "missing"}, Message: body}, &result))
			if len(result.Deliveries) != 3 {
				t.Fatal(result)
			}

			// A locally generated notification has no earlier send header, even though
			// it uses the same internal forwarding path as a multicast partition.
			d.directory.mu.Lock()
			origin := d.directory.entries["sender@test"]
			d.directory.mu.Unlock()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			wait := d.directory.forwardLocal(federation.Caller{SessionID: "sender@test", Name: "Sender@test", Product: "fixture-client", Groups: []string{"shared"}}, origin, "message.send", &protocol.MessageSendRequest{Target: "First", Message: "generated notification"}, "")
			reply, ok := wait(ctx.Done())
			if !ok || reply.Error != nil {
				t.Fatalf("notification: %+v", reply)
			}
			sender.peer.Shutdown()
			one.peer.Shutdown()
			two.peer.Shutdown()
			must(t, d.Close())
			raw, err := os.ReadFile(path)
			if mode == commslog.Off {
				if !os.IsNotExist(err) {
					t.Fatalf("disabled log created: %v", err)
				}
				return
			}
			must(t, err)
			if mode == commslog.Content && strings.Count(string(raw), `"body":"generated notification"`) != 1 {
				t.Fatalf("notification header missing or duplicated: %s", raw)
			}
			if mode == commslog.Metadata && strings.Contains(string(raw), "one body") {
				t.Fatal("metadata leaked content")
			}
			headers, dispatches, receipts, bodies := 0, 0, 0, 0
			scanner := bufio.NewScanner(strings.NewReader(string(raw)))
			for scanner.Scan() {
				var row struct {
					Type       commslog.EventType `json:"type"`
					Method     commslog.Method    `json:"method"`
					MessageID  string             `json:"message_id"`
					DeliveryID string             `json:"delivery_id"`
					From, To   *commslog.Endpoint
					Body       string
					Receipt    *commslog.DeliveryReceipt
				}
				must(t, json.Unmarshal(scanner.Bytes(), &row))
				if row.MessageID != result.MessageID {
					continue
				}
				if row.From == nil || row.From.SessionID != "sender@test" || row.From.Host != "test" || row.From.Name != "Sender@test" || row.From.Product != "fixture-client" {
					t.Fatalf("from: %s", scanner.Bytes())
				}
				if row.Body != "" {
					bodies++
					if row.Body != body {
						t.Fatal("body changed")
					}
				}
				switch {
				case row.Type == commslog.Request && row.Method == commslog.MessageSend:
					headers++
					if row.To == nil || strings.Join(row.To.Targets, ",") != "First,Second,missing" {
						t.Fatalf("requested targets: %s", scanner.Bytes())
					}
				case row.Method == commslog.MessageDeliver:
					dispatches++
					if row.To == nil || row.To.SessionID == "" || row.To.Host != "test" || row.To.Name == "" || row.DeliveryID == "" || row.Body != "" {
						t.Fatalf("dispatch: %s", scanner.Bytes())
					}
				case row.Type == commslog.Receipt:
					receipts++
					if row.Receipt == nil || row.To == nil {
						t.Fatalf("receipt: %s", scanner.Bytes())
					}
					if row.To.Target == "missing" {
						if row.To.SessionID != "" || row.Receipt.Disposition != "rejected" || row.Receipt.Reason != "unknown_session" {
							t.Fatalf("invented missing identity: %s", scanner.Bytes())
						}
					} else if row.To.SessionID == "" || row.Receipt.Disposition != "injected" || row.DeliveryID == "" {
						t.Fatalf("resolved receipt: %s", scanner.Bytes())
					}
				}
			}
			must(t, scanner.Err())
			wantBodies := 0
			if mode == commslog.Content {
				wantBodies = 1
			}
			if headers != 1 || dispatches != 2 || receipts != 3 || bodies != wantBodies {
				t.Fatalf("headers=%d dispatch=%d receipt=%d bodies=%d\n%s", headers, dispatches, receipts, bodies, raw)
			}
		})
	}
}

func TestCommunicationLogFederatedMessageCorrelation(t *testing.T) {
	secrets := map[string]string{}
	for index, host := range []string{"alpha", "beta"} {
		key := make([]byte, federation.SecretBytes)
		key[0] = byte(index + 1)
		secrets[host] = base64.StdEncoding.EncodeToString(key)
	}
	tlsConfig, err := federation.ServerTLS(secrets)
	must(t, err)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	must(t, err)
	hub := federation.StartHub(listener, tlsConfig, io.Discard)
	t.Cleanup(hub.Close)
	paths := map[string]string{}
	daemons := map[string]*Daemon{}
	for _, host := range []string{"alpha", "beta"} {
		dir := testsocket.Directory(t)
		paths[host] = filepath.Join(t.TempDir(), "comms", "sessionbus.jsonl")
		d, err := Start(Config{Host: host, SocketPath: filepath.Join(dir, "bus.sock"), TablePath: filepath.Join(dir, "rows"), HubAddress: listener.Addr().String(), HubSecret: secrets[host], CommsLog: commslog.Options{Mode: commslog.Content, Path: paths[host], MaxFileBytes: 1 << 20, MaxFiles: 2, QueueBytes: 1 << 20}})
		must(t, err)
		t.Cleanup(func() { _ = d.Close() })
		daemons[host] = d
	}
	sender := connectPeer(t, daemons["alpha"].config.SocketPath, "sender", "sender", "team")
	local := connectPeer(t, daemons["alpha"].config.SocketPath, "local", "local", "team")
	remote := connectPeer(t, daemons["beta"].config.SocketPath, "remote", "remote", "team")
	// Local hello does not wait for the asynchronous federation supervisor.
	// Observe the remote row through the hub before measuring group fan-out.
	awaitRemotePeer(t, sender, "beta", "remote@beta")
	var sent protocol.MessageSendResult
	must(t, sender.call("message.send", protocol.MessageSendRequest{Group: "team", Message: "federated once"}, &sent))
	if len(sent.Deliveries) != 2 {
		t.Fatal(sent)
	}
	sender.peer.Shutdown()
	local.peer.Shutdown()
	remote.peer.Shutdown()
	for host, d := range daemons {
		must(t, d.Close())
		if d.comms.Stats().LostInvalid != 0 {
			t.Fatalf("invalid events at %s: %+v", host, d.comms.Stats())
		}
		raw, err := os.ReadFile(paths[host])
		must(t, err)
		scanner := bufio.NewScanner(strings.NewReader(string(raw)))
		bodies, dispatches := 0, 0
		for scanner.Scan() {
			var event commslog.Event
			must(t, json.Unmarshal(scanner.Bytes(), &event))
			if event.MessageID != sent.MessageID {
				continue
			}
			if event.From == nil || event.From.SessionID != "sender@alpha" {
				t.Fatalf("wrong source %s", scanner.Bytes())
			}
			if event.Body != "" {
				bodies++
				if event.Body != "federated once" {
					t.Fatal("body changed")
				}
			}
			if event.Method == commslog.MessageDeliver {
				dispatches++
				if event.To == nil || event.To.Host != host || event.To.SessionID == "" || event.DeliveryID == "" {
					t.Fatalf("dispatch: %s", scanner.Bytes())
				}
				matched := false
				for _, delivery := range sent.Deliveries {
					if delivery.DeliveryID == event.DeliveryID && delivery.SessionID == event.To.SessionID {
						matched = true
					}
				}
				if !matched {
					t.Fatal("delivery correlation missing")
				}
			}
		}
		must(t, scanner.Err())
		if bodies != 1 || dispatches != 1 {
			t.Fatalf("%s body=%d dispatch=%d\n%s", host, bodies, dispatches, raw)
		}
	}
}

func TestCommunicationLogRunMetadataAndSupersession(t *testing.T) {
	products := t.TempDir()
	installFixture(t, products, "wake-worker")
	t.Setenv("PATH", products+string(os.PathListSeparator)+os.Getenv("PATH"))
	dir := testsocket.Directory(t)
	path := filepath.Join(t.TempDir(), "comms", "sessionbus.jsonl")
	d, err := Start(Config{SocketPath: filepath.Join(dir, "bus.sock"), TablePath: filepath.Join(dir, "rows"), CommsLog: commslog.Options{Mode: commslog.Content, Path: path, MaxFileBytes: 1 << 20, MaxFiles: 2, QueueBytes: 1 << 20}})
	must(t, err)
	t.Cleanup(func() { _ = d.Close() })
	parent := connectPeer(t, d.config.SocketPath, "parent", "parent", "team")
	var lane protocol.LaneSpawnResult
	notify := false
	must(t, parent.call("lane.spawn", protocol.LaneSpawnRequest{Product: "wake-worker", Name: "child", Open: &protocol.OpenOptions{}, Notify: &notify}, &lane))
	var sent protocol.MessageSendResult
	must(t, parent.call("message.send", protocol.MessageSendRequest{Target: lane.SessionID, Message: "wake child"}, &sent))
	var ref protocol.RunRef
	// Collect and acknowledge the delivery-started Run before explicit reuse.
	var initial protocol.RunStatus
	must(t, parent.call("turn.wait", protocol.WaitRequest{SessionID: lane.SessionID}, &initial))
	must(t, parent.call("turn.ack", protocol.RunRef{SessionID: lane.SessionID, RunID: initial.RunID}, &struct{}{}))
	secret := "native input and result must stay out of message logs"
	must(t, parent.call("turn.start", protocol.TurnRunRequest{SessionID: lane.SessionID, Input: secret}, &ref))
	var status protocol.RunStatus
	must(t, parent.call("turn.wait", protocol.WaitRequest{SessionID: lane.SessionID, RunID: ref.RunID}, &status))
	must(t, parent.call("turn.ack", ref, &struct{}{}))
	must(t, parent.call("session.close", protocol.SessionCloseRequest{SessionID: lane.SessionID, Forget: true}, &struct{}{}))
	replacement := connectPeer(t, d.config.SocketPath, "parent", "replacement", "team")
	select {
	case <-parent.peer.Closed():
	case <-time.After(time.Second):
		t.Fatal("supersession not joined")
	}
	replacement.peer.Shutdown()
	must(t, d.Close())
	if d.comms.Stats().LostInvalid != 0 {
		t.Fatalf("invalid projection: %+v", d.comms.Stats())
	}
	raw, err := os.ReadFile(path)
	must(t, err)
	if strings.Contains(string(raw), secret) {
		t.Fatal("native content leaked")
	}
	triggered, superseded := false, false
	scanner := bufio.NewScanner(strings.NewReader(string(raw)))
	for scanner.Scan() {
		var event commslog.Event
		must(t, json.Unmarshal(scanner.Bytes(), &event))
		if event.Method == commslog.MessageDeliver && event.Type == commslog.Lifecycle && event.State == commslog.Running && event.RunID != "" {
			triggered = true
		}
		if event.State == commslog.Superseded && event.From != nil && event.From.SessionID == "parent@local" {
			superseded = true
		}
	}
	must(t, scanner.Err())
	if !triggered || !superseded {
		t.Fatalf("trigger=%v superseded=%v\n%s", triggered, superseded, raw)
	}
}

func TestCommunicationLogBusySendNeverDispatches(t *testing.T) {
	path := filepath.Join(t.TempDir(), "comms", "log.jsonl")
	logger, err := commslog.Open(commslog.Options{Mode: commslog.Content, Path: path, Host: "alpha", Incarnation: "busy-test", MaxFileBytes: 1 << 20, MaxFiles: 2, QueueBytes: 1 << 20})
	must(t, err)
	d := &Daemon{host: "alpha", comms: logger}
	d.directory = newDirectory(d, nil)
	s := pullReviewSender(d)
	for i := 0; i < protocol.MaxOperations; i++ {
		s.requests[int64(i+1)] = &requestState{}
	}
	frame := protocol.Frame{ID: 1000, Method: "message.send", Request: true}
	s.dispatchRequest(frame, &protocol.MessageSendRequest{Target: "recipient", Message: "busy attempt"})
	reply := <-s.forwarded
	if reply.Error == nil || reply.Error.Code != protocol.Busy {
		t.Fatalf("reply: %+v", reply)
	}
	if len(s.commsRequests) != 0 {
		t.Fatal("busy log context retained")
	}
	must(t, logger.Close())
	raw, err := os.ReadFile(path)
	must(t, err)
	scanner := bufio.NewScanner(strings.NewReader(string(raw)))
	rows := []commslog.Event{}
	for scanner.Scan() {
		var event commslog.Event
		must(t, json.Unmarshal(scanner.Bytes(), &event))
		rows = append(rows, event)
	}
	must(t, scanner.Err())
	if len(rows) != 2 || rows[0].Body != "busy attempt" || rows[0].MessageID == "" || rows[1].MessageID != rows[0].MessageID || rows[1].ErrorCode != protocol.Busy || rows[1].DeliveryID != "" || rows[1].Body != "" {
		t.Fatalf("records: %s", raw)
	}
}

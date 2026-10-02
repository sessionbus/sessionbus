// SPDX-License-Identifier: GPL-3.0-only

package daemon

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/conn"
	"github.com/antst/sessionbus/bus/internal/structuredprocess"
	sessionkit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
	"github.com/antst/sessionbus/bus/sdk/go/testsocket"
)

func TestMain(m *testing.M) {
	name := filepath.Base(os.Args[0])
	if strings.HasPrefix(name, "exit-worker") {
		fmt.Fprintln(os.Stderr, "fixture failed before hello")
		os.Exit(7)
	}
	if strings.HasPrefix(name, "no-hello-worker") {
		if path := os.Getenv("NO_HELLO_READY"); path != "" {
			_ = os.WriteFile(path, []byte("ready"), 0o600)
		}
		select {}
	}
	if strings.HasPrefix(name, "rehello-worker") {
		signal.Ignore(syscall.SIGTERM)
		if err := runRehelloWorker(name); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(8)
		}
		os.Exit(0)
	}
	if strings.HasPrefix(name, "stderr-parent") {
		child := exec.Command("sh", "-c", `while [ ! -e "$STDERR_DESCENDANT_RELEASE" ]; do sleep 0.01; done; : > "$STDERR_DESCENDANT_DONE"`)
		child.Stderr = os.Stderr
		_ = child.Start()
		fmt.Fprintln(os.Stderr, "parent exited while descendant held stderr")
		_ = os.WriteFile(os.Getenv("STDERR_PARENT_READY"), []byte("ready"), 0o600)
		os.Exit(7)
	}
	if strings.HasPrefix(name, "mutual-worker") {
		product := &mutualSendProduct{fixtureProduct: fixtureProduct{product: name}}
		product.worker = sessionkit.NewWorker(product)
		_ = product.worker.Serve(context.Background())
		os.Exit(0)
	}
	if strings.HasPrefix(name, "wake-worker") || strings.HasPrefix(name, "fixture-worker") || strings.HasPrefix(name, "open-exit-worker") || strings.HasPrefix(name, "fixed-worker") || strings.HasPrefix(name, "error-worker") || strings.HasPrefix(name, "close-error-worker") || strings.HasPrefix(name, "sequence-worker") || strings.HasPrefix(name, "racing-worker") {
		worker := sessionkit.NewWorker(&fixtureProduct{product: name})
		_ = worker.Serve(context.Background())
		os.Exit(0)
	}
	if strings.HasPrefix(name, "ordered-worker") {
		if err := runOrderedWorker(name); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(8)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func runOrderedWorker(product string) error {
	fd, err := net.Dial("unix", os.Getenv("SESSIONBUS_SOCKET"))
	if err != nil {
		return err
	}
	defer fd.Close()
	reader := bufio.NewReaderSize(fd, protocol.MaxFrameBytes)
	hello := protocol.WorkerHello{Protocol: 1, LaunchToken: os.Getenv("SESSIONBUS_LAUNCH_TOKEN"), HelloDescription: protocol.HelloDescription{SupportsMessageRun: true, Product: product, SupportedOpenFields: []string{}, ExtraArguments: []protocol.ExtraArgument{}}}
	if err = rawCall(fd, reader, 1, "session.hello", hello, &struct{}{}); err != nil {
		return err
	}
	open, err := readRawFrame(reader)
	if err != nil || open.Method != "session.open" {
		return fmt.Errorf("open request: %v (%q)", err, open.Method)
	}
	body, err := protocol.ResultBytes(open.ID, open.Method, protocol.OpenResult{SessionID: "ordered-native"})
	if err != nil {
		return err
	}
	if _, err = fd.Write(body); err != nil {
		return err
	}
	var listed protocol.SessionListResult
	if err = rawCall(fd, reader, 2, "session.list", protocol.SessionListRequest{SessionID: "ordered-native"}, &listed); err != nil || len(listed.Sessions) != 1 {
		return fmt.Errorf("post-open list before commit: %v (%d rows)", err, len(listed.Sessions))
	}
	if listed.SelfInfo == nil || listed.SelfInfo.SessionID != "ordered-native@local" || listed.SelfInfo.Name != "parent/ordered@local" || listed.SelfInfo.Product != product || !slices.Equal(listed.SelfInfo.Groups, listed.Sessions[0].Groups) {
		return fmt.Errorf("committed lane self identity: %#v", listed.SelfInfo)
	}
	if err = rawCall(fd, reader, 3, "session.list", protocol.SessionListRequest{SessionID: "parent"}, &listed); err != nil || len(listed.Sessions) != 1 || listed.Sessions[0].SessionID != "parent@local" || listed.SelfInfo == nil || listed.SelfInfo.SessionID != "ordered-native@local" {
		return fmt.Errorf("lane list filtered to parent: %#v, %v", listed, err)
	}
	var sent protocol.MessageSendResult
	return rawCall(fd, reader, 4, "message.send", protocol.MessageSendRequest{Target: "parent", Message: "after-commit"}, &sent)
}

func runRehelloWorker(product string) error {
	fd, err := net.Dial("unix", os.Getenv("SESSIONBUS_SOCKET"))
	if err != nil {
		return err
	}
	defer fd.Close()
	reader := bufio.NewReaderSize(fd, protocol.MaxFrameBytes)
	hello := protocol.WorkerHello{Protocol: 1, LaunchToken: os.Getenv("SESSIONBUS_LAUNCH_TOKEN"), HelloDescription: protocol.HelloDescription{SupportsMessageRun: true, Product: product, SupportedOpenFields: []string{}, ExtraArguments: []protocol.ExtraArgument{}}}
	if err = rawCall(fd, reader, 1, "session.hello", hello, &struct{}{}); err != nil {
		return err
	}
	firstErr := rawCallIgnoringRequests(fd, reader, 2, "session.list", protocol.SessionListRequest{}, &protocol.SessionListResult{})
	secondErr := rawCallIgnoringRequests(fd, reader, 3, "session.hello", hello, &struct{}{})
	first, second := rpcCode(firstErr), rpcCode(secondErr)
	_ = os.WriteFile(os.Getenv("REHELLO_LOG"), []byte(fmt.Sprintf("%d,%d", first, second)), 0o600)
	return nil
}

func rawCall(fd net.Conn, reader *bufio.Reader, id int64, method string, params, result any) error {
	body, err := protocol.RequestBytes(id, method, params)
	if err == nil {
		_, err = fd.Write(body)
	}
	if err != nil {
		return err
	}
	frame, err := readRawFrame(reader)
	if err != nil {
		return err
	}
	if frame.ID != id || frame.Request {
		return errors.New("unexpected Sessionbus response")
	}
	if frame.Error != nil {
		return frame.Error
	}
	return protocol.UnmarshalResult(method, frame.Result, result)
}

func rawCallIgnoringRequests(fd net.Conn, reader *bufio.Reader, id int64, method string, params, result any) error {
	body, err := protocol.RequestBytes(id, method, params)
	if err == nil {
		_, err = fd.Write(body)
	}
	if err != nil {
		return err
	}
	for {
		frame, readErr := readRawFrame(reader)
		if readErr != nil {
			return readErr
		}
		if frame.Request {
			continue
		}
		if frame.ID != id {
			return errors.New("unexpected Sessionbus response")
		}
		if frame.Error != nil {
			return frame.Error
		}
		return protocol.UnmarshalResult(method, frame.Result, result)
	}
}

func readRawFrame(reader *bufio.Reader) (protocol.Frame, error) {
	body, err := reader.ReadBytes('\n')
	if err != nil {
		return protocol.Frame{}, err
	}
	return protocol.DecodeFrame(body[:len(body)-1])
}

type fixtureProduct struct {
	product string
	mu      sync.Mutex
	stop    chan struct{}
}

func (p *fixtureProduct) Hello(context.Context) (sessionkit.HelloDescription, error) {
	return sessionkit.HelloDescription{SupportsMessageRun: !strings.HasPrefix(p.product, "fixture-worker-no-wake"), Product: p.product, Version: "test", SupportedOpenFields: []string{"cwd", "permission_mode", "model", "reasoning_effort", "arguments"}, ExtraArguments: []sessionkit.ExtraArgument{}}, nil
}
func (p *fixtureProduct) Open(_ context.Context, request sessionkit.OpenRequest) (sessionkit.OpenResult, error) {
	if request.Policy != nil && request.Policy.Trace != "" {
		return sessionkit.OpenResult{}, errors.New("live trace response leaked into worker policy")
	}
	if strings.HasPrefix(p.product, "open-exit-worker") {
		os.Exit(9)
	}
	if request.Name == "" || len(request.Groups) < 2 {
		return sessionkit.OpenResult{}, errors.New("identity missing")
	}
	if strings.HasPrefix(p.product, "error-worker") {
		return sessionkit.OpenResult{}, errors.New("open rejected")
	}
	if strings.HasPrefix(p.product, "racing-worker") {
		_ = os.WriteFile(os.Getenv("RACE_READY"), []byte("ready"), 0o600)
		for {
			if _, err := os.Stat(os.Getenv("RACE_RELEASE")); err == nil {
				break
			}
			time.Sleep(time.Millisecond)
		}
		return sessionkit.OpenResult{SessionID: "shared-id"}, nil
	}
	if path := os.Getenv("OPEN_LOG"); path != "" {
		raw, err := json.Marshal(request.Open)
		if err != nil {
			return sessionkit.OpenResult{}, err
		}
		file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			return sessionkit.OpenResult{}, err
		}
		_, writeErr := file.Write(append(raw, '\n'))
		if closeErr := file.Close(); writeErr == nil {
			writeErr = closeErr
		}
		if writeErr != nil {
			return sessionkit.OpenResult{}, writeErr
		}
	}
	if request.ResumeSessionID != "" {
		return sessionkit.OpenResult{SessionID: request.ResumeSessionID}, nil
	}
	if strings.HasPrefix(p.product, "fixed-worker") {
		return sessionkit.OpenResult{SessionID: "shared-id"}, nil
	}
	return sessionkit.OpenResult{SessionID: fmt.Sprintf("native-%d", os.Getpid())}, nil
}

func (p *fixtureProduct) Run(_ context.Context, run *sessionkit.Run, seed sessionkit.RunInput) (sessionkit.TurnResult, error) {
	var input string
	if seed.Text != nil {
		input = *seed.Text
	} else {
		input = seed.Delivery.Body
		if seed.Delivery.MessageID == "" || seed.Delivery.From.SessionID == "" {
			return sessionkit.TurnResult{}, errors.New("missing delivery identity")
		}
		if input == "uncertain" {
			_ = run.ReportDelivery(sessionkit.DeliveryReceipt{}, &sessionkit.ProtocolError{Code: protocol.Internal, Message: "internal"})
		} else {
			_ = run.ReportDelivery(sessionkit.DeliveryReceipt{Disposition: "written"}, nil)
		}
	}
	if input == "fail" {
		return sessionkit.TurnResult{}, errors.New("stream malformed")
	}
	if input == "block" {
		p.mu.Lock()
		p.stop = make(chan struct{})
		stop := p.stop
		run.Native = stop
		if run.Interrupted() {
			close(stop)
			p.stop = nil
		}
		p.mu.Unlock()
		<-stop
		return sessionkit.TurnResult{Outcome: "interrupted", Result: "stopped"}, nil
	}
	return sessionkit.TurnResult{Outcome: "completed", Result: input}, nil
}
func (p *fixtureProduct) Interrupt(_ context.Context, _ *sessionkit.Run) error {
	if strings.HasPrefix(p.product, "sequence-worker") {
		_ = os.WriteFile(os.Getenv("SEQUENCE_FILE"), []byte("turn.run\nturn.interrupt\n"), 0o600)
	}
	p.mu.Lock()
	if p.stop != nil {
		close(p.stop)
		p.stop = nil
	}
	p.mu.Unlock()
	return nil
}
func (*fixtureProduct) Deliver(_ context.Context, request sessionkit.DeliveryRequest, _ *sessionkit.Run) (sessionkit.DeliveryReceipt, error) {
	if request.MessageID == "" || request.From.SessionID == "" {
		return sessionkit.DeliveryReceipt{}, errors.New("delivery identity missing")
	}
	return sessionkit.DeliveryReceipt{Disposition: "injected"}, nil
}
func (p *fixtureProduct) Close(context.Context, sessionkit.SessionCloseRequest) error {
	if strings.HasPrefix(p.product, "close-error-worker") {
		return errors.New("native cleanup failed")
	}
	return nil
}

func TestCloseErrorAfterFailedRunStillCloses(t *testing.T) {
	directory := t.TempDir()
	installFixture(t, directory, "close-error-worker")
	t.Setenv("PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))
	_, socket := startDaemon(t)
	parent := connectPeer(t, socket, "parent", "parent", "shared")
	var spawned protocol.LaneSpawnResult
	must(t, parent.call("lane.spawn", protocol.LaneSpawnRequest{Name: "child", Product: "close-error-worker", Open: &protocol.OpenOptions{}}, &spawned))
	var turn protocol.RunStatus
	must(t, parent.call("turn.run", protocol.TurnRunRequest{SessionID: spawned.SessionID, Input: "fail"}, &turn))
	if turn.State != "unavailable" || turn.Reason != "stream malformed" {
		t.Fatalf("failed run = %#v", turn)
	}
	must(t, parent.call("session.close", protocol.SessionCloseRequest{SessionID: spawned.SessionID}, &struct{}{}))
	var listed protocol.SessionListResult
	must(t, parent.call("session.list", protocol.SessionListRequest{SessionID: spawned.SessionID}, &listed))
	if len(listed.Sessions) != 1 || listed.Sessions[0].Connected || listed.Sessions[0].Running {
		t.Fatalf("closed row = %#v", listed.Sessions)
	}
}

type peerClient struct {
	peer       *sessionkit.Peer
	superseded <-chan struct{}
	deliveries chan protocol.DeliveryRequest
	identity   protocol.PeerHello
}

func connectPeer(t *testing.T, socket, id, name string, groups ...string) *peerClient {
	t.Helper()
	t.Setenv("SESSIONBUS_SOCKET", socket)
	client := &peerClient{deliveries: make(chan protocol.DeliveryRequest, 8)}
	client.identity = protocol.PeerHello{Protocol: 1, Product: "fixture-client", SessionID: id, Name: name, Groups: groups, Info: map[string]any{"ready": true}}
	peer, err := sessionkit.ConnectPeer(client.identity, func(_ context.Context, _ sessionkit.PeerIdentity, request sessionkit.DeliveryRequest) (sessionkit.DeliveryReceipt, error) {
		client.deliveries <- request
		return sessionkit.DeliveryReceipt{Disposition: "injected"}, nil
	})
	must(t, err)
	select {
	case <-peer.Ready():
	case <-time.After(time.Second):
		t.Fatal("peer ready timed out")
	}
	client.peer, client.superseded = peer, peer.Closed()
	t.Cleanup(func() {
		peer.Shutdown()
		select {
		case <-peer.Closed():
		case <-time.After(time.Second):
			t.Fatal("peer close timed out")
		}
	})
	return client
}

func (p *peerClient) call(method string, params, result any) error {
	raw, err := p.peer.Call(context.Background(), method, params)
	if err == nil && result != nil {
		err = json.Unmarshal(raw, result)
	}
	return err
}

func TestNameAndIDPartGrammar(t *testing.T) {
	if !validNamePart("  List agent sessions  ") || validNamePart("bad\u0001title") {
		t.Fatal("printable name grammar changed")
	}
	if validIDPart("session id") || !validIDPart("session-id") {
		t.Fatal("session ID grammar changed")
	}
	if !validNamePart(strings.Repeat("🙂", 128)) || validNamePart(strings.Repeat("🙂", 129)) {
		t.Fatal("name limit does not count Unicode code points")
	}
}

func TestDurableTableWritesSixColumnsAndLoads(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "sessions")
	table, rows, err := openTable(path)
	must(t, err)
	if len(rows) != 0 {
		t.Fatalf("new table rows = %#v", rows)
	}
	want := row{SessionID: "native@local", Product: "fixture-worker", Name: "parent/child@local", Groups: []string{"session:parent@local", "session:parent@local/child"}, Open: protocol.OpenOptions{Cwd: "/work"}, CreatedAt: time.Unix(10, 0).UTC()}
	must(t, table.write(want))
	raw, err := os.ReadFile(filepath.Join(path, rowFile(want.SessionID)))
	must(t, err)
	var stored map[string]json.RawMessage
	must(t, json.Unmarshal(raw, &stored))
	for _, name := range []string{"session_id", "product", "name", "groups", "open", "created_at"} {
		if stored[name] == nil {
			t.Fatalf("stored row lacks %q: %s", name, raw)
		}
	}
	if len(stored) != 6 {
		t.Fatalf("stored row has %d keys: %s", len(stored), raw)
	}
	_, rows, err = openTable(path)
	must(t, err)
	if len(rows) != 1 || rows[0].Name != want.Name || rows[0].Open.Cwd != "/work" {
		t.Fatalf("loaded rows = %#v", rows)
	}
	must(t, table.delete(want.SessionID))
	_, rows, err = openTable(path)
	must(t, err)
	if len(rows) != 0 {
		t.Fatal("deleted row remained")
	}
}

func TestLegacyDurableRowListsCompatibilityPolicy(t *testing.T) {
	directory := testsocket.Directory(t)
	path := filepath.Join(directory, "sessions")
	table, _, err := openTable(path)
	must(t, err)
	legacy := row{
		SessionID: "legacy@local", Product: "fixture-worker", Name: "parent/legacy@local",
		Groups: []string{"session:parent@local", "session:parent@local/legacy", "shared"},
		Open:   protocol.OpenOptions{Cwd: "/work"}, CreatedAt: time.Unix(10, 0).UTC(),
	}
	must(t, table.write(legacy))
	raw, err := os.ReadFile(filepath.Join(path, rowFile(legacy.SessionID)))
	must(t, err)
	var fields map[string]json.RawMessage
	must(t, json.Unmarshal(raw, &fields))
	if len(fields) != 6 || fields["policy"] != nil {
		t.Fatalf("legacy fixture is not a six-field row: %s", raw)
	}

	socket := filepath.Join(directory, "sessionbus.sock")
	daemon, err := Start(Config{SocketPath: socket, TablePath: path})
	must(t, err)
	t.Cleanup(func() { _ = daemon.Close() })
	peer := connectPeer(t, socket, "observer", "observer", "shared")
	var listed protocol.SessionListResult
	must(t, peer.call("session.list", protocol.SessionListRequest{SessionID: legacy.SessionID}, &listed))
	if len(listed.Sessions) != 1 {
		t.Fatalf("legacy sessions = %#v", listed.Sessions)
	}
	policy := listed.Sessions[0].Policy
	if policy == nil || !policy.Persistent || policy.AutoCloseMS != 0 || policy.Notify || policy.OwnerSessionID != "" || policy.NotifyTarget != "" {
		t.Fatalf("legacy policy = %#v", policy)
	}
}

func TestTableAllowsDuplicateNamesAndRejectsUnknownColumns(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "sessions")
	must(t, os.MkdirAll(path, 0o700))
	base := row{Product: "worker", Name: "same@local", Groups: []string{"parent", "own"}, Open: protocol.OpenOptions{}, CreatedAt: time.Unix(10, 0).UTC()}
	first := base
	first.SessionID = "one@local"
	second := base
	second.SessionID = "two@local"
	raw, err := json.Marshal(first)
	must(t, err)
	must(t, os.WriteFile(filepath.Join(path, rowFile(first.SessionID)), raw, 0o600))
	raw, err = json.Marshal(second)
	must(t, err)
	must(t, os.WriteFile(filepath.Join(path, rowFile(second.SessionID)), raw, 0o600))
	_, rows, err := openTable(path)
	must(t, err)
	if len(rows) != 2 || rows[0].SessionID == rows[1].SessionID {
		t.Fatalf("equal-name rows = %+v", rows)
	}
	var object map[string]any
	must(t, json.Unmarshal(raw, &object))
	object["unknown"] = true
	raw, err = json.Marshal(object)
	must(t, err)
	must(t, os.Remove(filepath.Join(path, rowFile(first.SessionID))))
	must(t, os.WriteFile(filepath.Join(path, rowFile(second.SessionID)), raw, 0o600))
	if _, _, err := openTable(path); err == nil {
		t.Fatal("unknown durable column was accepted")
	}
}

func TestProductConfigRejectsDuplicatesAndOmitsEmptyDiscovery(t *testing.T) {
	directory := testsocket.Directory(t)
	config := Config{SocketPath: filepath.Join(directory, "duplicate.sock"), TablePath: filepath.Join(directory, "sessions.json"), Products: []string{"tool", "tool"}}
	if _, err := Start(config); err == nil || err.Error() != "duplicate advertised product" {
		t.Fatalf("duplicate products = %v", err)
	}
	config.SocketPath, config.Products = filepath.Join(directory, "empty.sock"), []string{}
	d, err := Start(config)
	must(t, err)
	t.Cleanup(func() { _ = d.Close() })
	peer := connectPeer(t, config.SocketPath, "peer", "peer", "group")
	var listed protocol.SessionListResult
	must(t, peer.call("session.list", protocol.SessionListRequest{}, &listed))
	if listed.Hosts != nil {
		t.Fatalf("empty products advertised as %#v", listed.Hosts)
	}
}

func TestDaemonCloseEndsRawAndActiveSpawnConnections(t *testing.T) {
	directory := testsocket.Directory(t)
	installFixture(t, directory, "no-hello-worker")
	t.Setenv("PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))
	ready := filepath.Join(directory, "ready")
	t.Setenv("NO_HELLO_READY", ready)
	socket := filepath.Join(directory, "sessionbus.sock")
	d, err := Start(Config{SocketPath: socket, TablePath: filepath.Join(directory, "sessions.json")})
	must(t, err)
	raw, err := net.Dial("unix", socket)
	must(t, err)
	parent := connectPeer(t, socket, "parent", "parent", "group")
	spawned := make(chan error, 1)
	go func() {
		spawned <- parent.call("lane.spawn", protocol.LaneSpawnRequest{Name: "child", Product: "no-hello-worker", Open: &protocol.OpenOptions{}}, &protocol.LaneSpawnResult{})
	}()
	waitFile(t, ready)
	must(t, d.Close())
	_, rows, loadErr := openTable(d.config.TablePath)
	if <-spawned == nil || loadErr != nil || len(rows) != 0 {
		t.Fatal("daemon close allowed an active spawn to commit")
	}
	_ = raw.SetReadDeadline(time.Now().Add(time.Second))
	if read, _ := raw.Read(make([]byte, 1)); read != 0 {
		t.Fatal("raw accepted connection survived daemon close")
	}
}

func TestProcessWaitDoesNotDependOnDescendantStderr(t *testing.T) {
	directory := t.TempDir()
	installFixture(t, directory, "stderr-parent")
	ready := filepath.Join(directory, "parent-ready")
	release := filepath.Join(directory, "release-descendant")
	descendantDone := filepath.Join(directory, "descendant-done")
	t.Setenv("STDERR_PARENT_READY", ready)
	t.Setenv("STDERR_DESCENDANT_RELEASE", release)
	t.Setenv("STDERR_DESCENDANT_DONE", descendantDone)
	t.Cleanup(func() { _ = os.WriteFile(release, nil, 0o600) })
	child, err := structuredprocess.Start(filepath.Join(directory, "stderr-parent"), os.Environ())
	must(t, err)
	waitFileWithoutDeadline(t, ready)
	<-child.Done()
	must(t, os.WriteFile(release, nil, 0o600))
	waitFileWithoutDeadline(t, descendantDone)
	lines, _ := child.Details()
	if !strings.Contains(strings.Join(lines, "\n"), "parent exited") {
		t.Fatalf("stderr tail = %#v", lines)
	}
}

func waitFileWithoutDeadline(t *testing.T, path string) {
	t.Helper()
	for {
		if _, err := os.Stat(path); err == nil {
			return
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestPeerHelloRehelloReplacementAndSupersession(t *testing.T) {
	daemon, socket := startDaemon(t)
	first := connectPeer(t, socket, "peer-one", "First session", "team")
	first.identity.Name, first.identity.Info = "  renamed  session  ", map[string]any{"revision": float64(2)}
	must(t, first.peer.Rehello(context.Background(), first.identity.Name, first.identity.Info))
	var listed protocol.SessionListResult
	must(t, first.call("session.list", protocol.SessionListRequest{}, &listed))
	if len(listed.Sessions) != 1 || listed.Sessions[0].Name != "  renamed  session  @local" {
		t.Fatalf("same-id re-hello = %#v", listed.Sessions)
	}

	first.identity.SessionID, first.identity.Name, first.identity.Groups = "peer-two", "second", []string{"other"}
	must(t, first.peer.Replace(context.Background(), first.identity))
	select {
	case <-first.superseded:
		t.Fatal("same-connection identity replacement sent superseded")
	default:
	}
	listed = protocol.SessionListResult{}
	must(t, first.call("session.list", protocol.SessionListRequest{}, &listed))
	if len(listed.Sessions) != 1 || listed.Sessions[0].SessionID != "peer-two@local" {
		t.Fatalf("different-id re-hello = %#v", listed.Sessions)
	}

	second := connectPeer(t, socket, "peer-two", "replacement", "other")
	select {
	case <-first.superseded:
	case <-time.After(time.Second):
		t.Fatal("displaced peer was not notified before close")
	}
	first.identity.Name = "stale"
	if first.peer.Rehello(context.Background(), first.identity.Name, first.identity.Info) == nil {
		t.Fatal("displaced pointer re-hello succeeded")
	}
	listed = protocol.SessionListResult{}
	must(t, second.call("session.list", protocol.SessionListRequest{}, &listed))
	if len(listed.Sessions) != 1 || listed.Sessions[0].Name != "replacement@local" {
		t.Fatalf("replacement list = %#v", listed.Sessions)
	}

	bad, err := sessionkit.Dial(socket)
	must(t, err)
	defer bad.Close()
	badIdentity := protocol.PeerHello{Protocol: 1, Product: "fixture-client", SessionID: "bad-groups", Name: "bad", Groups: []string{"one", "two"}, Info: map[string]any{}}
	_, err = bad.Call(context.Background(), "session.hello", badIdentity)
	must(t, err)
	badIdentity.Groups = []string{"two", "one"}
	_, err = bad.Call(context.Background(), "session.hello", badIdentity)
	if code := rpcCode(err); code != protocol.InvalidHello {
		t.Fatalf("changed groups code = %d", code)
	}
	_ = daemon
}

func TestSupersessionDoesNotWaitForNonReadingPeer(t *testing.T) {
	_, socket := startDaemon(t)
	slow, err := net.Dial("unix", socket)
	must(t, err)
	t.Cleanup(func() { _ = slow.Close() })
	hello := protocol.PeerHello{Protocol: 1, Product: "fixture-client", SessionID: "slow", Name: "slow", Groups: []string{"team"}, Info: map[string]any{"blob": strings.Repeat("x", 512<<10)}}
	body, err := protocol.RequestBytes(1, "session.hello", hello)
	must(t, err)
	_, err = slow.Write(body)
	must(t, err)
	reader := bufio.NewReader(slow)
	_, err = reader.ReadBytes('\n')
	must(t, err)
	body, err = protocol.RequestBytes(2, "session.list", protocol.SessionListRequest{})
	must(t, err)
	_, err = slow.Write(body)
	must(t, err)

	started := time.Now()
	replacement := connectPeer(t, socket, "slow", "replacement", "team")
	if time.Since(started) >= supersedeWriteBound {
		t.Fatal("replacement waited for the displaced peer")
	}
	time.Sleep(supersedeWriteBound + 50*time.Millisecond)
	_ = slow.SetReadDeadline(time.Now().Add(time.Second))
	if _, err = io.Copy(io.Discard, reader); err != nil {
		t.Fatalf("displaced peer did not close after the write bound: %v", err)
	}
	var listed protocol.SessionListResult
	must(t, replacement.call("session.list", protocol.SessionListRequest{SessionID: "slow"}, &listed))
	if len(listed.Sessions) != 1 || listed.Sessions[0].Name != "replacement@local" {
		t.Fatalf("current peer = %#v", listed.Sessions)
	}
}

func TestWorkerHelloIsSingleUseAndUncommitted(t *testing.T) {
	directory := t.TempDir()
	installFixture(t, directory, "rehello-worker")
	t.Setenv("PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))
	logPath := filepath.Join(directory, "rehello")
	t.Setenv("REHELLO_LOG", logPath)
	_, socket := startDaemon(t)
	parent := connectPeer(t, socket, "parent", "parent", "shared")
	var spawned protocol.LaneSpawnResult
	spawnErr := parent.call("lane.spawn", protocol.LaneSpawnRequest{Name: "child", Product: "rehello-worker", Open: &protocol.OpenOptions{}}, &spawned)
	if code := rpcCode(spawnErr); code != protocol.SpawnFailed {
		t.Fatalf("re-hello worker spawn code = %d (%v)", code, spawnErr)
	}
	waitFile(t, logPath)
	raw, err := os.ReadFile(logPath)
	must(t, err)
	want := fmt.Sprintf("%d,%d", protocol.NotCommitted, protocol.InvalidHello)
	if string(raw) != want {
		t.Fatalf("worker admission codes = %q, want %q", raw, want)
	}
}

func TestSpawnRunCloseResumeForgetAndRestart(t *testing.T) {
	directory := testsocket.Directory(t)
	installFixture(t, directory, "fixture-worker")
	t.Setenv("PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("OPEN_LOG", filepath.Join(directory, "open.log"))
	tablePath, socket := filepath.Join(directory, "sessions.json"), filepath.Join(directory, "sessionbus.sock")
	d, err := Start(Config{SocketPath: socket, TablePath: tablePath, Products: []string{"fixture-worker"}})
	must(t, err)
	parent := connectPeer(t, socket, "parent", "parent", "team")

	var description protocol.LaneDescribeResult
	must(t, parent.call("lane.describe", protocol.LaneDescribeRequest{Product: "fixture-worker"}, &description))
	if description.Product != "fixture-worker" || len(description.SupportedOpenFields) != 5 {
		t.Fatalf("description = %#v", description)
	}
	if code := rpcCode(parent.call("lane.describe", protocol.LaneDescribeRequest{Product: "fixture-worker", Host: "remote"}, &description)); code != protocol.UnknownHost {
		t.Fatalf("remote describe code = %d", code)
	}
	if code := rpcCode(parent.call("lane.describe", protocol.LaneDescribeRequest{Product: "missing-worker"}, &description)); code != protocol.UnknownProduct {
		t.Fatalf("missing product code = %d", code)
	}
	installFixture(t, directory, "exit-worker")
	if code := rpcCode(parent.call("lane.describe", protocol.LaneDescribeRequest{Product: "exit-worker"}, &description)); code != protocol.SpawnFailed {
		t.Fatalf("exiting product code = %d", code)
	}

	var spawned protocol.LaneSpawnResult
	open := protocol.OpenOptions{Cwd: "/work", Arguments: []string{"--flag"}}
	must(t, parent.call("lane.spawn", protocol.LaneSpawnRequest{Name: "child", Product: "fixture-worker", ExtraGroups: []string{"shared"}, Open: &open}, &spawned))
	if !strings.HasSuffix(spawned.SessionID, "@local") {
		t.Fatalf("spawn id = %q", spawned.SessionID)
	}
	var listed protocol.SessionListResult
	must(t, parent.call("session.list", protocol.SessionListRequest{SessionID: spawned.SessionID}, &listed))
	if len(listed.Sessions) != 1 || listed.Sessions[0].Name != "parent/child@local" || !listed.Sessions[0].Connected {
		t.Fatalf("spawned summary = %#v", listed.Sessions)
	}
	wantGroups := []string{"session:parent@local", "session:parent@local/child", "shared"}
	if !slices.Equal(listed.Sessions[0].Groups, wantGroups) {
		t.Fatalf("groups = %#v, want %#v", listed.Sessions[0].Groups, wantGroups)
	}

	var turn protocol.RunStatus
	must(t, parent.call("turn.run", protocol.TurnRunRequest{SessionID: spawned.SessionID, Input: "echo"}, &turn))
	if turn.Result == nil || turn.Result.Outcome != "completed" || turn.Result.Result != "echo" {
		t.Fatalf("turn = %#v", turn)
	}
	if code := rpcCode(parent.call("turn.interrupt", protocol.SessionTarget{SessionID: spawned.SessionID}, &struct{}{})); code != protocol.NotRunning {
		t.Fatalf("idle interrupt code = %d", code)
	}
	var equalName protocol.LaneSpawnResult
	must(t, parent.call("lane.spawn", protocol.LaneSpawnRequest{Name: "child", Product: "fixture-worker", Open: &protocol.OpenOptions{}}, &equalName))
	if equalName.SessionID == spawned.SessionID {
		t.Fatal("equal names reused an ID")
	}
	must(t, parent.call("session.close", protocol.SessionCloseRequest{SessionID: equalName.SessionID, Forget: true}, &struct{}{}))
	blocked := make(chan error, 1)
	go func() {
		blocked <- parent.call("turn.run", protocol.TurnRunRequest{SessionID: spawned.SessionID, Input: "block"}, &turn)
	}()
	waitRunning(t, parent, spawned.SessionID)
	if code := rpcCode(parent.call("turn.run", protocol.TurnRunRequest{SessionID: spawned.SessionID, Input: "second"}, &turn)); code != protocol.Busy {
		t.Fatalf("concurrent run code = %d", code)
	}
	must(t, parent.call("turn.interrupt", protocol.SessionTarget{SessionID: spawned.SessionID}, &struct{}{}))
	must(t, <-blocked)

	closed := make(chan error, 2)
	for range 2 {
		go func() {
			closed <- parent.call("session.close", protocol.SessionCloseRequest{SessionID: spawned.SessionID}, &struct{}{})
		}()
	}
	codes := []int{rpcCode(<-closed), rpcCode(<-closed)}
	if !(codes[0] == 0 && codes[1] == protocol.Busy || codes[1] == 0 && codes[0] == protocol.Busy) {
		t.Fatalf("concurrent close codes = %#v", codes)
	}
	listed = protocol.SessionListResult{}
	must(t, parent.call("session.list", protocol.SessionListRequest{SessionID: spawned.SessionID}, &listed))
	if len(listed.Sessions) != 1 || listed.Sessions[0].Connected {
		t.Fatalf("closed summary = %#v", listed.Sessions)
	}
	must(t, parent.call("lane.spawn", protocol.LaneSpawnRequest{ResumeSessionID: spawned.SessionID}, &spawned))
	rawOpen, err := os.ReadFile(filepath.Join(directory, "open.log"))
	must(t, err)
	lines := strings.Split(strings.TrimSpace(string(rawOpen)), "\n")
	if len(lines) != 3 || lines[0] != lines[2] || lines[1] != "{}" || !strings.Contains(lines[0], `"arguments":["--flag"]`) {
		t.Fatalf("resume open values = %q", lines)
	}
	must(t, parent.call("session.close", protocol.SessionCloseRequest{SessionID: spawned.SessionID, Forget: true}, &struct{}{}))
	if code := rpcCode(parent.call("session.list", protocol.SessionListRequest{SessionID: spawned.SessionID}, &listed)); code != protocol.UnknownSession {
		t.Fatalf("forgotten list code = %d", code)
	}

	// Recreate one row, then prove restart loads it offline without a recovery spawn.
	must(t, parent.call("lane.spawn", protocol.LaneSpawnRequest{Name: "restart", Product: "fixture-worker", Open: &protocol.OpenOptions{}}, &spawned))
	must(t, parent.call("session.close", protocol.SessionCloseRequest{SessionID: spawned.SessionID}, &struct{}{}))
	must(t, d.Close())
	d, err = Start(Config{SocketPath: socket, TablePath: tablePath})
	must(t, err)
	t.Cleanup(func() { _ = d.Close() })
	parent = connectPeer(t, socket, "parent", "parent", "team")
	listed = protocol.SessionListResult{}
	must(t, parent.call("session.list", protocol.SessionListRequest{SessionID: spawned.SessionID}, &listed))
	if len(listed.Sessions) != 1 || listed.Sessions[0].Connected || listed.Sessions[0].Running {
		t.Fatalf("restart summary = %#v", listed.Sessions)
	}
}

func TestOfflineLaneCloseAndForget(t *testing.T) {
	directory := testsocket.Directory(t)
	installFixture(t, directory, "fixture-worker")
	t.Setenv("PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))
	tablePath, socket := filepath.Join(directory, "sessions.json"), filepath.Join(directory, "sessionbus.sock")
	first, err := Start(Config{SocketPath: socket, TablePath: tablePath, Products: []string{"fixture-worker"}})
	must(t, err)
	t.Cleanup(func() { _ = first.Close() })
	owner := connectPeer(t, socket, "owner", "owner", "team")
	var lane protocol.LaneSpawnResult
	must(t, owner.call("lane.spawn", protocol.LaneSpawnRequest{Name: "child", Product: "fixture-worker", Open: &protocol.OpenOptions{}}, &lane))
	must(t, owner.call("session.close", protocol.SessionCloseRequest{SessionID: lane.SessionID}, &struct{}{}))

	historyPath := filepath.Join(directory, "native-history.jsonl")
	history := []byte("native history remains product-owned\n")
	must(t, os.WriteFile(historyPath, history, 0o600))
	if code := rpcCode(owner.call("session.close", protocol.SessionCloseRequest{SessionID: lane.SessionID}, &struct{}{})); code != protocol.UnknownSession {
		t.Fatalf("archived plain close = %d", code)
	}
	assertOfflineLane(t, owner, lane.SessionID)
	must(t, first.Close())

	second, err := Start(Config{SocketPath: socket, TablePath: tablePath, Products: []string{"fixture-worker"}})
	must(t, err)
	t.Cleanup(func() { _ = second.Close() })
	owner = connectPeer(t, socket, "owner", "owner", "team")
	assertOfflineLane(t, owner, lane.SessionID)
	hidden := connectPeer(t, socket, "hidden", "hidden", "other")
	if code := rpcCode(hidden.call("session.close", protocol.SessionCloseRequest{SessionID: lane.SessionID, Forget: true}, &struct{}{})); code != protocol.UnknownSession {
		t.Fatalf("invisible offline forget code = %d", code)
	}

	second.directory.mu.Lock()
	item := second.directory.entries[lane.SessionID]
	item.claimed = true
	second.directory.mu.Unlock()
	if code := rpcCode(owner.call("session.close", protocol.SessionCloseRequest{SessionID: lane.SessionID}, &struct{}{})); code != protocol.UnknownSession {
		t.Fatalf("claimed archived plain close code = %d", code)
	}
	if code := rpcCode(owner.call("session.close", protocol.SessionCloseRequest{SessionID: lane.SessionID, Forget: true}, &struct{}{})); code != protocol.Busy {
		t.Fatalf("claimed archived forget code = %d", code)
	}
	second.directory.mu.Lock()
	item.claimed = false
	second.directory.mu.Unlock()

	must(t, os.Chmod(tablePath, 0o500))
	forgetErr := owner.call("session.close", protocol.SessionCloseRequest{SessionID: lane.SessionID, Forget: true}, &struct{}{})
	must(t, os.Chmod(tablePath, 0o700))
	if code := rpcCode(forgetErr); code != protocol.Internal {
		t.Fatalf("failed durable delete code = %d (%v)", code, forgetErr)
	}
	var rpcErr *protocol.RPCError
	if !errors.As(forgetErr, &rpcErr) || len(rpcErr.Data) == 0 {
		t.Fatalf("failed durable delete lacks error data: %v", forgetErr)
	}
	assertOfflineLane(t, owner, lane.SessionID)

	must(t, owner.call("session.close", protocol.SessionCloseRequest{SessionID: lane.SessionID, Forget: true}, &struct{}{}))
	var listed protocol.SessionListResult
	if code := rpcCode(owner.call("session.list", protocol.SessionListRequest{SessionID: lane.SessionID}, &listed)); code != protocol.UnknownSession {
		t.Fatalf("forgotten offline row code = %d", code)
	}
	gotHistory, err := os.ReadFile(historyPath)
	must(t, err)
	if !bytes.Equal(gotHistory, history) {
		t.Fatalf("native history changed: %q", gotHistory)
	}
	if _, err = os.Stat(filepath.Join(tablePath, rowFile(lane.SessionID))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("forgotten durable row still exists: %v", err)
	}
}

func assertOfflineLane(t *testing.T, caller *peerClient, id string) {
	t.Helper()
	var listed protocol.SessionListResult
	must(t, caller.call("session.list", protocol.SessionListRequest{SessionID: id}, &listed))
	if len(listed.Sessions) != 1 || listed.Sessions[0].Connected || listed.Sessions[0].Running {
		t.Fatalf("offline lane = %#v", listed.Sessions)
	}
}

func TestVisibilityDeliveryAndLaneIdentityCollision(t *testing.T) {
	directory := t.TempDir()
	installFixture(t, directory, "fixed-worker")
	t.Setenv("PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))
	_, socket := startDaemon(t)
	first := connectPeer(t, socket, "first", "first", "shared")
	second := connectPeer(t, socket, "second", "second", "shared")
	hidden := connectPeer(t, socket, "hidden", "hidden", "private")

	var sent protocol.MessageSendResult
	must(t, first.call("message.send", protocol.MessageSendRequest{Target: "second", Message: "hello"}, &sent))
	if len(sent.Deliveries) != 1 || sent.Deliveries[0].Disposition != "injected" {
		t.Fatalf("delivery result = %#v", sent)
	}
	select {
	case delivered := <-second.deliveries:
		if delivered.Body != "hello" || delivered.From.SessionID != "first@local" {
			t.Fatalf("delivery = %#v", delivered)
		}
	case <-time.After(time.Second):
		t.Fatal("delivery not received")
	}
	sent = protocol.MessageSendResult{}
	must(t, first.call("message.send", protocol.MessageSendRequest{Target: "hidden", Message: "no"}, &sent))
	if len(sent.Deliveries) != 1 || sent.Deliveries[0].Disposition != "rejected" || sent.Deliveries[0].Reason != "unknown_session" {
		t.Fatalf("invisible target receipt = %#v", sent.Deliveries)
	}

	var lane protocol.LaneSpawnResult
	must(t, first.call("lane.spawn", protocol.LaneSpawnRequest{Name: "lane", Product: "fixed-worker", ExtraGroups: []string{"shared"}, Open: &protocol.OpenOptions{}}, &lane))
	must(t, first.call("session.close", protocol.SessionCloseRequest{SessionID: lane.SessionID}, &struct{}{}))
	collision, err := sessionkit.Dial(socket)
	must(t, err)
	hello := protocol.PeerHello{Protocol: 1, Product: "fixture-client", SessionID: "shared-id", Name: "collision", Groups: []string{"shared"}, Info: map[string]any{}}
	_, err = collision.Call(context.Background(), "session.hello", hello)
	if code := rpcCode(err); code != protocol.InvalidHello {
		t.Fatalf("lane collision code = %d", code)
	}
	_ = collision.Close()
	_ = hidden
}

func TestPeerAndFreshLaneCannotClaimOneID(t *testing.T) {
	directory := t.TempDir()
	installFixture(t, directory, "racing-worker")
	t.Setenv("PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))
	ready := filepath.Join(directory, "ready")
	release := filepath.Join(directory, "release")
	t.Setenv("RACE_READY", ready)
	t.Setenv("RACE_RELEASE", release)
	_, socket := startDaemon(t)
	parent := connectPeer(t, socket, "parent", "parent", "shared")
	spawnResult := make(chan error, 1)
	go func() {
		spawnResult <- parent.call("lane.spawn", protocol.LaneSpawnRequest{Name: "child", Product: "racing-worker", Open: &protocol.OpenOptions{}}, &protocol.LaneSpawnResult{})
	}()
	waitFile(t, ready)
	peer, err := sessionkit.Dial(socket)
	must(t, err)
	t.Cleanup(func() { _ = peer.Close() })
	helloResult := make(chan error, 1)
	start := make(chan struct{})
	go func() {
		<-start
		hello := protocol.PeerHello{Protocol: 1, Product: "fixture-client", SessionID: "shared-id", Name: "holder", Groups: []string{"shared"}, Info: map[string]any{}}
		_, callErr := peer.Call(context.Background(), "session.hello", hello)
		helloResult <- callErr
	}()
	go func() {
		<-start
		_ = os.WriteFile(release, []byte("release"), 0o600)
	}()
	close(start)
	peerCode, spawnCode := rpcCode(<-helloResult), rpcCode(<-spawnResult)
	if !(peerCode == 0 && spawnCode == protocol.SpawnFailed || peerCode == protocol.InvalidHello && spawnCode == 0) {
		t.Fatalf("identity race: peer=%d spawn=%d", peerCode, spawnCode)
	}
}

func TestOpenCommitPrecedesNextWorkerFrame(t *testing.T) {
	directory := t.TempDir()
	installFixture(t, directory, "ordered-worker")
	t.Setenv("PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))
	d, socket := startDaemon(t)
	parent := connectPeer(t, socket, "parent", "parent", "team")
	var spawned protocol.LaneSpawnResult
	must(t, parent.call("lane.spawn", protocol.LaneSpawnRequest{Name: "ordered", Product: "ordered-worker", Open: &protocol.OpenOptions{}}, &spawned))
	select {
	case delivered := <-parent.deliveries:
		if delivered.Body != "after-commit" {
			t.Fatalf("delivery = %#v", delivered)
		}
	case <-time.After(time.Second):
		t.Fatal("worker's post-open request was not dispatched after commit")
	}
	_ = d
}

func TestSpawnDecisionAndExitDuringOpen(t *testing.T) {
	directory := t.TempDir()
	d := &Daemon{host: "local", table: &table{path: filepath.Join(directory, "sessions")}, shutdown: make(chan struct{})}
	d.directory = newDirectory(d, nil)
	start := newLaunch("worker", false, true)
	start.parent = &ownership{id: "parent@local", token: "fixture"}
	item, code := d.directory.reserveFresh(row{Product: "worker", Name: "parent/child@local", Groups: []string{"parent", "child"}}, start)
	if code != 0 {
		t.Fatal(code)
	}
	start.owner = newSession(d)
	s := start.owner
	s.launch, s.identity, s.openID, s.processExited = start, item, 1, true
	local, peer := net.Pipe()
	s.wire = conn.Start(local, s.inbox, &d.group)
	finished := make(chan struct{})
	go func() {
		s.run()
		close(finished)
	}()
	result, err := json.Marshal(protocol.OpenResult{SessionID: "winner"})
	must(t, err)
	start.timer.Stop()
	start.timer.Reset(time.Nanosecond)
	s.inbox <- conn.Frame{Value: protocol.Frame{ID: 1, Result: result}}
	select {
	case outcome := <-start.reply:
		if outcome.code != 0 && outcome.code != protocol.Timeout {
			t.Fatalf("outcome = %#v", outcome)
		}
	case <-time.After(time.Second):
		t.Fatal("open and timeout produced no winner")
	}
	s.wire.Close()
	_ = peer.Close()
	<-finished
	d.group.Done()
	d.group.Wait()
	select {
	case duplicate := <-start.reply:
		t.Fatalf("second transaction outcome = %#v", duplicate)
	default:
	}

	installFixture(t, directory, "open-exit-worker")
	t.Setenv("PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))
	_, socket := startDaemon(t)
	parent := connectPeer(t, socket, "parent", "parent", "team")
	var spawned protocol.LaneSpawnResult
	if code := rpcCode(parent.call("lane.spawn", protocol.LaneSpawnRequest{Name: "exit", Product: "open-exit-worker", Open: &protocol.OpenOptions{}}, &spawned)); code != protocol.SpawnFailed {
		t.Fatalf("exit during open code = %d", code)
	}
}

func TestOpenFailurePreservesSpawnDataAndInvalidProductDoesNotExec(t *testing.T) {
	directory := t.TempDir()
	installFixture(t, directory, "error-worker")
	t.Setenv("PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))
	_, socket := startDaemon(t)
	parent := connectPeer(t, socket, "parent", "parent", "team")
	request := protocol.LaneSpawnRequest{Name: "error", Product: "error-worker", Open: &protocol.OpenOptions{}}
	var spawned protocol.LaneSpawnResult
	err := parent.call("lane.spawn", request, &spawned)
	var rpcError *protocol.RPCError
	if !errors.As(err, &rpcError) || rpcError.Code != protocol.SpawnFailed {
		t.Fatalf("open error = %v", err)
	}
	var data protocol.SpawnFailedData
	must(t, json.Unmarshal(rpcError.Data, &data))
	if !slices.Contains(data.StderrTail, "open rejected") {
		t.Fatalf("spawn data = %#v", data)
	}
	request.Name, request.Product = "invalid", "../worker"
	if code := rpcCode(parent.call("lane.spawn", request, &spawned)); code != protocol.UnknownProduct {
		t.Fatalf("invalid product code = %d", code)
	}
	longName := connectPeer(t, socket, "long-parent", strings.Repeat("p", 128), "team")
	request.Name, request.Product = "leaf", "error-worker"
	if code := rpcCode(longName.call("lane.spawn", request, &spawned)); code != protocol.InvalidFrame {
		t.Fatalf("overlong composed name code = %d", code)
	}
}

func TestCallerEOFKeepsRunUntilWorkerTerminalOrClose(t *testing.T) {
	directory := t.TempDir()
	installFixture(t, directory, "fixture-worker")
	t.Setenv("PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))
	_, socket := startDaemon(t)
	owner := connectPeer(t, socket, "owner", "owner", "shared")
	other := connectPeer(t, socket, "other", "other", "shared")
	persistent := true
	var spawned protocol.LaneSpawnResult
	must(t, owner.call("lane.spawn", protocol.LaneSpawnRequest{Persistent: &persistent, Name: "child", Product: "fixture-worker", ExtraGroups: []string{"shared"}, Open: &protocol.OpenOptions{}}, &spawned))
	result := protocol.RunStatus{}
	run := make(chan error, 1)
	go func() {
		run <- owner.call("turn.run", protocol.TurnRunRequest{SessionID: spawned.SessionID, Input: "block"}, &result)
	}()
	waitRunning(t, other, spawned.SessionID)
	owner.peer.Shutdown()
	if <-run == nil {
		t.Fatal("abandoned caller unexpectedly received the result")
	}
	var listed protocol.SessionListResult
	must(t, other.call("session.list", protocol.SessionListRequest{SessionID: spawned.SessionID}, &listed))
	if len(listed.Sessions) != 1 || !listed.Sessions[0].Running {
		t.Fatalf("abandoned run disappeared: %#v", listed.Sessions)
	}
	must(t, other.call("turn.interrupt", protocol.SessionTarget{SessionID: spawned.SessionID}, &struct{}{}))
	waitNotRunning(t, other, spawned.SessionID)

	owner = connectPeer(t, socket, "owner-two", "owner-two", "shared")
	go func() {
		run <- owner.call("turn.run", protocol.TurnRunRequest{SessionID: spawned.SessionID, Input: "block"}, &result)
	}()
	waitRunning(t, other, spawned.SessionID)
	owner.peer.Shutdown()
	<-run
	must(t, other.call("session.close", protocol.SessionCloseRequest{SessionID: spawned.SessionID}, &struct{}{}))
	must(t, other.call("session.list", protocol.SessionListRequest{SessionID: spawned.SessionID}, &listed))
	if listed.Sessions[0].Connected || listed.Sessions[0].Running {
		t.Fatalf("closed lane stayed live: %#v", listed.Sessions[0])
	}
}

func TestCloseStopsWorkerBeforeWritingToSlowCaller(t *testing.T) {
	directory := t.TempDir()
	installFixture(t, directory, "fixture-worker")
	t.Setenv("PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))
	_, socket := startDaemon(t)
	owner := connectPeer(t, socket, "owner", "owner", "shared")
	var spawned protocol.LaneSpawnResult
	must(t, owner.call("lane.spawn", protocol.LaneSpawnRequest{Name: "child", Product: "fixture-worker", ExtraGroups: []string{"shared"}, Open: &protocol.OpenOptions{}}, &spawned))
	slow, err := net.Dial("unix", socket)
	must(t, err)
	t.Cleanup(func() { _ = slow.Close() })
	hello, _ := protocol.RequestBytes(1, "session.hello", protocol.PeerHello{Protocol: 1, Product: "fixture-client", SessionID: "slow", Name: "slow", Groups: []string{"shared"}, Info: map[string]any{}})
	_, err = slow.Write(hello)
	must(t, err)
	reader := bufio.NewReader(slow)
	_, err = reader.ReadBytes('\n')
	must(t, err)
	closeRequest, _ := protocol.RequestBytes(2, "session.close", protocol.SessionCloseRequest{SessionID: spawned.SessionID})
	_, err = slow.Write(closeRequest)
	must(t, err)
	deadline := time.Now().Add(time.Second)
	for {
		var listed protocol.SessionListResult
		if owner.call("session.list", protocol.SessionListRequest{SessionID: spawned.SessionID}, &listed) == nil && len(listed.Sessions) == 1 && !listed.Sessions[0].Connected {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("worker cleanup waited for the caller to read its reply")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestWorkerCallsLeaveInAdmissionOrder(t *testing.T) {
	directory := t.TempDir()
	installFixture(t, directory, "sequence-worker")
	t.Setenv("PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))
	sequence := filepath.Join(directory, "sequence")
	t.Setenv("SEQUENCE_FILE", sequence)
	_, socket := startDaemon(t)
	parent := connectPeer(t, socket, "parent", "parent", "shared")
	var spawned protocol.LaneSpawnResult
	must(t, parent.call("lane.spawn", protocol.LaneSpawnRequest{Name: "child", Product: "sequence-worker", Open: &protocol.OpenOptions{}}, &spawned))
	var result protocol.RunStatus
	run := make(chan error, 1)
	go func() {
		run <- parent.call("turn.run", protocol.TurnRunRequest{SessionID: spawned.SessionID, Input: "block"}, &result)
	}()
	waitRunning(t, parent, spawned.SessionID)
	must(t, parent.call("turn.interrupt", protocol.SessionTarget{SessionID: spawned.SessionID}, &struct{}{}))
	must(t, <-run)
	raw, err := os.ReadFile(sequence)
	must(t, err)
	if string(raw) != "turn.run\nturn.interrupt\n" {
		t.Fatalf("worker order = %q", raw)
	}
	must(t, parent.call("session.close", protocol.SessionCloseRequest{SessionID: spawned.SessionID}, &struct{}{}))
}

func TestMessageSendReturnsOneOrderedReceiptPerResolvedLabel(t *testing.T) {
	_, socket := startDaemon(t)
	sender := connectPeer(t, socket, "sender", "sender", "shared")
	one := connectPeer(t, socket, "one", "one session", "shared")
	two := connectPeer(t, socket, "two", "two", "shared")
	_ = connectPeer(t, socket, "ambiguous-a", "same", "shared")
	_ = connectPeer(t, socket, "ambiguous-b", "same", "shared")
	var sent protocol.MessageSendResult
	if code := rpcCode(sender.call("message.send", protocol.MessageSendRequest{Targets: []string{"one", "bad\u0001name"}, Message: "invalid"}, &sent)); code != protocol.InvalidFrame {
		t.Fatalf("invalid target grammar code = %d", code)
	}
	select {
	case delivery := <-one.deliveries:
		t.Fatalf("delivery preceded target validation: %#v", delivery)
	default:
	}
	sender = connectPeer(t, socket, "sender-two", "sender-two", "shared")
	sent = protocol.MessageSendResult{}
	must(t, sender.call("message.send", protocol.MessageSendRequest{Targets: []string{"one session", "missing@remote", "same", "two"}, Message: "fanout"}, &sent))
	if len(sent.Deliveries) != 4 || sent.Deliveries[0].Disposition != "injected" || sent.Deliveries[1].Reason != "unknown_host" || sent.Deliveries[2].Reason != "ambiguous" || sent.Deliveries[3].Disposition != "injected" {
		t.Fatalf("mixed receipts = %#v", sent.Deliveries)
	}
	for _, peer := range []*peerClient{one, two} {
		select {
		case delivery := <-peer.deliveries:
			if delivery.Body != "fanout" {
				t.Fatalf("delivery = %#v", delivery)
			}
		case <-time.After(time.Second):
			t.Fatal("resolvable target did not receive delivery")
		}
	}
}

func startDaemon(t *testing.T) (*Daemon, string) {
	t.Helper()
	directory := testsocket.Directory(t)
	socket := filepath.Join(directory, "sessionbus.sock")
	d, err := Start(Config{SocketPath: socket, TablePath: filepath.Join(directory, "sessions.json")})
	must(t, err)
	t.Cleanup(func() { _ = d.Close() })
	return d, socket
}

func installFixture(t *testing.T, directory, name string) {
	t.Helper()
	executable, err := os.Executable()
	must(t, err)
	must(t, os.Symlink(executable, filepath.Join(directory, name)))
}

func rpcCode(err error) int {
	var rpcError *protocol.RPCError
	if errors.As(err, &rpcError) {
		return rpcError.Code
	}
	return 0
}

func waitRunning(t *testing.T, peer *peerClient, id string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		var listed protocol.SessionListResult
		if peer.call("session.list", protocol.SessionListRequest{SessionID: id}, &listed) == nil && len(listed.Sessions) == 1 && listed.Sessions[0].Running {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("turn never became running")
}

func waitNotRunning(t *testing.T, peer *peerClient, id string) {
	t.Helper()
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); {
		var listed protocol.SessionListResult
		if peer.call("session.list", protocol.SessionListRequest{SessionID: id}, &listed) == nil && len(listed.Sessions) == 1 && !listed.Sessions[0].Running {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("turn stayed running")
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func waitFile(t *testing.T, path string) {
	t.Helper()
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("file %s was not created", path)
}

func TestUnnamedPeerHelloDeliveryAndTitleAssertions(t *testing.T) {
	d, socket := startDaemon(t)
	unnamed := connectPeer(t, socket, "native-id", "", "team")
	named := connectPeer(t, socket, "named-id", "named", "team")
	for _, title := range []string{"", "first title", ""} {
		must(t, unnamed.peer.Rehello(context.Background(), title, map[string]any{}))
		var listed map[string]any
		must(t, named.call("session.list", protocol.SessionListRequest{SessionID: "native-id@local"}, &listed))
		row := listed["sessions"].([]any)[0].(map[string]any)
		want := qualify(title, "local")
		if title == "" {
			if _, ok := row["name"]; ok {
				t.Fatalf("absent name serialized: %#v", row)
			}
		} else if row["name"] != want {
			t.Fatalf("title = %#v", row)
		}
		for _, input := range []protocol.MessageSendRequest{{Target: "native-id", Message: "direct"}, {Group: "team", Message: "group"}} {
			var sent protocol.MessageSendResult
			must(t, named.call("message.send", input, &sent))
			if len(sent.Deliveries) != 1 || sent.Deliveries[0].SessionID != "native-id@local" || sent.Deliveries[0].Disposition != "injected" {
				t.Fatalf("unnamed target: %#v", sent)
			}
			<-unnamed.deliveries
		}
		var sent protocol.MessageSendResult
		must(t, unnamed.call("message.send", protocol.MessageSendRequest{Target: "named", Message: "source"}, &sent))
		source := (<-named.deliveries).From
		raw, err := json.Marshal(source)
		must(t, err)
		if source.Name != want || title == "" && strings.Contains(string(raw), `"name"`) {
			t.Fatalf("source = %s", raw)
		}
	}
	d.directory.mu.Lock()
	found := matchingEntries(d.directory.connected, "", []string{"team"})
	d.directory.mu.Unlock()
	if len(found) != 0 {
		t.Fatal("unnamed peer matched empty name")
	}
	var sent protocol.MessageSendResult
	must(t, named.call("message.send", protocol.MessageSendRequest{Target: "first title", Message: "removed"}, &sent))
	if sent.Deliveries[0].Reason != "unknown_session" {
		t.Fatalf("removed name matched: %#v", sent)
	}
}

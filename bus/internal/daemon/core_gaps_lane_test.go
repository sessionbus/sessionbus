// SPDX-License-Identifier: GPL-3.0-only

package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	sessionkit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
	"github.com/antst/sessionbus/bus/sdk/go/testsocket"
)

// Extra fixture workers for the re-executed test binary. They are dispatched
// by binary name exactly like TestMain's fixtures, before TestMain runs.
func init() {
	name := filepath.Base(os.Args[0])
	switch {
	case strings.HasPrefix(name, "lanegap-interrupt-worker"):
		worker := sessionkit.NewWorker(&laneGapInterruptProduct{fixtureProduct: fixtureProduct{product: name}})
		_ = worker.Serve(context.Background())
		os.Exit(0)
	case strings.HasPrefix(name, "lanegap-deaf-worker"):
		signal.Ignore(syscall.SIGTERM)
		_ = os.WriteFile(os.Getenv("LANEGAP_PID_FILE"), []byte(strconv.Itoa(os.Getpid())), 0o600)
		worker := sessionkit.NewWorker(&laneGapDeafProduct{fixtureProduct: fixtureProduct{product: name}})
		_ = worker.Serve(context.Background())
		for {
			time.Sleep(time.Hour)
		}
	}
}

// laneGapInterruptProduct holds its native turn until the test releases it and
// counts native interrupt invocations in LANEGAP_INTERRUPT_LOG.
type laneGapInterruptProduct struct{ fixtureProduct }

func (*laneGapInterruptProduct) Run(ctx context.Context, _ *sessionkit.Run, _ sessionkit.RunInput) (sessionkit.TurnResult, error) {
	for {
		if _, err := os.Stat(os.Getenv("LANEGAP_RELEASE")); err == nil {
			return sessionkit.TurnResult{Outcome: "interrupted", Result: "released"}, nil
		}
		select {
		case <-ctx.Done():
			return sessionkit.TurnResult{}, ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
}

func (*laneGapInterruptProduct) Interrupt(context.Context, *sessionkit.Run) error {
	file, err := os.OpenFile(os.Getenv("LANEGAP_INTERRUPT_LOG"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	_, err = file.WriteString("interrupt\n")
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	return err
}

// laneGapDeafProduct never finishes native close, so the kit never answers
// session.close. With SIGTERM ignored, only SIGKILL ends the process.
type laneGapDeafProduct struct{ fixtureProduct }

func (*laneGapDeafProduct) Close(context.Context, sessionkit.SessionCloseRequest) error {
	_ = os.WriteFile(os.Getenv("LANEGAP_CLOSE_SEEN"), []byte("close"), 0o600)
	for {
		time.Sleep(time.Hour)
	}
}

// laneGapTimers records each auto-close deadline armed through the daemon's
// policy timer hook. A deadline fires only when the test fires it.
type laneGapTimers struct {
	mu      sync.Mutex
	delays  []time.Duration
	fires   []chan time.Time
	stopped []bool
}

func (c *laneGapTimers) timer(delay time.Duration) (<-chan time.Time, func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	index := len(c.delays)
	fire := make(chan time.Time, 1)
	c.delays, c.fires, c.stopped = append(c.delays, delay), append(c.fires, fire), append(c.stopped, false)
	return fire, func() {
		c.mu.Lock()
		c.stopped[index] = true
		c.mu.Unlock()
	}
}

func (c *laneGapTimers) snapshot() ([]time.Duration, []bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.delays), slices.Clone(c.stopped)
}

func (c *laneGapTimers) fire(index int) {
	c.mu.Lock()
	fire := c.fires[index]
	c.mu.Unlock()
	fire <- time.Time{}
}

func laneGapInstall(t *testing.T, product string) string {
	t.Helper()
	directory := t.TempDir()
	installFixture(t, directory, product)
	t.Setenv("PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))
	return directory
}

func laneGapSpawn(t *testing.T, owner *peerClient, name, product string) protocol.LaneSpawnResult {
	t.Helper()
	zero, no := int64(0), false
	var lane protocol.LaneSpawnResult
	must(t, owner.call("lane.spawn", protocol.LaneSpawnRequest{Name: name, Product: product, Open: &protocol.OpenOptions{}, AutoCloseMS: &zero, Notify: &no}, &lane))
	return lane
}

func laneGapSpawnFailure(t *testing.T, err error) protocol.SpawnFailedData {
	t.Helper()
	var rpcError *protocol.RPCError
	if !errors.As(err, &rpcError) || rpcError.Code != protocol.SpawnFailed {
		t.Fatalf("error = %v, want spawn_failed", err)
	}
	var data protocol.SpawnFailedData
	must(t, json.Unmarshal(rpcError.Data, &data))
	return data
}

func laneGapAckEmpty(t *testing.T, owner *peerClient, ref protocol.RunRef) {
	t.Helper()
	raw, err := owner.peer.Call(context.Background(), "turn.ack", ref)
	must(t, err)
	var result map[string]any
	if json.Unmarshal(raw, &result) != nil || result == nil || len(result) != 0 {
		t.Fatalf("ack %s = %s, want {}", ref.RunID, raw)
	}
}

func laneGapOldest(t *testing.T, owner *peerClient, id string) string {
	t.Helper()
	var status protocol.RunStatus
	must(t, owner.call("turn.status", protocol.ReadRequest{SessionID: id}, &status))
	return status.RunID
}

func laneGapLines(t *testing.T, path string) int {
	t.Helper()
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0
	}
	must(t, err)
	return strings.Count(string(raw), "\n")
}

// CG1: PROTOCOL.md 638-640 and 713-716.
func TestLaneGapExitBeforeHelloReportsExitCodeAndStderr(t *testing.T) {
	laneGapInstall(t, "exit-worker")
	_, socket := startDaemon(t)
	parent := connectPeer(t, socket, "parent", "parent", "team")
	spawn := protocol.LaneSpawnRequest{Name: "child", Product: "exit-worker", Open: &protocol.OpenOptions{}}
	describe := protocol.LaneDescribeRequest{Product: "exit-worker"}
	// The repeated spawn also proves the failed spawn released its composed name.
	for _, call := range []struct {
		method string
		params any
	}{{"lane.spawn", spawn}, {"lane.describe", describe}, {"lane.spawn", spawn}} {
		data := laneGapSpawnFailure(t, parent.call(call.method, call.params, nil))
		if data.ExitCode == nil || *data.ExitCode != 7 || !slices.Contains(data.StderrTail, "fixture failed before hello") {
			t.Fatalf("%s failure data = %+v", call.method, data)
		}
	}
	if code := rpcCode(parent.call("session.list", protocol.SessionListRequest{SessionID: "parent/child"}, &protocol.SessionListResult{})); code != protocol.UnknownSession {
		t.Fatalf("failed spawn published a row: code %d", code)
	}
}

// CG2: PROTOCOL.md 328-330.
func TestLaneGapFreshOpenReturningExistingLaneIDFails(t *testing.T) {
	laneGapInstall(t, "fixed-worker")
	d, socket := startDaemon(t)
	parent := connectPeer(t, socket, "parent", "parent", "team")
	existing := laneGapSpawn(t, parent, "one", "fixed-worker")
	if existing.SessionID != "shared-id@local" {
		t.Fatalf("fixed worker id = %q", existing.SessionID)
	}
	for _, connected := range []bool{true, false} {
		duplicate := "live-duplicate"
		if !connected {
			must(t, parent.call("session.close", protocol.SessionCloseRequest{SessionID: existing.SessionID}, &struct{}{}))
			duplicate = "offline-duplicate"
		}
		zero := int64(0)
		err := parent.call("lane.spawn", protocol.LaneSpawnRequest{Name: duplicate, Product: "fixed-worker", Open: &protocol.OpenOptions{}, AutoCloseMS: &zero}, &protocol.LaneSpawnResult{})
		if data := laneGapSpawnFailure(t, err); !slices.Contains(data.StderrTail, "session id already exists") {
			t.Fatalf("connected=%t failure data = %+v", connected, data)
		}
		var listed protocol.SessionListResult
		must(t, parent.call("session.list", protocol.SessionListRequest{SessionID: existing.SessionID}, &listed))
		if len(listed.Sessions) != 1 || listed.Sessions[0].Name != "parent/one@local" || listed.Sessions[0].Connected != connected {
			t.Fatalf("connected=%t existing row = %+v", connected, listed.Sessions)
		}
		if code := rpcCode(parent.call("session.list", protocol.SessionListRequest{SessionID: "parent/" + duplicate}, &listed)); code != protocol.UnknownSession {
			t.Fatalf("duplicate %s was published: code %d", duplicate, code)
		}
		_, rows, err := openTable(d.config.TablePath)
		must(t, err)
		if len(rows) != 1 || rows[0].Name != "parent/one@local" {
			t.Fatalf("connected=%t durable rows = %+v", connected, rows)
		}
		if connected {
			var turn protocol.RunStatus
			must(t, parent.call("turn.run", protocol.TurnRunRequest{SessionID: existing.SessionID, Input: "still attached"}, &turn))
			if turn.State != "done" || turn.Result == nil || turn.Result.Result != "still attached" {
				t.Fatalf("existing lane after duplicate open = %+v", turn)
			}
		}
	}
}

// CG3: PROTOCOL.md 504-512.
func TestLaneGapAckConsumesOnlyOldestTerminalRecord(t *testing.T) {
	laneGapInstall(t, "fixture-worker")
	_, socket := startDaemon(t)
	owner := connectPeer(t, socket, "owner", "owner", "team")
	lane := laneGapSpawn(t, owner, "child", "fixture-worker")
	var first, second protocol.RunStatus
	must(t, owner.call("turn.run", protocol.TurnRunRequest{SessionID: lane.SessionID, Input: "first"}, &first))
	must(t, owner.call("turn.run", protocol.TurnRunRequest{SessionID: lane.SessionID, Input: "second"}, &second))
	if first.State != "done" || second.State != "done" {
		t.Fatalf("records are not terminal: %+v %+v", first, second)
	}
	firstRef := protocol.RunRef{SessionID: lane.SessionID, RunID: first.RunID}
	secondRef := protocol.RunRef{SessionID: lane.SessionID, RunID: second.RunID}

	if code := rpcCode(owner.call("turn.ack", secondRef, &struct{}{})); code != protocol.Busy {
		t.Fatalf("out-of-order ack code = %d, want busy", code)
	}
	if oldest := laneGapOldest(t, owner, lane.SessionID); oldest != first.RunID {
		t.Fatalf("oldest after refused ack = %q, want %q", oldest, first.RunID)
	}
	var retained protocol.RunStatus
	must(t, owner.call("turn.status", protocol.ReadRequest{SessionID: lane.SessionID, RunID: second.RunID}, &retained))
	if retained.State != "done" || retained.Result == nil || retained.Result.Result != "second" {
		t.Fatalf("refused ack disturbed later record: %+v", retained)
	}

	laneGapAckEmpty(t, owner, firstRef)
	laneGapAckEmpty(t, owner, firstRef)
	if oldest := laneGapOldest(t, owner, lane.SessionID); oldest != second.RunID {
		t.Fatalf("oldest after ack = %q, want %q", oldest, second.RunID)
	}
	laneGapAckEmpty(t, owner, secondRef)
	laneGapAckEmpty(t, owner, secondRef)

	generation, _, _ := strings.Cut(second.RunID, "/")
	if err := owner.call("turn.ack", protocol.RunRef{SessionID: lane.SessionID, RunID: generation + "/3"}, &struct{}{}); err == nil {
		t.Fatal("never-issued run ID was acknowledged as consumed")
	}
}

// CG4: PROTOCOL.md 497-503 and 726.
func TestLaneGapRecordBoundRejectsNewWorkWithBusy(t *testing.T) {
	laneGapInstall(t, "fixture-worker")
	_, socket := startDaemon(t)
	owner := connectPeer(t, socket, "owner", "owner", "team")
	lane := laneGapSpawn(t, owner, "child", "fixture-worker")
	var first protocol.RunStatus
	for index := range protocol.MaxOperations {
		var status protocol.RunStatus
		must(t, owner.call("turn.run", protocol.TurnRunRequest{SessionID: lane.SessionID, Input: "r"}, &status))
		if status.State != "done" {
			t.Fatalf("run %d = %+v", index+1, status)
		}
		if index == 0 {
			first = status
		}
	}
	if code := rpcCode(owner.call("turn.start", protocol.TurnRunRequest{SessionID: lane.SessionID, Input: "257th"}, &protocol.RunRef{})); code != protocol.Busy {
		t.Fatalf("257th explicit admission code = %d, want busy", code)
	}
	var sent protocol.MessageSendResult
	must(t, owner.call("message.send", protocol.MessageSendRequest{Target: lane.SessionID, Message: "wake"}, &sent))
	if len(sent.Deliveries) != 1 || sent.Deliveries[0].Disposition != "rejected" || sent.Deliveries[0].Reason != "busy" {
		t.Fatalf("message-triggered work at full cursor = %+v", sent.Deliveries)
	}
	if oldest := laneGapOldest(t, owner, lane.SessionID); oldest != first.RunID {
		t.Fatalf("oldest record %q was evicted; oldest now %q", first.RunID, oldest)
	}

	must(t, owner.call("turn.ack", protocol.RunRef{SessionID: lane.SessionID, RunID: first.RunID}, &struct{}{}))
	var next protocol.RunStatus
	must(t, owner.call("turn.run", protocol.TurnRunRequest{SessionID: lane.SessionID, Input: "after ack"}, &next))
	if next.State != "done" || !strings.HasSuffix(next.RunID, "/257") {
		t.Fatalf("run after ack = %+v", next)
	}
	if code := rpcCode(owner.call("turn.start", protocol.TurnRunRequest{SessionID: lane.SessionID, Input: "again"}, &protocol.RunRef{})); code != protocol.Busy {
		t.Fatalf("one ack released more than one record: code %d", code)
	}
}

// CG5: PROTOCOL.md 536.
func TestLaneGapRepeatedInterruptInvokesNativeOnce(t *testing.T) {
	directory := laneGapInstall(t, "lanegap-interrupt-worker")
	release, interrupts := filepath.Join(directory, "release"), filepath.Join(directory, "interrupts")
	t.Setenv("LANEGAP_RELEASE", release)
	t.Setenv("LANEGAP_INTERRUPT_LOG", interrupts)
	t.Cleanup(func() { _ = os.WriteFile(release, nil, 0o600) })
	_, socket := startDaemon(t)
	owner := connectPeer(t, socket, "owner", "owner", "team")
	lane := laneGapSpawn(t, owner, "child", "lanegap-interrupt-worker")
	var ref protocol.RunRef
	must(t, owner.call("turn.start", protocol.TurnRunRequest{SessionID: lane.SessionID, Input: "hold"}, &ref))
	target := protocol.SessionTarget{SessionID: lane.SessionID}

	must(t, owner.call("turn.interrupt", target, &struct{}{}))
	must(t, owner.call("turn.interrupt", target, &struct{}{}))
	var status protocol.RunStatus
	must(t, owner.call("turn.status", protocol.ReadRequest{SessionID: lane.SessionID, RunID: ref.RunID}, &status))
	if status.State != "running" {
		t.Fatalf("second interrupt did not reach the same outstanding run: %+v", status)
	}
	if got := laneGapLines(t, interrupts); got != 1 {
		t.Fatalf("native interrupt invoked %d times for one run, want 1", got)
	}

	must(t, os.WriteFile(release, nil, 0o600))
	must(t, owner.call("turn.wait", protocol.WaitRequest{SessionID: lane.SessionID, RunID: ref.RunID}, &status))
	if status.State != "done" || status.Result == nil || status.Result.Outcome != "interrupted" {
		t.Fatalf("released run = %+v", status)
	}
	if code := rpcCode(owner.call("turn.interrupt", target, &struct{}{})); code != protocol.NotRunning {
		t.Fatalf("idle interrupt code = %d", code)
	}
	if got := laneGapLines(t, interrupts); got != 1 {
		t.Fatalf("native interrupt invoked %d times after the run ended, want 1", got)
	}
}

// CG6: PROTOCOL.md 546-550 and 630-631. closeBound is a constant with no
// hook, so this test waits the real 10 seconds.
func TestLaneGapCloseBoundKillsWorkerIgnoringClose(t *testing.T) {
	directory := laneGapInstall(t, "lanegap-deaf-worker")
	pidFile, closeSeen := filepath.Join(directory, "pid"), filepath.Join(directory, "close-seen")
	t.Setenv("LANEGAP_PID_FILE", pidFile)
	t.Setenv("LANEGAP_CLOSE_SEEN", closeSeen)
	_, socket := startDaemon(t)
	owner := connectPeer(t, socket, "owner", "owner", "team")
	lane := laneGapSpawn(t, owner, "deaf", "lanegap-deaf-worker")
	raw, err := os.ReadFile(pidFile)
	must(t, err)
	pid, err := strconv.Atoi(string(raw))
	must(t, err)
	t.Cleanup(func() {
		if t.Failed() {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	})

	closed := make(chan error, 1)
	started := time.Now()
	go func() {
		closed <- owner.call("session.close", protocol.SessionCloseRequest{SessionID: lane.SessionID}, &struct{}{})
	}()
	var closeErr error
	select {
	case closeErr = <-closed:
	case <-time.After(closeBound + 5*time.Second):
		t.Fatal("session.close outlived closeBound plus slack")
	}
	elapsed := time.Since(started)
	if _, err = os.Stat(closeSeen); err != nil {
		t.Fatalf("worker never received session.close: %v", err)
	}
	if elapsed < closeBound {
		t.Fatalf("close finished after %v, before closeBound %v", elapsed, closeBound)
	}
	if err = syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("TERM-immune worker %d was not killed and reaped: %v", pid, err)
	}
	assertOfflineLane(t, owner, lane.SessionID)
	t.Logf("close after %v returned %v", elapsed.Round(time.Millisecond), closeErr)
}

// CG7: PROTOCOL.md 519-521.
func TestLaneGapUnavailableTerminalArmsNoAutoClose(t *testing.T) {
	laneGapInstall(t, "fixture-worker")
	directory := testsocket.Directory(t)
	timers := &laneGapTimers{}
	fixed := time.Unix(1000, 0)
	socket := filepath.Join(directory, "sessionbus.sock")
	d, err := Start(Config{SocketPath: socket, TablePath: filepath.Join(directory, "sessions"), now: func() time.Time { return fixed }, policyTimer: timers.timer})
	must(t, err)
	t.Cleanup(func() { _ = d.Close() })
	owner := connectPeer(t, socket, "owner", "owner", "team")
	grace, no := int64(45000), false
	var lane protocol.LaneSpawnResult
	must(t, owner.call("lane.spawn", protocol.LaneSpawnRequest{Name: "child", Product: "fixture-worker", Open: &protocol.OpenOptions{}, AutoCloseMS: &grace, Notify: &no}, &lane))
	armed := func(want int) []bool {
		t.Helper()
		delays, stopped := timers.snapshot()
		if len(delays) != want {
			t.Fatalf("armed deadlines = %v, want %d", delays, want)
		}
		for _, delay := range delays {
			if delay != time.Duration(grace)*time.Millisecond {
				t.Fatalf("armed delay %v, want the lane's %dms grace", delay, grace)
			}
		}
		return stopped
	}
	run := func(input, state string) protocol.RunStatus {
		t.Helper()
		var status protocol.RunStatus
		must(t, owner.call("turn.run", protocol.TurnRunRequest{SessionID: lane.SessionID, Input: input}, &status))
		if status.State != state {
			t.Fatalf("%s run = %+v, want %s", input, status, state)
		}
		return status
	}
	armed(0)

	run("fail", "unavailable")
	armed(0)

	blocked := make(chan protocol.RunStatus, 1)
	go func() {
		var status protocol.RunStatus
		_ = owner.call("turn.run", protocol.TurnRunRequest{SessionID: lane.SessionID, Input: "block"}, &status)
		blocked <- status
	}()
	waitRunning(t, owner, lane.SessionID)
	must(t, owner.call("turn.interrupt", protocol.SessionTarget{SessionID: lane.SessionID}, &struct{}{}))
	if status := <-blocked; status.State != "done" || status.Result == nil || status.Result.Outcome != "interrupted" {
		t.Fatalf("interrupted run = %+v", status)
	}
	if stopped := armed(1); stopped[0] {
		t.Fatal("interrupted native terminal deadline was stopped")
	}

	run("fail", "unavailable")
	if stopped := armed(1); !stopped[0] {
		t.Fatal("new admission kept the previous grace")
	}

	run("echo", "done")
	if stopped := armed(2); stopped[1] {
		t.Fatal("completed native terminal deadline was stopped")
	}
	timers.fire(1)
	for deadline := time.Now().Add(5 * time.Second); ; {
		var listed protocol.SessionListResult
		must(t, owner.call("session.list", protocol.SessionListRequest{SessionID: lane.SessionID}, &listed))
		if len(listed.Sessions) == 1 && !listed.Sessions[0].Connected {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expired auto-close deadline did not close the lane: %+v", listed.Sessions)
		}
		time.Sleep(time.Millisecond)
	}
}

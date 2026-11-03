// SPDX-License-Identifier: GPL-3.0-only

package daemon

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/roster"
	kit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func init() {
	name := filepath.Base(os.Args[0])
	if !strings.HasPrefix(name, "archive-worker") {
		return
	}
	signal.Ignore(syscall.SIGTERM)
	product := &archiveProduct{fixtureProduct: fixtureProduct{product: name}}
	w := kit.NewWorker(product)
	product.caller = w.Caller()
	_ = w.Serve(context.Background())
	_ = os.WriteFile(os.Getenv("ARCHIVE_JOIN_READY"), []byte("ready"), 0600)
	for {
		if _, err := os.Stat(os.Getenv("ARCHIVE_JOIN_RELEASE")); err == nil {
			os.Exit(0)
		}
		time.Sleep(time.Millisecond)
	}
}

type archiveProduct struct {
	fixtureProduct
	caller *kit.Caller
}

func (p *archiveProduct) Run(ctx context.Context, run *kit.Run, input kit.RunInput) (kit.TurnResult, error) {
	if input.Text == nil || *input.Text != "children" {
		return p.fixtureProduct.Run(ctx, run, input)
	}
	var children []string
	for _, persistent := range []bool{false, true} {
		child, err := p.caller.Spawn(ctx, kit.LaneSpawnRequest{Name: "child-" + strconv.FormatBool(persistent), Product: "fixture-worker", ExtraGroups: []string{"team"}, Persistent: &persistent, Open: &kit.OpenOptions{}})
		if err != nil {
			return kit.TurnResult{}, err
		}
		children = append(children, child.SessionID)
	}
	raw, err := json.Marshal(children)
	return kit.TurnResult{Outcome: "completed", Result: string(raw)}, err
}

func (*archiveProduct) Close(context.Context, kit.SessionCloseRequest) error {
	return os.WriteFile(os.Getenv("ARCHIVE_NATIVE_HISTORY"), []byte("native close appended a terminal\n"), 0600)
}

func archiveWaitFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("fixture did not reach", path)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestArchiveReplyRequiresJoinNotDetach(t *testing.T) {
	products := laneGapInstall(t, "archive-worker")
	ready, release, history := filepath.Join(products, "ready"), filepath.Join(products, "release"), filepath.Join(products, "history")
	t.Setenv("ARCHIVE_JOIN_READY", ready)
	t.Setenv("ARCHIVE_JOIN_RELEASE", release)
	t.Setenv("ARCHIVE_NATIVE_HISTORY", history)
	d, socket := startDaemon(t)
	t.Cleanup(func() { _ = os.WriteFile(release, nil, 0600) })
	owner := connectPeer(t, socket, "owner", "owner", "team")
	var lane protocol.LaneSpawnResult
	must(t, owner.call("lane.spawn", protocol.LaneSpawnRequest{Name: "child", Product: "archive-worker", Open: &protocol.OpenOptions{}}, &lane))
	fd, err := net.Dial("unix", roster.Socket(socket))
	must(t, err)
	defer fd.Close()
	must(t, fd.SetDeadline(time.Now().Add(3*time.Second)))
	must(t, json.NewEncoder(fd).Encode(roster.ArchiveRequest{Action: roster.ArchiveAction, SessionID: lane.SessionID}))
	result := make(chan error, 1)
	go func() {
		var got roster.ArchiveResult
		err := json.NewDecoder(fd).Decode(&got)
		if err == nil && (got.State != "offline" || !got.Changed || got.Error != "") {
			err = &protocol.RPCError{Message: "wrong archive result"}
		}
		result <- err
	}()
	archiveWaitFile(t, ready)
	// Native Close appended this legitimate tail; archive must not erase it.
	raw, err := os.ReadFile(history)
	must(t, err)
	if string(raw) != "native close appended a terminal\n" {
		t.Fatal(string(raw))
	}
	deadline := time.Now().Add(time.Second)
	for {
		d.directory.mu.Lock()
		item := d.directory.entries[lane.SessionID]
		detached := item.attachment == nil && item.claimed
		done := item.done
		d.directory.mu.Unlock()
		if detached {
			select {
			case <-done:
			default:
				t.Fatal("detach did not end presence")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("fixture did not detach")
		}
		time.Sleep(time.Millisecond)
	}
	if got := archiveID(t, socket, lane.SessionID); got.Error != "busy" {
		t.Fatalf("detached unjoined archive = %+v", got)
	}
	select {
	case err := <-result:
		t.Fatalf("answered before process join: %v", err)
	default:
	}
	must(t, os.WriteFile(release, nil, 0600))
	must(t, <-result)
	assertOfflineLane(t, owner, lane.SessionID)
}

func TestArchiveDeadlineAndReplacementNeverRetarget(t *testing.T) {
	for _, variant := range []string{"before-admission", "lost-reply", "completed-snapshot"} {
		t.Run(variant, func(t *testing.T) {
			d, s, _ := reviewSession(t)
			d.host = "local"
			item := &entry{row: row{SessionID: "native@local"}, attachment: s, done: make(chan struct{})}
			d.directory.entries[item.row.SessionID] = item
			s.identity = item
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if variant == "before-admission" {
				cancel()
				got := d.operatorArchive(ctx, roster.ArchiveRequest{Action: roster.ArchiveAction, SessionID: item.row.SessionID})
				if got.State != "unknown" || got.Error != "archive_timeout" || len(s.inbox) != 0 {
					t.Fatal(got)
				}
				return
			}
			finished := make(chan roster.ArchiveResult, 1)
			go func() {
				finished <- d.operatorArchive(ctx, roster.ArchiveRequest{Action: roster.ArchiveAction, SessionID: item.row.SessionID})
			}()
			request := (<-s.inbox).(routedRequest)
			if variant == "lost-reply" {
				cancel()
				got := <-finished
				if got.State != "unknown" || got.Error != "archive_timeout" {
					t.Fatal(got)
				}
			}
			if request.destination != item {
				t.Fatal("wrong generation selected")
			}
			// A late actor reply must fit without a live operator waiter. A newly
			// resumed entry must never receive another close from this request.
			replacement := &entry{row: item.row, done: make(chan struct{})}
			d.directory.mu.Lock()
			d.directory.entries[item.row.SessionID] = replacement
			d.directory.mu.Unlock()
			request.reply <- answer{value: struct{}{}}
			if variant == "completed-snapshot" {
				if got := <-finished; got.State != "offline" || !got.Changed || got.Error != "" {
					t.Fatal(got)
				}
			}
			if len(s.inbox) != 0 || d.directory.entries[item.row.SessionID] != replacement {
				t.Fatal("late reply retargeted a replacement")
			}
		})
	}
}

func TestArchiveOwnerEndKeepsPersistentChildren(t *testing.T) {
	products := laneGapInstall(t, "archive-worker")
	installFixture(t, products, "fixture-worker")
	release := filepath.Join(products, "release")
	must(t, os.WriteFile(release, nil, 0600))
	t.Setenv("ARCHIVE_JOIN_RELEASE", release)
	t.Setenv("ARCHIVE_JOIN_READY", filepath.Join(products, "ready"))
	t.Setenv("ARCHIVE_NATIVE_HISTORY", filepath.Join(products, "history"))
	_, socket := startDaemon(t)
	owner := connectPeer(t, socket, "owner", "owner", "team")
	var lane protocol.LaneSpawnResult
	must(t, owner.call("lane.spawn", protocol.LaneSpawnRequest{Name: "parent", Product: "archive-worker", Open: &protocol.OpenOptions{}}, &lane))
	var run protocol.RunStatus
	must(t, owner.call("turn.run", protocol.TurnRunRequest{SessionID: lane.SessionID, Input: "children"}, &run))
	if run.Result == nil {
		t.Fatal("missing child creation result")
	}
	var children []string
	must(t, json.Unmarshal([]byte(run.Result.Result), &children))
	if len(children) != 2 {
		t.Fatal(children)
	}
	if result := archiveID(t, socket, lane.SessionID); result.Error != "" {
		t.Fatal(result)
	}
	// Parent success is not a recursive barrier: observe the separate child
	// lifecycle until its own normal policy close settles.
	deadline := time.Now().Add(2 * time.Second)
	for {
		var listed protocol.SessionListResult
		must(t, owner.call("session.list", protocol.SessionListRequest{SessionID: children[0]}, &listed))
		if len(listed.Sessions) == 1 && !listed.Sessions[0].Connected {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("nonpersistent child did not close")
		}
		time.Sleep(time.Millisecond)
	}
	var listed protocol.SessionListResult
	must(t, owner.call("session.list", protocol.SessionListRequest{SessionID: children[1]}, &listed))
	if len(listed.Sessions) != 1 || !listed.Sessions[0].Connected {
		t.Fatal("persistent child was closed")
	}
	if got := archiveID(t, socket, children[1]); got.Error != "" {
		t.Fatal(got)
	}
}

func TestArchiveClaimClosingAndInboxBounds(t *testing.T) {
	d, s, _ := reviewSession(t)
	d.host = "local"
	item := &entry{row: row{SessionID: "native@local"}, claimed: true, done: closedChannel()}
	d.directory.entries[item.row.SessionID] = item
	input := roster.ArchiveRequest{Action: roster.ArchiveAction, SessionID: item.row.SessionID}
	if got := d.operatorArchive(context.Background(), input); got.Error != "busy" {
		t.Fatal(got)
	}
	item.claimed, item.attachment = false, s
	s.identity = item
	for i := 0; i < cap(s.inbox); i++ {
		s.inbox <- ownerEndEvent{}
	}
	if got := d.operatorArchive(context.Background(), input); got.Error != "busy" {
		t.Fatal(got)
	}
	for len(s.inbox) > 0 {
		<-s.inbox
	}
	d.directory.closing = true
	if got := d.operatorArchive(context.Background(), input); got.Error != "daemon_closing" || len(s.inbox) != 0 {
		t.Fatal(got)
	}
}

func TestArchiveUsesExistingCloseKillBound(t *testing.T) {
	products := laneGapInstall(t, "lanegap-deaf-worker")
	t.Setenv("LANEGAP_PID_FILE", filepath.Join(products, "pid"))
	t.Setenv("LANEGAP_CLOSE_SEEN", filepath.Join(products, "close"))
	_, socket := startDaemon(t)
	owner := connectPeer(t, socket, "owner", "owner", "team")
	lane := laneGapSpawn(t, owner, "deaf", "lanegap-deaf-worker")
	raw, err := os.ReadFile(filepath.Join(products, "pid"))
	must(t, err)
	pid, err := strconv.Atoi(string(raw))
	must(t, err)
	start := time.Now()
	got := archiveID(t, socket, lane.SessionID)
	if got.State != "offline" || got.Error != "" || time.Since(start) < closeBound {
		t.Fatal(got, time.Since(start))
	}
	if err := syscall.Kill(pid, 0); err != syscall.ESRCH {
		t.Fatalf("wrapper not reaped: %v", err)
	}
}

func TestArchiveOperatorCapacityPreserved(t *testing.T) {
	d, socket := startDaemon(t)
	var held []net.Conn
	defer func() {
		for _, fd := range held {
			_ = fd.Close()
		}
	}()
	for i := 0; i < 8; i++ {
		fd, err := net.Dial("unix", roster.Socket(socket))
		must(t, err)
		held = append(held, fd)
	}
	deadline := time.Now().Add(time.Second)
	for len(d.operator.slots) != 8 {
		if time.Now().After(deadline) {
			t.Fatal("operator admissions did not settle")
		}
		time.Sleep(time.Millisecond)
	}
	extra, err := net.Dial("unix", roster.Socket(socket))
	must(t, err)
	defer extra.Close()
	must(t, extra.SetDeadline(time.Now().Add(time.Second)))
	if _, err = bufio.NewReader(extra).ReadByte(); !errors.Is(err, io.EOF) {
		t.Fatalf("ninth handler was not rejected with EOF: %v", err)
	}
}

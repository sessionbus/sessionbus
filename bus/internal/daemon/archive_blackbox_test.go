// SPDX-License-Identifier: GPL-3.0-only

package daemon

import (
	"bufio"
	"bytes"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/roster"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

// These black-box controls use only pre-feature APIs, so the same file can run
// on the parent revision and reject its absent operator action.
type archiveObservation struct {
	Schema    string `json:"schema"`
	SessionID string `json:"session_id"`
	State     string `json:"state"`
	Changed   bool   `json:"changed"`
	Error     string `json:"error"`
}

func archiveRaw(t *testing.T, socket, raw string) archiveObservation {
	t.Helper()
	fd, err := net.Dial("unix", roster.Socket(socket))
	must(t, err)
	defer fd.Close()
	must(t, fd.SetDeadline(time.Now().Add(14*time.Second)))
	_, err = fd.Write([]byte(raw + "\n"))
	must(t, err)
	body, err := bufio.NewReader(fd).ReadBytes('\n')
	must(t, err)
	var result archiveObservation
	must(t, json.Unmarshal(body, &result))
	if result.Schema != "sessionbus.lane-archive.v1" {
		t.Fatalf("not an archive result: %s", body)
	}
	return result
}

func archiveID(t *testing.T, socket, id string) archiveObservation {
	t.Helper()
	raw, err := json.Marshal(map[string]string{"action": "lane.archive", "session_id": id})
	must(t, err)
	return archiveRaw(t, socket, string(raw))
}

func TestArchiveOperatorClosesOwnerlessPersistentLane(t *testing.T) {
	products := laneGapInstall(t, "fixture-worker")
	t.Setenv("OPEN_LOG", filepath.Join(products, "open.log"))
	d, socket := startDaemon(t)
	owner := connectPeer(t, socket, "owner", "owner", "private")
	persistent := true
	var lane protocol.LaneSpawnResult
	open := &protocol.OpenOptions{Cwd: "/work", Arguments: []string{"keep-recipe"}}
	must(t, owner.call("lane.spawn", protocol.LaneSpawnRequest{Name: "child", Product: "fixture-worker", Persistent: &persistent, ExtraGroups: []string{"resume-team"}, Open: open}, &lane))
	before, err := os.ReadFile(filepath.Join(d.table.path, rowFile(lane.SessionID)))
	must(t, err)
	history := filepath.Join(products, "native-history")
	must(t, os.WriteFile(history, []byte("existing native history\n"), 0600))
	owner.peer.Shutdown()
	<-owner.peer.Closed()
	deadline := time.Now().Add(time.Second)
	for {
		d.directory.mu.Lock()
		ended := d.directory.entries[lane.SessionID].parent.ended
		d.directory.mu.Unlock()
		if ended {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("owner did not end")
		}
		time.Sleep(time.Millisecond)
	}
	result := archiveID(t, socket, lane.SessionID)
	if result.Error != "" || result.State != "offline" || !result.Changed || result.SessionID != lane.SessionID {
		t.Fatalf("archive = %+v", result)
	}
	after, err := os.ReadFile(filepath.Join(d.table.path, rowFile(lane.SessionID)))
	must(t, err)
	if !bytes.Equal(before, after) {
		t.Fatal("archive rewrote the saved recipe")
	}
	raw, err := os.ReadFile(history)
	must(t, err)
	if string(raw) != "existing native history\n" {
		t.Fatal("archive edited native history")
	}
	if again := archiveID(t, socket, lane.SessionID); again.State != "offline" || again.Changed || again.Error != "" {
		t.Fatalf("offline idempotence = %+v", again)
	}
	newOwner := connectPeer(t, socket, "next", "next", "resume-team")
	var resumed protocol.LaneSpawnResult
	must(t, newOwner.call("lane.spawn", protocol.LaneSpawnRequest{ResumeSessionID: lane.SessionID}, &resumed))
	if resumed.SessionID != lane.SessionID {
		t.Fatal("resume changed native identity")
	}
	var run protocol.RunStatus
	must(t, newOwner.call("turn.run", protocol.TurnRunRequest{SessionID: resumed.SessionID, Input: "useful after archive"}, &run))
	if run.Result == nil || run.Result.Result != "useful after archive" {
		t.Fatalf("resumed work = %+v", run)
	}
	if result := archiveID(t, socket, resumed.SessionID); result.Error != "" || !result.Changed {
		t.Fatal(result)
	}
}

func TestArchiveOperatorBusyRunAndStrictRefusals(t *testing.T) {
	laneGapInstall(t, "fixture-worker")
	_, socket := startDaemon(t)
	owner := connectPeer(t, socket, "owner", "owner", "team")
	var lane protocol.LaneSpawnResult
	must(t, owner.call("lane.spawn", protocol.LaneSpawnRequest{Name: "child", Product: "fixture-worker", Open: &protocol.OpenOptions{}}, &lane))
	for target, reason := range map[string]string{"owner/child@local": "unknown_session", "missing": "unknown_session", "owner@local": "not_managed_lane", "anything@remote": "local_only", "bad id": "invalid_request"} {
		if got := archiveID(t, socket, target); got.Error != reason || got.Changed || got.State != "" {
			t.Fatalf("%s = %+v", target, got)
		}
	}
	for _, pattern := range []string{
		`{"session_id":%q}`, `{"action":null,"session_id":%q}`, `{"action":"other","session_id":%q}`,
		`{"action":"lane.archive","session_id":%q,"local":false}`,
		`{"action":"lane.archive","session_id":%q,"local":true}`,
		`{"action":"lane.archive","session_id":%q,"extra":false}`,
	} {
		raw, _ := json.Marshal(lane.SessionID)
		// Replace the single quoted placeholder without allowing Go formatting
		// to interpret the JSON itself.
		body := bytes.ReplaceAll([]byte(pattern), []byte("%q"), raw)
		if got := archiveRaw(t, socket, string(body)); got.Error != "invalid_request" || got.Changed {
			t.Fatalf("invalid request = %+v", got)
		}
	}
	var listed protocol.SessionListResult
	must(t, owner.call("session.list", protocol.SessionListRequest{SessionID: lane.SessionID}, &listed))
	if len(listed.Sessions) != 1 || !listed.Sessions[0].Connected {
		t.Fatal("refusal mutated lane")
	}
	var started protocol.RunStatus
	must(t, owner.call("turn.start", protocol.TurnRunRequest{SessionID: lane.SessionID, Input: "block"}, &started))
	waitRunning(t, owner, lane.SessionID)
	if got := archiveID(t, socket, lane.SessionID); got.State != "offline" || got.Error != "" || !got.Changed {
		t.Fatalf("busy archive = %+v", got)
	}
	assertOfflineLane(t, owner, lane.SessionID)
	// The unchanged roster request still projects the retained lane.
	if got := readRoster(t, socket, true); len(got.Local.Sessions) != 2 || !got.Complete {
		t.Fatalf("roster after archive = %+v", got)
	}
}

// SPDX-License-Identifier: GPL-3.0-only

package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestDurablePolicyStillRejectsResponseTrace(t *testing.T) {
	path := t.TempDir()
	store, _, err := openTable(path)
	must(t, err)
	value := row{SessionID: "child@local", Product: "fixture", Name: "child@local", Groups: []string{"one", "two"}, CreatedAt: time.Now(), Policy: &protocol.LanePolicy{}}
	must(t, store.write(value))
	file := filepath.Join(path, rowFile(value.SessionID))
	raw, err := os.ReadFile(file)
	must(t, err)
	for _, mode := range []any{"off", "events", "content", "", nil} {
		var fields map[string]any
		must(t, json.Unmarshal(raw, &fields))
		fields["policy"].(map[string]any)["trace"] = mode
		changed, err := json.Marshal(fields)
		must(t, err)
		must(t, os.WriteFile(file, changed, 0600))
		if _, _, err := openTable(path); err == nil {
			t.Errorf("durable trace key accepted: %#v", mode)
		}
	}
	must(t, os.WriteFile(file, raw, 0600))
	value.Policy.Trace = "off"
	if err := store.write(value); err == nil {
		t.Fatal("response-only trace written to disk")
	}
	after, err := os.ReadFile(file)
	must(t, err)
	if string(after) != string(raw) {
		t.Fatal("refused write changed durable bytes")
	}
}

// Exercise the same Action/closed-result SDK path used by product tool bridges.
func traceActionSpawn(t *testing.T, parent *peerClient, input protocol.LaneSpawnRequest, want string) protocol.LaneSpawnResult {
	t.Helper()
	args, err := json.Marshal(input)
	must(t, err)
	raw, err := parent.peer.Caller.Action(context.Background(), "spawn", args)
	must(t, err)
	var result protocol.LaneSpawnResult
	must(t, json.Unmarshal(raw, &result))
	if result.Policy == nil || result.Policy.Trace != want {
		t.Fatalf("spawn policy = %s, want trace %q", raw, want)
	}
	return result
}

func TestSpawnPolicyReportsLiveTraceWithoutPersisting(t *testing.T) {
	d, parent := traceFixture(t)
	for _, persistent := range []bool{false, true} {
		for _, mode := range []string{"", "off", "events", "content"} {
			name := fmt.Sprintf("persistent-%t-mode-%s", persistent, mode)
			want := mode
			if want == "" {
				want = "off"
			}
			child := traceActionSpawn(t, parent, protocol.LaneSpawnRequest{Name: name, Product: "fixture-worker", Open: &protocol.OpenOptions{}, Trace: mode, Persistent: &persistent}, want)
			// Resume must use its new request, even after the previous live
			// parent selected another mode.
			traceConfigure(t, parent, child.SessionID, "content")
			must(t, parent.call("session.close", protocol.SessionCloseRequest{SessionID: child.SessionID}, &struct{}{}))
			traceActionSpawn(t, parent, protocol.LaneSpawnRequest{ResumeSessionID: child.SessionID, Trace: mode}, want)
		}
	}
	var listed protocol.SessionListResult
	must(t, parent.call("session.list", protocol.SessionListRequest{}, &listed))
	for _, row := range listed.Sessions {
		if row.Policy != nil && row.Policy.Trace != "" {
			t.Fatalf("trace leaked to list: %+v", row)
		}
	}
	if strings.Contains(traceTableBytes(t, d.config.TablePath), `"trace"`) {
		t.Fatal("trace persisted in lane rows")
	}
}

func TestSpawnPolicyReportsCommittedStateOnlyToLiveParent(t *testing.T) {
	d := &Daemon{}
	d.directory = newDirectory(d, nil)
	dir := d.directory
	parent := &ownership{id: "parent@local", token: "old"}
	parentEntry := &entry{lifetime: parent, attachment: &session{}}
	dir.entries[parent.id] = parentEntry
	worker := &session{}
	item := &entry{row: row{SessionID: "child@local", Policy: &protocol.LanePolicy{}}, parent: parent, attachment: worker, traceMode: "events"}
	dir.entries[item.row.SessionID] = item
	start := &launch{entry: item, parent: parent, input: &protocol.LaneSpawnRequest{Trace: "content"}}
	got := dir.spawnResult(start, worker)
	if got.Policy.Trace != "events" {
		t.Fatalf("echoed request instead of live state: %+v", got.Policy)
	}
	got.Policy.Trace = "content"
	if item.row.Policy.Trace != "" {
		t.Fatal("response mutated stored policy")
	}
	for _, persistent := range []bool{false, true} {
		item.row.Policy.Persistent = persistent
		parent.ended = true
		if got := dir.spawnResult(start, worker); got.Policy.Trace != "" {
			t.Fatal("ended parent received live trace")
		}
		parent.ended = false
		parentEntry.lifetime = &ownership{id: parent.id, token: "replacement"}
		if got := dir.spawnResult(start, worker); got.Policy.Trace != "" {
			t.Fatal("replacement lifetime authorized by ID")
		}
		parentEntry.lifetime = parent
		item.parent = parentEntry.lifetime
		start.parent = &ownership{id: parent.id, token: "different-caller"}
		if got := dir.spawnResult(start, worker); got.Policy.Trace != "" {
			t.Fatal("unrelated caller authorized by ID")
		}
		start.parent = parent
	}
}

func TestSpawnPolicyFederatedTrace(t *testing.T) {
	daemons, parent := traceFederation(t)
	for _, mode := range []string{"", "off", "events", "content"} {
		want := mode
		if want == "" {
			want = "off"
		}
		child := traceActionSpawn(t, parent, protocol.LaneSpawnRequest{Name: "remote-" + want + mode, Host: "beta", Product: "fixture-worker", Open: &protocol.OpenOptions{}, Trace: mode}, want)
		must(t, parent.call("session.close", protocol.SessionCloseRequest{SessionID: child.SessionID}, &struct{}{}))
		traceActionSpawn(t, parent, protocol.LaneSpawnRequest{ResumeSessionID: child.SessionID}, "off")
	}
	var listed protocol.SessionListResult
	must(t, parent.call("session.list", protocol.SessionListRequest{Host: "beta"}, &listed))
	for _, row := range listed.Sessions {
		if row.Policy != nil && row.Policy.Trace != "" {
			t.Fatal("remote roster leaked trace")
		}
	}
	if strings.Contains(traceTableBytes(t, daemons["beta"].config.TablePath), `"trace"`) {
		t.Fatal("remote trace persisted")
	}
}

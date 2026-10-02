// SPDX-License-Identifier: GPL-3.0-only

package daemon

import (
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func assertConnectedInvariant(t *testing.T, d *directory) {
	t.Helper()
	d.mu.Lock()
	defer d.mu.Unlock()
	for id, item := range d.entries {
		if item.attachment != nil {
			if d.connected[id] != item {
				t.Fatalf("attached entry %q missing or replaced in active corpus", id)
			}
		} else if d.connected[id] != nil {
			t.Fatalf("archived entry %q is active", id)
		}
	}
	for id, item := range d.connected {
		if d.entries[id] != item || item.attachment == nil {
			t.Fatalf("stale active entry %q", id)
		}
	}
}

func TestConnectedDirectoryInvariantAcrossAttachmentTransitions(t *testing.T) {
	daemon := &Daemon{host: "local"}
	d := newDirectory(daemon, []row{{SessionID: "archive@local", Name: "same@local", Groups: []string{"team", "own"}, CreatedAt: time.Unix(1, 0)}})
	daemon.directory = d
	assertConnectedInvariant(t, d)
	oldOwner, newOwner := &session{}, &session{}
	hello := &protocol.PeerHello{SessionID: "parent", Name: "same", Product: "peer", Groups: []string{"team"}}
	old, _, _, ok := d.installPeer(oldOwner, hello, "local")
	if !ok {
		t.Fatal("install peer")
	}
	oldOwner.identity = old
	assertConnectedInvariant(t, d)
	hello.Name = "renamed"
	if same, _, _, ok := d.installPeer(oldOwner, hello, "local"); !ok || same != old {
		t.Fatal("rename changed identity")
	}
	assertConnectedInvariant(t, d)
	parent, displaced, _, ok := d.installPeer(newOwner, hello, "local")
	if !ok || displaced != oldOwner || parent == old {
		t.Fatal("same-ID replacement")
	}
	newOwner.identity = parent
	assertConnectedInvariant(t, d)
	d.detach(old, oldOwner)
	assertConnectedInvariant(t, d)
	d.offline(old, oldOwner, true)
	assertConnectedInvariant(t, d)
	d.mu.Lock()
	d.removeConnected(old)
	d.mu.Unlock()
	assertConnectedInvariant(t, d)
	if d.connected[parent.row.SessionID] != parent {
		t.Fatal("stale owner erased replacement")
	}

	start := newLaunch("worker", false, true)
	start.parent = parent.lifetime
	defer start.timer.Stop()
	lane, code := d.reserveFresh(row{Name: "parent/lane@local", Product: "worker", Groups: []string{"team", "own"}, Policy: &protocol.LanePolicy{Persistent: true}}, start)
	if code != 0 {
		t.Fatal(code)
	}
	defer daemon.group.Done()
	assertConnectedInvariant(t, d)
	if d.reserveID(lane, "archive@local") || !d.reserveID(lane, "native@local") {
		t.Fatal("native ID fencing")
	}
	assertConnectedInvariant(t, d)
	worker := &session{}
	if !d.publish(start, worker, time.Unix(2, 0)) {
		t.Fatal("publish lane")
	}
	assertConnectedInvariant(t, d)
	d.admitted(lane, "turn.execute")
	assertConnectedInvariant(t, d)
	d.finishRun(lane, worker)
	assertConnectedInvariant(t, d)
	if d.connected[lane.row.SessionID] != lane {
		t.Fatal("terminal idle lane became archived")
	}
	d.detach(lane, worker)
	assertConnectedInvariant(t, d)
	d.offline(lane, worker, false)
	assertConnectedInvariant(t, d)

	for _, commit := range []bool{false, true} {
		resume := newLaunch("worker", false, false)
		resume.parent = parent.lifetime
		resume.input = &protocol.LaneSpawnRequest{ResumeSessionID: lane.row.SessionID}
		defer resume.timer.Stop()
		resumed, code := d.reserveResume(lane.row.SessionID, []string{"team"}, resume)
		if code != 0 {
			t.Fatal(code)
		}
		defer daemon.group.Done()
		assertConnectedInvariant(t, d)
		if !commit {
			d.releaseLaunch(resume)
			assertConnectedInvariant(t, d)
			if d.entries[lane.row.SessionID] != lane {
				t.Fatal("failed resume lost archived row")
			}
			continue
		}
		if !d.publish(resume, worker, time.Time{}) {
			t.Fatal("publish resumed lane")
		}
		assertConnectedInvariant(t, d)
		d.offline(resumed, worker, true)
		assertConnectedInvariant(t, d)
		if d.entries[lane.row.SessionID] != nil {
			t.Fatal("forget retained row")
		}
	}
	hello.SessionID = "changed-parent"
	changed, _, _, ok := d.installPeer(newOwner, hello, "local")
	if !ok {
		t.Fatal("different-ID rehello")
	}
	newOwner.identity = changed
	assertConnectedInvariant(t, d)
	d.detach(changed, newOwner)
	assertConnectedInvariant(t, d)
}

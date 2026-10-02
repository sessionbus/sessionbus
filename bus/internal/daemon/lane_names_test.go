// SPDX-License-Identifier: GPL-3.0-only

package daemon

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
	"github.com/antst/sessionbus/bus/sdk/go/testsocket"
)

func TestDuplicateLaneNamesKeepDistinctIDsAcrossRestart(t *testing.T) {
	directory := testsocket.Directory(t)
	installFixture(t, directory, "fixture-worker")
	t.Setenv("PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))
	config := Config{SocketPath: filepath.Join(directory, "bus.sock"), TablePath: filepath.Join(directory, "rows")}
	d, err := Start(config)
	must(t, err)
	t.Cleanup(func() { _ = d.Close() })
	parent := connectPeer(t, config.SocketPath, "parent", "parent", "team")
	var lanes [2]protocol.LaneSpawnResult
	for index := range lanes {
		must(t, parent.call("lane.spawn", protocol.LaneSpawnRequest{Name: "same", Product: "fixture-worker", Open: &protocol.OpenOptions{}}, &lanes[index]))
	}
	if lanes[0].SessionID == lanes[1].SessionID {
		t.Fatal("equal names allocated the same native ID")
	}
	var listed protocol.SessionListResult
	must(t, parent.call("session.list", protocol.SessionListRequest{SessionID: "parent/same"}, &listed))
	if len(listed.Sessions) != 2 {
		t.Fatalf("name lookup = %+v", listed.Sessions)
	}
	var sent protocol.MessageSendResult
	must(t, parent.call("message.send", protocol.MessageSendRequest{Target: "parent/same", Message: "ambiguous"}, &sent))
	if len(sent.Deliveries) != 1 || sent.Deliveries[0].Reason != "ambiguous" || sent.Deliveries[0].Disposition != "rejected" {
		t.Fatalf("duplicate active name receipt = %+v", sent)
	}
	if code := rpcCode(parent.call("turn.status", protocol.ReadRequest{SessionID: "parent/same"}, &protocol.RunStatus{})); code != protocol.UnknownSession {
		t.Fatalf("ambiguous control = %d", code)
	}
	for _, lane := range lanes {
		var run protocol.RunStatus
		must(t, parent.call("turn.run", protocol.TurnRunRequest{SessionID: lane.SessionID, Input: lane.SessionID}, &run))
		if run.SessionID != lane.SessionID || run.Result == nil || run.Result.Result != lane.SessionID {
			t.Fatalf("exact-ID run = %+v", run)
		}
		must(t, parent.call("session.close", protocol.SessionCloseRequest{SessionID: lane.SessionID}, &struct{}{}))
	}
	must(t, d.Close())
	d, err = Start(config)
	must(t, err)
	parent = connectPeer(t, config.SocketPath, "parent", "parent", "team")
	must(t, parent.call("session.list", protocol.SessionListRequest{SessionID: "parent/same"}, &listed))
	if len(listed.Sessions) != 2 || listed.Sessions[0].Connected || listed.Sessions[1].Connected {
		t.Fatalf("reloaded duplicates = %+v", listed.Sessions)
	}
	if code := rpcCode(parent.call("session.close", protocol.SessionCloseRequest{SessionID: "parent/same", Forget: true}, &struct{}{})); code != protocol.UnknownSession {
		t.Fatalf("ambiguous archived forget = %d", code)
	}
	var resumed protocol.LaneSpawnResult
	must(t, parent.call("lane.spawn", protocol.LaneSpawnRequest{ResumeSessionID: lanes[0].SessionID}, &resumed))
	if resumed.SessionID != lanes[0].SessionID {
		t.Fatalf("resume changed ID = %+v", resumed)
	}
	must(t, parent.call("session.close", protocol.SessionCloseRequest{SessionID: resumed.SessionID, Forget: true}, &struct{}{}))
	must(t, parent.call("session.list", protocol.SessionListRequest{SessionID: "parent/same"}, &listed))
	if len(listed.Sessions) != 1 || listed.Sessions[0].SessionID != lanes[1].SessionID {
		t.Fatalf("forget removed another equal name = %+v", listed.Sessions)
	}
}

func TestActiveLaneCommandsExcludeArchivedNamesAndIDs(t *testing.T) {
	directory := t.TempDir()
	installFixture(t, directory, "fixture-worker")
	t.Setenv("PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))
	_, socket := startDaemon(t)
	parent := connectPeer(t, socket, "parent", "parent", "team")
	var archived, active protocol.LaneSpawnResult
	request := protocol.LaneSpawnRequest{Name: "same", Product: "fixture-worker", Open: &protocol.OpenOptions{}}
	must(t, parent.call("lane.spawn", request, &archived))
	must(t, parent.call("session.close", protocol.SessionCloseRequest{SessionID: archived.SessionID}, &struct{}{}))
	must(t, parent.call("lane.spawn", request, &active))
	var run protocol.RunStatus
	must(t, parent.call("turn.run", protocol.TurnRunRequest{SessionID: "parent/same", Input: "active only"}, &run))
	if run.SessionID != active.SessionID || run.Result == nil || run.Result.Result != "active only" {
		t.Fatalf("name chose archive = %+v", run)
	}
	var status protocol.RunStatus
	must(t, parent.call("turn.status", protocol.ReadRequest{SessionID: "parent/same"}, &status))
	must(t, parent.call("turn.wait", protocol.WaitRequest{SessionID: "parent/same"}, &status))
	if status.SessionID != active.SessionID || status.RunID != run.RunID {
		t.Fatalf("name collection chose archive = %+v", status)
	}
	must(t, parent.call("turn.ack", protocol.RunRef{SessionID: "parent/same", RunID: run.RunID}, &struct{}{}))
	var started protocol.RunRef
	must(t, parent.call("turn.start", protocol.TurnRunRequest{SessionID: "parent/same", Input: "start"}, &started))
	if started.SessionID != active.SessionID {
		t.Fatalf("name start chose archive = %+v", started)
	}
	must(t, parent.call("turn.wait", protocol.WaitRequest{SessionID: active.SessionID, RunID: started.RunID}, &status))
	if code := rpcCode(parent.call("turn.interrupt", protocol.SessionTarget{SessionID: "parent/same"}, &struct{}{})); code != protocol.NotRunning {
		t.Fatalf("connected idle interrupt = %d", code)
	}
	var sent protocol.MessageSendResult
	must(t, parent.call("message.send", protocol.MessageSendRequest{Target: "parent/same", Message: "one active"}, &sent))
	if len(sent.Deliveries) != 1 || sent.Deliveries[0].SessionID != active.SessionID || sent.Deliveries[0].Disposition == "rejected" {
		t.Fatalf("active-over-archive send = %+v", sent)
	}
	must(t, parent.call("session.close", protocol.SessionCloseRequest{SessionID: "parent/same"}, &struct{}{}))
	for _, label := range []string{"parent/same", active.SessionID, archived.SessionID} {
		calls := []struct {
			method string
			input  any
		}{
			{"turn.run", protocol.TurnRunRequest{SessionID: label, Input: "unknown"}},
			{"turn.start", protocol.TurnRunRequest{SessionID: label, Input: "unknown"}},
			{"turn.status", protocol.ReadRequest{SessionID: label}},
			{"turn.wait", protocol.WaitRequest{SessionID: label}},
			{"turn.ack", protocol.RunRef{SessionID: label, RunID: run.RunID}},
			{"turn.interrupt", protocol.SessionTarget{SessionID: label}},
			{"session.close", protocol.SessionCloseRequest{SessionID: label}},
		}
		for _, call := range calls {
			if code := rpcCode(parent.call(call.method, call.input, &struct{}{})); code != protocol.UnknownSession {
				t.Fatalf("archived %s %q = %d", call.method, label, code)
			}
		}
		sent = protocol.MessageSendResult{}
		must(t, parent.call("message.send", protocol.MessageSendRequest{Target: label, Message: "unknown"}, &sent))
		if len(sent.Deliveries) != 1 || sent.Deliveries[0].Reason != "unknown_session" || sent.Deliveries[0].DeliveryID != "" {
			t.Fatalf("archive send %q = %+v", label, sent)
		}
	}
}

func TestDefaultAndHostOnlyListAndGroupSendExcludeArchives(t *testing.T) {
	directory := t.TempDir()
	installFixture(t, directory, "fixture-worker")
	t.Setenv("PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))
	_, socket := startDaemon(t)
	parent := connectPeer(t, socket, "parent", "parent", "team")
	receiver := connectPeer(t, socket, "receiver", "receiver", "team")
	var lane protocol.LaneSpawnResult
	must(t, parent.call("lane.spawn", protocol.LaneSpawnRequest{Name: "archived", Product: "fixture-worker", Open: &protocol.OpenOptions{}, ExtraGroups: []string{"team"}}, &lane))
	must(t, parent.call("session.close", protocol.SessionCloseRequest{SessionID: lane.SessionID}, &struct{}{}))
	for _, request := range []protocol.SessionListRequest{{}, {Host: "local"}} {
		var listed protocol.SessionListResult
		must(t, parent.call("session.list", request, &listed))
		if len(listed.Sessions) != 2 || listed.SelfInfo == nil || listed.SelfInfo.SessionID != "parent@local" {
			t.Fatalf("default/host-only list = %+v", listed)
		}
		for _, item := range listed.Sessions {
			if !item.Connected || item.SessionID == lane.SessionID {
				t.Fatalf("archive in active list = %+v", item)
			}
		}
	}
	for _, label := range []string{lane.SessionID, "parent/archived"} {
		var listed protocol.SessionListResult
		must(t, parent.call("session.list", protocol.SessionListRequest{SessionID: label}, &listed))
		if len(listed.Sessions) != 1 || listed.Sessions[0].SessionID != lane.SessionID || listed.Sessions[0].Connected {
			t.Fatalf("record discovery = %+v", listed)
		}
	}
	var sent protocol.MessageSendResult
	must(t, parent.call("message.send", protocol.MessageSendRequest{Group: "team", Message: "live group"}, &sent))
	if len(sent.Deliveries) != 1 || sent.Deliveries[0].SessionID != "receiver@local" {
		t.Fatalf("archived group recipient = %+v", sent)
	}
	if received := <-receiver.deliveries; received.Body != "live group" {
		t.Fatalf("live group delivery = %+v", received)
	}
}

func TestArchivedLaneIDsAreUnknownToActiveCommands(t *testing.T) {
	directory := t.TempDir()
	installFixture(t, directory, "fixture-worker")
	t.Setenv("PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))
	_, socket := startDaemon(t)
	parent := connectPeer(t, socket, "parent", "parent", "team")
	var lane protocol.LaneSpawnResult
	must(t, parent.call("lane.spawn", protocol.LaneSpawnRequest{Name: "archived", Product: "fixture-worker", Open: &protocol.OpenOptions{}}, &lane))
	var run protocol.RunStatus
	must(t, parent.call("turn.run", protocol.TurnRunRequest{SessionID: lane.SessionID, Input: "done"}, &run))
	must(t, parent.call("session.close", protocol.SessionCloseRequest{SessionID: lane.SessionID}, &struct{}{}))
	for _, label := range []string{lane.SessionID, "parent/archived"} {
		for _, call := range []struct {
			method string
			input  any
		}{
			{"turn.run", protocol.TurnRunRequest{SessionID: label, Input: "unknown"}},
			{"turn.start", protocol.TurnRunRequest{SessionID: label, Input: "unknown"}},
			{"turn.status", protocol.ReadRequest{SessionID: label}},
			{"turn.wait", protocol.WaitRequest{SessionID: label}},
			{"turn.ack", protocol.RunRef{SessionID: label, RunID: run.RunID}},
			{"turn.interrupt", protocol.SessionTarget{SessionID: label}},
			{"session.close", protocol.SessionCloseRequest{SessionID: label}},
		} {
			t.Run(label+"/"+call.method, func(t *testing.T) {
				if code := rpcCode(parent.call(call.method, call.input, &struct{}{})); code != protocol.UnknownSession {
					t.Fatalf("archived target = %d, want unknown_session", code)
				}
			})
		}
	}
	assertOfflineLane(t, parent, lane.SessionID)
}

func TestGroupSendExcludesArchivedLane(t *testing.T) {
	directory := t.TempDir()
	installFixture(t, directory, "fixture-worker")
	t.Setenv("PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))
	_, socket := startDaemon(t)
	parent := connectPeer(t, socket, "parent", "parent", "team")
	receiver := connectPeer(t, socket, "receiver", "receiver", "team")
	var lane protocol.LaneSpawnResult
	must(t, parent.call("lane.spawn", protocol.LaneSpawnRequest{Name: "archived", Product: "fixture-worker", Open: &protocol.OpenOptions{}, ExtraGroups: []string{"team"}}, &lane))
	must(t, parent.call("session.close", protocol.SessionCloseRequest{SessionID: lane.SessionID}, &struct{}{}))
	var sent protocol.MessageSendResult
	must(t, parent.call("message.send", protocol.MessageSendRequest{Group: "team", Message: "live only"}, &sent))
	if len(sent.Deliveries) != 1 || sent.Deliveries[0].SessionID != "receiver@local" {
		t.Fatalf("group recipients = %+v", sent)
	}
	if received := <-receiver.deliveries; received.Body != "live only" {
		t.Fatalf("live group delivery = %+v", received)
	}
}

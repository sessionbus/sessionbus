// SPDX-License-Identifier: GPL-3.0-only

package daemon

import (
	"encoding/base64"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/federation"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
	"github.com/antst/sessionbus/bus/sdk/go/testsocket"
)

func loadFixturePolicy(t *testing.T, raw string) *federation.Policy {
	t.Helper()
	path := filepath.Join(t.TempDir(), "policy.json")
	must(t, os.WriteFile(path, []byte(raw), 0o600))
	policy, err := federation.LoadPolicy(path)
	must(t, err)
	return policy
}

func policyDaemonFixture(t *testing.T, advertised ...string) (*federation.Hub, map[string]*Daemon, map[string]*peerClient) {
	t.Helper()
	products := t.TempDir()
	installFixture(t, products, "fixture-worker")
	t.Setenv("PATH", products+string(os.PathListSeparator)+os.Getenv("PATH"))
	secrets := map[string]string{}
	for index, host := range []string{"alpha", "beta"} {
		secret := make([]byte, federation.SecretBytes)
		secret[0] = byte(index + 1)
		secrets[host] = base64.StdEncoding.EncodeToString(secret)
	}
	configuration, err := federation.ServerTLS(secrets)
	must(t, err)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	must(t, err)
	hub := federation.StartHub(listener, configuration, io.Discard)
	t.Cleanup(hub.Close)
	daemons, peers := map[string]*Daemon{}, map[string]*peerClient{}
	for _, host := range []string{"alpha", "beta"} {
		dir := testsocket.Directory(t)
		d, err := Start(Config{Host: host, SocketPath: filepath.Join(dir, "bus.sock"), TablePath: filepath.Join(dir, "rows"), HubAddress: listener.Addr().String(), HubSecret: secrets[host], Products: append([]string(nil), advertised...)})
		must(t, err)
		t.Cleanup(func() { _ = d.Close() })
		daemons[host] = d
		peers[host] = connectPeer(t, d.config.SocketPath, "parent", "parent", "team")
	}
	until := time.Now().Add(5 * time.Second)
	for {
		var listed protocol.SessionListResult
		if peers["alpha"].call("session.list", protocol.SessionListRequest{Host: "beta"}, &listed) == nil {
			break
		}
		if time.Now().After(until) {
			t.Fatal("fixture federation registration did not complete")
		}
		time.Sleep(time.Millisecond)
	}
	return hub, daemons, peers
}

// These are real private daemons and a Go fixture Worker, not native AI or a
// host installation. The destination's existing group checks enforce the
// envelope actually forwarded by the hub.
func TestFederationPolicyDestinationRejectsPrivateSpoofBothDirections(t *testing.T) {
	hub, daemons, _ := policyDaemonFixture(t)
	for _, hosts := range [][2]string{{"alpha", "beta"}, {"beta", "alpha"}} {
		source, destination := hosts[0], hosts[1]
		victim := connectPeer(t, daemons[destination].config.SocketPath, "victim", "victim", "private-team")
		attacker := connectPeer(t, daemons[source].config.SocketPath, "attacker", "attacker", "team", "session:victim@"+destination)
		request := protocol.SessionListRequest{SessionID: "victim@" + destination}
		var listed protocol.SessionListResult
		// The exact same peer traffic without policy establishes existing local
		// group declarations, rather than treating a new CLI flag as proof.
		if !hub.ReloadPolicy(nil) {
			t.Fatal("unrestricted reload failed")
		}
		must(t, attacker.call("session.list", request, &listed))
		if len(listed.Sessions) != 1 || listed.Sessions[0].SessionID != request.SessionID {
			t.Fatal("unrestricted private-group continuity control failed", listed)
		}
		if !hub.ReloadPolicy(loadFixturePolicy(t, `{"hosts":{"alpha":{"groups":["team"]},"beta":{"groups":["team"]}}}`)) {
			t.Fatal("restricted reload failed")
		}
		if code := rpcCode(attacker.call("session.list", request, &listed)); code != protocol.UnknownSession {
			t.Fatal("actual destination accepted forged private visibility", source, destination, code)
		}
		var result protocol.MessageSendResult
		must(t, attacker.call("message.send", protocol.MessageSendRequest{Target: request.SessionID, Message: "must stay hidden"}, &result))
		if len(result.Deliveries) != 1 || result.Deliveries[0].Disposition != "rejected" || result.Deliveries[0].Reason != "unknown_session" {
			t.Fatal("private spoof was delivered or misreported", result)
		}
		var local protocol.SessionListResult
		must(t, victim.call("session.list", protocol.SessionListRequest{SessionID: "victim"}, &local))
		if len(local.Sessions) != 1 {
			t.Fatal("hub policy changed same-host open behavior")
		}
	}
}

func TestFederationPolicyRealWorkerCreatorCollectsAfterRevocation(t *testing.T) {
	hub, daemons, peers := policyDaemonFixture(t)
	if !hub.ReloadPolicy(loadFixturePolicy(t, `{"hosts":{"alpha":{"groups":["team"],"lanes_to":["beta"]},"beta":{"lanes_on":["alpha"]}}}`)) {
		t.Fatal("policy reload failed")
	}
	parent := peers["alpha"]
	other := connectPeer(t, daemons["alpha"].config.SocketPath, "other", "other", "team")
	zero := int64(0)
	var child protocol.LaneSpawnResult
	must(t, parent.call("lane.spawn", protocol.LaneSpawnRequest{Name: "policy-child", Host: "beta", Product: "fixture-worker", Open: &protocol.OpenOptions{}, ExtraGroups: []string{"team"}, AutoCloseMS: &zero}, &child))
	var listed protocol.SessionListResult
	must(t, other.call("session.list", protocol.SessionListRequest{SessionID: child.SessionID}, &listed))
	if !hub.ReloadPolicy(loadFixturePolicy(t, `{"hosts":{"alpha":{"groups":[],"lanes_to":[]}}}`)) {
		t.Fatal("revocation reload failed")
	}
	if code := rpcCode(other.call("session.list", protocol.SessionListRequest{SessionID: child.SessionID}, &listed)); code != protocol.PolicyDenied {
		t.Fatal("noncreator retained access through a revoked named group", code)
	}
	must(t, parent.call("session.list", protocol.SessionListRequest{SessionID: child.SessionID}, &listed))
	if len(listed.Sessions) != 1 || listed.Sessions[0].SessionID != child.SessionID {
		t.Fatal("creator known-ID access disappeared", listed)
	}
	var run protocol.RunStatus
	must(t, parent.call("turn.run", protocol.TurnRunRequest{SessionID: child.SessionID, Input: "useful after policy revocation"}, &run))
	if run.State != "done" || run.Result == nil || !strings.Contains(run.Result.Result, "useful after policy revocation") {
		t.Fatal("creator did not receive the actual retained Run result", run)
	}
	must(t, parent.call("turn.ack", protocol.RunRef{SessionID: run.SessionID, RunID: run.RunID}, &struct{}{}))
	must(t, parent.call("session.close", protocol.SessionCloseRequest{SessionID: child.SessionID, Forget: true}, &struct{}{}))
	if code := rpcCode(parent.call("session.list", protocol.SessionListRequest{SessionID: child.SessionID}, &listed)); code != protocol.PolicyDenied {
		t.Fatal("forgotten relation retained an admitted target", code)
	}
	var denied protocol.LaneSpawnResult
	if code := rpcCode(parent.call("lane.spawn", protocol.LaneSpawnRequest{Name: "denied", Host: "beta", Product: "fixture-worker", Open: &protocol.OpenOptions{}}, &denied)); code == 0 || code == protocol.ForwardLost {
		t.Fatal("lane revocation admitted new work or invented transport loss", code)
	}
}

// A table-empty resume may prove creation access or an explicit private-group
// grant. Both resume the actual fixture Worker; neither establishes creator
// identity or a reverse notification/transfer relation at the hub.
func TestFederationPolicyPrivateOnlyResumeContinuesRealWorker(t *testing.T) {
	for _, delegated := range []bool{false, true} {
		t.Run(map[bool]string{false: "creator", true: "extra-group-grantee"}[delegated], func(t *testing.T) {
			hub, daemons, peers := policyDaemonFixture(t)
			caller := peers["alpha"]
			creator := caller
			input := protocol.LaneSpawnRequest{Name: "retained", Host: "beta", Product: "fixture-worker", Open: &protocol.OpenOptions{}}
			if delegated {
				caller = connectPeer(t, daemons["alpha"].config.SocketPath, "grantee", "grantee", "team")
				creator = peers["beta"]
				input.Host = ""
				input.ExtraGroups = []string{"session:grantee@alpha"}
			}
			zero := int64(0)
			no := false
			input.AutoCloseMS = &zero
			input.Notify = &no
			var lane protocol.LaneSpawnResult
			must(t, creator.call("lane.spawn", input, &lane))
			must(t, creator.call("session.close", protocol.SessionCloseRequest{SessionID: lane.SessionID}, &struct{}{}))
			if !hub.ReloadPolicy(loadFixturePolicy(t, `{"hosts":{"alpha":{"groups":[]}}}`)) {
				t.Fatal("reload failed")
			}
			must(t, caller.call("lane.spawn", protocol.LaneSpawnRequest{ResumeSessionID: lane.SessionID, AutoCloseMS: &zero, Notify: &no}, &lane))
			var listed protocol.SessionListResult
			must(t, caller.call("session.list", protocol.SessionListRequest{SessionID: lane.SessionID}, &listed))
			if len(listed.Sessions) != 1 || listed.Sessions[0].SessionID != lane.SessionID {
				t.Fatal("resumed lane not accessible by known ID", listed)
			}
			var run protocol.RunStatus
			must(t, caller.call("turn.run", protocol.TurnRunRequest{SessionID: lane.SessionID, Input: "continue deliberate access"}, &run))
			if run.State != "done" || run.Result == nil || !strings.Contains(run.Result.Result, "continue deliberate access") {
				t.Fatal("resumed actual work was not collected", run)
			}
			must(t, caller.call("turn.ack", protocol.RunRef{SessionID: run.SessionID, RunID: run.RunID}, &struct{}{}))
			must(t, caller.call("session.close", protocol.SessionCloseRequest{SessionID: lane.SessionID, Forget: true}, &struct{}{}))
		})
	}
}

// SPDX-License-Identifier: GPL-3.0-only
package daemon

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
	"github.com/antst/sessionbus/bus/sdk/go/testsocket"
)

// Private external-version fixture: never installed on the development host.
func TestPolicyExecutableSDKWorkerVersionMatrix(t *testing.T) {
	if os.Getenv("MATRIX_PRODUCT_PATH") == "" {
		t.Skip("external pinned-version matrix")
	}
	products := []string{"fixture-worker-matrix-old-go", "fixture-worker-matrix-new-go", "fixture-worker-matrix-old-js", "fixture-worker-matrix-new-js"}
	t.Setenv("PATH", os.Getenv("MATRIX_PRODUCT_PATH")+string(os.PathListSeparator)+os.Getenv("PATH"))
	for _, product := range products {
		t.Run(product, func(t *testing.T) {
			dir := testsocket.Directory(t)
			socket := filepath.Join(dir, "daemon.sock")
			d, err := Start(Config{SocketPath: socket, TablePath: filepath.Join(dir, "rows"), Products: []string{product}})
			must(t, err)
			t.Cleanup(func() { must(t, d.Close()) })
			parent := connectPeer(t, socket, "matrix-parent", "matrix-parent", "matrix-team")
			zero := int64(0)
			var lane protocol.LaneSpawnResult
			must(t, parent.call("lane.spawn", protocol.LaneSpawnRequest{Name: "matrix", Product: product, AutoCloseMS: &zero, Open: &protocol.OpenOptions{}}, &lane))
			d.directory.mu.Lock()
			token := d.directory.entries[lane.SessionID].attachment.launch.token
			d.directory.mu.Unlock()
			if !strings.HasPrefix(token, os.Getenv("MATRIX_EXPECT_TOKEN_PREFIX")) {
				t.Fatal("daemon bootstrap changed", token)
			}
			var run protocol.RunStatus
			must(t, parent.call("turn.run", protocol.TurnRunRequest{SessionID: lane.SessionID, Input: product}, &run))
			if run.State != "done" || run.Result == nil || run.Result.Outcome != "completed" || run.Result.Result != product {
				t.Fatalf("unusable matrix Run: %#v", run)
			}
			t.Logf("daemon=%s worker=%s lane=%s run=%s state=%s outcome=%s result=%s", os.Getenv("MATRIX_DAEMON_HEAD"), product, lane.SessionID, run.RunID, run.State, run.Result.Outcome, run.Result.Result)
			must(t, parent.call("turn.ack", protocol.RunRef{SessionID: run.SessionID, RunID: run.RunID}, &struct{}{}))
			must(t, parent.call("session.close", protocol.SessionCloseRequest{SessionID: lane.SessionID, Forget: true}, &struct{}{}))
		})
	}
}

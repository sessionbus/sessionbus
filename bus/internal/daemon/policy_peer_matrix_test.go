// SPDX-License-Identifier: GPL-3.0-only
package daemon

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
	"github.com/antst/sessionbus/bus/sdk/go/testsocket"
)

// Compiled unchanged against exact old/new SDKs and daemon source. Private
// fixtures only: they neither install nor call the host's real daemon.
func TestPolicySDKPeerProcess(t *testing.T) {
	socket := os.Getenv("POLICY_MATRIX_PEER_SOCKET")
	if socket == "" {
		t.Skip("external peer helper")
	}
	peer := connectPeer(t, socket, "external-peer", "external-peer", "matrix")
	var listed protocol.SessionListResult
	must(t, peer.call("session.list", protocol.SessionListRequest{}, &listed))
	if len(listed.Sessions) != 1 || listed.Sessions[0].SessionID != "external-peer@local" {
		t.Fatal("old/new SDK peer could not list itself", listed)
	}
	t.Logf("sdk=%s daemon=%s peer Info=%v", os.Getenv("POLICY_MATRIX_SDK"), os.Getenv("MATRIX_DAEMON_HEAD"), listed.Sessions[0].Info)
}

func TestPolicyExecutableSDKPeerVersionMatrix(t *testing.T) {
	products := os.Getenv("MATRIX_PRODUCT_PATH")
	if products == "" {
		t.Skip("external version matrix")
	}
	dir := testsocket.Directory(t)
	socket := filepath.Join(dir, "daemon.sock")
	d, err := Start(Config{SocketPath: socket, TablePath: filepath.Join(dir, "rows")})
	must(t, err)
	defer d.Close()
	for _, version := range []string{"old", "new"} {
		t.Run(version, func(t *testing.T) {
			cmd := exec.Command(filepath.Join(products, version+"-go-fixture"), "-test.run=^TestPolicySDKPeerProcess$", "-test.v", "-test.timeout=5s")
			cmd.Env = append(os.Environ(), "POLICY_MATRIX_PEER_SOCKET="+socket, "POLICY_MATRIX_SDK="+version)
			data, err := cmd.CombinedOutput()
			t.Logf("%s", data)
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

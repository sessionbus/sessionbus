// SPDX-License-Identifier: GPL-3.0-only
package daemon_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/daemon"
	sessionkit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/antst/sessionbus/bus/sdk/go/testsocket"
)

func TestTransferJavaScriptAndGoSDKsInteroperateThroughPrivateDaemon(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("existing JavaScript SDK runtime unavailable")
	}
	fixture, err := filepath.Abs("../../sdk/js/transfer-fixture.cjs")
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"custom", "default"} {
		for _, jsRole := range []string{"source", "destination"} {
			t.Run(mode+"/"+jsRole, func(t *testing.T) {
				dir := testsocket.Directory(t)
				socket := filepath.Join(dir, "bus.sock")
				d, err := daemon.Start(daemon.Config{SocketPath: socket, TablePath: filepath.Join(dir, "sessions")})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = d.Close() })
				sourcePath, destinationPath := filepath.Join(dir, "private-source"), filepath.Join(dir, "private-destination")
				data := bytes.Repeat([]byte("mixed SDK frame/hash/publication\n"), 7000)
				if err = os.WriteFile(sourcePath, data, 0600); err != nil {
					t.Fatal(err)
				}
				executor := &fixtureTransferExecutor{}
				ownPath := sourcePath
				kind := "offer"
				if jsRole == "source" {
					executor.destination = destinationPath
				} else {
					executor.source = sourcePath
					ownPath = destinationPath
					kind = "request"
				}
				var selected sessionkit.TransferExecutor = executor
				if mode == "default" {
					selected = nil
				}
				peer, notices := transferPeer(t, socket, "go", selected)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				command := exec.CommandContext(ctx, node, fixture, socket, jsRole, ownPath, mode)
				var stdout, stderr bytes.Buffer
				command.Stdout = &stdout
				command.Stderr = &stderr
				if err = command.Start(); err != nil {
					t.Fatal(err)
				}
				// Only this exact private SDK child is owned by the fixture; CommandContext
				// bounds it if the integration fails. No product/native CLI is launched.
				waited := make(chan error, 1)
				go func() { waited <- command.Wait() }()
				var id string
				select {
				case notice := <-notices:
					if notice.Kind != kind {
						t.Fatal(notice)
					}
					id = notice.ID
				case err = <-waited:
					t.Fatal("JS exited before pending notice", err, stderr.String())
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				acceptPath := destinationPath
				if jsRole == "destination" {
					acceptPath = sourcePath
				}
				if _, err = peer.Transfers.Accept(ctx, id, acceptPath, false); err != nil {
					t.Fatal(err)
				}
				terminal := awaitTransferNotice(t, notices, id, "terminal")
				if terminal.Outcome != "completed" {
					t.Fatal(terminal)
				}
				if err = <-waited; err != nil {
					t.Fatal("JS SDK", err, stderr.String())
				}
				actual, err := os.ReadFile(destinationPath)
				if err != nil || !bytes.Equal(actual, data) {
					t.Fatal("mixed SDK byte mismatch", len(actual), err)
				}
				var result struct {
					Role, Outcome string
					Bytes         int
				}
				if err = json.Unmarshal(stdout.Bytes(), &result); err != nil || result.Outcome != "completed" || result.Bytes != len(data) {
					t.Fatal(stdout.String(), err)
				}
			})
		}
	}
}

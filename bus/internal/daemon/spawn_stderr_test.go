// SPDX-License-Identifier: GPL-3.0-only

package daemon

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

const stderrOpenLines = 25

// The stderr-open-worker fixture writes stderr lines, answers session.open as
// STDERR_OPEN_MODE selects, and writes its last line only after the daemon has
// closed the worker connection. It ignores the orderly SIGTERM and exits 3.
func init() {
	name := filepath.Base(os.Args[0])
	if !strings.HasPrefix(name, "stderr-open-worker") {
		return
	}
	signal.Ignore(syscall.SIGTERM)
	if err := runStderrOpenWorker(name); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(8)
	}
	os.Exit(3)
}

func runStderrOpenWorker(product string) error {
	fd, err := net.Dial("unix", os.Getenv("SESSIONBUS_SOCKET"))
	if err != nil {
		return err
	}
	defer fd.Close()
	reader := bufio.NewReaderSize(fd, protocol.MaxFrameBytes)
	hello := protocol.WorkerHello{Protocol: 1, LaunchToken: os.Getenv("SESSIONBUS_LAUNCH_TOKEN"), HelloDescription: protocol.HelloDescription{SupportsMessageRun: true, Product: product, SupportedOpenFields: []string{}, ExtraArguments: []protocol.ExtraArgument{}}}
	if err = rawCall(fd, reader, 1, "session.hello", hello, &struct{}{}); err != nil {
		return err
	}
	open, err := readRawFrame(reader)
	if err != nil || open.Method != "session.open" {
		return fmt.Errorf("open request: %v (%q)", err, open.Method)
	}
	for line := 1; line < stderrOpenLines; line++ {
		fmt.Fprintf(os.Stderr, "worker stderr %02d\n", line)
	}
	var body []byte
	switch mode := os.Getenv("STDERR_OPEN_MODE"); mode {
	case "invalid-id":
		body, err = protocol.ResultBytes(open.ID, open.Method, protocol.OpenResult{SessionID: "invalid id"})
	default:
		reported := protocol.SpawnFailedData{StderrTail: []string{"worker rejected open"}}
		if mode == "exit-code" {
			code := 42
			reported.ExitCode = &code
		}
		body, err = protocol.ErrorBytes(open.ID, protocol.SpawnFailed, reported)
	}
	if err == nil {
		_, err = fd.Write(body)
	}
	if err != nil {
		return err
	}
	_, _ = io.Copy(io.Discard, reader)
	fmt.Fprintf(os.Stderr, "worker stderr %02d after close\n", stderrOpenLines)
	return nil
}

func TestWorkerSpawnFailedPrependsExitedWorkerStderr(t *testing.T) {
	laneGapInstall(t, "stderr-open-worker")
	_, socket := startDaemon(t)
	parent := connectPeer(t, socket, "parent", "parent", "team")
	// The daemon's tail keeps the last 20 lines. The last one exists only after
	// the daemon closed the worker, so the answer waited for the worker's exit.
	var captured []string
	for line := stderrOpenLines - 19; line < stderrOpenLines; line++ {
		captured = append(captured, fmt.Sprintf("worker stderr %02d", line))
	}
	want := append(captured, fmt.Sprintf("worker stderr %02d after close", stderrOpenLines), "worker rejected open")
	for _, c := range []struct {
		mode string
		exit int
	}{{"observed-exit", 3}, {"exit-code", 42}} {
		t.Setenv("STDERR_OPEN_MODE", c.mode)
		data := laneGapSpawnFailure(t, parent.call("lane.spawn", protocol.LaneSpawnRequest{Name: c.mode, Product: "stderr-open-worker", Open: &protocol.OpenOptions{}}, nil))
		if !slices.Equal(data.StderrTail, want) || data.ExitCode == nil || *data.ExitCode != c.exit {
			t.Fatalf("%s failure data = %q exit %v", c.mode, data.StderrTail, data.ExitCode)
		}
	}
}

func TestDaemonSpawnFailureIsNotMergedAgain(t *testing.T) {
	laneGapInstall(t, "stderr-open-worker")
	t.Setenv("STDERR_OPEN_MODE", "invalid-id")
	_, socket := startDaemon(t)
	parent := connectPeer(t, socket, "parent", "parent", "team")
	// failure() already read the live worker's details when the daemon rejected
	// the id; finishLane must not add the exited worker's stderr to it.
	data := laneGapSpawnFailure(t, parent.call("lane.spawn", protocol.LaneSpawnRequest{Name: "invalid", Product: "stderr-open-worker", Open: &protocol.OpenOptions{}}, nil))
	if !slices.Equal(data.StderrTail, []string{"worker returned an invalid session id"}) || data.ExitCode != nil {
		t.Fatalf("daemon failure data = %q exit %v", data.StderrTail, data.ExitCode)
	}
}

// SPDX-License-Identifier: GPL-3.0-only

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/internal/roster"
	"github.com/antst/sessionbus/bus/sdk/go/testsocket"
)

// This test intentionally uses only APIs present on the parent commit so its
// identical bytes also prove that the old executable lacks the command.
func TestArchiveCommandAgainstOperator(t *testing.T) {
	for _, variant := range []string{"closed", "offline", "busy", "unknown", "old-daemon", "invalid-response", "wrong-target"} {
		t.Run(variant, func(t *testing.T) {
			socket := filepath.Join(testsocket.Directory(t), "presence.sock")
			listener, err := net.Listen("unix", roster.Socket(socket))
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			if err := listener.(*net.UnixListener).SetDeadline(time.Now().Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			served := make(chan error, 1)
			go func() {
				fd, err := listener.Accept()
				if err != nil {
					served <- err
					return
				}
				defer fd.Close()
				_ = fd.SetDeadline(time.Now().Add(time.Second))
				var request map[string]any
				if err = json.NewDecoder(fd).Decode(&request); err != nil {
					served <- err
					return
				}
				if len(request) != 2 || request["action"] != "lane.archive" || request["session_id"] != "native@local" {
					served <- fmt.Errorf("wrong request: %#v", request)
					return
				}
				reply := `{"schema":"sessionbus.lane-archive.v1","session_id":"native@local","state":"offline","changed":true}`
				switch variant {
				case "offline":
					reply = strings.Replace(reply, `"changed":true`, `"changed":false`, 1)
				case "busy":
					reply = `{"schema":"sessionbus.lane-archive.v1","session_id":"native@local","changed":false,"error":"busy"}`
				case "unknown":
					reply = `{"schema":"sessionbus.lane-archive.v1","session_id":"native@local","state":"unknown","changed":false,"error":"archive_timeout"}`
				case "old-daemon":
					served <- nil
					return
				case "invalid-response":
					reply = `{"schema":"sessionbus.roster.v1","complete":true}`
				case "wrong-target":
					reply = strings.Replace(reply, "native@local", "other@local", 1)
				}
				_, err = fmt.Fprintln(fd, reply)
				served <- err
			}()
			var output bytes.Buffer
			err = runTo([]string{"lane", "archive", "--socket", socket, "--timeout", "2s", "native@local"}, &output)
			if serveErr := <-served; serveErr != nil {
				t.Fatalf("operator: %v; command: %v; output=%s", serveErr, err, output.String())
			}
			wantSuccess := variant == "closed" || variant == "offline"
			if (err == nil) != wantSuccess {
				t.Fatalf("error=%v output=%s", err, output.String())
			}
			if wantSuccess && !strings.Contains(output.String(), `"state":"offline"`) {
				t.Fatal(output.String())
			}
			if (variant == "old-daemon" || variant == "invalid-response") && !strings.Contains(err.Error(), "unknown") {
				t.Fatal(err)
			}
		})
	}
}

func TestArchiveCommandHelpAndArguments(t *testing.T) {
	t.Setenv("SESSIONBUS_HUB_SECRET_FILE", "/not-present/private-secret")
	for _, args := range [][]string{{"help", "lane", "archive"}, {"lane", "archive", "--help"}} {
		var output bytes.Buffer
		if err := runTo(args, &output); err != nil || !strings.Contains(output.String(), "resume recipe") {
			t.Fatal(args, err, output.String())
		}
	}
	for _, args := range [][]string{{"lane"}, {"lane", "other"}, {"lane", "archive"}, {"lane", "archive", "a", "b"}, {"lane", "archive", "--timeout", "0", "native"}, {"lane", "archive", "--unknown"}} {
		if err := runTo(args, &bytes.Buffer{}); err == nil {
			t.Fatal("accepted invalid arguments", args)
		}
	}
}

func TestArchiveCommandTimeoutDoesNotRetry(t *testing.T) {
	socket := filepath.Join(testsocket.Directory(t), "presence.sock")
	listener, err := net.Listen("unix", roster.Socket(socket))
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	release := make(chan struct{})
	served := make(chan error, 1)
	go func() {
		fd, err := listener.Accept()
		if err != nil {
			served <- err
			return
		}
		defer fd.Close()
		var request map[string]any
		err = json.NewDecoder(fd).Decode(&request)
		<-release
		served <- err
	}()
	err = runTo([]string{"lane", "archive", "--socket", socket, "--timeout", "50ms", "native"}, &bytes.Buffer{})
	close(release)
	if serveErr := <-served; serveErr != nil {
		t.Fatal(serveErr)
	}
	if err == nil || !strings.Contains(err.Error(), "outcome unknown") {
		t.Fatal(err)
	}
	if err := listener.(*net.UnixListener).SetDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if fd, err := listener.Accept(); err == nil {
		_ = fd.Close()
		t.Fatal("archive retried after timeout")
	}
}

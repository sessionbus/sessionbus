// SPDX-License-Identifier: GPL-3.0-only

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/antst/sessionbus/bus/internal/roster"
	sessionkit "github.com/antst/sessionbus/bus/sdk/go"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func runLane(arguments []string, output io.Writer) error {
	if len(arguments) == 0 || arguments[0] != "archive" {
		return errors.New("usage: sessionbus lane archive [--socket PATH] [--timeout 15s] <local-session-id>")
	}
	set := flag.NewFlagSet("sessionbus lane archive", flag.ContinueOnError)
	set.SetOutput(output)
	socket := set.String("socket", sessionkit.Socket(), "daemon's public Unix socket path")
	timeout := set.Duration("timeout", 15*time.Second, "total deadline; an admitted close continues if this expires")
	set.Usage = func() {
		fmt.Fprintln(output, "Usage: sessionbus lane archive [flags] <local-session-id>\n\nStop a local managed lane, including an ownerless persistent lane, and retain its\nnative history and resume recipe. No model session or group membership is needed;\naccess uses the same-user operator socket. Already-offline lanes succeed.\nThe offline result is a snapshot; another caller may resume the ID afterwards.\nNormal nonpersistent-child cleanup follows, but success joins only this wrapper.\nA timeout leaves the outcome unknown; do not infer that closing was cancelled.")
		set.PrintDefaults()
	}
	if err := set.Parse(arguments[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if set.NArg() != 1 || set.Arg(0) == "" || *timeout <= 0 {
		return errors.New("archive requires one local session ID and a positive timeout")
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	fd, err := (&net.Dialer{}).DialContext(ctx, "unix", roster.Socket(*socket))
	if err != nil {
		return fmt.Errorf("connect operator archive: %w", err)
	}
	defer fd.Close()
	deadline, _ := ctx.Deadline()
	_ = fd.SetDeadline(deadline)
	input := roster.ArchiveRequest{Action: roster.ArchiveAction, SessionID: set.Arg(0)}
	if err = json.NewEncoder(fd).Encode(input); err != nil {
		return fmt.Errorf("archive outcome unknown (no retry): %w", err)
	}
	body, err := bufio.NewReaderSize(fd, protocol.MaxFrameBytes).ReadSlice('\n')
	if err != nil {
		return fmt.Errorf("archive outcome unknown (daemon may be unsupported or close may continue): %w", err)
	}
	var result roster.ArchiveResult
	if protocol.DecodeJSON(body, &result) != nil || !validArchiveResult(input.SessionID, result) {
		return errors.New("archive outcome unknown: invalid operator archive response")
	}
	if err = json.NewEncoder(output).Encode(result); err != nil {
		return err
	}
	if result.Error != "" {
		return fmt.Errorf("archive: %s", result.Error)
	}
	return nil
}

func validArchiveResult(target string, result roster.ArchiveResult) bool {
	if result.Schema != roster.ArchiveSchema || result.State != "" && result.State != "offline" && result.State != "unknown" {
		return false
	}
	if result.Error != "" {
		return !result.Changed && result.State != "offline"
	}
	if result.State != "offline" || result.SessionID == "" {
		return false
	}
	if !strings.Contains(target, "@") {
		at := strings.LastIndexByte(result.SessionID, '@')
		return at > 0 && result.SessionID[:at] == target && at < len(result.SessionID)-1
	}
	return result.SessionID == target
}

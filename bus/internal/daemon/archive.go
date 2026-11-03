// SPDX-License-Identifier: GPL-3.0-only

package daemon

import (
	"context"
	"encoding/json"
	"net"
	"time"

	"github.com/antst/sessionbus/bus/internal/roster"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func (d *Daemon) serveArchive(fd net.Conn, raw []byte) {
	input, err := roster.DecodeArchive(raw)
	result := roster.ArchiveResult{Schema: roster.ArchiveSchema, Error: "invalid_request"}
	if err == nil {
		_ = fd.SetDeadline(time.Now().Add(13 * time.Second))
		ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
		defer cancel()
		result = d.operatorArchive(ctx, input)
	}
	_ = json.NewEncoder(fd).Encode(result)
}

func (d *Daemon) operatorArchive(ctx context.Context, input roster.ArchiveRequest) roster.ArchiveResult {
	result := roster.ArchiveResult{Schema: roster.ArchiveSchema}
	id, host, code := canonicalTarget(input.SessionID, d.host, validIDPart)
	if code != 0 {
		result.Error = "invalid_request"
		return result
	}
	result.SessionID = id
	if host != d.host {
		result.Error = "local_only"
		return result
	}
	reply := make(chan answer, 1)
	request := routedRequest{method: "session.close", params: &protocol.SessionCloseRequest{SessionID: id}, reply: reply}
	d.directory.mu.Lock()
	item := d.directory.entries[id]
	switch {
	case ctx.Err() != nil:
		result.State, result.Error = "unknown", "archive_timeout"
	case d.directory.closing:
		result.Error = "daemon_closing"
	case item == nil:
		result.Error = "unknown_session"
	case item.peer:
		result.Error = "not_managed_lane"
	case item.claimed:
		// A detached claimed row can still own a process or a pending resume.
		result.Error = "busy"
	case item.attachment == nil:
		result.State = "offline"
	default:
		code = d.directory.routeLocked(item, request.method, request)
		if code != 0 {
			result.Error = archiveError(code)
		}
	}
	d.directory.mu.Unlock()
	if result.Error != "" || result.State != "" {
		return result
	}
	select {
	case answer := <-reply:
		if answer.code != 0 {
			result.Error = archiveError(answer.code)
		} else {
			// finishLane sends this buffered reply after process join/offline.
			// Do not reselect: another caller may already have resumed the ID.
			result.State, result.Changed = "offline", true
		}
	case <-ctx.Done():
		result.State, result.Error = "unknown", "archive_timeout"
	case <-d.shutdown:
		result.State, result.Error = "unknown", "daemon_closing"
	}
	return result
}

func archiveError(code int) string {
	switch code {
	case protocol.Busy:
		return "busy"
	case protocol.NotConnected:
		return "not_connected"
	case protocol.UnknownSession:
		return "unknown_session"
	default:
		return "archive_failed"
	}
}

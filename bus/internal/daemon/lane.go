// SPDX-License-Identifier: GPL-3.0-only

package daemon

import (
	"encoding/json"
	"syscall"
	"time"

	"github.com/antst/sessionbus/bus/internal/commslog"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

const closeBound = 10 * time.Second

// workerSpawnFailed marks a spawn_failed that the worker itself returned from
// open. Its stderr is complete only after exit, so finishLane adds it there;
// failures built with failure() are never merged again.
type workerSpawnFailed protocol.SpawnFailedData

func (s *session) startLane(start *launch, helloFrame protocol.Frame, hello protocol.HelloDescription) {
	s.launch, s.identity, s.owned = start, start.entry, 1
	unsupportedField := unsupported(start.entry.row.Open, hello.SupportedOpenFields)
	if !start.describe && !hello.SupportsMessageRun {
		unsupportedField = "supports_message_run"
	}
	if start.describe || unsupportedField != "" {
		body, _ := protocol.ResultBytes(helloFrame.ID, helloFrame.Method, struct{}{})
		s.wire.Finish(body, supersedeWriteBound)
		result := answer{value: &hello}
		if unsupportedField != "" {
			result = answer{code: protocol.UnsupportedOpen}
		}
		s.launchResult, s.stopping = &result, true
		return
	}
	s.result(helloFrame, struct{}{})
	s.beginOpen()
}

func (s *session) beginOpen() {
	s.nextOut++
	value := s.launch.entry.row
	resume := ""
	if !s.launch.fresh {
		resume = unqualify(value.SessionID)
	}
	request := protocol.OpenRequest{
		Name: value.Name, Groups: append([]string(nil), value.Groups...),
		ResumeSessionID: resume, Open: value.Open, Policy: value.Policy,
	}
	body, err := protocol.RequestBytes(s.nextOut, "session.open", request)
	if err != nil || !s.wire.Send(body) {
		s.abortLaunch(answer{code: protocol.SpawnFailed, data: failure(s.child, "worker closed before open")})
		s.hardStop()
		return
	}
	s.openID = s.nextOut
}

func (s *session) finishOpen(frame protocol.Frame) {
	s.openID = 0
	if frame.Error != nil {
		result := errorAnswer(frame.Error)
		var reported workerSpawnFailed
		if result.code == protocol.SpawnFailed && json.Unmarshal(frame.Error.Data, &reported) == nil {
			result.data = reported
		}
		s.abortLaunch(result)
		s.orderlyStop()
		return
	}
	decoded, err := protocol.DecodeResult("session.open", frame.Result)
	if err != nil {
		s.abortLaunch(answer{code: protocol.SpawnFailed, data: failure(s.child, "invalid open result")})
		s.hardStop()
		return
	}
	opened := decoded.(*protocol.OpenResult)
	item := s.launch.entry
	if !validIDPart(opened.SessionID) || !s.launch.fresh && opened.SessionID != unqualify(item.row.SessionID) {
		s.abortLaunch(answer{code: protocol.SpawnFailed, data: failure(s.child, "worker returned an invalid session id")})
		s.orderlyStop()
		return
	}
	if s.launch.fresh {
		item.row.SessionID = qualify(opened.SessionID, s.daemon.host)
		if !s.daemon.directory.reserveID(item, item.row.SessionID) {
			s.abortLaunch(answer{code: protocol.SpawnFailed, data: failure(s.child, "session id already exists")})
			s.orderlyStop()
			return
		}
		persisted := cloneRow(item.row)
		persisted.CreatedAt = time.Now().UTC()
		if err = s.daemon.table.write(persisted); err != nil {
			s.abortLaunch(answer{code: protocol.Internal, data: err.Error()})
			s.orderlyStop()
			return
		}
		if !s.daemon.directory.publish(s.launch, s, persisted.CreatedAt) {
			_ = s.daemon.table.delete(item.row.SessionID)
			s.abortLaunch(answer{code: protocol.Internal, data: "session publication failed"})
			s.orderlyStop()
			return
		}
	} else if err := s.daemon.table.write(item.row); err != nil {
		s.abortLaunch(answer{code: protocol.Internal, data: err.Error()})
		s.orderlyStop()
		return
	} else if !s.daemon.directory.publish(s.launch, s, time.Time{}) {
		if restoreErr := s.daemon.table.write(s.launch.previous.row); restoreErr != nil {
			s.abortLaunch(answer{code: protocol.Internal, data: restoreErr.Error()})
		} else {
			s.abortLaunch(answer{code: protocol.AlreadyConnected})
		}
		s.orderlyStop()
		return
	}
	s.identity, s.committed = item, true
	s.logSession(commslog.Connected)
	s.launch.timer.Stop()
	s.launch.reply <- answer{value: s.daemon.directory.spawnResult(s.launch, s)}
}

func (s *session) beginClose(request routedRequest) {
	s.stopAutoClose()
	s.settleDeferredDeliveries(protocol.Busy)
	s.closeCall = &request
	s.closeTimer = time.NewTimer(closeBound)
	s.forget = request.params.(*protocol.SessionCloseRequest).Forget
	s.writeDeferredClose()
}

// A blocking run already owns a read. Install it after execute admission and
// before writing close, while the original close deadline keeps running.
func (s *session) writeDeferredClose() {
	if s.closeCall == nil || s.closed || s.stopping {
		return
	}
	for _, pending := range s.pending {
		if pending.method == "session.close" || pending.method == "turn.execute" && pending.collect {
			return
		}
	}
	request := *s.closeCall
	s.nextOut++
	body, err := protocol.RequestBytes(s.nextOut, "session.close", request.params)
	if err != nil || !s.wire.Send(body) {
		s.hardStop()
		return
	}
	s.daemon.directory.admitted(s.identity, request.method)
	s.pending[s.nextOut] = request
}

func (s *session) finishCloseResponse(frame protocol.Frame) {
	if frame.Error != nil {
		s.wire.Close()
		return
	}
	if _, err := protocol.DecodeResult("session.close", frame.Result); err != nil {
		s.wire.Close()
		return
	}
	s.orderlyStop()
}

func (s *session) handleProcess(event processEvent) {
	if event.launch != s.launch {
		return
	}
	if event.child != nil && !event.exited {
		s.child = event.child
		if s.stopSignal != 0 {
			s.child.Signal(s.stopSignal)
		}
		return
	}
	if s.processExited {
		return
	}
	s.processExited = true
	if s.owned > 0 {
		s.owned--
	}
	if s.committed && !s.closed && s.closeTimer == nil {
		s.closeTimer = time.NewTimer(closeBound)
	}
	if event.err != nil && s.launchResult == nil && !s.committed {
		s.abortLaunch(answer{code: protocol.SpawnFailed, data: failure(nil, event.err.Error())})
	}
}

func (s *session) abortLaunch(value answer) {
	if s.launchResult != nil || s.committed {
		return
	}
	s.launchResult = &value
}

func (s *session) orderlyStop() {
	s.recordStop(syscall.SIGTERM)
	if !s.processExited && s.closeTimer == nil {
		s.closeTimer = time.NewTimer(closeBound)
	}
}

func (s *session) hardStop() {
	s.recordStop(syscall.SIGKILL)
}

func (s *session) recordStop(signal syscall.Signal) {
	s.stopping = true
	if s.stopSignal == 0 || signal == syscall.SIGKILL {
		s.stopSignal = signal
	}
	s.wire.Close()
	if s.child != nil {
		s.child.Signal(s.stopSignal)
	}
}

func (s *session) finishLane() {
	s.stopAutoClose()
	s.launch.timer.Stop()
	if s.closeTimer != nil {
		s.closeTimer.Stop()
	}
	if !s.committed {
		s.daemon.directory.releaseLaunch(s.launch)
	}
	if s.launchResult != nil {
		if reported, ok := s.launchResult.data.(workerSpawnFailed); ok {
			s.launchResult.data = withStderr(s.child, protocol.SpawnFailedData(reported))
		}
		s.launch.reply <- *s.launchResult
	}
	if s.committed {
		if s.forget {
			if err := s.daemon.table.delete(s.identity.row.SessionID); err != nil {
				s.closeAnswer = answer{code: protocol.Internal, data: err.Error()}
				s.forget = false
			}
		}
		s.daemon.directory.offline(s.identity, s, s.forget)
	}
	if s.closeCall != nil {
		if s.closeAnswer.code == 0 && s.closeAnswer.value == nil {
			s.closeAnswer.value = struct{}{}
		}
		s.closeCall.reply <- s.closeAnswer
	}
}

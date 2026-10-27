// SPDX-License-Identifier: GPL-3.0-only

package federation

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/antst/sessionbus/bus/internal/conn"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

type IncomingCall struct {
	From    Caller
	Request PublicRequest
}

type Wait func(<-chan struct{}) (Reply, bool)

type OutgoingCall struct {
	Value Forward
	Reply chan Reply
}

type HostsCall struct{ Reply chan Reply }

type daemonReply struct {
	release func()
	id      int64
	value   Reply
}

type pendingCall struct {
	transfer, bulk bool
	method         string
	reply          chan Reply
	trace          bool
	target         string
}

func ServeDaemon(ctx context.Context, host string, fd net.Conn, inbox chan any, admit func(IncomingCall) (Wait, error), stderr io.Writer, lifetime ...func(LifetimeEvent)) error {
	return ServeDaemonWithTransfers(ctx, host, fd, inbox, admit, stderr, TransferServices{}, lifetime...)
}

// TransferServices are bound to this one negotiated connection, never reused
// by reconnect. Bulk inputs and endpoint callbacks have separate bounded paths.
type TransferServices struct {
	Server TransferServer
	Bulk   <-chan TransferCall
	Gate   func(protocol.Frame) bool
}

func ServeDaemonWithTransfers(ctx context.Context, host string, fd net.Conn, inbox chan any, admit func(IncomingCall) (Wait, error), stderr io.Writer, services TransferServices, lifetime ...func(LifetimeEvent)) error {
	var group, helpers sync.WaitGroup
	wire := conn.Start(fd, inbox, &group)
	machineContext, cancelMachine := context.WithCancel(ctx)
	go func() {
		select {
		case <-wire.Done():
			cancelMachine()
		case <-machineContext.Done():
		}
	}()
	if services.Gate != nil {
		wire.SetBulkGate(services.Gate)
	}
	traceCapable, wakeCapable := SupportsTrace(fd), SupportsWake(fd)
	lastIn := int64(0)
	queued := map[*pendingCall]bool{}
	pending := map[int64]pendingCall{}
	stopping := ctx.Done()
	var cause error
	defer func() {
		cancelMachine()
		wire.Close()
		group.Wait()
		helpers.Wait()
	}()

	for {
		var raw any
		select {
		case <-stopping:
			wire.Close()
			stopping, cause = nil, ctx.Err()
			continue
		case event := <-wire.Dispatched():
			installCall(event, queued, pending)
			continue
		case event := <-wire.Bulk():
			raw = event
		case event := <-services.Bulk:
			raw = event
		case raw = <-inbox:
		}
		switch event := raw.(type) {
		case TransferCall:
			if cause != nil || !SupportsTransfer(fd) || !queueTransferCall(wire, queued, pending, event) {
				event.Reply <- errorReply(protocol.Internal, "transfer_unavailable")
			}
		case OwnerEndCall:
			if callCount(pending, queued, false) >= protocol.MaxOperations {
				event.Reply <- errorReply(protocol.ForwardLost, nil)
				wire.Close()
				continue
			}
			if cause != nil || !queueCall(wire, queued, pendingCall{method: ownerEndMethod, reply: event.Reply}, false, func(id int64) ([]byte, error) { return requestBytes(id, ownerEndMethod, event.Value) }) {
				event.Reply <- errorReply(protocol.ForwardLost, nil)
				wire.Close()
				continue
			}
		case OutgoingCall:
			if callCount(pending, queued, false) >= protocol.MaxOperations {
				event.Reply <- errorReply(protocol.ForwardLost, nil)
				continue
			}
			request := event.Value.Request
			if request.Completion && !wakeCapable {
				event.Reply <- errorReply(protocol.ForwardLost, nil)
				continue
			}
			if requestRequiresTrace(request) && !traceCapable {
				event.Reply <- traceUnsupported()
				continue
			}
			if request.Trace && !traceCapable {
				request.Trace = false
				event.Value.Request = request
			}
			_, forwarded, err := forwardRequestBytes(1, event.Value)
			event.Value, request = forwarded, forwarded.Request
			target, targetErr := targetHost(request)
			if cause != nil || targetErr != nil || err != nil || !queueCall(wire, queued, pendingCall{method: request.Method, reply: event.Reply, trace: request.Trace, target: target}, false, func(id int64) ([]byte, error) { body, _, err := forwardRequestBytes(id, forwarded); return body, err }) {
				code, data := protocol.ForwardLost, any(nil)
				if targetErr != nil || err != nil {
					code, data = protocol.InvalidFrame, "forwarded request exceeds the frame limit"
				}
				event.Reply <- errorReply(code, data)
				if targetErr == nil && err == nil {
					wire.Close()
					cause = errFrame
				}
				continue
			}
		case RosterCall:
			if cause != nil || !queueCall(wire, queued, pendingCall{method: rosterMethod, reply: event.Reply}, false, func(id int64) ([]byte, error) { return requestBytes(id, rosterMethod, struct{}{}) }) {
				event.Reply <- errorReply(protocol.ForwardLost, nil)
				continue
			}
		case HostsCall:
			if cause != nil || !queueCall(wire, queued, pendingCall{method: hostsMethod, reply: event.Reply}, false, func(id int64) ([]byte, error) { return requestBytes(id, hostsMethod, struct{}{}) }) {
				event.Reply <- errorReply(protocol.ForwardLost, nil)
				wire.Close()
				cause = errFrame
				continue
			}
		case daemonReply:
			if event.release != nil {
				event.release()
			}
			body, err := responseBytes(event.id, event.value, true)
			if cause == nil && (err != nil || !wire.Send(body)) {
				cause = errFrame
				wire.Close()
			}
		case conn.Frame:
			wire.DrainDispatched(func(d conn.Dispatched) { installCall(d, queued, pending) })
			if cause == nil && event.Value.Request && isTransferFrame(event.Value.Method) {
				if !SupportsTransfer(fd) || services.Server == nil || !endpointTransferControl(event.Value.Method) {
					cause = errFrame
					wire.Close()
					if event.Release != nil {
						event.Release()
					}
					continue
				}
				if event.Err == conn.ErrBulkCapacity {
					body, _ := ResponseBytes(event.Value.ID, errorReply(protocol.Busy, nil))
					_ = wire.Send(body)
					continue
				}
				if event.Err != nil {
					cause = event.Err
					wire.Close()
					if event.Release != nil {
						event.Release()
					}
					continue
				}
				if _, err := decodeTransferParams(event.Value.Method, event.Value.Params); err != nil {
					cause = err
					wire.Close()
					if event.Release != nil {
						event.Release()
					}
					continue
				}
				helpers.Add(1)
				go func(frame conn.Frame) {
					defer helpers.Done()
					result, err := services.Server(machineContext, frame.Value.Method, frame.Value.Params)
					reply := Reply{Result: result}
					if err != nil {
						reply = errorReply(protocol.Internal, "transfer_endpoint_unavailable")
					} else if !validateTransferResult(frame.Value.Method, result) {
						reply = errorReply(protocol.Internal, "invalid_transfer_result")
					}
					post := daemonReply{id: frame.Value.ID, value: reply, release: frame.Release}
					select {
					case inbox <- post:
					case <-wire.Done():
						if frame.Release != nil {
							frame.Release()
						}
					}
				}(event)
				continue
			}
			if cause == nil {
				cause = daemonFrame(host, traceCapable, wakeCapable, event, pending, wire, inbox, admit, &helpers, &lastIn, lifetime)
				if cause != nil {
					fmt.Fprintln(stderr, cause)
					wire.Close()
				}
			}
		case conn.Closed:
			wire.DrainDispatched(func(d conn.Dispatched) { installCall(d, queued, pending) })
			settleQueuedCalls(queued)
			settlePending(pending)
			if cause != nil {
				return cause
			}
			return event.Cause
		}
	}
}

func daemonFrame(host string, traceCapable, wakeCapable bool, event conn.Frame, pending map[int64]pendingCall, wire *conn.Conn, inbox chan any, admit func(IncomingCall) (Wait, error), helpers *sync.WaitGroup, lastIn *int64, lifetime []func(LifetimeEvent)) error {
	frame := event.Value
	if event.Err != nil {
		return event.Err
	}
	if !frame.Request {
		call, ok := pending[frame.ID]
		if !ok {
			return errFrame
		}
		reply, valid := decodePendingReply(call, frame)
		if !valid {
			return errFrame
		}
		delete(pending, frame.ID)
		call.reply <- reply
		return nil
	}
	if !event.Ordered && frame.ID <= *lastIn {
		return errFrame
	}
	if frame.Method == rosterMethod {
		*lastIn = max(*lastIn, frame.ID)
		var request map[string]json.RawMessage
		if protocol.DecodeJSON(frame.Params, &request) != nil || request == nil || len(request) != 0 {
			return errFrame
		}
		wait, err := admit(IncomingCall{Request: PublicRequest{Method: rosterMethod, Params: frame.Params}})
		if err != nil {
			return err
		}
		value, ok := wait(wire.Done())
		if !ok {
			return errFrame
		}
		body, err := ResponseBytes(frame.ID, value)
		if err != nil || !wire.Send(body) {
			return errFrame
		}
		return nil
	}
	if frame.Method == ownerEndMethod || frame.Method == hostEndMethod {
		*lastIn = max(*lastIn, frame.ID)
		event := LifetimeEvent{}
		if frame.Method == ownerEndMethod {
			var value OwnerEnd
			if protocol.DecodeJSON(frame.Params, &value) != nil || value.Host != host || !validHost(value.Source) || !ownedPart(value.SessionID, value.Source, false) || value.Lifetime == "" || value.Attachment == "" {
				return errFrame
			}
			event.Owner = &value
		} else {
			var value HostEnd
			if protocol.DecodeJSON(frame.Params, &value) != nil || !validHost(value.Host) || value.Attachment == "" {
				return errFrame
			}
			event.Host = &value
		}
		for _, apply := range lifetime {
			apply(event)
		}
		body, _ := ResponseBytes(frame.ID, emptyReply())
		if !wire.Send(body) {
			return errFrame
		}
		return nil
	}
	if frame.Method != forwardMethod || !event.Ordered && frame.ID <= *lastIn {
		return errFrame
	}
	*lastIn = max(*lastIn, frame.ID)
	source, err := sourceHost(frame.Params)
	if err != nil {
		return err
	}
	value, target, err := decodeForward(frame.Params, source)
	if err != nil || target != host {
		return errFrame
	}
	if value.Request.Completion && !wakeCapable {
		body, responseErr := responseBytes(frame.ID, errorReply(protocol.ForwardLost, nil), true)
		if responseErr != nil || !wire.Send(body) {
			return errFrame
		}
		return nil
	}
	if requestRequiresTrace(value.Request) && !traceCapable {
		body, responseErr := responseBytes(frame.ID, traceUnsupported(), true)
		if responseErr != nil || !wire.Send(body) {
			return errFrame
		}
		return nil
	}
	if value.Request.Trace && !traceCapable {
		value.Request.Trace = false
	}
	wait, err := admit(IncomingCall{From: value.From, Request: value.Request})
	if err != nil {
		return err
	}
	helpers.Add(1)
	go func(id int64) {
		defer helpers.Done()
		if value, ok := wait(wire.Done()); ok {
			if !wire.Post(daemonReply{id: id, value: value}) {
				wire.Close()
			}
		}
	}(frame.ID)
	return nil
}

func DecodeHosts(reply Reply) ([]string, error) {
	if reply.Error != nil {
		return nil, errFrame
	}
	var value hostsResult
	if protocol.DecodeJSON(reply.Result, &value) != nil || value.Hosts == nil || !uniqueStrings(value.Hosts) {
		return nil, errFrame
	}
	for _, host := range value.Hosts {
		if !validHost(host) {
			return nil, errFrame
		}
	}
	return value.Hosts, nil
}

func sourceHost(raw []byte) (string, error) {
	var value struct {
		From struct {
			SessionID string `json:"session_id"`
		} `json:"from"`
	}
	if jsonErr := json.Unmarshal(raw, &value); jsonErr != nil {
		return "", errFrame
	}
	return suffix(value.From.SessionID)
}

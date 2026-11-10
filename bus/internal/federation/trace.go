// SPDX-License-Identifier: GPL-3.0-only

package federation

import (
	"encoding/json"
	"strings"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

const TraceALPN = "sessionbus-trace/1"
const MaxTraceRecipients = 256

type TraceDestination struct {
	SessionID string `json:"session_id"`
	Lifetime  string `json:"lifetime"`
}

type TraceRecipient struct {
	Child   string `json:"child"`
	Target  string `json:"target,omitempty"`
	Mode    string `json:"mode"`
	Version string `json:"version"`
	Owner   Caller `json:"owner"`
}

type traceReplyEnvelope struct {
	Result json.RawMessage   `json:"result,omitempty"`
	Error  json.RawMessage   `json:"error,omitempty"`
	Trace  []json.RawMessage `json:"trace"`
}

func requestRequiresTrace(request PublicRequest) bool {
	if request.TraceCopy != nil || request.Method == "trace.configure" {
		return true
	}
	if request.Method == "lane.spawn" {
		var fields struct {
			Trace string `json:"trace"`
		}
		return json.Unmarshal(request.Params, &fields) == nil && fields.Trace != ""
	}
	return false
}

// Optional eligibility collection must not make an otherwise valid ordinary
// forward exceed the wire limit. Explicit trace controls live in Params or
// TraceCopy and are never removed here.
func forwardRequestBytes(id int64, value Forward) ([]byte, Forward, error) {
	body, err := requestBytes(id, forwardMethod, value)
	if err == nil || !value.Request.Trace {
		return body, value, err
	}
	value.Request.Trace = false
	body, err = requestBytes(id, forwardMethod, value)
	return body, value, err
}

func validateTraceRequest(request PublicRequest, rawRequest map[string]json.RawMessage) error {
	traceRaw, hasTrace := rawRequest["trace"]
	copyRaw, hasCopy := rawRequest["trace_copy"]
	if hasTrace {
		var enabled bool
		if json.Unmarshal(traceRaw, &enabled) != nil || !enabled || !request.Trace {
			return errFrame
		}
	}
	if hasCopy && (isNull(copyRaw) || request.TraceCopy == nil) {
		return errFrame
	}
	if request.Trace && (request.Method != "message.send" || request.TraceCopy != nil) {
		return errFrame
	}
	if request.TraceCopy == nil {
		return nil
	}
	if request.Method != "message.send" || request.Trace {
		return errFrame
	}
	if !validTraceDestination(*request.TraceCopy) {
		return errFrame
	}
	params, err := protocol.DecodeParams(request.Method, request.Params)
	if err != nil {
		return errFrame
	}
	message := params.(*protocol.MessageSendRequest)
	if message.Target != request.TraceCopy.SessionID || message.Group != "" || len(message.Targets) != 0 {
		return errFrame
	}
	return nil
}

func validTraceDestination(value TraceDestination) bool {
	return value.Lifetime != "" && len(value.Lifetime) <= 1024 && value.SessionID != "" && len(value.SessionID) <= 1024
}

func encodeTraceReply(reply Reply) ([]byte, error) {
	if reply.Trace == nil || (len(reply.Result) == 0) == !hasReplyError(reply) || len(reply.Trace) > MaxTraceRecipients {
		return nil, errFrame
	}
	envelope := traceReplyEnvelope{Result: reply.Result, Trace: make([]json.RawMessage, len(reply.Trace))}
	if hasReplyError(reply) {
		envelope.Result = nil
		envelope.Error = replyErrorRaw(reply)
	}
	seen := make(map[string]struct{}, len(reply.Trace))
	for index, recipient := range reply.Trace {
		key := traceRecipientKey(recipient)
		if !validTraceRecipient(recipient, "") {
			return nil, errFrame
		}
		if _, ok := seen[key]; ok {
			return nil, errFrame
		}
		seen[key] = struct{}{}
		// Availability is the originating request's opt-in, never trace-owner
		// identity. Keep the legacy owner envelope even over a new link.
		recipient.Owner.Availability = false
		recipient.Owner.Policy = false
		envelope.Trace[index], _ = json.Marshal(recipient)
	}
	value, err := json.Marshal(envelope)
	if err != nil {
		return nil, errFrame
	}
	return value, nil
}

func decodeTraceReply(method string, raw []byte, target string) (Reply, error) {
	var envelope traceReplyEnvelope
	if protocol.DecodeJSON(raw, &envelope) != nil || envelope.Trace == nil || len(envelope.Trace) > MaxTraceRecipients || (len(envelope.Result) == 0) == (len(envelope.Error) == 0) {
		return Reply{}, errFrame
	}
	reply := Reply{Trace: make([]TraceRecipient, len(envelope.Trace))}
	if len(envelope.Result) != 0 {
		if _, err := protocol.DecodeResult(method, envelope.Result); err != nil {
			return Reply{}, errFrame
		}
		reply.Result = append(json.RawMessage(nil), envelope.Result...)
	} else {
		errorValue, err := protocol.DecodeError(envelope.Error)
		if err != nil {
			return Reply{}, errFrame
		}
		reply.Error = errorValue
		reply.ErrorRaw = append(json.RawMessage(nil), envelope.Error...)
	}
	seen := make(map[string]struct{}, len(envelope.Trace))
	for index, rawRecipient := range envelope.Trace {
		recipient, err := decodeTraceRecipient(rawRecipient, target)
		if err != nil {
			return Reply{}, err
		}
		key := traceRecipientKey(recipient)
		if _, ok := seen[key]; ok {
			return Reply{}, errFrame
		}
		seen[key] = struct{}{}
		reply.Trace[index] = recipient
	}
	return reply, nil
}

func traceRecipientKey(recipient TraceRecipient) string {
	return recipient.Child + "\x00" + recipient.Target + "\x00" + recipient.Mode + "\x00" + recipient.Version + "\x00" + recipient.Owner.SessionID + "\x00" + recipient.Owner.OwnerLifetime + "\x00" + recipient.Owner.SourceAttachment
}

func decodeTraceRecipient(raw []byte, target string) (TraceRecipient, error) {
	var fields struct {
		Child   string          `json:"child"`
		Target  string          `json:"target"`
		Mode    string          `json:"mode"`
		Version string          `json:"version"`
		Owner   json.RawMessage `json:"owner"`
	}
	if protocol.DecodeJSON(raw, &fields) != nil || len(fields.Owner) == 0 || isNull(fields.Owner) {
		return TraceRecipient{}, errFrame
	}
	var owner Caller
	if protocol.DecodeJSON(fields.Owner, &owner) != nil {
		return TraceRecipient{}, errFrame
	}
	var ownerFields map[string]json.RawMessage
	if json.Unmarshal(fields.Owner, &ownerFields) != nil || len(ownerFields["name"]) != 0 && owner.Name == "" {
		return TraceRecipient{}, errFrame
	}
	value := TraceRecipient{Child: fields.Child, Target: fields.Target, Mode: fields.Mode, Version: fields.Version, Owner: owner}
	if !validTraceRecipient(value, target) {
		return TraceRecipient{}, errFrame
	}
	value.Owner.Groups = append([]string(nil), owner.Groups...)
	return value, nil
}

func validTraceRecipient(value TraceRecipient, target string) bool {
	childHost, err := suffix(value.Child)
	if err != nil || !ownedPart(value.Child, childHost, false) || target != "" && childHost != target || len(value.Target) > 1024 || value.Mode != "events" && value.Mode != "content" || value.Version == "" || len(value.Version) > 1024 {
		return false
	}
	ownerHost, err := suffix(value.Owner.SessionID)
	if err != nil || !validCaller(value.Owner, ownerHost) || value.Owner.OwnerLifetime == "" || len(value.Owner.OwnerLifetime) > 1024 || len(value.Owner.SourceAttachment) > 1024 {
		return false
	}
	if ownerHost == childHost {
		return value.Owner.SourceAttachment == ""
	}
	return value.Owner.SourceAttachment != ""
}

func isNull(raw []byte) bool {
	return string(raw) == "null" || strings.TrimSpace(string(raw)) == "null"
}

func traceUnsupported() Reply { return errorReply(protocol.UnsupportedTrace, nil) }

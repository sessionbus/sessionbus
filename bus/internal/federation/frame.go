// SPDX-License-Identifier: GPL-3.0-only

package federation

import (
	"encoding/json"
	"errors"
	"strconv"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

const (
	hostsMethod   = "federation.hosts"
	forwardMethod = "federation.forward"
)

var errFrame = errors.New("invalid federation frame")

type Caller struct {
	OwnerLifetime    string   `json:"owner_lifetime,omitempty"`
	SourceAttachment string   `json:"source_attachment,omitempty"`
	SessionID        string   `json:"session_id"`
	Name             string   `json:"name,omitempty"`
	Product          string   `json:"product"`
	PrivateGroup     string   `json:"private_group"`
	Groups           []string `json:"groups"`
}

type PublicRequest struct {
	// Completion is daemon-only provenance, never a public message.send field.
	Completion bool              `json:"completion,omitempty"`
	Method     string            `json:"method"`
	Params     json.RawMessage   `json:"params"`
	MessageID  string            `json:"message_id,omitempty"`
	Trace      bool              `json:"trace,omitempty"`
	TraceCopy  *TraceDestination `json:"trace_copy,omitempty"`
}

type Forward struct {
	From    Caller        `json:"from"`
	Request PublicRequest `json:"request"`
}

type Reply struct {
	Result   json.RawMessage
	Error    *protocol.RPCError
	ErrorRaw json.RawMessage
	Trace    []TraceRecipient
}

type hostsResult struct {
	Hosts []string `json:"hosts"`
}

func requestBytes(id int64, method string, value any) ([]byte, error) {
	params, err := json.Marshal(value)
	if err != nil {
		return nil, errFrame
	}
	body, err := protocol.EncodeRequest(id, method, params)
	if err != nil {
		return nil, errFrame
	}
	return body, nil
}

func ResponseBytes(id int64, reply Reply) ([]byte, error) {
	return responseBytes(id, reply, false)
}

func responseBytes(id int64, reply Reply, internal bool) ([]byte, error) {
	if internal && reply.Trace != nil {
		value, err := encodeTraceReply(reply)
		if err != nil {
			return nil, err
		}
		if body, frameErr := rawResponseBytes(id, `,"result":`, value); frameErr == nil {
			return body, nil
		}
		withoutRecipients := reply
		withoutRecipients.Trace = []TraceRecipient{}
		value, err = encodeTraceReply(withoutRecipients)
		if err != nil {
			return nil, err
		}
		if body, frameErr := rawResponseBytes(id, `,"result":`, value); frameErr == nil {
			return body, nil
		}
		// Eligibility metadata is diagnostic. If even the empty envelope would
		// make an otherwise valid reply too large, preserve the ordinary reply.
		return responseBytes(id, reply, false)
	}
	key, value := `,"result":`, reply.Result
	if reply.Error != nil {
		key, value = `,"error":`, reply.ErrorRaw
		if len(value) == 0 {
			value, _ = json.Marshal(reply.Error)
		}
	}
	if len(value) == 0 {
		return nil, errFrame
	}
	return rawResponseBytes(id, key, value)
}

func rawResponseBytes(id int64, key string, value []byte) ([]byte, error) {
	body := append([]byte(`{"jsonrpc":"2.0","id":`), strconv.FormatInt(id, 10)...)
	body = append(body, key...)
	body = append(body, value...)
	body = append(body, '}', '\n')
	if len(body) > protocol.MaxFrameBytes {
		return nil, errFrame
	}
	return body, nil
}

func errorReply(code int, data any) Reply {
	message := map[int]string{protocol.Internal: "internal", protocol.Busy: "busy", protocol.UnknownSession: "unknown_session", protocol.UnsupportedOpen: "unsupported_open_field", protocol.InvalidFrame: "invalid_frame", protocol.UnknownHost: "unknown_host", protocol.ForwardLost: "forward_lost", protocol.UnsupportedTrace: "unsupported_trace"}[code]
	raw, _ := json.Marshal(data)
	if data == nil {
		raw = nil
	}
	value := &protocol.RPCError{Code: code, Message: message, Data: raw}
	errorRaw, _ := json.Marshal(value)
	return Reply{Error: value, ErrorRaw: errorRaw}
}

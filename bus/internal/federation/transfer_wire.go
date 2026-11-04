// SPDX-License-Identifier: GPL-3.0-only
package federation

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

// These controls exist only between transfer/1 negotiated authenticated links.
// No local path, native permission, caller-chosen sender or generic RPC tunnel
// is carried here. Chunk/finish use their ID-only pinned route after admission.
const (
	TransferBindReadyMethod = "transfer.bind_ready"
	TransferCreateMethod    = "transfer.create"
	TransferBindMethod      = "transfer.bind"
	TransferPrepareMethod   = "transfer.prepare_route"
	TransferNoticeMethod    = "transfer.notice_route"
	TransferHoldMethod      = "transfer.hold"
	TransferDropMethod      = "transfer.drop"
	TransferEndMethod       = "transfer.end"
	TransferAnnounceMethod  = "transfer.announce"
)

type TransferCreate struct {
	From       Caller `json:"from"`
	Generation string `json:"generation"`
	Target     string `json:"target"`
	Kind       string `json:"kind"`
	Name       string `json:"name,omitempty"`
	What       string `json:"what,omitempty"`
}
type TransferCreated struct {
	Transfer      protocol.TransferDescriptor `json:"transfer"`
	OriginBinding string                      `json:"origin_binding"`
}
type TransferBind struct {
	From       Caller `json:"from"`
	Target     string `json:"target"`
	Generation string `json:"generation,omitempty"`
}
type TransferBound struct {
	Binding    string                  `json:"binding"`
	Generation string                  `json:"generation"`
	Identity   protocol.DeliverySource `json:"identity"`
}
type TransferPreparedRoute struct {
	Binding string                   `json:"binding"`
	Prepare protocol.TransferPrepare `json:"prepare"`
}
type TransferNoticeRoute struct {
	Binding string                  `json:"binding"`
	Notice  protocol.TransferNotice `json:"notice"`
}
type TransferHold struct {
	Binding  string `json:"binding"`
	ID       string `json:"id"`
	Terminal bool   `json:"terminal"`
}
type TransferHeld struct {
	Held bool `json:"held"`
}
type TransferBindingTarget struct {
	Binding string `json:"binding"`
}
type TransferCall struct {
	Method string
	Params any
	Reply  chan Reply
}

// TransferServer handles only the fixed endpoint controls below, off the link
// owner/reader. The connection context is the exact admitted hub generation.
type TransferServer func(context.Context, string, json.RawMessage) (json.RawMessage, error)

func transferToken(s string) bool {
	if len(s) < 1 || len(s) > 64 {
		return false
	}
	for _, c := range s {
		if c != '-' && c != '_' && (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') {
			return false
		}
	}
	return true
}
func transferMethod(method string) bool {
	switch method {
	case TransferCreateMethod, TransferBindReadyMethod, TransferBindMethod, TransferPrepareMethod, TransferNoticeMethod, TransferHoldMethod, TransferDropMethod, TransferEndMethod, TransferAnnounceMethod, "transfer.accept", "transfer.status", "transfer.cancel", "transfer.fail", "transfer.start", "transfer.chunk", "transfer.finish":
		return true
	}
	return false
}
func decodeTransferParams(method string, raw json.RawMessage) (any, error) {
	if !transferMethod(method) || !protocol.TransferFrameFits(method, len(raw)+128) {
		return nil, errFrame
	}
	switch method {
	case TransferCreateMethod:
		var p TransferCreate
		if protocol.DecodeJSON(raw, &p) != nil || !transferToken(p.Generation) || p.Kind != "offer" && p.Kind != "request" {
			return nil, errFrame
		}
		host, err := suffix(p.From.SessionID)
		if err != nil || !validCaller(p.From, host) || p.From.SourceAttachment != "" || p.From.OwnerLifetime == "" {
			return nil, errFrame
		}
		if p.Kind == "offer" {
			if p.What != "" {
				return nil, errFrame
			}
			if _, err = protocol.EncodeParams("transfer.offer", protocol.TransferOffer{Target: p.Target, Name: p.Name}); err != nil {
				return nil, errFrame
			}
		} else {
			if _, err = protocol.EncodeParams("transfer.request", protocol.TransferRequest{Target: p.Target, Name: p.Name, What: p.What}); err != nil {
				return nil, errFrame
			}
		}
		return &p, nil
	case TransferBindMethod:
		var p TransferBind
		if protocol.DecodeJSON(raw, &p) != nil || p.Generation != "" && !transferToken(p.Generation) {
			return nil, errFrame
		}
		host, err := suffix(p.From.SessionID)
		if err != nil || !validCaller(p.From, host) || p.From.OwnerLifetime == "" {
			return nil, errFrame
		}
		if _, err = suffix(p.Target); err != nil {
			return nil, errFrame
		}
		return &p, nil
	case TransferPrepareMethod:
		var p TransferPreparedRoute
		if protocol.DecodeJSON(raw, &p) != nil || !transferToken(p.Binding) {
			return nil, errFrame
		}
		if _, err := protocol.EncodeParams("transfer.prepare", p.Prepare); err != nil {
			return nil, errFrame
		}
		return &p, nil
	case TransferNoticeMethod:
		var p TransferNoticeRoute
		if protocol.DecodeJSON(raw, &p) != nil || !transferToken(p.Binding) {
			return nil, errFrame
		}
		if _, err := protocol.EncodeParams("transfer.notice", p.Notice); err != nil {
			return nil, errFrame
		}
		return &p, nil
	case TransferHoldMethod:
		var p TransferHold
		if protocol.DecodeJSON(raw, &p) != nil || !transferToken(p.Binding) || !transferToken(p.ID) {
			return nil, errFrame
		}
		return &p, nil
	case TransferBindReadyMethod, TransferDropMethod, TransferEndMethod:
		var p TransferBindingTarget
		if protocol.DecodeJSON(raw, &p) != nil || !transferToken(p.Binding) {
			return nil, errFrame
		}
		return &p, nil
	case TransferAnnounceMethod:
		return protocol.DecodeParams("transfer.status", raw)
	default:
		return protocol.DecodeParams(method, raw)
	}
}
func validateTransferResult(method string, raw json.RawMessage) bool {
	if !protocol.TransferFrameFits(method, len(raw)+128) {
		return false
	}
	switch method {
	case TransferCreateMethod:
		var p TransferCreated
		if protocol.DecodeJSON(raw, &p) != nil || !transferToken(p.OriginBinding) {
			return false
		}
		_, err := protocol.EncodeResult("transfer.offer", p.Transfer)
		return err == nil
	case TransferBindMethod:
		var p TransferBound
		if protocol.DecodeJSON(raw, &p) != nil || !transferToken(p.Binding) || !transferToken(p.Generation) {
			return false
		}
		_, err := protocol.EncodeParams("transfer.notice", protocol.TransferNotice{Kind: "offer", ID: "binding-check", Name: "file", From: &p.Identity})
		return err == nil
	case TransferPrepareMethod:
		_, err := protocol.DecodeResult("transfer.prepare", raw)
		return err == nil
	case TransferHoldMethod:
		var p struct {
			Held *bool `json:"held"`
		}
		return protocol.DecodeJSON(raw, &p) == nil && p.Held != nil
	case TransferBindReadyMethod, TransferNoticeMethod, TransferDropMethod, TransferEndMethod, TransferAnnounceMethod:
		var p map[string]json.RawMessage
		return protocol.DecodeJSON(raw, &p) == nil && p != nil && len(p) == 0
	default:
		_, err := protocol.DecodeResult(method, raw)
		return err == nil
	}
}
func transferReply(value any, err error) Reply {
	if err != nil {
		return errorReply(protocol.Internal, "transfer_unavailable")
	}
	raw, e := json.Marshal(value)
	if e != nil {
		return errorReply(protocol.Internal, "transfer_unavailable")
	}
	return Reply{Result: raw}
}
func endpointTransferControl(method string) bool {
	switch method {
	case TransferBindReadyMethod, TransferBindMethod, TransferPrepareMethod, TransferNoticeMethod, TransferHoldMethod, TransferDropMethod, "transfer.start", "transfer.chunk", "transfer.finish":
		return true
	}
	return false
}
func isTransferFrame(method string) bool { return strings.HasPrefix(method, "transfer.") }

// DecodeTransferParams validates only the fixed internal transfer controls.
func DecodeTransferParams(method string, raw json.RawMessage) (any, error) {
	return decodeTransferParams(method, raw)
}

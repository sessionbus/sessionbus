// SPDX-License-Identifier: MIT

package sessionkit

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

var Actions = []string{"list", "send", "spawn", "describe", "trace", "run", "start", "wait", "status", "interrupt", "close", "forget", "ack", "transfer.offer", "transfer.request", "transfer.accept", "transfer.cancel", "transfer.status"}

func (c *Caller) Action(ctx context.Context, action string, args json.RawMessage) (json.RawMessage, error) {
	if strings.HasPrefix(action, "transfer.") {
		return c.transfers.Action(ctx, action, args)
	}
	method := map[string]string{
		"list": "session.list", "send": "message.send", "spawn": "lane.spawn", "describe": "lane.describe",
		"trace": "trace.configure",
		"run":   "turn.run", "start": "turn.start", "status": "turn.status", "wait": "turn.wait", "interrupt": "turn.interrupt", "close": "session.close",
	}[action]
	if method != "" {
		params, err := protocol.DecodeParams(method, args)
		var result json.RawMessage
		if err == nil {
			err = c.call(ctx, method, params, &result)
		}
		return result, err
	}
	switch action {
	case "ack":
		return localAction(args, func(request RunRef) (struct{}, error) { return struct{}{}, c.Ack(ctx, request) })
	case "forget":
		return localAction(args, func(request SessionCloseRequest) (struct{}, error) {
			request.Forget = true
			return struct{}{}, c.Close(ctx, request)
		})
	default:
		return nil, errors.New("unknown action")
	}
}

func localAction[T, R any](raw json.RawMessage, call func(T) (R, error)) (json.RawMessage, error) {
	var request T
	err := protocol.DecodeJSON(raw, &request)
	if err != nil {
		return nil, err
	}
	result, err := call(request)
	if err == nil {
		return json.Marshal(result)
	}
	return nil, err
}

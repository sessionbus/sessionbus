// SPDX-License-Identifier: MIT

package sessionkit

import (
	"context"
	"encoding/json"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

type callFunc func(context.Context, string, any, any) error
type Caller struct {
	call      callFunc
	transfers *Transfers
}
type WaitRequest = protocol.WaitRequest

func newCaller(call callFunc) *Caller {
	return &Caller{call: call}
}

func NewCaller(call func(context.Context, string, any) (json.RawMessage, error)) *Caller {
	return newCaller(func(ctx context.Context, method string, params, result any) error {
		raw, err := call(ctx, method, params)
		if err != nil {
			return err
		}
		return protocol.UnmarshalResult(method, raw, result)
	})
}

// NewCallerWithTransfers binds local tool actions to the SDK-owned executor.
// Paths are consumed here and never passed to the generic wire-call callback.
func NewCallerWithTransfers(call func(context.Context, string, any) (json.RawMessage, error), transfers *Transfers) *Caller {
	caller := NewCaller(call)
	caller.transfers = transfers
	return caller
}

func callAs[T any](ctx context.Context, call callFunc, method string, params any) (T, error) {
	var result T
	_, err := protocol.EncodeParams(method, params)
	if err == nil {
		err = call(ctx, method, params, &result)
	}
	return result, err
}

func (c *Caller) List(ctx context.Context, request SessionListRequest) (SessionListResult, error) {
	return callAs[SessionListResult](ctx, c.call, "session.list", request)
}
func (c *Caller) Send(ctx context.Context, request MessageSendRequest) (MessageSendResult, error) {
	return callAs[MessageSendResult](ctx, c.call, "message.send", request)
}
func (c *Caller) Describe(ctx context.Context, request LaneDescribeRequest) (LaneDescribeResult, error) {
	return callAs[LaneDescribeResult](ctx, c.call, "lane.describe", request)
}
func (c *Caller) Spawn(ctx context.Context, request LaneSpawnRequest) (LaneSpawnResult, error) {
	return callAs[LaneSpawnResult](ctx, c.call, "lane.spawn", request)
}
func (c *Caller) Trace(ctx context.Context, request TraceConfigureRequest) (TraceConfigureResult, error) {
	return callAs[TraceConfigureResult](ctx, c.call, "trace.configure", request)
}
func (c *Caller) Resume(ctx context.Context, sessionID string) (LaneSpawnResult, error) {
	return c.Spawn(ctx, LaneSpawnRequest{ResumeSessionID: sessionID})
}
func (c *Caller) Run(ctx context.Context, request TurnRunRequest) (RunStatus, error) {
	return callAs[RunStatus](ctx, c.call, "turn.run", request)
}
func (c *Caller) Interrupt(ctx context.Context, request SessionTarget) error {
	_, err := callAs[struct{}](ctx, c.call, "turn.interrupt", request)
	return err
}
func (c *Caller) Close(ctx context.Context, request SessionCloseRequest) error {
	_, err := callAs[struct{}](ctx, c.call, "session.close", request)
	return err
}

func (c *Caller) Start(ctx context.Context, request TurnRunRequest) (RunRef, error) {
	return callAs[RunRef](ctx, c.call, "turn.start", request)
}
func (c *Caller) Status(ctx context.Context, request ReadRequest) (RunStatus, error) {
	return callAs[RunStatus](ctx, c.call, "turn.status", request)
}
func (c *Caller) Wait(request WaitRequest) (RunStatus, error) {
	return c.WaitContext(context.Background(), request)
}

// WaitContext abandons only this non-consuming read, never the worker's result.
func (c *Caller) WaitContext(ctx context.Context, request WaitRequest) (RunStatus, error) {
	return callAs[RunStatus](ctx, c.call, "turn.wait", request)
}

// Ack commits consumption. Once submitted, its response wins later cancellation.
func (c *Caller) Ack(ctx context.Context, request RunRef) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := callAs[struct{}](context.WithoutCancel(ctx), c.call, "turn.ack", request)
	return err
}
func (c *Caller) disconnected() {} // Results belong to the resident worker.

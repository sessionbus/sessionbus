// SPDX-License-Identifier: MIT

package protocol

import (
	"encoding/json"
	"math"
)

const (
	MaxFrameBytes = 1 << 20
	MaxTextRunes  = 262144
	MaxRequestID  = 1<<53 - 1
	MaxOperations = 256

	InvalidFrame     = -32600
	InvalidHello     = -32602
	Internal         = -32603
	UnknownSession   = -32001
	NotConnected     = -32002
	Busy             = -32003
	NotRunning       = -32004
	AlreadyConnected = -32005
	UnknownProduct   = -32007
	UnsupportedOpen  = -32008
	SpawnFailed      = -32009
	Timeout          = -32010
	NotCommitted     = -32011
	Superseded       = -32012
	NameTaken        = -32013
	UnknownHost      = -32014
	ForwardLost      = -32015
	UnsupportedTrace = -32016
)

type ExtraArgument struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	TakesValue  bool   `json:"takes_value"`
}

type Integer int

func (value *Integer) UnmarshalJSON(raw []byte) error {
	var number float64
	if json.Unmarshal(raw, &number) != nil || math.Trunc(number) != number {
		return errInvalid
	}
	*value = Integer(number)
	return nil
}

type HelloDescription struct {
	SupportsMessageRun  bool            `json:"supports_message_run,omitempty"`
	Product             string          `json:"product"`
	Version             string          `json:"version,omitempty"`
	SupportedOpenFields []string        `json:"supported_open_fields"`
	ExtraArguments      []ExtraArgument `json:"extra_arguments"`
}

type WorkerHello struct {
	Protocol         Integer `json:"protocol"`
	LaunchToken      string  `json:"launch_token"`
	HelloDescription `tstype:",extends"`
}

// PeerHello is a complete identity assertion; an empty Name omits the native name.
type PeerHello struct {
	Protocol  Integer        `json:"protocol"`
	Product   string         `json:"product"`
	SessionID string         `json:"session_id"`
	Name      string         `json:"name,omitempty"`
	Groups    []string       `json:"groups"`
	Info      map[string]any `json:"info"`
}

type OpenOptions struct {
	Cwd             string   `json:"cwd,omitempty"`
	PermissionMode  string   `json:"permission_mode,omitempty"`
	Model           string   `json:"model,omitempty"`
	ReasoningEffort string   `json:"reasoning_effort,omitempty"`
	Arguments       []string `json:"arguments,omitempty"`
}

type LanePolicy struct {
	Persistent     bool   `json:"persistent"`
	AutoCloseMS    int64  `json:"auto_close_ms"`
	Notify         bool   `json:"notify"`
	NotifyTarget   string `json:"notify_target,omitempty"`
	OwnerSessionID string `json:"owner_session_id,omitempty"`
	// Trace is the live mode reported only in the spawning parent's response.
	// Absence means unreported, not off; it is never durable lane policy.
	Trace string `json:"trace,omitempty"`
}

type RunRef struct {
	SessionID string `json:"session_id"`
	RunID     string `json:"run_id"`
}
type ReadRequest struct {
	SessionID string `json:"session_id"`
	RunID     string `json:"run_id,omitempty"`
}
type WaitRequest struct {
	SessionID string `json:"session_id"`
	RunID     string `json:"run_id,omitempty"`
	TimeoutMS *int64 `json:"timeout_ms,omitempty"`
}
type RunStatus struct {
	SessionID string      `json:"session_id"`
	RunID     string      `json:"run_id"`
	State     string      `json:"state"`
	Result    *TurnResult `json:"result,omitempty"`
	Reason    string      `json:"reason,omitempty"`
}
type ExecuteRequest struct {
	SessionID string `json:"session_id"`
	RunID     string `json:"run_id"`
	Input     string `json:"input"`
}
type TurnReady struct {
	SessionID string `json:"session_id"`
	RunID     string `json:"run_id"`
	State     string `json:"state"`
	Outcome   string `json:"outcome,omitempty"`
	Reason    string `json:"reason,omitempty"`
}

type OpenRequest struct {
	Policy          *LanePolicy `json:"policy,omitempty"`
	Name            string      `json:"name"`
	Groups          []string    `json:"groups"`
	ResumeSessionID string      `json:"resume_session_id,omitempty"`
	Open            OpenOptions `json:"open"`
}

type OpenResult struct {
	SessionID string `json:"session_id"`
}

type TurnRunRequest struct {
	SessionID string `json:"session_id"`
	Input     string `json:"input"`
}

type TurnResult struct {
	Outcome          string `json:"outcome"`
	Result           string `json:"result"`
	Truncated        bool   `json:"truncated,omitempty"`
	NativeStopReason string `json:"native_stop_reason,omitempty"`
}

type DeliverySource struct {
	SessionID string   `json:"session_id"`
	Name      string   `json:"name,omitempty"`
	Product   string   `json:"product"`
	Groups    []string `json:"groups"`
}

type DeliveryRequest struct {
	RunID     string         `json:"run_id,omitempty"`
	MessageID string         `json:"message_id"`
	From      DeliverySource `json:"from"`
	Body      string         `json:"body"`
}

type DeliveryReceipt struct {
	Disposition string `json:"disposition"`
	Reason      string `json:"reason,omitempty"`
}

type HostProducts struct {
	Host     string   `json:"host"`
	Products []string `json:"products"`
}

type SessionSummary struct {
	Policy    *LanePolicy    `json:"policy,omitempty"`
	SessionID string         `json:"session_id"`
	Kind      string         `json:"kind"`
	Product   string         `json:"product"`
	Name      string         `json:"name,omitempty"`
	Groups    []string       `json:"groups"`
	Connected bool           `json:"connected"`
	Running   bool           `json:"running"`
	Info      map[string]any `json:"info,omitempty"`
}

type SessionListRequest struct {
	SessionID string `json:"session_id,omitempty"`
	Host      string `json:"host,omitempty"`
}
type SessionListResult struct {
	// SelfInfo is the originating caller, independent of list filters. Older
	// daemons may omit it; never infer self from a session name or list order.
	SelfInfo *SessionSelfInfo `json:"self_info,omitempty"`
	Sessions []SessionSummary `json:"sessions"`
	Hosts    []HostProducts   `json:"hosts,omitempty"`
}

// SessionSelfInfo uses the same public identity fields as a message source.
// It excludes connection credentials, owner tokens and transport metadata.
type SessionSelfInfo DeliverySource

type MessageSendRequest struct {
	Target  string   `json:"target,omitempty"`
	Targets []string `json:"targets,omitempty"`
	Group   string   `json:"group,omitempty"`
	Host    string   `json:"host,omitempty"`
	Message string   `json:"message"`
}

type MessageSendDelivery struct {
	Target      string `json:"target"`
	SessionID   string `json:"session_id,omitempty"`
	DeliveryID  string `json:"delivery_id,omitempty"`
	Disposition string `json:"disposition"`
	Reason      string `json:"reason,omitempty"`
}

type MessageSendResult struct {
	MessageID  string                `json:"message_id"`
	Deliveries []MessageSendDelivery `json:"deliveries"`
}

type LaneDescribeRequest struct {
	Product string `json:"product"`
	Host    string `json:"host,omitempty"`
}

type LaneDescribeResult = HelloDescription

type LaneSpawnRequest struct {
	Persistent      *bool        `json:"persistent,omitempty"`
	AutoCloseMS     *int64       `json:"auto_close_ms,omitempty"`
	Notify          *bool        `json:"notify,omitempty"`
	NotifyTarget    string       `json:"notify_target,omitempty"`
	Name            string       `json:"name,omitempty"`
	Product         string       `json:"product,omitempty"`
	Host            string       `json:"host,omitempty"`
	ResumeSessionID string       `json:"resume_session_id,omitempty"`
	ExtraGroups     []string     `json:"extra_groups,omitempty"`
	Open            *OpenOptions `json:"open,omitempty"`
	Trace           string       `json:"trace,omitempty"`
}

type LaneSpawnResult struct {
	Policy    *LanePolicy `json:"policy,omitempty"`
	SessionID string      `json:"session_id"`
}
type TraceConfigureRequest struct {
	SessionID string `json:"session_id"`
	Mode      string `json:"mode"`
}
type TraceConfigureResult struct {
	SessionID string `json:"session_id"`
	Mode      string `json:"mode"`
}
type SessionTarget struct {
	SessionID string `json:"session_id"`
}
type SessionCloseRequest struct {
	SessionID string `json:"session_id"`
	Forget    bool   `json:"forget,omitempty"`
}

type SpawnFailedData struct {
	ExitCode   *int     `json:"exit_code,omitempty"`
	StderrTail []string `json:"stderr_tail"`
}

type RPCError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (e *RPCError) Error() string { return e.Message }

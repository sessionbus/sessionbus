// SPDX-License-Identifier: GPL-3.0-only
package federation

import (
	"encoding/json"

	"github.com/antst/sessionbus/bus/internal/conn"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func queueCall(wire *conn.Conn, queued map[*pendingCall]bool, call pendingCall, bulk bool, encode func(int64) ([]byte, error), stream ...string) bool {
	p := &call
	if !wire.Request(bulk, p, encode, stream...) {
		return false
	}
	queued[p] = true
	return true
}
func installCall(d conn.Dispatched, queued map[*pendingCall]bool, pending map[int64]pendingCall) {
	if p, ok := d.Value.(*pendingCall); ok && queued[p] {
		delete(queued, p)
		pending[d.ID] = *p
	}
}
func settleQueuedCalls(queued map[*pendingCall]bool) {
	for p := range queued {
		delete(queued, p)
		p.reply <- errorReply(-32015, nil)
	}
}

func queueTransferCall(wire *conn.Conn, queued map[*pendingCall]bool, pending map[int64]pendingCall, event TransferCall) bool {
	if !transferMethod(event.Method) {
		return false
	}
	bulk := event.Method == "transfer.chunk"
	count := 0
	for _, call := range pending {
		if call.bulk == bulk {
			count++
		}
	}
	for call := range queued {
		if call.bulk == bulk {
			count++
		}
	}
	limit := protocol.MaxOperations
	if bulk {
		limit = 4
	}
	if count >= limit {
		return false
	}
	raw, err := json.Marshal(event.Params)
	if err != nil {
		return false
	}
	if _, err = decodeTransferParams(event.Method, raw); err != nil {
		return false
	}
	return queueCall(wire, queued, pendingCall{method: event.Method, reply: event.Reply, transfer: true, bulk: bulk}, bulk, func(id int64) ([]byte, error) { return protocol.EncodeRequest(id, event.Method, raw) }, protocol.TransferStreamID(event.Params))
}

func callCount(pending map[int64]pendingCall, queued map[*pendingCall]bool, bulk bool) int {
	n := 0
	for _, p := range pending {
		if p.bulk == bulk {
			n++
		}
	}
	for p := range queued {
		if p.bulk == bulk {
			n++
		}
	}
	return n
}

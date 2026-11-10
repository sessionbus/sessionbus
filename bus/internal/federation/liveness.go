// SPDX-License-Identifier: GPL-3.0-only

package federation

import (
	"sync/atomic"
	"time"

	"github.com/antst/sessionbus/bus/internal/conn"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

const probeInterval = 5 * time.Second
const silenceAfter = 15 * time.Second

type livenessTick struct{ at time.Time }

// Non-atomic fields belong to the existing connection owner. The registry
// reads silent and sets observeRoster only while relation charges exist.
type linkLiveness struct {
	observeRoster atomic.Bool
	lastReply     time.Time
	waiting       bool
	silent        atomic.Bool
}

func (l *linkLiveness) replied() {
	l.lastReply = time.Now()
	l.waiting = false
	l.silent.Store(false)
}

func (l *linkLiveness) tick(now time.Time, method string, wire *conn.Conn, queued map[*pendingCall]bool, pending map[int64]pendingCall) {
	if now.Sub(l.lastReply) >= silenceAfter && !l.silent.Swap(true) {
		for p := range queued {
			if p.forwarded {
				notifyCall(p, silenceReply(p))
			}
		}
		for id, p := range pending {
			if p.forwarded {
				notifyCall(&p, silenceReply(&p))
				pending[id] = p
			}
		}
	}
	if !l.waiting && queueCall(wire, queued, pendingCall{method: method, probe: l}, false, func(id int64) ([]byte, error) { return requestBytes(id, method, struct{}{}) }) {
		l.waiting = true // Includes queued, selected, written and late response.
	}
}

func silenceReply(p *pendingCall) Reply {
	if p.method == "session.list" {
		return unavailableList(p.target, p.availability)
	}
	return errorReply(protocol.ForwardLost, nil)
}

// Nil retires only the waiter, never this wire correlation or its cap charge.
func notifyCall(p *pendingCall, reply Reply) {
	if p.reply != nil {
		p.reply <- reply
		p.reply = nil
	}
}

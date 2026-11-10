// SPDX-License-Identifier: GPL-3.0-only
package federation

import (
	"github.com/antst/sessionbus/bus/internal/conn"
	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func (l *hostLink) refuseOrigin(frame conn.Frame, code int, data any) {
	if frame.Release != nil {
		frame.Release()
	}
	_ = l.wire.Send(ErrorBytes(frame.Value.ID, code, data))
}

// All producers share this reservation. Responses retain it until the owner
// consumes them; neither mark release nor a helper's return frees capacity.
func (l *hostLink) admitOrigin(frame conn.Frame, refusal int) bool {
	if l.originAdmitted >= protocol.MaxOperations {
		l.refuseOrigin(frame, refusal, nil)
		return false
	}
	l.originAdmitted++
	return true
}

func releaseOriginReply(event originReply) {
	if event.release != nil {
		event.release()
	}
}

// The post-enqueue check also covers a send winning after the final drain.
// Real bulk callbacks already use sync.Once, so owner/drain overlap is safe.
func (l *hostLink) originReplyLive(event originReply) bool {
	select {
	case <-l.wire.Done():
		releaseOriginReply(event)
		return false
	default:
		return true
	}
}

func (l *hostLink) postOriginReply(event originReply) bool {
	if !l.originReplyLive(event) {
		return false
	}
	select {
	case l.originReplies <- event:
		return l.originReplyLive(event)
	case <-l.wire.Done():
		releaseOriginReply(event)
		return false
	}
}

func (l *hostLink) consumeOriginReply(event originReply) {
	l.originAdmitted--
	releaseOriginReply(event)
}

// Called only after conn.Closed: the reader has closed this wire's Done and
// cannot enqueue another ingress Bulk frame. Late replies release themselves.
func (l *hostLink) retireOriginReplies() {
	l.originAdmitted = 0
	for {
		select {
		case event := <-l.originReplies:
			releaseOriginReply(event)
		default:
			goto bulk
		}
	}
bulk:
	for {
		select {
		case event := <-l.wire.Bulk():
			if event.Release != nil {
				event.Release()
			}
		default:
			return
		}
	}
}

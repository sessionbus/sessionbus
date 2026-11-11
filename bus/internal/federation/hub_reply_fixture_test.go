// SPDX-License-Identifier: GPL-3.0-only
package federation

import (
	"net"
	"sync"
	"testing"

	"github.com/antst/sessionbus/bus/internal/conn"
)

// Existing registry/callback fixtures do not run a link owner. Give them the
// real wire lifetime and dedicated replies without pretending to test admission.
func prepareReplyFixture(t *testing.T, link *hostLink) {
	t.Helper()
	left, right := net.Pipe()
	inbox := make(chan any, 8)
	var group sync.WaitGroup
	link.wire = conn.Start(left, inbox, &group)
	link.originReplies = make(chan originReply, conn.OutboxSize+8)
	t.Cleanup(func() {
		link.wire.Close()
		_ = right.Close()
		group.Wait()
	})
}

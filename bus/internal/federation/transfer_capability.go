// SPDX-License-Identifier: GPL-3.0-only

package federation

import (
	"crypto/tls"
	"net"
)

// TransferALPN is a negotiated superset, not a replacement for older modes.
// Both legs must prove it before any transfer control/chunk is forwarded.
const TransferALPN = "sessionbus-transfer/1"

func SupportsTransfer(fd net.Conn) bool {
	c, ok := fd.(*tls.Conn)
	return ok && c.ConnectionState().NegotiatedProtocol == TransferALPN
}

func supportsWakeProtocol(p string) bool   { return p == TransferALPN || p == WakeALPN }
func supportsTraceProtocol(p string) bool  { return supportsWakeProtocol(p) || p == TraceALPN }
func supportsRosterProtocol(p string) bool { return supportsTraceProtocol(p) || p == RosterALPN }

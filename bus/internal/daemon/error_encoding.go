// SPDX-License-Identifier: GPL-3.0-only
package daemon

import "github.com/antst/sessionbus/bus/internal/federation"

// An error made by the daemon must not end the caller's ownership.
func errorBytes(id int64, code int, data any) []byte {
	return federation.ErrorBytes(id, code, data)
}

// SPDX-License-Identifier: GPL-3.0-only

//go:build !linux && !darwin

package federation

import (
	"errors"
	"syscall"
)

func setTCPDropTime(syscall.RawConn) error {
	return errors.New("TCP drop-time option unsupported on this platform")
}

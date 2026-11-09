// SPDX-License-Identifier: GPL-3.0-only

package federation

import (
	"syscall"

	"golang.org/x/sys/unix"
)

func setTCPDropTime(raw syscall.RawConn) error {
	var setting error
	err := raw.Control(func(fd uintptr) {
		setting = unix.SetsockoptInt(int(fd), unix.IPPROTO_TCP, unix.TCP_RXT_CONNDROPTIME, 150)
	})
	if err != nil {
		return err
	}
	return setting
}

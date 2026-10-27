// SPDX-License-Identifier: MIT

package sessionkit

import (
	"errors"

	"golang.org/x/sys/unix"
)

func unsupportedTransferHardlink(err error) bool {
	// link(2): EPERM includes filesystems without hard links. Trying an
	// atomic rename still leaves directory permissions enforced by the OS.
	return errors.Is(err, unix.EPERM) || errors.Is(err, unix.EOPNOTSUPP)
}

func unsupportedTransferRename(err error) bool {
	return errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EINVAL) || errors.Is(err, unix.EOPNOTSUPP)
}

func renameTransferNoReplace(fromFD int, from string, toFD int, to string) error {
	return unix.Renameat2(fromFD, from, toFD, to, unix.RENAME_NOREPLACE)
}

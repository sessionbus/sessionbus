// SPDX-License-Identifier: MIT

//go:build !linux && !darwin

package sessionkit

import "errors"

// Other Unix targets preserve the existing hard-link-only behavior.
func unsupportedTransferHardlink(error) bool { return false }
func unsupportedTransferRename(error) bool   { return false }
func renameTransferNoReplace(int, string, int, string) error {
	return errors.ErrUnsupported
}

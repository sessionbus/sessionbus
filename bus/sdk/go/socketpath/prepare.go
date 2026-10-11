// SPDX-License-Identifier: MIT

package socketpath

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"golang.org/x/sys/unix"
)

// Prepare validates and creates the selected Sessionbus runtime directory.
func Prepare(socket string) error {
	return PrepareSockets(socket, Lane(socket, ""))
}

// PrepareSockets validates the supplied socket paths and creates the directory
// containing socket. Additional paths are checked for length only.
func PrepareSockets(socket string, additionalPaths ...string) error {
	root := filepath.Dir(socket)
	if !filepath.IsAbs(root) {
		return fmt.Errorf("sessionbus runtime directory %q is not absolute", root)
	}
	limit := 107
	if runtime.GOOS == "darwin" {
		limit = 103
	}
	for _, path := range append([]string{socket}, additionalPaths...) {
		if len([]byte(path)) > limit {
			return fmt.Errorf("sessionbus Unix socket path limit %d exceeded by %q", limit, path)
		}
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return err
	}
	// The configured root's ancestors are trusted user/system runtime roots.
	// O_NOFOLLOW binds final-component validation and chmod to one object.
	fd, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("open sessionbus runtime directory %q: %w", root, err)
	}
	defer unix.Close(fd)
	var stat unix.Stat_t
	if err = unix.Fstat(fd, &stat); err != nil {
		return fmt.Errorf("inspect sessionbus runtime directory %q: %w", root, err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFDIR || int(stat.Uid) != os.Getuid() {
		return fmt.Errorf("sessionbus runtime directory %q must be current-user-owned", root)
	}
	if stat.Mode&0o777 != 0o700 {
		if err = unix.Fchmod(fd, 0o700); err != nil {
			return fmt.Errorf("set sessionbus runtime directory mode 0700: %w", err)
		}
	}
	return nil
}

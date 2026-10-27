// SPDX-License-Identifier: MIT

package sessionkit

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"

	"golang.org/x/sys/unix"
)

// FilesystemTransferExecutor reads and writes directly as the current OS user.
// It is the SDK default in every product/mode; it does not consult native policy.
// Only accepted transfers open handles. Publication uses one sibling temp, with
// atomic no-replace unless the receiver explicitly selected overwrite.
type FilesystemTransferExecutor struct{}

var errTransferNoReplaceUnsupported = errors.New("atomic no-replace publication unsupported")

func (FilesystemTransferExecutor) OpenTransferSource(ctx context.Context, path string) (TransferSource, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// A FIFO/device must not hang prepare before the regular-file check.
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	stat, err := file.Stat()
	if err == nil && !stat.Mode().IsRegular() {
		err = errors.New("transfer source is not a regular file")
	}
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	return &filesystemTransferSource{file: file, stat: stat, ctx: ctx}, nil
}

type filesystemTransferSource struct {
	file *os.File
	stat os.FileInfo
	ctx  context.Context
	once sync.Once
	err  error
}

func (s *filesystemTransferSource) Size() int64 { return s.stat.Size() }
func (s *filesystemTransferSource) Read(raw []byte) (int, error) {
	if err := s.ctx.Err(); err != nil {
		return 0, err
	}
	return s.file.Read(raw)
}
func (s *filesystemTransferSource) Verify() error {
	if err := s.ctx.Err(); err != nil {
		return err
	}
	stat, err := s.file.Stat()
	if err != nil {
		return err
	}
	if !os.SameFile(s.stat, stat) || s.stat.Size() != stat.Size() || !s.stat.ModTime().Equal(stat.ModTime()) {
		return errors.New("transfer source changed")
	}
	return nil
}
func (s *filesystemTransferSource) Close() error {
	s.once.Do(func() { s.err = s.file.Close() })
	return s.err
}

func (FilesystemTransferExecutor) OpenTransferDestination(ctx context.Context, path string, overwrite bool) (TransferDestination, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dir, name := filepath.Split(path)
	if name == "" || name == "." || name == ".." {
		return nil, ErrTransferBinding
	}
	if dir == "" {
		dir = "."
	}
	fd, err := unix.Open(dir, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	parent := os.NewFile(uintptr(fd), dir)
	var entropy [16]byte
	if _, err = rand.Read(entropy[:]); err != nil {
		_ = parent.Close()
		return nil, err
	}
	temp := ".sessionbus-transfer-" + hex.EncodeToString(entropy[:]) + ".tmp"
	tempFD, err := unix.Openat(fd, temp, unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC, 0o600)
	if err != nil {
		_ = parent.Close()
		return nil, err
	}
	d := &filesystemTransferDestination{ctx: ctx, parent: parent, file: os.NewFile(uintptr(tempFD), temp), name: name, temp: temp, overwrite: overwrite, link: unix.Linkat, renameNoReplace: renameTransferNoReplace}
	if err = ctx.Err(); err != nil {
		return nil, errors.Join(err, d.Abort())
	}
	return d, nil
}

type filesystemTransferDestination struct {
	mu                              sync.Mutex
	ctx                             context.Context
	parent, file                    *os.File
	name, temp                      string
	overwrite, attempted, published bool
	aborted                         bool
	commitErr                       error
	link                            func(int, string, int, string, int) error
	renameNoReplace                 func(int, string, int, string) error
}

func (d *filesystemTransferDestination) Write(raw []byte) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.ctx.Err(); err != nil {
		return 0, err
	}
	if d.attempted || d.aborted {
		return 0, io.ErrClosedPipe
	}
	return d.file.Write(raw)
}
func (d *filesystemTransferDestination) Commit() (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.attempted {
		return d.published, d.commitErr
	}
	d.attempted = true
	if d.aborted {
		d.commitErr = io.ErrClosedPipe
		return false, d.commitErr
	}
	d.commitErr = d.file.Close()
	d.file = nil
	if d.commitErr != nil {
		return false, d.commitErr
	}
	fd := int(d.parent.Fd())
	moved := d.overwrite
	if d.overwrite {
		d.commitErr = unix.Renameat(fd, d.temp, fd, d.name)
	} else {
		d.commitErr = d.link(fd, d.temp, fd, d.name, 0)
		if unsupportedTransferHardlink(d.commitErr) {
			d.commitErr = d.renameNoReplace(fd, d.temp, fd, d.name)
			moved = d.commitErr == nil
			if unsupportedTransferRename(d.commitErr) {
				d.commitErr = errors.Join(errTransferNoReplaceUnsupported, d.commitErr)
			}
		}
	}
	if d.commitErr != nil {
		return false, d.commitErr
	}
	d.published = true
	if moved {
		d.temp = ""
	} else {
		d.commitErr = unix.Unlinkat(fd, d.temp, 0)
		if d.commitErr == nil {
			d.temp = ""
		}
	}
	if d.temp == "" {
		d.commitErr = errors.Join(d.commitErr, d.parent.Close())
		d.parent = nil
	}
	return true, d.commitErr
}
func (d *filesystemTransferDestination) Abort() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.aborted = true
	var err error
	if d.file != nil {
		err = d.file.Close()
		d.file = nil
	}
	if d.parent != nil {
		if d.temp != "" {
			removed := unix.Unlinkat(int(d.parent.Fd()), d.temp, 0)
			if errors.Is(removed, unix.ENOENT) {
				removed = nil
			}
			err = errors.Join(err, removed)
		}
		err = errors.Join(err, d.parent.Close())
		d.parent = nil
	}
	return err
}

// SPDX-License-Identifier: MIT

package sessionkit

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"golang.org/x/sys/unix"
)

func TestDefaultTransferFilesystemSourceIdentityAndOSFailures(t *testing.T) {
	x := FilesystemTransferExecutor{}
	dir := t.TempDir()
	path := filepath.Join(dir, "source")
	if err := os.WriteFile(path, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "source-link")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	s, err := x.OpenTransferSource(context.Background(), link)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if raw, err := io.ReadAll(s); err != nil || string(raw) != "data" || s.Size() != 4 || s.Verify() != nil {
		t.Fatal(string(raw), err)
	}
	if err = os.WriteFile(path, []byte("changed"), 0o600); err != nil {
		t.Fatal(err)
	}
	if s.Verify() == nil {
		t.Fatal("same-handle source change was missed")
	}
	if err = s.Close(); err != nil || s.Close() != nil {
		t.Fatal("source close is not idempotent", err)
	}
	fifo := filepath.Join(dir, "fifo")
	if err = unix.Mkfifo(fifo, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{dir, fifo, filepath.Join(dir, "missing")} {
		if source, err := x.OpenTransferSource(context.Background(), path); err == nil {
			_ = source.Close()
			t.Fatal("nonregular/missing source admitted", path)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = x.OpenTransferSource(ctx, path); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err = x.OpenTransferDestination(ctx, filepath.Join(dir, "cancelled"), false); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestDefaultTransferFilesystemAtomicPublicationAndCleanup(t *testing.T) {
	for _, data := range []string{"", "entire new content"} {
		t.Run(data, func(t *testing.T) {
			x := FilesystemTransferExecutor{}
			dir := t.TempDir()
			target := filepath.Join(dir, "target")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			d, err := x.OpenTransferDestination(ctx, target, false)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = io.WriteString(d, data); err != nil {
				t.Fatal(err)
			}
			if _, err = os.Stat(target); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("destination visible before publication", err)
			}
			if ok, err := d.Commit(); !ok || err != nil {
				t.Fatal(ok, err)
			}
			cancel()
			if ok, err := d.Commit(); !ok || err != nil {
				t.Fatal("duplicate commit lost publication", ok, err)
			}
			if err = d.Abort(); err != nil || d.Abort() != nil {
				t.Fatal("cleanup failed after commit", err)
			}
			if raw, err := os.ReadFile(target); err != nil || string(raw) != data {
				t.Fatal("abort retracted committed file", string(raw), err)
			}
			assertNoTransferTemps(t, dir)
		})
	}
	x := FilesystemTransferExecutor{}
	dir := t.TempDir()
	target := filepath.Join(dir, "existing")
	if err := os.WriteFile(target, []byte("original"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, overwrite := range []bool{false, true} {
		d, err := x.OpenTransferDestination(context.Background(), target, overwrite)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = io.WriteString(d, "replacement"); err != nil {
			t.Fatal(err)
		}
		ok, err := d.Commit()
		if ok != overwrite || !overwrite && !errors.Is(err, unix.EEXIST) || overwrite && err != nil {
			t.Fatal("overwrite choice ignored", ok, err)
		}
		if err = d.Abort(); err != nil {
			t.Fatal(err)
		}
		want := "original"
		if overwrite {
			want = "replacement"
		}
		if raw, err := os.ReadFile(target); err != nil || string(raw) != want {
			t.Fatal(string(raw), err)
		}
		assertNoTransferTemps(t, dir)
	}
	d, err := x.OpenTransferDestination(context.Background(), filepath.Join(dir, "aborted"), false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = io.WriteString(d, "unpublished"); err != nil {
		t.Fatal(err)
	}
	if err = d.Abort(); err != nil {
		t.Fatal(err)
	}
	if ok, err := d.Commit(); ok || err == nil {
		t.Fatal("commit after abort published", ok, err)
	}
	assertNoTransferTemps(t, dir)
}

func TestDefaultTransferFilesystemCompetingWritersAndPinnedParent(t *testing.T) {
	x := FilesystemTransferExecutor{}
	dir := t.TempDir()
	target := filepath.Join(dir, "winner")
	var all []TransferDestination
	for range 2 {
		d, err := x.OpenTransferDestination(context.Background(), target, false)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = io.WriteString(d, "whole content"); err != nil {
			t.Fatal(err)
		}
		all = append(all, d)
	}
	winners := make(chan bool, 2)
	var wg sync.WaitGroup
	for _, d := range all {
		wg.Add(1)
		go func() { defer wg.Done(); ok, _ := d.Commit(); winners <- ok; _ = d.Abort() }()
	}
	wg.Wait()
	if a, b := <-winners, <-winners; a == b {
		t.Fatal("no-replace race did not have exactly one winner", a, b)
	}
	assertNoTransferTemps(t, dir)
	parent := filepath.Join(dir, "parent")
	if err := os.Mkdir(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	d, err := x.OpenTransferDestination(context.Background(), filepath.Join(parent, "bound"), false)
	if err != nil {
		t.Fatal(err)
	}
	moved := filepath.Join(dir, "moved")
	if err = os.Rename(parent, moved); err != nil {
		t.Fatal(err)
	}
	if err = os.Mkdir(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err = io.WriteString(d, "bound parent"); err != nil {
		t.Fatal(err)
	}
	if ok, err := d.Commit(); !ok || err != nil {
		t.Fatal(ok, err)
	}
	if err = d.Abort(); err != nil {
		t.Fatal(err)
	}
	if raw, err := os.ReadFile(filepath.Join(moved, "bound")); err != nil || string(raw) != "bound parent" {
		t.Fatal(string(raw), err)
	}
	if _, err = os.Stat(filepath.Join(parent, "bound")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("parent replacement redirected publication", err)
	}
	assertNoTransferTemps(t, moved)
}

func assertNoTransferTemps(t *testing.T, directory string) {
	t.Helper()
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".sessionbus-transfer-") {
			t.Fatal("owned temporary file survived cleanup", entry.Name())
		}
	}
}

func TestDefaultTransferManagersHaveDirectFilesystemExecutor(t *testing.T) {
	manager, err := NewConnectionTransfers(nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, manager := range []*Transfers{manager, NewWorker(nil).Transfers()} {
		if _, ok := manager.executor.(FilesystemTransferExecutor); !ok {
			t.Fatal("standard/raw owner has no direct filesystem default")
		}
	}
}

// SPDX-License-Identifier: GPL-3.0-only
package release_test

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func diagnosticFixture(t *testing.T, role, platform, recipe string) (string, string, string) {
	t.Helper()
	root, _, marker := releaseFixture(t, role)
	write(t, filepath.Join(root, "payload", "install"), "#!/bin/sh\nset -eu\n"+recipe)
	asset := "sessionbus-" + role + "-" + strings.ToLower(platform) + "-" + runtime.GOARCH + ".tar.gz"
	archive := filepath.Join(root, asset)
	command := exec.Command("tar", "-czf", archive, "-C", filepath.Join(root, "payload"), "ROLE", "install")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("tar: %v\n%s", err, output)
	}
	body, err := os.ReadFile(archive)
	if err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(root, "SHA256SUMS"), fmt.Sprintf("%x  %s\n", sha256.Sum256(body), asset))
	return root, asset, marker
}

func TestDownloadRecipeFailureDiagnostics(t *testing.T) {
	cases := []struct {
		name            string
		recipe          string
		status          int
		failDiagnostics bool
		failCleanup     bool
		failOutput      bool
	}{
		{name: "success", recipe: "printf 'fixture installed\\n'\n", status: 0},
		{name: "silent failure", recipe: "exit 23\n", status: 23},
		{name: "diagnostics fail", recipe: "exit 23\n", status: 23, failDiagnostics: true},
		{name: "cleanup fails", recipe: "exit 23\n", status: 23, failCleanup: true},
		{name: "diagnostic output fails", recipe: "exit 23\n", status: 23, failOutput: true},
		{name: "payload errexit", recipe: "false\nprintf bad > \"$INSTALL_TEST_MARKER\"\n", status: 1},
	}
	for _, role := range []string{"host", "hub"} {
		for _, platform := range []string{"Linux", "Darwin"} {
			for _, test := range cases {
				t.Run(role+"/"+platform+"/"+test.name, func(t *testing.T) {
					root, _, marker := diagnosticFixture(t, role, platform, test.recipe)
					fake, home, tmp := t.TempDir(), t.TempDir(), t.TempDir()
					calls := filepath.Join(fake, "calls")
					write(t, filepath.Join(fake, "uname"), "#!/bin/sh\ncase \"$1\" in -s) echo \"$INSTALL_TEST_OS\";; -m) echo \"$INSTALL_TEST_ARCH\";; esac\n")
					for _, name := range []string{"systemctl", "journalctl", "tail"} {
						script := "#!/bin/sh\nprintf '" + name + " %s\\n' \"$*\" >> \"$DIAGNOSTIC_CALLS\"\n[ \"$DIAGNOSTIC_FAILURE\" != yes ] || exit 127\n"
						if name == "tail" {
							script += "exec \"$REAL_TAIL\" \"$@\"\n"
						} else {
							script += "echo 'fixture daemon failed: invalid durable session table'\nexit 3\n"
						}
						write(t, filepath.Join(fake, name), script)
					}
					if test.failCleanup {
						write(t, filepath.Join(fake, "rm"), "#!/bin/sh\nexit 97\n")
					}
					service := "sessionbus"
					if role == "hub" {
						service = "sessionbus-hub"
					}
					log := filepath.Join(home, ".local/state/sessionbus/logs/net.antst."+service+".log")
					var lines strings.Builder
					for number := 1; number <= 60; number++ {
						fmt.Fprintf(&lines, "%s-log-%d\n", role, number)
					}
					write(t, log, lines.String())
					realTail, err := exec.LookPath("tail")
					if err != nil {
						t.Fatal(err)
					}
					failed := "no"
					if test.failDiagnostics {
						failed = "yes"
					}
					command := exec.Command("sh", filepath.Join("..", "install-"+role+".sh"))
					command.Env = append(environmentWithout("SESSIONBUS_VERSION", "SESSIONBUS_DOWNLOAD_ROOT", "PATH", "HOME", "TMPDIR", "INSTALL_TEST_MARKER", "INSTALL_TEST_OS", "INSTALL_TEST_ARCH", "DIAGNOSTIC_CALLS", "DIAGNOSTIC_FAILURE", "REAL_TAIL"),
						"SESSIONBUS_DOWNLOAD_ROOT="+(&url.URL{Scheme: "file", Path: root}).String(),
						"PATH="+fake+":"+os.Getenv("PATH"), "HOME="+home, "TMPDIR="+tmp,
						"INSTALL_TEST_MARKER="+marker, "INSTALL_TEST_OS="+platform,
						"INSTALL_TEST_ARCH="+runtime.GOARCH, "DIAGNOSTIC_CALLS="+calls,
						"DIAGNOSTIC_FAILURE="+failed, "REAL_TAIL="+realTail,
					)
					var output []byte
					var runErr error
					if test.failOutput {
						full, err := os.OpenFile("/dev/full", os.O_WRONLY, 0)
						if os.IsNotExist(err) {
							t.Skip("output failure fixture requires /dev/full")
						}
						if err != nil {
							t.Fatal(err)
						}
						defer full.Close()
						command.Stderr = full
						output, runErr = command.Output()
					} else {
						output, runErr = command.CombinedOutput()
					}
					got := 0
					if runErr != nil {
						var exitErr *exec.ExitError
						if !errors.As(runErr, &exitErr) {
							t.Fatal(runErr)
						}
						got = exitErr.ExitCode()
					}
					if got != test.status {
						t.Fatalf("exit = %d, want %d: %s", got, test.status, output)
					}
					recorded, err := os.ReadFile(calls)
					if err != nil && !os.IsNotExist(err) {
						t.Fatal(err)
					}
					if test.status == 0 {
						if string(output) != "fixture installed\n" || len(recorded) != 0 {
							t.Fatalf("success output or unexpected diagnostics: %q, %q", output, recorded)
						}
					} else {
						want := fmt.Sprintf("failed during release installer (exit %d).\n", test.status)
						if !test.failOutput && !strings.Contains(string(output), want) {
							t.Fatalf("missing phase/status: %s", output)
						}
						if platform == "Linux" {
							want := "systemctl --user --no-pager status " + service + ".service\njournalctl --user -u " + service + ".service --no-pager -n 30\n"
							if string(recorded) != want {
								t.Fatalf("wrong diagnostic commands: %q, want %q", recorded, want)
							}
							if !test.failDiagnostics && !test.failOutput && !strings.Contains(string(output), "invalid durable session table") {
								t.Fatalf("missing service error: %s", output)
							}
						} else {
							if string(recorded) != "tail -n 30 "+log+"\n" {
								t.Fatalf("wrong log: %q", recorded)
							}
							if !test.failDiagnostics && !test.failOutput && (!strings.Contains(string(output), role+"-log-31\n") || !strings.Contains(string(output), role+"-log-60\n") || strings.Contains(string(output), role+"-log-30\n")) {
								t.Fatalf("log output not limited to final 30 lines: %s", output)
							}
						}
					}
					if _, err := os.Stat(marker); !os.IsNotExist(err) {
						t.Fatalf("payload continued after false: %v", err)
					}
					left, err := os.ReadDir(tmp)
					if err != nil {
						t.Fatal(err)
					}
					if (len(left) != 0) != test.failCleanup {
						t.Fatalf("unexpected cleanup result: %v", left)
					}
				})
			}
		}
	}
}

func TestDownloadPreRecipeFailureReportsPhase(t *testing.T) {
	for _, role := range []string{"host", "hub"} {
		for _, phase := range []string{"download", "checksum verification"} {
			t.Run(role+"/"+phase, func(t *testing.T) {
				root, asset, marker := releaseFixture(t, role)
				status := 1
				if phase == "download" {
					if err := os.Remove(filepath.Join(root, asset)); err != nil {
						t.Fatal(err)
					}
					status = 37 // curl's file:// read error
				} else {
					write(t, filepath.Join(root, "SHA256SUMS"), strings.Repeat("0", 64)+"  "+asset+"\n")
				}
				fake, tmp := t.TempDir(), t.TempDir()
				calls := filepath.Join(fake, "calls")
				for _, name := range []string{"systemctl", "journalctl", "tail"} {
					write(t, filepath.Join(fake, name), "#!/bin/sh\necho unexpected >> \"$DIAGNOSTIC_CALLS\"\n")
				}
				command := exec.Command("sh", filepath.Join("..", "install-"+role+".sh"))
				command.Env = append(environmentWithout("SESSIONBUS_VERSION", "SESSIONBUS_DOWNLOAD_ROOT", "PATH", "TMPDIR", "INSTALL_TEST_MARKER", "DIAGNOSTIC_CALLS"),
					"SESSIONBUS_DOWNLOAD_ROOT="+(&url.URL{Scheme: "file", Path: root}).String(),
					"PATH="+fake+":"+os.Getenv("PATH"), "TMPDIR="+tmp, "INSTALL_TEST_MARKER="+marker, "DIAGNOSTIC_CALLS="+calls,
				)
				output, err := command.CombinedOutput()
				var exitErr *exec.ExitError
				if !errors.As(err, &exitErr) || exitErr.ExitCode() != status {
					t.Fatalf("exit: %v\n%s", err, output)
				}
				if !strings.Contains(string(output), fmt.Sprintf("failed during %s (exit %d).\n", phase, status)) {
					t.Fatalf("missing phase/status: %s", output)
				}
				for _, path := range []string{marker, calls} {
					if _, err := os.Stat(path); !os.IsNotExist(err) {
						t.Fatalf("unexpected installer/diagnostic execution: %s, %v", path, err)
					}
				}
				left, err := os.ReadDir(tmp)
				if err != nil || len(left) != 0 {
					t.Fatalf("temporary files: %v, %v", left, err)
				}
			})
		}
	}
}

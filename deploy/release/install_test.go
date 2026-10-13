// SPDX-License-Identifier: GPL-3.0-only
package release_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func write(t *testing.T, path, text string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(text), 0700); err != nil {
		t.Fatal(err)
	}
}

func TestRealRecipeReinstallPreservesKeysAndOtherRole(t *testing.T) {
	script, err := os.ReadFile("install")
	if err != nil {
		t.Fatal(err)
	}
	for _, role := range []string{"host", "hub"} {
		t.Run(role, func(t *testing.T) {
			home := filepath.Join(t.TempDir(), "home with spaces")
			src := filepath.Join(t.TempDir(), "source with spaces")
			fake := t.TempDir()
			write(t, filepath.Join(src, "install"), string(script))
			write(t, filepath.Join(src, "ROLE"), role)
			write(t, filepath.Join(src, "REVISION"), "abcdef123456")
			write(t, filepath.Join(src, "LICENSE"), "license")
			write(t, filepath.Join(src, "THIRD-PARTY-NOTICES.txt"), "notices")
			cmds := []string{"sessionbus-hub"}
			if role == "host" {
				cmds = []string{"sessionbus", "sessionbus-call", "example-peer"}
			}
			for _, name := range cmds {
				write(t, filepath.Join(src, "bin", name), "#!/bin/sh\n[ \"${1:-}\" != secret ] || printf 'fixed-secret\\n'\nexit 0\n")
			}
			write(t, filepath.Join(fake, "uname"), "#!/bin/sh\nprintf 'Linux\\n'\n")
			write(t, filepath.Join(fake, "systemctl"), "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$HOME/service-calls\"\nif [ \"$2\" = cat ]; then test -f \"$HOME/.config/systemd/user/$3\"; fi\n")
			write(t, filepath.Join(fake, "loginctl"), "#!/bin/sh\n[ \"$1\" = show-user ] || exit 1\nprintf 'yes\\n'\n")
			// An unrelated role is installed already; the recipe must leave it alone.
			other := "hub"
			if role == "hub" {
				other = "host"
			}
			sentinel := filepath.Join(home, ".local/libexec/sessionbus", other, "keep")
			write(t, sentinel, "unchanged")
			command := func() {
				t.Helper()
				c := exec.Command("sh", filepath.Join(src, "install"))
				c.Env = append(os.Environ(), "HOME="+home, "XDG_CONFIG_HOME="+filepath.Join(home, ".config"), "PATH="+fake+":"+os.Getenv("PATH"))
				if out, err := c.CombinedOutput(); err != nil {
					t.Fatalf("install: %v\n%s", err, out)
				}
			}
			// Earlier manual installers used a real current directory.
			old := filepath.Join(home, ".local/libexec/sessionbus", role, "current", "keep-prior")
			write(t, old, "prior-payload")
			command()
			envPath := filepath.Join(home, ".config/sessionbus/service.env")
			customEnv := "# user configuration\nSESSIONBUS_PRODUCTS=custom-peer,dashi\n"
			if role == "host" {
				b, err := os.ReadFile(envPath)
				want := "SESSIONBUS_PRODUCTS=claude-peer,codex-peer,grok-peer,kilo-peer,omp-peer,opencode-peer,pi-peer,qwen-peer,dashi\n"
				if err != nil || !strings.Contains(string(b), want) {
					t.Fatalf("default products: %q, %v", b, err)
				}
				if err := os.WriteFile(envPath, []byte(customEnv), 0600); err != nil {
					t.Fatal(err)
				}
			}
			unitName, environmentName := "sessionbus.service", "service.env"
			if role == "hub" {
				unitName, environmentName = "sessionbus-hub.service", "hub.env"
			}
			unitPath := filepath.Join(home, ".config/systemd/user", unitName)
			unit, err := os.ReadFile(unitPath)
			if err != nil {
				t.Fatal(err)
			}
			want := "EnvironmentFile=-" + filepath.Join(home, ".config/sessionbus", environmentName)
			if !strings.Contains(string(unit), "\n"+want+"\n") {
				t.Fatalf("rendered unit does not name the environment file: want %q", want)
			}
			if analyzer, err := exec.LookPath("systemd-analyze"); err == nil {
				// verify may exit zero even when it ignores an invalid directive.
				out, err := exec.Command(analyzer, "verify", unitPath).CombinedOutput()
				if err != nil || strings.Contains(string(out), "EnvironmentFile=") {
					t.Fatalf("systemd unit parsing: %v\n%s", err, out)
				}
			}
			preserved, err := filepath.Glob(filepath.Join(home, ".local/libexec/sessionbus", role, "releases/prior.*/current/keep-prior"))
			if err != nil || len(preserved) != 1 {
				t.Fatalf("prior directory lost: %v %v", preserved, err)
			}
			b, err := os.ReadFile(preserved[0])
			if err != nil || string(b) != "prior-payload" {
				t.Fatal("prior payload changed", err)
			}
			key := filepath.Join(home, ".config/sessionbus/host.key")
			if role == "hub" {
				key = filepath.Join(home, ".config/sessionbus/hub.json")
			}
			if err := os.WriteFile(key, []byte("preserve-existing"), 0600); err != nil {
				t.Fatal(err)
			}
			command()
			if role == "host" {
				b, err := os.ReadFile(envPath)
				if err != nil || string(b) != customEnv {
					t.Fatalf("service.env changed on reinstall: %q, %v", b, err)
				}
			}
			b, err = os.ReadFile(key)
			if err != nil || string(b) != "preserve-existing" {
				t.Fatal("secret overwritten", err)
			}
			b, err = os.ReadFile(sentinel)
			if err != nil || string(b) != "unchanged" {
				t.Fatal("other role changed", err)
			}
			target, err := filepath.EvalSymlinks(filepath.Join(home, ".local/libexec/sessionbus", role, "current"))
			if err != nil || !strings.HasSuffix(target, "/releases/abcdef123456") {
				t.Fatal("current link", target, err)
			}
			st, err := os.Stat(key)
			if err != nil || st.Mode().Perm() != 0600 {
				t.Fatal("secret permissions", err)
			}
		})
	}
}

func TestLinuxRecipeRequiresLingeringBeforeServiceActivation(t *testing.T) {
	script, err := os.ReadFile("install")
	if err != nil {
		t.Fatal(err)
	}
	for _, role := range []string{"host", "hub"} {
		for _, state := range []string{"yes", "no", "denied", "query-failed", "unknown"} {
			t.Run(role+"/"+state, func(t *testing.T) {
				home, src, fake := t.TempDir(), t.TempDir(), t.TempDir()
				service, key, environment := "sessionbus", "host.key", "service.env"
				binaries := []string{"sessionbus", "sessionbus-call"}
				if role == "hub" {
					service, key, environment = "sessionbus-hub", "hub.json", "hub.env"
					binaries = []string{"sessionbus-hub"}
				}
				for name, content := range map[string]string{
					"install": string(script), "ROLE": role, "REVISION": "abcdef123456",
					"LICENSE": "license", "THIRD-PARTY-NOTICES.txt": "notices",
				} {
					write(t, filepath.Join(src, name), content)
				}
				for _, name := range binaries {
					write(t, filepath.Join(src, "bin", name), "#!/bin/sh\nexit 0\n")
				}
				config := filepath.Join(home, ".config/sessionbus")
				write(t, filepath.Join(config, key), "preserved-key")
				write(t, filepath.Join(config, environment), "preserved-environment")
				unit := filepath.Join(home, ".config/systemd/user", service+".service")
				write(t, unit, "old-unit")
				root := filepath.Join(home, ".local/libexec/sessionbus", role)
				prior := filepath.Join(root, "releases/111111111111")
				current := filepath.Join(root, "current")
				write(t, filepath.Join(prior, "keep"), "old-payload")
				if err := os.Symlink(prior, current); err != nil {
					t.Fatal(err)
				}
				write(t, filepath.Join(fake, "uname"), "#!/bin/sh\nprintf 'Linux\n'\n")
				write(t, filepath.Join(fake, "id"), "#!/bin/sh\n[ \"$1\" = -un ] || exit 1\nprintf 'linger-test-user\n'\n")
				write(t, filepath.Join(fake, "systemctl"), `#!/bin/sh
printf 'systemctl %s\n' "$*" >> "$HOME/calls"
if [ "$2" = cat ]; then test -f "$HOME/.config/systemd/user/$3"; fi
`)
				write(t, filepath.Join(fake, "loginctl"), `#!/bin/sh
printf 'loginctl %s\n' "$*" >> "$HOME/calls"
[ "$(cat "$HOME/.config/systemd/user/$TEST_SERVICE.service")" = old-unit ] || exit 90
[ "$(readlink "$TEST_CURRENT")" = "$TEST_PRIOR" ] || exit 94
! grep -q '^systemctl --user stop ' "$HOME/calls" || exit 95
[ ! -e "$TEST_ROOT/releases/abcdef123456" ] || exit 96
for stage in "$TEST_ROOT/releases"/.stage.*; do [ ! -e "$stage" ] || exit 97; done
[ "$1" != --no-ask-password ] || shift
case "$1" in
 show-user)
  [ "$2" = linger-test-user ] && [ "$3" = -p ] && [ "$4" = Linger ] && [ "$5" = --value ] || exit 91
  case "$TEST_LINGER_STATE" in
   yes) printf 'yes\n';;
   query-failed) printf 'yes\n'; exit 1;;
   unknown) printf 'unrecognized\n';;
   *) printf 'no\n';;
  esac
  ;;
 enable-linger)
  [ "$2" = linger-test-user ] || exit 92
  [ "$TEST_LINGER_STATE" != denied ] || exit 1
  printf 'enabled\n' > "$HOME/linger-enabled"
  ;;
 *) exit 93;;
esac
`)
				cmd := exec.Command("sh", filepath.Join(src, "install"))
				cmd.Env = append(os.Environ(), "HOME="+home, "XDG_CONFIG_HOME="+filepath.Join(home, ".config"),
					"PATH="+fake+":"+os.Getenv("PATH"), "TEST_SERVICE="+service, "TEST_LINGER_STATE="+state,
					"TEST_ROOT="+root, "TEST_CURRENT="+current, "TEST_PRIOR="+prior)
				out, installErr := cmd.CombinedOutput()
				success := state == "yes" || state == "no"
				if success != (installErr == nil) {
					t.Fatalf("install result: %v\n%s", installErr, out)
				}
				calls, err := os.ReadFile(filepath.Join(home, "calls"))
				if err != nil {
					t.Fatal(err)
				}
				enabled := strings.Count(string(calls), "loginctl --no-ask-password enable-linger linger-test-user\n")
				wantEnable := 0
				if state == "no" || state == "denied" {
					wantEnable = 1
				}
				if enabled != wantEnable || strings.Contains(string(calls), "disable-linger") {
					t.Fatalf("account policy calls = %s", calls)
				}
				query := "loginctl show-user linger-test-user -p Linger --value\n"
				stop := "systemctl --user stop " + service + ".service\n"
				if strings.Count(string(calls), query) != 1 ||
					(success && (strings.Count(string(calls), stop) != 1 ||
						strings.Index(string(calls), query) > strings.Index(string(calls), stop))) ||
					(!success && strings.Contains(string(calls), stop)) {
					t.Fatalf("policy must be checked before stopping old service: %s", calls)
				}
				start := "systemctl --user start " + service + ".service\n"
				if success != strings.Contains(string(calls), start) {
					t.Fatalf("service activation after policy check: %s", calls)
				}
				if state == "no" && (!strings.Contains(string(out), "all user services of the account") ||
					strings.Index(string(calls), "enable-linger") > strings.Index(string(calls), stop)) {
					t.Fatalf("missing notice or wrong ordering: %s\n%s", out, calls)
				}
				if !success && (!strings.Contains(string(out), "administrator") ||
					!strings.Contains(string(out), "loginctl enable-linger linger-test-user") ||
					strings.Contains(string(out), "Installed Sessionbus")) {
					t.Fatalf("policy failure diagnostic: %s", out)
				}
				for name, want := range map[string]string{key: "preserved-key", environment: "preserved-environment"} {
					content, err := os.ReadFile(filepath.Join(config, name))
					if err != nil || string(content) != want {
						t.Fatalf("existing %s lost on success or policy refusal: %q, %v", name, content, err)
					}
				}
				unitBytes, err := os.ReadFile(unit)
				if err != nil || (success && !strings.Contains(string(unitBytes), "\n[Service]\n")) ||
					(!success && string(unitBytes) != "old-unit") {
					t.Fatalf("unit state after policy decision: %s, %v", unitBytes, err)
				}
				wantCurrent := prior
				if success {
					wantCurrent = filepath.Join(root, "releases/abcdef123456")
				}
				if target, err := os.Readlink(current); err != nil || target != wantCurrent {
					t.Fatalf("current release after policy decision: %q, %v", target, err)
				}
				if !success {
					entries, err := os.ReadDir(filepath.Join(root, "releases"))
					if err != nil || len(entries) != 1 || entries[0].Name() != "111111111111" {
						t.Fatalf("policy refusal changed release storage: %v, %v", entries, err)
					}
				}
			})
		}
	}
}

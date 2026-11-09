// SPDX-License-Identifier: GPL-3.0-only

package main

import (
	"bytes"
	"encoding/base64"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/antst/sessionbus/bus/internal/federation"
)

func TestLoadSecretsIsStrictAndMode0600(t *testing.T) {
	secret := base64.StdEncoding.EncodeToString(make([]byte, federation.SecretBytes))
	path := filepath.Join(t.TempDir(), "hub.json")
	write := func(body string, mode os.FileMode) {
		t.Helper()
		if err := os.WriteFile(path, []byte(body), mode); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, mode); err != nil {
			t.Fatal(err)
		}
	}
	write(`{"alpha":"`+secret+`"}`, 0o600)
	if value, err := loadSecrets(path); err != nil || value["alpha"] != secret {
		t.Fatalf("config = %#v, %v", value, err)
	}
	for _, body := range []string{
		`{"alpha":"` + secret + `","alpha":"` + secret + `"}`,
		`{"alpha":1}`,
		`{"alpha":"` + secret + `"} {}`,
	} {
		write(body, 0o600)
		if _, err := loadSecrets(path); err == nil {
			t.Fatalf("accepted %s", body)
		}
	}
	write(`{"alpha":"`+secret+`"}`, 0o644)
	if _, err := loadSecrets(path); err == nil {
		t.Fatal("accepted readable federation config")
	}
}

func TestHubUsesEnvironmentWhenFlagsAreAbsent(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing.json")
	t.Setenv("SESSIONBUS_HUB_LISTEN", "127.0.0.1:0")
	t.Setenv("SESSIONBUS_HUB_CONFIG", missing)
	if err := run(nil); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("environment defaults error = %v", err)
	}
}

func TestHubExplicitPolicyRefusesBeforeListenerAndPreservesSecrets(t *testing.T) {
	dir := t.TempDir()
	config := filepath.Join(dir, "hub.json")
	secret := base64.StdEncoding.EncodeToString(make([]byte, federation.SecretBytes))
	before := []byte(`{"alpha":"` + secret + `"}`)
	if err := os.WriteFile(config, before, 0o600); err != nil {
		t.Fatal(err)
	}
	missing := filepath.Join(dir, "missing-policy.json")
	// A deliberately invalid listen address must never win over the policy
	// load error: an explicit bad path cannot start an unrestricted listener.
	if err := runWithIO([]string{"--listen", "invalid-address", "--config", config, "--policy", missing}, io.Discard, io.Discard); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("listener preceded explicit policy validation", err)
	}
	if err := runWithIO([]string{"--listen", "invalid-address", "--config", config, "--policy", ""}, io.Discard, io.Discard); err == nil || !strings.Contains(err.Error(), "nonempty path") {
		t.Fatal("explicit empty policy became unrestricted", err)
	}
	policy := filepath.Join(dir, "policy.json")
	if err := os.WriteFile(policy, []byte(`{"hosts":{"alpha":{"groups":null}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runWithIO([]string{"--listen", "invalid-address", "--config", config, "--policy", policy}, io.Discard, io.Discard); err == nil || !strings.Contains(err.Error(), "must be an array") {
		t.Fatal("malformed policy fell through to listener", err)
	}
	after, err := os.ReadFile(config)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("policy startup edited the existing secrets map", err)
	}
}

func TestHubHelpListsCommandsFlagsEnvironmentAndExamples(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("XDG_CONFIG_HOME", "")
	t.Setenv("SESSIONBUS_HUB_CONFIG", "")
	for _, test := range []struct {
		name string
		args []string
		want []string
	}{
		{"long help", []string{"--help"}, []string{"sessionbus-hub add-host", "--listen ADDRESS", "SESSIONBUS_HUB_LISTEN", "${XDG_CONFIG_HOME:-$HOME/.config}/sessionbus/hub.json", "Examples:"}},
		{"help command", []string{"help"}, []string{"Commands:", "add-host", "SESSIONBUS_HUB_CONFIG"}},
		{"add-host help", []string{"add-host", "--help"}, []string{"--secret-file FILE", "XDG_CONFIG_HOME", "offline map"}},
		{"help add-host", []string{"help", "add-host"}, []string{"sessionbus-hub add-host", "Restart sessionbus-hub"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if err := runWithIO(test.args, &stdout, &stderr); err != nil {
				t.Fatal(err)
			}
			if stderr.Len() != 0 {
				t.Fatalf("stderr = %q", stderr.String())
			}
			for _, want := range test.want {
				if !strings.Contains(stdout.String(), want) {
					t.Fatalf("help lacks %q:\n%s", want, stdout.String())
				}
			}
		})
	}
	if entries, err := os.ReadDir(os.Getenv("HOME")); err != nil || len(entries) != 0 {
		t.Fatalf("help touched config: %#v, %v", entries, err)
	}
}

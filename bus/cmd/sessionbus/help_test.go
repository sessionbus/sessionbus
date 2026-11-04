// SPDX-License-Identifier: GPL-3.0-only

package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestHelpCommandsDoNotStartDaemonOrReadSecret(t *testing.T) {
	t.Setenv("SESSIONBUS_HUB", "invalid:1")
	t.Setenv("SESSIONBUS_HUB_SECRET_FILE", "/not-present/private-secret")
	for _, args := range [][]string{{"--help"}, {"-h"}, {"help"}, {"help", "roster"}, {"roster", "--help"}, {"secret", "--help"}, {"help", "secret"}, {"help", "lane", "archive"}, {"lane", "archive", "--help"}} {
		var out bytes.Buffer
		if err := runTo(args, &out); err != nil {
			t.Fatalf("%v: %v", args, err)
		}
		if out.Len() == 0 {
			t.Fatalf("empty help: %v", args)
		}
	}
	var out bytes.Buffer
	if err := runTo([]string{"--help"}, &out); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"roster", "secret", "-socket", "-table", "-host", "-products", "-hub", "-hub-secret-file", "SESSIONBUS_SOCKET", "${XDG_CONFIG_HOME:-$HOME/.config}/sessionbus/service.env", "systemctl", "launchctl"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("missing %s: %s", want, out.String())
		}
	}
	if runTo([]string{"help", "nonexistent"}, &bytes.Buffer{}) == nil {
		t.Fatal("unknown help command succeeded")
	}
}

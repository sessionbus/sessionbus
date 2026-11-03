// SPDX-License-Identifier: GPL-3.0-only

package roster

import "testing"

func TestArchiveStrictPresence(t *testing.T) {
	valid := `{"action":"lane.archive","session_id":"native@local"}`
	got, err := DecodeArchive([]byte(valid))
	if err != nil || got.Action != ArchiveAction || got.SessionID != "native@local" {
		t.Fatal(got, err)
	}
	for _, raw := range []string{
		`null`, `[]`, `{}`, `{"session_id":"native"}`, `{"action":"lane.archive"}`,
		`{"action":null,"session_id":"native"}`, `{"action":"lane.archive","session_id":null}`,
		`{"action":"other","session_id":"native"}`, `{"action":"","session_id":"native"}`,
		`{"action":"lane.archive","session_id":""}`, `{"action":"lane.archive","session_id":1}`,
		`{"action":"lane.archive","session_id":"native","local":false}`,
		`{"action":"lane.archive","session_id":"native","target":"native"}`,
		`{"action":"lane.archive","action":"lane.archive","session_id":"native"}`,
		`{"action":"lane.archive","session_id":"native","session_id":"other"}`,
		valid + `{}`, `{"Action":"lane.archive","session_id":"native"}`,
	} {
		if _, err := DecodeArchive([]byte(raw)); err == nil {
			t.Errorf("accepted %s", raw)
		}
	}
}

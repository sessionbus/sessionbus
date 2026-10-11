// SPDX-License-Identifier: GPL-3.0-only

// Package roster defines the read-only, same-user operator projection.
// It deliberately does not reuse SessionSummary.Info or native Open options.
package roster

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"path/filepath"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

const Schema = "sessionbus.roster.v1"
const MaxRows = 4096
const MaxBytes = protocol.MaxFrameBytes - 1024

type Row struct {
	PermissionMode string   `json:"permission_mode,omitempty"`
	SessionID      string   `json:"session_id"`
	Name           string   `json:"name,omitempty"`
	Kind           string   `json:"kind"`
	Product        string   `json:"product"`
	Groups         []string `json:"groups"`
	Connected      bool     `json:"connected"`
	Running        bool     `json:"running"`
	Owner          string   `json:"owner,omitempty"`
	Persistent     bool     `json:"persistent"`
}

type Host struct {
	Host     string   `json:"host"`
	Products []string `json:"products"`
	Sessions []Row    `json:"sessions"`
	Error    string   `json:"error,omitempty"`
}

type Remote struct {
	Hosts []Host `json:"hosts"`
}

type Report struct {
	Schema     string `json:"schema"`
	Federation string `json:"federation"`
	Complete   bool   `json:"complete"`
	Local      Host   `json:"local"`
	Remote     []Host `json:"remote"`
	Error      string `json:"error,omitempty"`
}

type Request struct {
	Local bool `json:"local"`
}

// Socket returns a unique operator endpoint for each public daemon socket
// in one runtime directory.
func Socket(public string) string {
	sum := sha256.Sum256([]byte(filepath.Base(public)))
	return filepath.Join(filepath.Dir(public), fmt.Sprintf("op-%x.sock", sum[:16]))
}

func DecodeRemote(raw []byte) (Remote, error) {
	var value Remote
	if len(raw) > MaxBytes || protocol.DecodeJSON(raw, &value) != nil || value.Hosts == nil || len(value.Hosts) > MaxRows {
		return value, fmt.Errorf("invalid operator roster")
	}
	seen := map[string]bool{}
	for _, host := range value.Hosts {
		if host.Host == "" || seen[host.Host] || host.Sessions == nil || host.Products == nil || len(host.Sessions) > MaxRows {
			return value, fmt.Errorf("invalid operator roster host")
		}
		seen[host.Host] = true
		ids := map[string]bool{}
		for _, row := range host.Sessions {
			if row.SessionID == "" || ids[row.SessionID] || row.Groups == nil || (row.Kind != "peer" && row.Kind != "lane") {
				return value, fmt.Errorf("invalid operator roster row")
			}
			ids[row.SessionID] = true
		}
	}
	return value, nil
}

func Encode(value any) ([]byte, error) {
	body, err := json.Marshal(value)
	if err == nil && len(body) > MaxBytes {
		err = fmt.Errorf("operator roster exceeds response limit")
	}
	return body, err
}

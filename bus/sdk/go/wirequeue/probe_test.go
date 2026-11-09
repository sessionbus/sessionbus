// SPDX-License-Identifier: MIT

package wirequeue

import (
	"bytes"
	"errors"
	"testing"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func TestProbeReservationSurvivesFullControlAndBulkQueues(t *testing.T) {
	q := New()
	completed := 0
	done := func(err error) {
		if err != nil {
			t.Error(err)
		}
		completed++
		if completed == protocol.MaxOperations+5 {
			q.Close(nil)
		}
	}
	for i := 0; i < protocol.MaxOperations; i++ {
		if err := q.Submit(job(false, done)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 4; i++ {
		if err := q.Submit(job(true, done)); err != nil {
			t.Fatal(err)
		}
	}
	probe := job(false, done)
	probe.Probe, probe.Weight = true, 128
	if err := q.Submit(probe); err != nil {
		t.Fatal("ordinary saturation hid probe", err)
	}
	duplicate := job(false, nil)
	duplicate.Probe, duplicate.Weight = true, 128
	if err := q.Submit(duplicate); !errors.Is(err, ErrCapacity) {
		t.Fatal("unbounded probe queue", err)
	}
	var output bytes.Buffer
	if err := q.Run(&output); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	frames := bytes.Split(bytes.TrimSpace(output.Bytes()), []byte{'\n'})
	if len(frames) != protocol.MaxOperations+5 {
		t.Fatal(len(frames))
	}
	for i, raw := range frames {
		frame, err := protocol.DecodeFrame(raw)
		if err != nil || frame.ID != int64(i+1) {
			t.Fatalf("dispatch-order IDs: %d %#v %v", i, frame, err)
		}
	}
	firstBulk, _ := protocol.DecodeFrame(frames[9])
	if firstBulk.Method != "transfer.chunk" {
		t.Fatal("probe changed existing bulk fairness", firstBulk.Method)
	}
}

func TestProbeCloseIsBoundedAndCompletesExactlyOnce(t *testing.T) {
	q := New()
	completions := 0
	probe := job(false, func(err error) {
		if !errors.Is(err, ErrClosed) {
			t.Error(err)
		}
		completions++
	})
	probe.Probe, probe.Weight = true, 128
	if err := q.Submit(probe); err != nil {
		t.Fatal(err)
	}
	q.Close(nil)
	q.Close(nil)
	if completions != 1 {
		t.Fatal(completions)
	}
	for _, invalid := range []*Job{{Probe: true, Request: true, Weight: 129, Build: probe.Build}, {Probe: true, Bulk: true, Request: true, Weight: 128, Build: probe.Build}, {Probe: true, Weight: 128, Build: probe.Build}} {
		if err := New().Submit(invalid); !errors.Is(err, ErrCapacity) {
			t.Fatal("invalid probe admitted", err)
		}
	}
}

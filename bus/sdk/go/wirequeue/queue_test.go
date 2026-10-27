// SPDX-License-Identifier: MIT

package wirequeue

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

func job(bulk bool, done func(error)) *Job {
	method := "session.list"
	params := []byte(`{}`)
	if bulk {
		method = "transfer.chunk"
		params = []byte(`{"id":"x","offset":0,"data":"YQ=="}`)
	}
	return &Job{Bulk: bulk, Request: true, Weight: 256, Build: func(id int64) ([]byte, error) { return protocol.EncodeRequest(id, method, params) }, Complete: done}
}

func TestPriorityFairnessAndIDsFollowActualWireOrder(t *testing.T) {
	q := New()
	var b bytes.Buffer
	completed := 0
	done := func(e error) {
		if e != nil {
			t.Error(e)
		}
		completed++
		if completed == 12 {
			q.Close(nil)
		}
	}
	// Bulk is submitted first, but its request must not take ID1 before selection.
	for i := 0; i < 2; i++ {
		if e := q.Submit(job(true, done)); e != nil {
			t.Fatal(e)
		}
	}
	for i := 0; i < 10; i++ {
		if e := q.Submit(job(false, done)); e != nil {
			t.Fatal(e)
		}
	}
	if e := q.Run(&b); !errors.Is(e, ErrClosed) {
		t.Fatal(e)
	}
	frames := bytes.Split(bytes.TrimSpace(b.Bytes()), []byte{'\n'})
	if len(frames) != 12 {
		t.Fatal(len(frames))
	}
	for i, raw := range frames {
		frame, e := protocol.DecodeFrame(raw)
		if e != nil {
			t.Fatal(e)
		}
		if frame.ID != int64(i+1) {
			t.Fatalf("wire request IDs were reordered: %d/%d", i, frame.ID)
		}
		bulk := i == 8 || i == 11
		if (frame.Method == "transfer.chunk") != bulk {
			t.Fatalf("bad8control/1bulk fairness at%d: %s", i, frame.Method)
		}
	}
}

func TestBulkSaturationDoesNotConsumeControlAdmission(t *testing.T) {
	q := New()
	for i := 0; i < 4; i++ {
		if e := q.Submit(job(true, nil)); e != nil {
			t.Fatal(e)
		}
	}
	if e := q.Submit(job(true, nil)); e != ErrCapacity {
		t.Fatal(e)
	}
	for i := 0; i < protocol.MaxOperations; i++ {
		if e := q.Submit(job(false, nil)); e != nil {
			t.Fatal(e)
		}
	}
	if e := q.Submit(job(false, nil)); e != ErrCapacity {
		t.Fatal(e)
	}
	if u := q.Usage(); u.Closed || u.Bulk != 4 || u.Control != 256 {
		t.Fatal(u)
	}
	q.Close(nil)
}

type partial struct {
	bytes.Buffer
	fail bool
}

func (p *partial) Write(b []byte) (int, error) {
	if p.fail {
		return 0, io.ErrClosedPipe
	}
	p.fail = true
	return p.Buffer.Write(b[:min(3, len(b))])
}

func TestPartialWriteFailureSettlesEveryOwnerOnceWithoutReplay(t *testing.T) {
	q := New()
	var mu sync.Mutex
	counts := map[int]int{}
	for i := 0; i < 6; i++ {
		i := i
		j := job(i < 3, func(e error) {
			if e == nil {
				t.Error("lost write became success")
			}
			mu.Lock()
			counts[i]++
			mu.Unlock()
		})
		if e := q.Submit(j); e != nil {
			t.Fatal(e)
		}
	}
	p := &partial{}
	if e := q.Run(p); !errors.Is(e, io.ErrClosedPipe) {
		t.Fatal(e)
	}
	q.Close(errors.New("second terminal"))
	mu.Lock()
	defer mu.Unlock()
	for i := 0; i < 6; i++ {
		if counts[i] != 1 {
			t.Fatal(i, counts)
		}
	}
	if e := q.Submit(job(false, nil)); !errors.Is(e, io.ErrClosedPipe) {
		t.Fatal(e)
	}
	if len(p.Bytes()) != 3 {
		t.Fatal("partial frame replayed", p.Bytes())
	}
}

func TestRepliesKeepCorrelationWithoutAdvancingRequestID(t *testing.T) {
	q := New()
	var b bytes.Buffer
	n := 0
	done := func(e error) {
		if e != nil {
			t.Error(e)
		}
		n++
		if n == 2 {
			q.Close(nil)
		}
	}
	reply := &Job{Weight: 256, Build: func(id int64) ([]byte, error) {
		if id != 0 {
			t.Error(id)
		}
		return protocol.ResultBytes(91, "session.list", protocol.SessionListResult{Sessions: []protocol.SessionSummary{}})
	}, Complete: done}
	if e := q.Submit(reply); e != nil {
		t.Fatal(e)
	}
	if e := q.Submit(job(false, done)); e != nil {
		t.Fatal(e)
	}
	_ = q.Run(&b)
	lines := bytes.Split(bytes.TrimSpace(b.Bytes()), []byte{'\n'})
	var a, c map[string]any
	if e := json.Unmarshal(lines[0], &a); e != nil {
		t.Fatal(e)
	}
	if e := json.Unmarshal(lines[1], &c); e != nil {
		t.Fatal(e)
	}
	if a["id"] != float64(91) || c["id"] != float64(1) {
		t.Fatal(a, c)
	}
}

type heldWriter struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *heldWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	<-w.release
	return len(p), nil
}
func TestInWriteBulkStaysChargedAcrossQueueClose(t *testing.T) {
	q := New()
	writer := &heldWriter{entered: make(chan struct{}), release: make(chan struct{})}
	if err := q.Submit(job(true, nil)); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- q.Run(writer) }()
	select {
	case <-writer.entered:
	case <-time.After(time.Second):
		t.Fatal("writer did not select")
	}
	for i := 0; i < 3; i++ {
		if err := q.Submit(job(true, nil)); err != nil {
			t.Fatal(err)
		}
	}
	if err := q.Submit(job(true, nil)); err != ErrCapacity {
		t.Fatal("in-write frame escaped four-slot cap", err)
	}
	if u := q.Usage(); u.Bulk != 4 || u.BulkBytes != 4*256 {
		t.Fatal(u)
	}
	if err := q.Submit(job(false, nil)); err != nil {
		t.Fatal("bulk consumed control admission", err)
	}
	q.Close(nil)
	if u := q.Usage(); u.Bulk != 1 || u.BulkBytes != 256 {
		t.Fatal("close released actual in-write bytes early", u)
	}
	close(writer.release)
	<-done
	if u := q.Usage(); u.Bulk != 0 || u.BulkBytes != 0 {
		t.Fatal("actual write did not release", u)
	}
}

func TestRoundRobinBulkStreamsPreservePerStreamOrder(t *testing.T) {
	q := New()
	var output bytes.Buffer
	completed := 0
	for _, stream := range []string{"a", "a", "b", "b"} {
		j := job(true, func(err error) {
			if err != nil {
				t.Error(err)
			}
			completed++
			if completed == 4 {
				q.Close(nil)
			}
		})
		j.Stream = stream
		j.Build = func(id int64) ([]byte, error) {
			return protocol.EncodeRequest(id, "transfer.chunk", []byte(`{"id":"`+stream+`","offset":0,"data":"YQ=="}`))
		}
		if err := q.Submit(j); err != nil {
			t.Fatal(err)
		}
	}
	_ = q.Run(&output)
	var streams []string
	for _, line := range bytes.Split(bytes.TrimSpace(output.Bytes()), []byte{'\n'}) {
		var f struct {
			Params protocol.TransferChunk `json:"params"`
		}
		if err := json.Unmarshal(line, &f); err != nil {
			t.Fatal(err)
		}
		streams = append(streams, f.Params.ID)
	}
	if fmt.Sprint(streams) != "[a b a b]" {
		t.Fatal(streams)
	}
}

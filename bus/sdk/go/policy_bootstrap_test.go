// SPDX-License-Identifier: MIT

package sessionkit

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/antst/sessionbus/bus/sdk/go/protocol"
)

// Uses preexisting SDK APIs and raw hello fields so the IDENTICAL fixture can
// execute on the old SDK, where only the fp1 capability assertion must fail.
func TestWorkerPolicyBootstrapMatrix(t *testing.T) {
	for _, token := range []string{"legacy-random", "sbt1:token-old", "sbt1:av1:token-old", "sbt1:av1:fp1:token-new"} {
		t.Run(token, func(t *testing.T) {
			setEnvironment(t, token, "")
			left, right := net.Pipe()
			product := &fakeProduct{}
			worker := NewWorker(product)
			product.worker = worker
			worker.dial = func(context.Context, string, string) (net.Conn, error) { return left, nil }
			got := make(chan json.RawMessage, 1)
			serverDone := make(chan struct{})
			go func() {
				defer close(serverDone)
				encoded, err := bufio.NewReader(right).ReadBytes('\n')
				if err != nil {
					t.Error(err)
					return
				}
				frame, err := protocol.DecodeFrame(encoded[:len(encoded)-1])
				if err != nil {
					t.Error(err)
					return
				}
				if _, err := protocol.DecodeParams(frame.Method, frame.Params); err != nil {
					t.Error("closed hello decoder refused worker", err)
					return
				}
				got <- frame.Params
				body, err := protocol.ResultBytes(frame.ID, frame.Method, protocol.HelloResult{})
				if err != nil {
					t.Error(err)
					return
				}
				_, _ = right.Write(body)
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- worker.Serve(ctx) }()
			t.Cleanup(func() { worker.Shutdown(); _ = right.Close(); <-done; <-serverDone })
			var raw json.RawMessage
			select {
			case raw = <-got:
			case <-ctx.Done():
				t.Fatal("worker hello missing")
			}
			t.Logf("raw hello params: %s", raw)
			var fields map[string]any
			if err := json.Unmarshal(raw, &fields); err != nil {
				t.Fatal(err)
			}
			if fields["launch_token"] != token {
				t.Fatal("opaque launch token changed", fields)
			}
			if have := fields["supports_transfer"] == true; have != strings.HasPrefix(token, "sbt1:") {
				t.Fatal("old transfer recognition lost", fields)
			}
			if have := fields["supports_policy"] == true; have != strings.HasPrefix(token, "sbt1:av1:fp1:") {
				t.Fatal("policy bootstrap mismatch", fields)
			}
			if !strings.HasPrefix(token, "sbt1:av1:fp1:") {
				if _, exists := fields["supports_policy"]; exists {
					t.Fatal("old daemon received new hello field", fields)
				}
			}
		})
	}
}

//go:build darwin || linux

package hosted

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestCapacityGuards(t *testing.T) {
	t.Run("budget", func(t *testing.T) {
		w := newCapacityWire(roundTripFunc(func(*http.Request) (*http.Response, error) { t.Fatal("network must not run"); return nil, nil }))
		w.attempts.Store(600)
		r, _ := http.NewRequest("POST", "https://invalid.invalid", nil)
		if _, e := w.RoundTrip(r); e == nil {
			t.Fatal("budget allowed request")
		}
	})
	t.Run("server_error_stops", func(t *testing.T) {
		calls := 0
		w := newCapacityWire(roundTripFunc(func(*http.Request) (*http.Response, error) {
			calls++
			return &http.Response{StatusCode: 503, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(""))}, nil
		}))
		r, _ := http.NewRequest("POST", "https://invalid.invalid", nil)
		_, e := w.RoundTrip(r)
		require(t, e)
		if _, e = w.RoundTrip(r); e == nil || calls != 1 {
			t.Fatal("server error did not stop new requests")
		}
	})
	t.Run("cancelled_slot", func(t *testing.T) {
		w := newCapacityWire(roundTripFunc(func(*http.Request) (*http.Response, error) { t.Fatal("network must not run"); return nil, nil }))
		for i := 0; i < 16; i++ {
			w.slots <- struct{}{}
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		r, _ := http.NewRequestWithContext(ctx, "POST", "https://invalid.invalid", nil)
		if _, e := w.RoundTrip(r); e == nil {
			t.Fatal("cancelled slot wait succeeded")
		}
	})
	t.Run("percentiles", func(t *testing.T) {
		x := summarize([]float64{4, 1, 3, 2})
		if x.Samples != 4 || x.P50 != 2 || x.P95 != 4 || x.P99 != 4 || x.Max != 4 {
			t.Fatal("wrong nearest-rank quantiles")
		}
	})
	for _, mode := range []string{"drop", "delay"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			committed := make(chan struct{})
			w := newCapacityWire(roundTripFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("reply"))}, nil
			}))
			f := &renewalFault{client: &http.Client{Transport: w}, mode: mode, injected: committed}
			r, _ := http.NewRequestWithContext(ctx, "POST", "https://invalid.invalid/Renew", nil)
			done := make(chan error, 1)
			go func() { _, e := f.Do(r); done <- e }()
			<-committed
			if mode == "delay" {
				cancel()
			}
			if e := <-done; e == nil {
				t.Fatal("response fault not injected")
			}
			if !f.hit.Load() {
				t.Fatal("no committed response observed")
			}
		})
	}
}

func TestStagingCapacity(t *testing.T) {
	if os.Getenv("PINTLE_RUN_HOSTED_CAPACITY") != "1" {
		t.Skip("requires explicit just test-staging-capacity")
	}
	root, e := filepath.Abs("../..")
	require(t, e)
	var keys []stagingCredential
	require(t, json.Unmarshal(privateFile(t, filepath.Join(root, ".hermit/staging/credentials.json")), &keys))
	if len(keys) != 1 || len(keys[0].Namespaces) != 1 || keys[0].Namespaces[0] != "staging" {
		t.Fatal("one staging-only credential required")
	}
	endpoint := os.Getenv("PINTLE_STAGING_URL")
	require(t, validateTarget(endpoint, keys[0].Token))
	var expiry int64
	_, e = fmt.Sscan(keys[0].Expires, &expiry)
	require(t, e)
	if time.Until(time.UnixMilli(expiry)) < 15*time.Minute {
		t.Fatal("credential requires 15 minutes remaining")
	}
	RunCapacity(t, endpoint, keys[0].Token, func(r CapacityReport) {
		b := CapacityJSON(r)
		require(t, os.WriteFile(filepath.Join(root, "test-results/staging-capacity.json"), b, 0600))
		t.Logf("sanitized capacity report: %s", b)
	})
}

//go:build darwin || linux

package integration

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pintle "github.com/Noot-tooN/Pintle/sdk/go/pintle"
)

// Read a successful committed response before discarding it. No elapsed time
// chooses the fault ordering, and no server test hooks are needed.
type dropLeaseReply struct {
	method  string
	dropped atomic.Bool
}

func (d *dropLeaseReply) Do(r *http.Request) (*http.Response, error) {
	res, err := client.Do(r)
	if err != nil {
		return nil, err
	}
	if res.StatusCode == 200 && strings.HasSuffix(r.URL.Path, "/"+d.method) && d.dropped.CompareAndSwap(false, true) {
		_, err = io.Copy(io.Discard, io.LimitReader(res.Body, 16385))
		res.Body.Close()
		if err != nil {
			return nil, err
		}
		return nil, errors.New("test discarded committed " + d.method + " reply")
	}
	return res, nil
}
func TestSDKWithLease(t *testing.T) {
	credential := token(t)
	_, _, _, runtime := staticSetup(t, []object{staticKey(credential, "with-lease", "developer")})
	defer runtime.stop()
	newClient := func(transport *dropLeaseReply) *pintle.Client {
		var c *pintle.Client
		var err error
		if transport == nil {
			c, err = pintle.New(runtime.base, credential, nil)
		} else {
			c, err = pintle.New(runtime.base, credential, transport)
		}
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	t.Run("reuse-beyond-session-and-sequence-limits", func(t *testing.T) {
		c := newClient(nil)
		r := pintle.Resource{Namespace: "test", Key: "with-lease-reuse"}
		for i := 1; i <= 40; i++ {
			var work context.Context
			err := c.WithLease(ctx, r, pintle.LeaseOptions{}, func(w context.Context, l pintle.Lease) error {
				work = w
				if l.FencingToken != strconv.Itoa(i) {
					t.Errorf("fence %s want %d", l.FencingToken, i)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if work.Err() == nil {
				t.Fatal("work context escaped callback lifetime")
			}
		}
	})
	for _, method := range []string{"Acquire", "Renew", "Release"} {
		t.Run("lost-"+method, func(t *testing.T) {
			fault := &dropLeaseReply{method: method}
			c := newClient(fault)
			r := pintle.Resource{Namespace: "test", Key: "with-lease-drop-" + method}
			calls := 0
			err := c.WithLease(ctx, r, pintle.LeaseOptions{}, func(context.Context, pintle.Lease) error { calls++; return nil })
			var recovery *pintle.WithLeaseError
			if !fault.dropped.Load() || !errors.As(err, &recovery) {
				t.Fatalf("missing recoverable fault: %v", err)
			}
			wantCalls := 0
			if method == "Release" {
				wantCalls = 1
			}
			if calls != wantCalls {
				t.Fatalf("callback ran %d times, want %d", calls, wantCalls)
			}
			if method == "Acquire" && recovery.Wait != nil {
				t.Fatal("CancelPending did not reconcile committed grant")
			}
			if method == "Renew" {
				req := recovery.Renew
				if req == nil {
					t.Fatal("lost renewal identity")
				}
				status, err := c.GetAttemptStatus(ctx, r, req.RequestID, req.Attempt)
				if err != nil || status.Renewed == nil || status.Renewed.State != pintle.Released {
					t.Fatalf("renewal recovery: %v", err)
				}
			}
			if method == "Release" || method == "Renew" {
				req := recovery.Release
				if req == nil {
					t.Fatal("missing cleanup identity")
				}
				released, err := c.Release(ctx, *req)
				if err != nil || !released.Replayed {
					t.Fatalf("release not recovered by same request: %v", err)
				}
				// Renewal status above consumed its result; now the whole prefix is known.
				if err = c.AcknowledgeAttempt(ctx, r, *req.Attempt); err != nil {
					t.Fatal(err)
				}
			}
			// All three schedules release their grant; a deliberate subsequent call
			// receives exactly the next fence, with no hidden reacquisition/callback retry.
			if err = c.WithLease(ctx, r, pintle.LeaseOptions{}, func(_ context.Context, l pintle.Lease) error {
				if l.FencingToken != "2" {
					t.Errorf("extra grant: %s", l.FencingToken)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
	t.Run("parent-cancellation", func(t *testing.T) {
		c := newClient(nil)
		parent, stop := context.WithCancel(ctx)
		defer stop()
		r := pintle.Resource{Namespace: "test", Key: "with-lease-cancel"}
		err := c.WithLease(parent, r, pintle.LeaseOptions{}, func(work context.Context, _ pintle.Lease) error {
			stop()
			<-work.Done()
			return nil
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled work reported success: %v", err)
		}
		if err = c.WithLease(ctx, r, pintle.LeaseOptions{}, func(_ context.Context, l pintle.Lease) error {
			if l.FencingToken != "2" {
				t.Error("cleanup failed")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})
}

//go:build (darwin || linux) && testhooks

package integration

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	pintle "github.com/Noot-tooN/Pintle/sdk/go/pintle"
)

func TestP2WithLeaseQueuedHandoffAndCancellation(t *testing.T) {
	for _, cancelWaiter := range []bool{false, true} {
		name := "handoff"
		if cancelWaiter {
			name = "cancel"
		}
		t.Run(name, func(t *testing.T) {
			h := configuredSessionHarness(t, "P2_ENABLED:true")
			defer h.r.stop()
			var now atomic.Uint64
			now.Store(1000)
			transport := &controlledMaintenanceHTTP{controller: h.c, now: &now, name: "with-lease"}
			c := sdk(t, h.r.base, h.a, transport)
			r := pintle.Resource{Namespace: "test", Key: "with-lease-queue"}
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			ownerEntered := make(chan struct{})
			finishOwner := make(chan struct{})
			ownerResult := make(chan error, 1)
			waiterResult := make(chan error, 1)
			var active atomic.Int32
			go func() {
				ownerResult <- c.WithLease(ctx, r, pintle.LeaseOptions{}, func(_ context.Context, l pintle.Lease) error {
					if l.FencingToken != "1" || !active.CompareAndSwap(0, 1) {
						t.Error("invalid first callback")
					}
					close(ownerEntered)
					select {
					case <-finishOwner:
					case <-ctx.Done():
						return ctx.Err()
					}
					active.Store(0)
					return nil
				})
			}()
			select {
			case <-ownerEntered:
			case <-ctx.Done():
				t.Fatal("owner watchdog")
			}
			// First invocation has completed OpenSession, Acquire, Renew, ACK. The
			// concurrent invocation borrows a different session, then queues Acquire.
			registered := h.c.hold("maintenance-with-lease-6", "wait_registered")
			waiterCtx, stopWaiter := context.WithCancel(ctx)
			defer stopWaiter()
			go func() {
				waiterResult <- c.WithLease(waiterCtx, r, pintle.LeaseOptions{}, func(_ context.Context, l pintle.Lease) error {
					if cancelWaiter {
						t.Error("cancelled waiter callback ran")
					}
					if l.FencingToken != "2" || !active.CompareAndSwap(0, 1) {
						t.Error("callbacks overlap or fence changed")
					}
					active.Store(0)
					return nil
				})
			}()
			arrived(t, registered) // durable queue admission, not scheduler timing
			if cancelWaiter {
				stopWaiter()
			}
			release(registered, directive{Now: 1000})
			if cancelWaiter {
				select {
				case err := <-waiterResult:
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("waiter result: %v", err)
					}
				case <-ctx.Done():
					t.Fatal("waiter watchdog")
				}
			}
			close(finishOwner)
			select {
			case err := <-ownerResult:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("owner watchdog")
			}
			if !cancelWaiter {
				select {
				case err := <-waiterResult:
					if err != nil {
						t.Fatal(err)
					}
				case <-ctx.Done():
					t.Fatal("handoff watchdog")
				}
			} else {
				// The cancelled waiter must not consume a fence or be revived on release.
				if err := c.WithLease(ctx, r, pintle.LeaseOptions{}, func(_ context.Context, l pintle.Lease) error {
					if l.FencingToken != "2" {
						t.Errorf("cancel consumed grant: %s", l.FencingToken)
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

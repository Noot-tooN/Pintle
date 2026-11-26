package pintle

import (
	"context"
	"errors"
	v1 "github.com/Noot-tooN/Pintle/gen/go/pintle/v1"
	"google.golang.org/protobuf/proto"
	"testing"
	"time"
)

func TestSemaphorePreparationAndObservation(t *testing.T) {
	c, err := New("http://localhost:8787", "test", nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, capacity := range []uint32{0, 65, ^uint32(0)} {
		if _, err := c.Semaphore(capacity); err == nil {
			t.Fatal("invalid capacity")
		}
	}
	sem, err := c.Semaphore(2)
	if err != nil {
		t.Fatal(err)
	}
	s := &Session{client: sem, id: "1", resource: Resource{"test", "key"}, next: 1, window: 32}
	l := sampleLease()
	if _, err := s.NewRenew(l, time.Second); err == nil {
		t.Fatal("mutex renewal prepared on semaphore")
	}
	if _, err := s.NewRelease(l); err == nil {
		t.Fatal("mutex release prepared on semaphore")
	}
	a, err := s.NewAcquire("worker", time.Second)
	if err != nil || a.Capacity != 2 || a.Attempt.Sequence != 1 {
		t.Fatal("preparation consumed sequence or lost capacity")
	}
	w, err := wireWait(WaitRequest{Acquisition: a, WaitTimeout: time.Second})
	if err != nil || w.Acquisition.SemaphoreCapacity != 2 {
		t.Fatal("capacity lost on wire")
	}
	if _, err := c.TryAcquire(context.Background(), a); err == nil {
		t.Fatal("cross-primitive acquisition sent")
	}
	if err := c.WithPermit(context.Background(), s.resource, LeaseOptions{}, func(context.Context, Lease) error { t.Error("mutex callback"); return nil }); err == nil {
		t.Fatal("WithPermit accepted mutex client")
	}
	for _, p := range [][2]uint32{{0, 2}, {3, 2}, {1, 0}, {1, 65}} {
		l.PermitSlot, l.Capacity = p[0], p[1]
		if validateOwnership(l) == nil {
			t.Fatal("invalid permit identity")
		}
	}
	receipt := wireLease()
	receipt.SemaphoreCapacity = 2
	receipt.PermitSlot = 1
	for _, p := range [][2]uint32{{2, 2}, {1, 3}, {0, 0}} {
		current := proto.Clone(receipt).(*v1.GrantReceipt)
		current.PermitSlot, current.SemaphoreCapacity = p[0], p[1]
		if _, err := grant(receipt, false, &v1.LeaseObservation{State: v1.LeaseState_LEASE_STATE_ACTIVE, Current: current}); err == nil {
			t.Fatal("changed permit identity accepted")
		}
	}
}
func TestWithPermitLifecycleAndCancellation(t *testing.T) {
	for _, mode := range []string{"success", "cancel", "renew-uncertain"} {
		t.Run(mode, func(t *testing.T) {
			f := &leaseRPC{}
			c := leaseTestClient(f)
			c.capacity = 2
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			unknown := errors.New("renewal reply lost")
			if mode == "renew-uncertain" {
				f.renewHook = func(context.Context, int) error { return unknown }
			}
			called := 0
			err := c.WithPermit(ctx, Resource{"test", "key"}, LeaseOptions{}, func(work context.Context, l Lease) error {
				called++
				if l.Capacity != 2 || l.PermitSlot != 1 || l.FencingToken != "9007199254740993" {
					t.Error("permit identity changed")
				}
				if mode == "cancel" {
					cancel()
					<-work.Done()
				}
				return nil
			})
			if mode == "renew-uncertain" {
				if called != 0 || !errors.Is(err, unknown) {
					t.Fatalf("uncertain renewal ran callback: %v", err)
				}
				e := leaseFailure(t, err)
				if e.Renew == nil || e.Renew.Lease.Capacity != 2 {
					t.Fatal("missing semaphore recovery intent")
				}
			} else if called != 1 {
				t.Fatal("callback count")
			}
			if mode == "success" && err != nil {
				t.Fatal(err)
			}
			if mode == "cancel" && !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if f.releases != 1 || f.renews != 1 {
				t.Fatal("permit lifecycle incomplete")
			}
			if f.acquireRequest.Acquisition.SemaphoreCapacity != 2 {
				t.Fatal("capacity omitted")
			}
		})
	}
}

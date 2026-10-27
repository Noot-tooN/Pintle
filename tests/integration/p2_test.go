//go:build darwin || linux

package integration

import (
	"context"
	pintle "github.com/Noot-tooN/Pintle/sdk/go/pintle"
	"testing"
	"time"
)

func TestP2SDK(t *testing.T) {
	_, _, _, a, _, r := leaseSetup(t, "P2_ENABLED:true")
	defer r.stop()
	c := sdk(t, r.base, a, client)
	ctx := context.Background()
	resource := pintle.Resource{Namespace: "test", Key: "expiry"}
	s, err := c.OpenSession(ctx, resource)
	if err != nil {
		t.Fatal(err)
	}
	req, _ := s.NewAcquire("owner", 300*time.Millisecond)
	g, err := c.TryAcquire(ctx, req)
	before := requireLease(t, g, err)
	wait, err := s.NewWaitAcquire("waiter", time.Second, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.Acquire(ctx, wait)
	if err != nil {
		t.Fatal(err)
	}
	after := requireLease(t, got.Granted, nil)
	assertSame(t, before.FencingToken, "1")
	assertSame(t, after.FencingToken, "2")
	retry, err := c.Acquire(ctx, wait)
	if err != nil {
		t.Fatal(err)
	}
	assertSame(t, *retry.Granted.Receipt, *got.Granted.Receipt)
	cancelled, err := c.CancelPending(ctx, wait)
	if err != nil {
		t.Fatal(err)
	}
	assertSame(t, *cancelled.Granted.Receipt, *got.Granted.Receipt)
	unknown, _ := s.NewWaitAcquire("never-admitted", time.Second, time.Second)
	cancelled, err = c.CancelPending(ctx, unknown)
	if err != nil {
		t.Fatal(err)
	}
	assertSame(t, cancelled.State, pintle.Cancelled)
	retry, err = c.Acquire(ctx, unknown)
	if err != nil {
		t.Fatal(err)
	}
	assertSame(t, retry.State, pintle.Cancelled)
	status, err := s.Status(ctx, unknown.Acquisition.RequestID, unknown.Acquisition.Attempt)
	if err != nil {
		t.Fatal(err)
	}
	assertSame(t, status.Waiting, pintle.Cancelled)
}

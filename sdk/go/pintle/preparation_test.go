package pintle

import (
	"context"
	"strings"
	"testing"
	"time"
)

// Invalid preparation must not leave an unsendable gap in the contiguous ACK window.
func TestInvalidCapabilityDoesNotConsumeSessionPosition(t *testing.T) {
	for _, capability := range []string{"", "short", strings.Repeat("A", 64), strings.Repeat("z", 64)} {
		t.Run(capability, func(t *testing.T) {
			s := &Session{client: &Client{}, id: "1", resource: Resource{"test", "key"}, next: 1, window: 32}
			lease := sampleLease()
			lease.LeaseID = capability
			if _, err := s.NewRenew(lease, time.Second); err == nil {
				t.Fatal("invalid renewal capability accepted")
			}
			if _, err := s.NewRelease(lease); err == nil {
				t.Fatal("invalid release capability accepted")
			}
			r, err := s.NewAcquire("worker", time.Second)
			if err != nil || r.Attempt.Sequence != 1 {
				t.Fatal("invalid preparation consumed a sequence")
			}
		})
	}
}

func TestInvalidRenewalFenceFailsBeforeAllocationOrRPC(t *testing.T) {
	for _, fence := range []string{"", "0", "01", "invalid", "18446744073709551616"} {
		t.Run(fence, func(t *testing.T) {
			ctx := context.Background()
			f := &leaseRPC{lease: wireLease()}
			c := leaseTestClient(f)
			lease := sampleLease()
			lease.FencingToken = fence
			s := &Session{client: c, resource: lease.Resource, id: "1", next: 1, window: 32}
			if _, err := s.NewRenew(lease, time.Second); err == nil || s.next != 1 {
				t.Error("invalid renewal was prepared or consumed a sequence")
			}
			if _, err := c.Renew(ctx, RenewRequest{Lease: lease, TTL: time.Second}); err == nil || f.renews != 0 {
				t.Error("invalid renewal reached the transport or reported success")
			}
			m, err := c.Maintain(ctx, lease, time.Second)
			if m != nil {
				m.Stop()
			}
			if err == nil || f.opens != 0 {
				t.Error("maintenance opened a session for an invalid fence")
			}
		})
	}
}

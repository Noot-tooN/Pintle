package pintle

import (
	"strings"
	"testing"
	"time"
)

// Invalid preparation must not leave an unsendable gap in the contiguous ACK window.
func TestInvalidCapabilityDoesNotConsumeSessionPosition(t *testing.T) {
	for _, capability := range []string{"", "short", strings.Repeat("A", 64), strings.Repeat("z", 64)} {
		t.Run(capability, func(t *testing.T) {
			s := &Session{id: "1", resource: Resource{"test", "key"}, next: 1, window: 32}
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

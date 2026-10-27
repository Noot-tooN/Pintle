package pintle

import (
	"sync"
	"testing"
	"time"
)

func TestSessionConcurrentAllocationAndReleaseReservation(t *testing.T) {
	s := &Session{id: "1", resource: Resource{"test", "key"}, next: 1, window: 32}
	if _, err := s.NewAcquire("worker", 0); err == nil {
		t.Fatal("invalid TTL consumed a sequence")
	}
	if _, err := s.NewRenew(Lease{Resource: Resource{"test", "other"}}, time.Second); err == nil {
		t.Fatal("foreign resource accepted")
	}
	for _, invalidWait := range []time.Duration{0, -time.Millisecond, time.Microsecond, time.Minute + time.Millisecond} {
		if _, err := s.NewWaitAcquire("worker", time.Second, invalidWait); err == nil {
			t.Fatal("invalid wait timeout consumed a sequence")
		}
	}
	var wg sync.WaitGroup
	gate := make(chan struct{})
	sequences := make(chan uint64, 31)
	for i := 0; i < 31; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-gate
			r, err := s.NewAcquire("worker", time.Second)
			if err != nil {
				t.Error(err)
				return
			}
			sequences <- r.Attempt.Sequence
		}()
	}
	close(gate)
	wg.Wait()
	close(sequences)
	seen := make(map[uint64]bool)
	for n := range sequences {
		if seen[n] || n == 0 || n > 31 {
			t.Fatalf("duplicate/out-of-window sequence: %d", n)
		}
		seen[n] = true
	}
	if len(seen) != 31 {
		t.Fatal("missing sequence")
	}
	if _, err := s.NewAcquire("worker", time.Second); err == nil {
		t.Fatal("acquisition consumed reserved Release position")
	}
	r, err := s.NewRelease(sampleLease())
	if err != nil || r.Attempt.Sequence != 32 || r.Attempt.AcknowledgedThrough != 0 {
		t.Fatalf("reserved release unavailable or implicit ACK: %v", err)
	}
	if _, err := s.NewRelease(sampleLease()); err == nil {
		t.Fatal("window exceeded")
	}
}

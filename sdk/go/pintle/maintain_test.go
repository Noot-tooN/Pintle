package pintle

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type renewalReply struct {
	g   *GrantResult
	err error
}
type fakeRenewals struct {
	requests    chan RenewRequest
	replies     chan renewalReply
	waiting     chan struct{}
	ticks       chan struct{}
	mu          sync.Mutex
	next, acked uint64
	ackErr      error
	acks        int
}

func fakeOps() *fakeRenewals {
	return &fakeRenewals{requests: make(chan RenewRequest, 8), replies: make(chan renewalReply, 8), waiting: make(chan struct{}, 8), ticks: make(chan struct{}, 8), next: 1}
}
func (f *fakeRenewals) prepare(l Lease, d time.Duration) (RenewRequest, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r := RenewRequest{Lease: l, TTL: d, RequestID: fmt.Sprint("renew-", f.next), Attempt: &Attempt{SessionID: "2", Sequence: f.next, AcknowledgedThrough: f.acked}}
	f.next++
	return r, nil
}
func (f *fakeRenewals) renew(ctx context.Context, r RenewRequest) (*GrantResult, error) {
	f.requests <- r
	select {
	case x := <-f.replies:
		return x.g, x.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func (f *fakeRenewals) ack(_ context.Context, n uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acks++
	if f.ackErr != nil {
		return f.ackErr
	}
	f.acked = n
	return nil
}
func (f *fakeRenewals) wait(ctx context.Context, _ time.Duration) error {
	f.waiting <- struct{}{}
	select {
	case <-f.ticks:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func receive[T any](t *testing.T, c <-chan T) T {
	t.Helper()
	select {
	case v := <-c:
		return v
	case <-time.After(3 * time.Second):
		t.Fatal("watchdog: event not reached")
	}
	var z T
	return z
}
func sampleLease() Lease {
	return Lease{Resource: Resource{"test", "key"}, AcquisitionID: "acquire", HolderRef: "worker", LeaseID: strings.Repeat("a", 64), FencingToken: "1", Revision: "1", ExpiresAtUnixMS: 61000}
}
func success(l Lease) *GrantResult {
	l.Revision = "2"
	l.ExpiresAtUnixMS += 1000
	return &GrantResult{Receipt: &l, Current: &l, State: Active}
}
func TestMaintenanceRenewalAndUnknownRecovery(t *testing.T) {
	f := fakeOps()
	l := sampleLease()
	m := startMaintenance(context.Background(), l, time.Minute, f, f.wait, time.Now)
	defer m.Stop()
	first := receive(t, f.requests)
	if first.Attempt.Sequence != 1 {
		t.Fatal("wrong first sequence")
	}
	f.replies <- renewalReply{g: success(l)}
	receive(t, f.waiting)
	if m.Recovery() != nil || m.Latest().Revision != "2" {
		t.Fatal("successful renewal not consumed")
	}
	f.ticks <- struct{}{}
	second := receive(t, f.requests)
	if second.Attempt.Sequence != 2 || second.Attempt.AcknowledgedThrough != 1 {
		t.Fatal("helper did not ACK its own receipt")
	}
	snapshot := m.Recovery()
	snapshot.Attempt.Sequence = 99
	f.replies <- renewalReply{err: errors.New("unknown commit")}
	receive(t, m.Done())
	receive(t, m.Lost())
	if m.Err() == nil || m.Recovery().Attempt.Sequence != 2 || m.Recovery().RequestID != second.RequestID {
		t.Fatal("unknown attempt discarded or mutated")
	}
	for _, s := range []string{fmt.Sprint(m), fmt.Sprintf("%+v", m), fmt.Sprintf("%#v", m)} {
		if strings.Contains(s, l.LeaseID) {
			t.Fatal("capability leaked")
		}
	}
}
func TestMaintenanceBudgetBoundaries(t *testing.T) {
	for _, delta := range []time.Duration{20*time.Second - time.Nanosecond, 20 * time.Second, 20*time.Second + time.Nanosecond} {
		t.Run(delta.String(), func(t *testing.T) {
			f := fakeOps()
			var elapsed atomic.Int64
			l := sampleLease()
			m := startMaintenance(context.Background(), l, time.Minute, f, f.wait, func() time.Time { return time.Unix(0, elapsed.Load()) })
			defer m.Stop()
			receive(t, f.requests)
			elapsed.Store(int64(delta))
			f.replies <- renewalReply{g: success(l)}
			if delta < 20*time.Second {
				receive(t, f.waiting)
				if m.Recovery() != nil {
					t.Fatal("successful receipt retained")
				}
			} else {
				receive(t, m.Lost())
				if !errors.Is(m.Err(), ErrLeaseUncertain) || m.Recovery() == nil {
					t.Fatal("late response accepted")
				}
				if f.acks != 0 {
					t.Fatal("late receipt acknowledged")
				}
			}
		})
	}
}
func TestMaintenanceTerminalFailures(t *testing.T) {
	for _, kind := range []string{"expired", "wrong-owner", "ack-unknown", "parent-cancel"} {
		t.Run(kind, func(t *testing.T) {
			f := fakeOps()
			l := sampleLease()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			m := startMaintenance(ctx, l, time.Minute, f, f.wait, time.Now)
			defer m.Stop()
			receive(t, f.requests)
			g := success(l)
			switch kind {
			case "expired":
				g.State = Expired
				g.Current = nil
			case "wrong-owner":
				g.Current.LeaseID = strings.Repeat("b", 64)
			case "ack-unknown":
				f.mu.Lock()
				f.ackErr = errors.New("ack lost")
				f.mu.Unlock()
			case "parent-cancel":
				cancel()
			}
			if kind != "parent-cancel" {
				f.replies <- renewalReply{g: g}
			}
			receive(t, m.Done())
			if m.Err() == nil || m.Recovery() == nil {
				t.Fatal("failure lost recovery")
			}
			receive(t, m.Lost())
		})
	}
}
func TestMaintenanceStopResponseOrders(t *testing.T) {
	for _, responseFirst := range []bool{false, true} {
		t.Run(fmt.Sprint(responseFirst), func(t *testing.T) {
			f := fakeOps()
			l := sampleLease()
			m := startMaintenance(context.Background(), l, time.Minute, f, f.wait, time.Now)
			receive(t, f.requests)
			if responseFirst {
				f.replies <- renewalReply{g: success(l)}
				receive(t, f.waiting)
			}
			var wg sync.WaitGroup
			for i := 0; i < 4; i++ {
				wg.Add(1)
				go func() { defer wg.Done(); m.Stop() }()
			}
			wg.Wait()
			if !errors.Is(m.Err(), ErrMaintenanceStopped) {
				t.Fatal("stop reason missing")
			}
			receive(t, m.Lost())
			select {
			case <-f.requests:
				t.Fatal("renewal after Stop")
			default:
			}
		})
	}
}

func TestMaintenanceGenerationsAreIsolated(t *testing.T) {
	oldOps, freshOps := fakeOps(), fakeOps()
	var oldElapsed atomic.Int64
	oldLease := sampleLease()
	old := startMaintenance(context.Background(), oldLease, time.Minute, oldOps, oldOps.wait,
		func() time.Time { return time.Unix(0, oldElapsed.Load()) })
	defer old.Stop()
	oldRequest := receive(t, oldOps.requests)

	// Model the successor lease independently. The real server handoff is
	// exercised by TestP2MaintenanceSuccessorRecovery in the Wasm suite.
	freshLease := oldLease
	freshLease.LeaseID = strings.Repeat("b", 64)
	freshLease.FencingToken = "2"
	freshLease.AcquisitionID = "successor"
	fresh := startMaintenance(context.Background(), freshLease, time.Minute, freshOps, freshOps.wait,
		func() time.Time { return time.Unix(0, 0) })
	defer fresh.Stop()
	request := receive(t, freshOps.requests)
	if request.Lease != freshLease {
		t.Fatal("fresh maintenance used old ownership")
	}
	freshOps.replies <- renewalReply{g: success(freshLease)}
	receive(t, freshOps.waiting) // ACK consumed and the next renewal is blocked on a test tick.

	// Deliver an old successful reply only after its local budget has elapsed
	// and the successor is already renewing. It must not revive the old helper.
	oldElapsed.Store(int64(20 * time.Second))
	oldOps.replies <- renewalReply{g: success(oldLease)}
	receive(t, old.Done())
	receive(t, old.Lost())
	if !errors.Is(old.Err(), ErrLeaseUncertain) || old.Latest() != oldLease || oldOps.acks != 0 {
		t.Fatal("late old reply was accepted or acknowledged")
	}
	if r := old.Recovery(); r == nil || r.RequestID != oldRequest.RequestID || *r.Attempt != *oldRequest.Attempt {
		t.Fatal("old recovery identity lost")
	}
	old.Stop()
	select {
	case <-fresh.Lost():
		t.Fatal("stopping old maintenance stopped its successor")
	default:
	}
	freshOps.ticks <- struct{}{}
	request = receive(t, freshOps.requests)
	if request.Lease.LeaseID != freshLease.LeaseID || request.Attempt.Sequence != 2 || request.Attempt.AcknowledgedThrough != 1 {
		t.Fatal("successor renewal state contaminated")
	}
	freshOps.replies <- renewalReply{g: success(freshLease)}
	receive(t, freshOps.waiting)
	if fresh.Latest().LeaseID != freshLease.LeaseID || fresh.Latest().FencingToken != "2" || fresh.Recovery() != nil {
		t.Fatal("successor failed to consume its own renewal")
	}
	receive(t, old.Lost()) // permanently closed, even after successor progress
}

package pintle

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var ErrMaintenanceStopped = errors.New("lease maintenance stopped")
var ErrLeaseUncertain = errors.New("lease renewal is uncertain; stop work and reconcile")

// Maintenance renews a known lease using its own recovery session. Lost closes
// permanently on any uncertain renewal/ACK, inactive observation, cancellation,
// or Stop. An open Lost channel is NOT proof of current server ownership. Paused
// clients cannot be revoked by this signal; external side effects need fencing.
type Maintenance struct {
	mu         sync.Mutex
	latest     Lease
	pending    *RenewRequest
	err        error
	lost, done chan struct{}
	cancel     context.CancelFunc
}

func (m *Maintenance) Lost() <-chan struct{} { return m.lost }
func (m *Maintenance) Done() <-chan struct{} { return m.done }
func (m *Maintenance) Err() error            { m.mu.Lock(); defer m.mu.Unlock(); return m.err }
func (m *Maintenance) Latest() Lease         { m.mu.Lock(); defer m.mu.Unlock(); return m.latest }

// Recovery returns the exact last unacknowledged renewal, if any. It may have
// committed. Persist/reconcile this request; never invent a replacement attempt.
func (m *Maintenance) Recovery() *RenewRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return copyRenew(m.pending)
}
func copyRenew(r *RenewRequest) *RenewRequest {
	if r == nil {
		return nil
	}
	v := *r
	if v.Attempt != nil {
		a := *v.Attempt
		v.Attempt = &a
	}
	return &v
}

// Stop ends renewal and waits for its goroutine. It does not release the lease.
func (m *Maintenance) Stop()            { m.cancel(); <-m.done }
func (m *Maintenance) String() string   { return "Maintenance{capability=REDACTED}" }
func (m *Maintenance) GoString() string { return m.String() }

type renewalOps interface {
	prepare(Lease, time.Duration) (RenewRequest, error)
	renew(context.Context, RenewRequest) (*GrantResult, error)
	ack(context.Context, uint64) error
}
type sessionRenewal struct {
	s *Session
	c *Client
}

func (o sessionRenewal) prepare(l Lease, d time.Duration) (RenewRequest, error) {
	return o.s.NewRenew(l, d)
}
func (o sessionRenewal) renew(ctx context.Context, r RenewRequest) (*GrantResult, error) {
	return o.c.Renew(ctx, r)
}
func (o sessionRenewal) ack(ctx context.Context, n uint64) error { return o.s.Acknowledge(ctx, n) }
func waitRenewal(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Maintain opts into automatic Renew only. It opens a dedicated session and ACKs
// only renewal receipts consumed by this helper, never application acquisitions.
// A failed initial session allocation returns an error (an orphan may expire).
func (c *Client) Maintain(ctx context.Context, l Lease, d time.Duration) (*Maintenance, error) {
	if _, err := ttl(d); err != nil {
		return nil, err
	}
	if !validResource(l.Resource) || l.LeaseID == "" {
		return nil, fmt.Errorf("valid known lease required")
	}
	call, cancel := context.WithTimeout(ctx, d/3)
	s, err := c.OpenSession(call, l.Resource)
	cancel()
	if err != nil {
		return nil, err
	}
	return startMaintenance(ctx, l, d, sessionRenewal{s, c}, waitRenewal, time.Now), nil
}
func startMaintenance(parent context.Context, l Lease, d time.Duration, ops renewalOps, wait func(context.Context, time.Duration) error, now func() time.Time) *Maintenance {
	ctx, cancel := context.WithCancel(parent)
	m := &Maintenance{latest: l, lost: make(chan struct{}), done: make(chan struct{}), cancel: cancel}
	go func() {
		defer close(m.done)
		defer cancel()
		err := m.run(ctx, d, ops, wait, now)
		if parent.Err() != nil {
			err = parent.Err()
		} else if ctx.Err() != nil {
			err = ErrMaintenanceStopped
		}
		m.mu.Lock()
		m.err = err
		m.mu.Unlock()
		close(m.lost)
	}()
	return m
}
func (m *Maintenance) run(ctx context.Context, d time.Duration, ops renewalOps, wait func(context.Context, time.Duration) error, now func() time.Time) error {
	budget := d / 3
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		req, err := ops.prepare(m.Latest(), d)
		if err != nil {
			return err
		}
		m.mu.Lock()
		m.pending = copyRenew(&req)
		m.mu.Unlock()
		started := now()
		call, cancel := context.WithTimeout(ctx, budget)
		g, err := ops.renew(call, req)
		if err == nil && (call.Err() != nil || now().Sub(started) >= budget) {
			err = ErrLeaseUncertain
		}
		if err == nil && (g == nil || g.State != Active || g.Current == nil || g.Current.LeaseID != req.Lease.LeaseID || g.Current.FencingToken != req.Lease.FencingToken || g.Current.Resource != req.Lease.Resource) {
			err = ErrLeaseUncertain
		}
		if err == nil {
			m.mu.Lock()
			m.latest = *g.Current
			m.mu.Unlock()
			err = ops.ack(call, req.Attempt.Sequence)
			if err == nil && (call.Err() != nil || now().Sub(started) >= budget) {
				err = ErrLeaseUncertain
			}
		}
		cancel()
		if err != nil {
			return err
		}
		m.mu.Lock()
		m.pending = nil
		m.mu.Unlock()
		if err = wait(ctx, budget); err != nil {
			return err
		}
	}
}

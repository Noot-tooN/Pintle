package pintle

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"connectrpc.com/connect"
)

// LeaseOptions configures WithLease. Zero fields use the documented defaults.
type LeaseOptions struct {
	TTL            time.Duration // default 30s; 100..60000 whole milliseconds
	WaitTimeout    time.Duration // default 60s; local acquisition budget/server wait limit, 1..60000 whole ms
	CleanupTimeout time.Duration // default 5s; total cancellation/release/ACK budget, at most 60s
	HolderRef      string        // default "with-lease"; diagnostic label, not ownership
}

var ErrWaitExpired = errors.New("lease acquisition wait expired")
var ErrWaitCancelled = errors.New("lease acquisition was cancelled")

// WithLeaseError preserves unresolved protocol identities for explicit recovery.
// These requests may have committed. Never mutate them or retry the callback
// automatically. Release can also mean its reply was received but ACK failed.
// Recovery data contains capabilities: store privately, never log or publish it.
// The helper keeps identities in memory only; use the low-level API if requests
// must be journaled before sending for recovery across application crashes.
type WithLeaseError struct {
	Cause   error
	Wait    *WaitRequest
	Renew   *RenewRequest
	Release *ReleaseRequest
	Lease   *Lease // last observed lease; not proof of current ownership
}

func (e WithLeaseError) Error() string    { return fmt.Sprintf("pintle WithLease: %v", e.Cause) }
func (e WithLeaseError) Unwrap() error    { return e.Cause }
func (e WithLeaseError) String() string   { return e.Error() }
func (e WithLeaseError) GoString() string { return e.Error() }

// WithLease acquires one server-owned waiting intent, renews automatically and
// calls fn at most once, after an initial renewal and ACK. It cancels the work
// context on parent cancellation or uncertain/lost ownership. The lease passed
// to fn is a snapshot; its fence/capability stay stable, its expiry does not.
// Fencing is optional for the caller, but only destination enforcement protects
// external effects against stale holders. Cancellation cannot revoke sent work.
//
// fn must cooperate with cancellation and join its child work before returning.
// WithLease waits for fn even after cancellation; it never abandons a callback
// and releases underneath it. On return (including panic unwinding), it stops
// renewal and attempts bounded cleanup independently of parent cancellation.
// Panics propagate after cleanup. Cleanup failures during panic cannot be
// returned; lease/session expiry remains the fallback. Success says nothing
// about exactly-once external effects. Errors may wrap *WithLeaseError.
func (c *Client) WithLease(ctx context.Context, resource Resource, options LeaseOptions, fn func(context.Context, Lease) error) error {
	return c.withLease(ctx, resource, options, fn, waitRenewal, time.Now)
}

func leaseOptions(o LeaseOptions) (LeaseOptions, error) {
	if o.TTL == 0 {
		o.TTL = 30 * time.Second
	}
	if o.WaitTimeout == 0 {
		o.WaitTimeout = time.Minute
	}
	if o.CleanupTimeout == 0 {
		o.CleanupTimeout = 5 * time.Second
	}
	if o.HolderRef == "" {
		o.HolderRef = "with-lease"
	}
	if _, err := ttl(o.TTL); err != nil {
		return o, err
	}
	if o.WaitTimeout < time.Millisecond || o.WaitTimeout > time.Minute || o.WaitTimeout%time.Millisecond != 0 {
		return o, errors.New("wait timeout must be 1..60000 whole milliseconds")
	}
	if o.CleanupTimeout <= 0 || o.CleanupTimeout > time.Minute {
		return o, errors.New("cleanup timeout must be positive and at most 60s")
	}
	if !identifier(o.HolderRef, 64) {
		return o, errors.New("invalid holder")
	}
	return o, nil
}

func (c *Client) withLease(ctx context.Context, resource Resource, options LeaseOptions, fn func(context.Context, Lease) error, wait func(context.Context, time.Duration) error, now func() time.Time) (err error) {
	if !validResource(resource) || fn == nil {
		return errors.New("valid resource and callback required")
	}
	o, err := leaseOptions(options)
	if err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	acquiring, stopAcquiring := context.WithTimeout(ctx, o.WaitTimeout)
	defer stopAcquiring()
	s, err := c.borrowLeaseSession(acquiring, resource)
	if err != nil {
		return err
	}
	request, err := s.NewWaitAcquire(o.HolderRef, o.TTL, o.WaitTimeout)
	if err != nil {
		return err
	}
	recovery := &WithLeaseError{Wait: &request}
	var lease *Lease
	var maintenance *Maintenance
	var stopWork context.CancelCauseFunc
	var watcherDone chan struct{}
	// This single cleanup budget covers cancellation/grant reconciliation, release
	// and ACK. Never run an unbounded background cleanup or discard unknown results.
	defer func() {
		if stopWork != nil {
			stopWork(ErrMaintenanceStopped)
		}
		if maintenance != nil {
			maintenance.Stop()
			<-watcherDone
			recovery.Renew = maintenance.Recovery()
			if loss := maintenance.Err(); !errors.Is(loss, ErrMaintenanceStopped) {
				err = errors.Join(err, ErrLeaseUncertain, loss)
			} else if recovery.Renew != nil {
				err = errors.Join(err, ErrLeaseUncertain)
			}
		}
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), o.CleanupTimeout)
		defer cancel()
		if recovery.Wait != nil {
			result, cancelErr := c.CancelPending(cleanup, request)
			if cancelErr != nil {
				err = errors.Join(err, fmt.Errorf("cancel acquisition: %w", cancelErr))
			} else {
				recovery.Wait = nil
				if result.Granted != nil && result.Granted.State == Active {
					lease = result.Granted.Current
				}
			}
		}
		through := request.Acquisition.Attempt.Sequence
		if lease != nil {
			recovery.Lease = lease
			release, releaseErr := s.NewRelease(*lease)
			if releaseErr == nil {
				recovery.Release = &release
				_, releaseErr = c.Release(cleanup, release)
				through = release.Attempt.Sequence
			}
			if releaseErr != nil {
				err = errors.Join(err, fmt.Errorf("release lease: %w", releaseErr))
			}
			// A failed Release is unresolved even if it might mean expiry. Keep the
			// exact request and never ACK through it or an uncertain renewal.
			if releaseErr != nil {
				recovery.Cause = err
				err = recovery
				return
			}
		}
		if recovery.Wait == nil && recovery.Renew == nil {
			if ackErr := s.Acknowledge(cleanup, through); ackErr != nil {
				err = errors.Join(err, fmt.Errorf("acknowledge cleanup: %w", ackErr))
				if recovery.Release == nil {
					recovery.Wait = &request
				}
			} else {
				recovery.Release = nil
				c.leaseSessions.put(s)
			}
		}
		if err != nil {
			recovery.Cause = err
			err = recovery
		}
	}()
	result, err := c.Acquire(acquiring, request)
	acquireErr := acquiring.Err()
	stopAcquiring()
	if err != nil {
		return err
	}
	recovery.Wait = nil
	if result.Granted == nil {
		if result.State == WaitExpired {
			return ErrWaitExpired
		}
		return ErrWaitCancelled
	}
	if result.Granted.State != Active || result.Granted.Current == nil {
		return ErrLeaseUncertain
	}
	l := *result.Granted.Current
	lease = &l
	if acquireErr != nil {
		return acquireErr
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	// Reuse this private session so long callbacks ACK acquisition history and
	// keep recovery alive without consuming a second server session slot.
	maintenance = startMaintenance(ctx, l, o.TTL, sessionRenewal{s, c}, wait, now)
	work, cancelWork := context.WithCancelCause(ctx)
	stopWork = cancelWork
	watcherDone = make(chan struct{})
	go func() {
		defer close(watcherDone)
		select {
		case <-maintenance.Lost():
			cancelWork(errors.Join(ErrLeaseUncertain, maintenance.Err()))
		case <-work.Done():
		}
	}()
	select {
	case <-maintenance.ready:
	case <-maintenance.Lost():
		return errors.Join(ErrLeaseUncertain, maintenance.Err())
	case <-ctx.Done():
		return ctx.Err()
	}
	if err = work.Err(); err != nil {
		return err
	}
	select {
	case <-maintenance.Lost():
		return errors.Join(ErrLeaseUncertain, maintenance.Err())
	default:
	}
	err = fn(work, maintenance.Latest())
	// A callback returning nil after cancellation must not report overall success.
	if work.Err() != nil {
		err = errors.Join(err, context.Cause(work))
	}
	return err
}

// Only fully ACKed sessions are reusable, exclusively by one invocation. A
// bounded FIFO cache avoids one fresh server slot per short callback without an
// ever-growing resource map. Evicted/uncertain sessions expire server-side.
type leaseSessionCache struct {
	mu   sync.Mutex
	idle []*Session
}

const maxIdleLeaseSessions = 16

func (p *leaseSessionCache) take(r Resource) *Session {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, s := range p.idle {
		if s.resource == r {
			p.idle = append(p.idle[:i], p.idle[i+1:]...)
			return s
		}
	}
	return nil
}
func (p *leaseSessionCache) put(s *Session) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.idle) == maxIdleLeaseSessions {
		p.idle = p.idle[1:]
	}
	p.idle = append(p.idle, s)
}
func (c *Client) borrowLeaseSession(ctx context.Context, r Resource) (*Session, error) {
	if s := c.leaseSessions.take(r); s != nil {
		// A safe idle session may have expired. Refresh/check it before allocating
		// any new intent. Other errors fail closed, without a fresh acquisition.
		err := s.Heartbeat(ctx)
		if err == nil {
			return s, nil
		}
		if connect.CodeOf(err) != connect.CodeFailedPrecondition {
			return nil, err
		}
	}
	return c.OpenSession(ctx, r)
}

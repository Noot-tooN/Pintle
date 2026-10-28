package pintle

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/Noot-tooN/Pintle/gen/go/pintle/v1"
	"github.com/Noot-tooN/Pintle/gen/go/pintle/v1/pintlev1connect"
	"google.golang.org/protobuf/proto"
)

// Script only protocol replies and observable call ordering. The real generated
// transport and Rust authority are covered by the local Wrangler tests.
type leaseRPC struct {
	pintlev1connect.LockServiceClient
	mu                            sync.Mutex
	lease                         *v1.GrantReceipt
	opens, renews, releases       int
	acquireRequest, cancelRequest *v1.AcquireRequest
	releaseRequest                *v1.ReleaseRequest
	acks                          []string
	acquireHook                   func(context.Context) (*v1.AcquireResponse, error)
	cancelHook                    func(context.Context)
	cancelErr, releaseErr         error
	renewHook                     func(context.Context, int) error
	ackHook                       func(context.Context, string) error
	releaseHook                   func(context.Context)
}

func (f *leaseRPC) OpenSession(ctx context.Context, _ *connect.Request[v1.OpenSessionRequest]) (*connect.Response[v1.OpenSessionResponse], error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.opens++
	return connect.NewResponse(&v1.OpenSessionResponse{Session: sessionInfo(strconv.Itoa(f.opens), "0")}), nil
}
func sessionInfo(id, ack string) *v1.RecoverySession {
	return &v1.RecoverySession{SessionId: id, AcknowledgedThrough: ack, ExpiresAtUnixMs: "300000", RequestWindow: 32}
}
func (f *leaseRPC) grant() *v1.AcquireResponse {
	f.mu.Lock()
	defer f.mu.Unlock()
	l := proto.Clone(f.lease).(*v1.GrantReceipt)
	return &v1.AcquireResponse{Outcome: &v1.AcquireResponse_Acquired{Acquired: &v1.TryAcquireResponse{
		Outcome: &v1.TryAcquireResponse_Acquired{Acquired: l}, Observation: &v1.LeaseObservation{State: v1.LeaseState_LEASE_STATE_ACTIVE, Current: l},
	}}}
}
func (f *leaseRPC) Acquire(ctx context.Context, req *connect.Request[v1.AcquireRequest]) (*connect.Response[v1.AcquireResponse], error) {
	f.mu.Lock()
	f.acquireRequest = proto.Clone(req.Msg).(*v1.AcquireRequest)
	f.lease = wireLease()
	f.lease.Resource = req.Msg.Acquisition.Resource
	f.lease.RequestId = req.Msg.Acquisition.RequestId
	f.lease.HolderRef = req.Msg.Acquisition.HolderRef
	f.lease.FencingToken = "9007199254740993"
	f.mu.Unlock()
	if f.acquireHook != nil {
		r, err := f.acquireHook(ctx)
		return connect.NewResponse(r), err
	}
	return connect.NewResponse(f.grant()), nil
}
func (f *leaseRPC) CancelPending(ctx context.Context, req *connect.Request[v1.CancelPendingRequest]) (*connect.Response[v1.CancelPendingResponse], error) {
	f.mu.Lock()
	f.cancelRequest = proto.Clone(req.Msg.Acquire).(*v1.AcquireRequest)
	f.mu.Unlock()
	if f.cancelHook != nil {
		f.cancelHook(ctx)
	}
	if f.cancelErr != nil {
		return nil, f.cancelErr
	}
	return connect.NewResponse(&v1.CancelPendingResponse{Result: f.grant()}), nil
}
func (f *leaseRPC) Renew(ctx context.Context, _ *connect.Request[v1.RenewRequest]) (*connect.Response[v1.RenewResponse], error) {
	f.mu.Lock()
	f.renews++
	n := f.renews
	l := proto.Clone(f.lease).(*v1.GrantReceipt)
	f.mu.Unlock()
	if f.renewHook != nil {
		if err := f.renewHook(ctx, n); err != nil {
			return nil, err
		}
	}
	return connect.NewResponse(&v1.RenewResponse{Lease: l, Observation: &v1.LeaseObservation{State: v1.LeaseState_LEASE_STATE_ACTIVE, Current: l}}), nil
}
func (f *leaseRPC) Release(ctx context.Context, req *connect.Request[v1.ReleaseRequest]) (*connect.Response[v1.ReleaseResponse], error) {
	f.mu.Lock()
	f.releases++
	f.releaseRequest = proto.Clone(req.Msg).(*v1.ReleaseRequest)
	f.mu.Unlock()
	if f.releaseHook != nil {
		f.releaseHook(ctx)
	}
	if f.releaseErr != nil {
		return nil, f.releaseErr
	}
	return connect.NewResponse(&v1.ReleaseResponse{Revision: "3"}), nil
}
func (f *leaseRPC) AcknowledgeSession(ctx context.Context, req *connect.Request[v1.AcknowledgeSessionRequest]) (*connect.Response[v1.AcknowledgeSessionResponse], error) {
	f.mu.Lock()
	f.acks = append(f.acks, req.Msg.AcknowledgedThrough)
	f.mu.Unlock()
	if f.ackHook != nil {
		if err := f.ackHook(ctx, req.Msg.AcknowledgedThrough); err != nil {
			return nil, err
		}
	}
	return connect.NewResponse(&v1.AcknowledgeSessionResponse{Session: sessionInfo(req.Msg.SessionId, req.Msg.AcknowledgedThrough)}), nil
}
func leaseTestClient(f *leaseRPC) *Client {
	return &Client{rpc: f, credential: "test", leaseSessions: &leaseSessionCache{}}
}
func quietRenewal(ctx context.Context, _ time.Duration) error { <-ctx.Done(); return ctx.Err() }
func callLease(c *Client, ctx context.Context, fn func(context.Context, Lease) error, wait func(context.Context, time.Duration) error) error {
	return c.withLease(ctx, Resource{"test", "key"}, LeaseOptions{}, fn, wait, time.Now)
}
func leaseFailure(t *testing.T, err error) *WithLeaseError {
	t.Helper()
	var e *WithLeaseError
	if !errors.As(err, &e) {
		t.Fatalf("missing recoverable error: %v", err)
	}
	return e
}

func TestWithLeaseRenewsAndPreservesFence(t *testing.T) {
	f := &leaseRPC{}
	c := leaseTestClient(f)
	waiting, ticks := make(chan struct{}), make(chan struct{})
	wait := func(ctx context.Context, _ time.Duration) error {
		select {
		case waiting <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case <-ticks:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	var work context.Context
	err := callLease(c, context.Background(), func(ctx context.Context, l Lease) error {
		work = ctx
		if l.FencingToken != "9007199254740993" {
			t.Fatal("fence rounded")
		}
		receive(t, waiting) // first renewal AND ACK consumed before work
		for i := 0; i < 40; i++ {
			ticks <- struct{}{}
			receive(t, waiting)
		}
		return nil
	}, wait)
	if err != nil {
		t.Fatal(err)
	}
	if work.Err() == nil || f.renews != 41 || f.releases != 1 || len(c.leaseSessions.idle) != 1 {
		t.Fatal("lifecycle incomplete")
	}
	if f.releaseRequest.Attempt.Sequence != "43" || f.acks[len(f.acks)-1] != "43" {
		t.Fatal("release/ACK sequencing")
	}
}

func TestWithLeaseCallbackOutcomesAndPanic(t *testing.T) {
	callbackErr := errors.New("business work failed")
	for _, kind := range []string{"success", "error", "panic", "cancel"} {
		t.Run(kind, func(t *testing.T) {
			f := &leaseRPC{}
			c := leaseTestClient(f)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			f.releaseHook = func(ctx context.Context) {
				if ctx.Err() != nil {
					t.Error("cleanup inherited cancellation")
				}
				if _, ok := ctx.Deadline(); !ok {
					t.Error("unbounded cleanup")
				}
			}
			var got error
			var panicked any
			func() {
				defer func() { panicked = recover() }()
				got = callLease(c, ctx, func(work context.Context, _ Lease) error {
					switch kind {
					case "error":
						return callbackErr
					case "panic":
						panic("original panic")
					case "cancel":
						cancel()
						<-work.Done()
						return nil
					}
					return nil
				}, quietRenewal)
			}()
			if f.releases != 1 {
				t.Fatal("missing release")
			}
			switch kind {
			case "success":
				if got != nil {
					t.Fatal(got)
				}
			case "error":
				if !errors.Is(got, callbackErr) {
					t.Fatal(got)
				}
			case "panic":
				if panicked != "original panic" {
					t.Fatal("panic swallowed")
				}
			case "cancel":
				if !errors.Is(got, context.Canceled) {
					t.Fatal(got)
				}
			}
		})
	}
}

func TestWithLeaseLossCancelsButWaitsForCallback(t *testing.T) {
	f := &leaseRPC{}
	c := leaseTestClient(f)
	ticks := make(chan struct{})
	waiting := make(chan struct{})
	stopped := make(chan struct{})
	finish := make(chan struct{})
	f.renewHook = func(_ context.Context, n int) error {
		if n == 2 {
			return errors.New("lost reply")
		}
		return nil
	}
	wait := func(ctx context.Context, _ time.Duration) error {
		select {
		case waiting <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case <-ticks:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	result := make(chan error, 1)
	go func() {
		result <- callLease(c, context.Background(), func(work context.Context, _ Lease) error {
			<-work.Done()
			if !errors.Is(context.Cause(work), ErrLeaseUncertain) {
				t.Error("missing loss cause")
			}
			close(stopped)
			<-finish // deliberately slow cooperative cleanup
			return nil
		}, wait)
	}()
	receive(t, waiting)
	ticks <- struct{}{}
	receive(t, stopped)
	f.mu.Lock()
	releases := f.releases
	f.mu.Unlock()
	if releases != 0 {
		t.Fatal("released underneath running callback")
	}
	select {
	case <-result:
		t.Fatal("abandoned callback")
	default:
	}
	close(finish)
	err := receive(t, result)
	e := leaseFailure(t, err)
	if !errors.Is(err, ErrLeaseUncertain) || e.Renew == nil || e.Renew.Attempt.Sequence != 3 || e.Release == nil {
		t.Fatal("uncertain renewal identity lost")
	}
	if len(f.acks) != 1 || len(c.leaseSessions.idle) != 0 {
		t.Fatal("ACK crossed unresolved renewal or reused session")
	}
}

func TestWithLeaseInitialRenewalFailuresNeverRunCallback(t *testing.T) {
	for _, kind := range []string{"renew", "ack", "late"} {
		t.Run(kind, func(t *testing.T) {
			f := &leaseRPC{}
			c := leaseTestClient(f)
			var elapsed atomic.Int64
			f.renewHook = func(context.Context, int) error {
				if kind == "renew" {
					return errors.New("renew failed")
				}
				if kind == "late" {
					elapsed.Store(int64(10 * time.Second))
				}
				return nil
			}
			if kind == "ack" {
				f.ackHook = func(context.Context, string) error { return errors.New("ack failed") }
			}
			err := c.withLease(context.Background(), Resource{"test", "key"}, LeaseOptions{}, func(context.Context, Lease) error { t.Error("unsafe callback ran"); return nil }, quietRenewal, func() time.Time { return time.Unix(0, elapsed.Load()) })
			e := leaseFailure(t, err)
			if !errors.Is(err, ErrLeaseUncertain) || e.Renew == nil || f.releases != 1 {
				t.Fatal("initial failure not preserved/cleaned")
			}
		})
	}
}

func TestWithLeaseAcquireCleanup(t *testing.T) {
	unknown := errors.New("acquire reply lost")
	for _, kind := range []string{"lost-grant", "cancel-unknown", "expired", "cancelled", "parent-cancel"} {
		t.Run(kind, func(t *testing.T) {
			f := &leaseRPC{}
			c := leaseTestClient(f)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			f.acquireHook = func(context.Context) (*v1.AcquireResponse, error) {
				switch kind {
				case "expired":
					return &v1.AcquireResponse{Outcome: &v1.AcquireResponse_Waiting{Waiting: v1.WaitState_WAIT_STATE_EXPIRED}}, nil
				case "cancelled":
					return &v1.AcquireResponse{Outcome: &v1.AcquireResponse_Waiting{Waiting: v1.WaitState_WAIT_STATE_CANCELLED}}, nil
				case "parent-cancel":
					cancel()
					return f.grant(), nil
				default:
					return nil, unknown
				}
			}
			if kind == "cancel-unknown" {
				f.cancelErr = errors.New("cancel reply lost")
			}
			err := callLease(c, ctx, func(context.Context, Lease) error { t.Error("callback after failed acquire"); return nil }, quietRenewal)
			e := leaseFailure(t, err)
			switch kind {
			case "lost-grant":
				if !errors.Is(err, unknown) || f.releases != 1 || !proto.Equal(f.acquireRequest, f.cancelRequest) || e.Wait != nil {
					t.Fatal("grant/cancel reconciliation")
				}
			case "cancel-unknown":
				if e.Wait == nil || f.releases != 0 || len(f.acks) != 0 || !proto.Equal(f.acquireRequest, f.cancelRequest) {
					t.Fatal("unknown acquisition discarded")
				}
			case "expired":
				if !errors.Is(err, ErrWaitExpired) || f.releases != 0 {
					t.Fatal(err)
				}
			case "cancelled":
				if !errors.Is(err, ErrWaitCancelled) || f.releases != 0 {
					t.Fatal(err)
				}
			case "parent-cancel":
				if !errors.Is(err, context.Canceled) || f.releases != 1 {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestWithLeaseReleaseAndAckRecovery(t *testing.T) {
	for _, kind := range []string{"release", "ack"} {
		t.Run(kind, func(t *testing.T) {
			failure := errors.New("cleanup reply lost")
			callbackErr := errors.New("business error")
			f := &leaseRPC{}
			c := leaseTestClient(f)
			if kind == "release" {
				f.releaseErr = failure
			} else {
				f.ackHook = func(_ context.Context, n string) error {
					if n == "3" {
						return failure
					}
					return nil
				}
			}
			err := callLease(c, context.Background(), func(context.Context, Lease) error { return callbackErr }, quietRenewal)
			e := leaseFailure(t, err)
			if !errors.Is(err, failure) || !errors.Is(err, callbackErr) || e.Release == nil || e.Release.RequestID != f.releaseRequest.RequestId || e.Release.Attempt.Sequence != 3 {
				t.Fatal("cleanup identity/cause lost")
			}
			if len(c.leaseSessions.idle) != 0 {
				t.Fatal("reused uncertain session")
			}
			if kind == "release" && len(f.acks) != 1 {
				t.Fatal("ACK after failed release")
			}
			for _, v := range []any{e, *e} {
				for _, format := range []string{"%v", "%+v", "%#v"} {
					if strings.Contains(fmt.Sprintf(format, v), strings.Repeat("a", 64)) {
						t.Fatal("capability exposed")
					}
				}
			}
		})
	}
}

func TestWithLeaseValidationAndCancelledEntry(t *testing.T) {
	c, _ := New("http://localhost:8787", "test", noNetwork{t})
	fn := func(context.Context, Lease) error { t.Error("invalid callback ran"); return nil }
	for _, o := range []LeaseOptions{{TTL: -1}, {TTL: 99 * time.Millisecond}, {WaitTimeout: -1}, {WaitTimeout: time.Minute + 1}, {CleanupTimeout: -1}, {CleanupTimeout: time.Minute + 1}, {HolderRef: "bad holder"}} {
		if c.WithLease(context.Background(), Resource{"test", "key"}, o, fn) == nil {
			t.Fatal("invalid options accepted")
		}
	}
	if c.WithLease(context.Background(), Resource{}, LeaseOptions{}, fn) == nil || c.WithLease(context.Background(), Resource{"test", "key"}, LeaseOptions{}, nil) == nil {
		t.Fatal("invalid input accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if !errors.Is(c.WithLease(ctx, Resource{"test", "key"}, LeaseOptions{}, fn), context.Canceled) {
		t.Fatal("cancelled entry")
	}
}

func TestWithLeaseSessionReuseAndExpiry(t *testing.T) {
	f := &leaseRPC{}
	c := leaseTestClient(f)
	fn := func(context.Context, Lease) error { return nil }
	for i := 0; i < 40; i++ {
		if err := callLease(c, context.Background(), fn, quietRenewal); err != nil {
			t.Fatal(err)
		}
	}
	if f.opens != 1 || len(c.leaseSessions.idle) != 1 {
		t.Fatal("new session per callback")
	}
	expired := true
	f.ackHook = func(context.Context, string) error {
		if expired {
			expired = false
			return connect.NewError(connect.CodeFailedPrecondition, errors.New("session expired"))
		}
		return nil
	}
	if err := callLease(c, context.Background(), fn, quietRenewal); err != nil {
		t.Fatal(err)
	}
	if f.opens != 2 {
		t.Fatal("expired idle session not replaced")
	}
	f.ackHook = func(context.Context, string) error {
		return connect.NewError(connect.CodeUnavailable, errors.New("unknown heartbeat"))
	}
	if err := callLease(c, context.Background(), fn, quietRenewal); connect.CodeOf(err) != connect.CodeUnavailable || f.opens != 2 {
		t.Fatal("unknown session check silently retried")
	}
}

func TestWithLeaseSessionCacheIsBoundedAndExclusive(t *testing.T) {
	p := &leaseSessionCache{}
	for i := 0; i < 100; i++ {
		p.put(&Session{resource: Resource{"test", strconv.Itoa(i)}})
	}
	if len(p.idle) != maxIdleLeaseSessions || p.take(Resource{"test", "0"}) != nil {
		t.Fatal("cache not bounded")
	}
	var wg sync.WaitGroup
	var borrowed atomic.Int32
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if p.take(Resource{"test", "99"}) != nil {
				borrowed.Add(1)
			}
		}()
	}
	wg.Wait()
	if borrowed.Load() != 1 {
		t.Fatal("session shared by overlapping callbacks")
	}
}

func TestWithLeaseCallbackCompletionDuringRenewal(t *testing.T) {
	f := &leaseRPC{}
	c := leaseTestClient(f)
	waiting, tick, renewing, finish := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	f.renewHook = func(ctx context.Context, n int) error {
		if n == 2 {
			close(renewing)
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	}
	wait := func(ctx context.Context, _ time.Duration) error {
		select {
		case waiting <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case <-tick:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	done := make(chan error, 1)
	go func() {
		done <- callLease(c, context.Background(), func(context.Context, Lease) error { <-finish; return nil }, wait)
	}()
	receive(t, waiting)
	close(tick)
	receive(t, renewing)
	close(finish)
	err := receive(t, done)
	e := leaseFailure(t, err)
	if !errors.Is(err, ErrLeaseUncertain) || e.Renew == nil || e.Release == nil || len(f.acks) != 1 || f.releases != 1 {
		t.Fatal("in-flight renewal was silently abandoned")
	}
}

func TestWithLeaseInactiveGrantAndCleanupBudget(t *testing.T) {
	t.Run("inactive-grant", func(t *testing.T) {
		f := &leaseRPC{}
		c := leaseTestClient(f)
		f.acquireHook = func(context.Context) (*v1.AcquireResponse, error) {
			r := f.grant()
			r.GetAcquired().Observation = &v1.LeaseObservation{State: v1.LeaseState_LEASE_STATE_EXPIRED}
			return r, nil
		}
		err := callLease(c, context.Background(), func(context.Context, Lease) error { t.Error("inactive grant used"); return nil }, quietRenewal)
		if !errors.Is(err, ErrLeaseUncertain) || f.renews != 0 || f.releases != 0 {
			t.Fatal("inactive grant lifecycle")
		}
	})
	t.Run("one-cleanup-budget", func(t *testing.T) {
		f := &leaseRPC{releaseErr: context.DeadlineExceeded}
		c := leaseTestClient(f)
		parent, cancel := context.WithCancel(context.Background())
		defer cancel()
		f.acquireHook = func(context.Context) (*v1.AcquireResponse, error) {
			cancel()
			return nil, errors.New("unknown grant")
		}
		var cleanupDeadline time.Time
		f.cancelHook = func(ctx context.Context) {
			var ok bool
			cleanupDeadline, ok = ctx.Deadline()
			if !ok || ctx.Err() != nil {
				t.Error("cancellation cleanup must have its own live deadline")
			}
		}
		// Compare deadlines across both cleanup RPCs; a fresh timeout per RPC
		// would incorrectly extend the total budget. No sleep chooses an outcome.
		f.releaseHook = func(ctx context.Context) {
			if deadline, ok := ctx.Deadline(); !ok || !deadline.Equal(cleanupDeadline) {
				t.Error("release reset the shared cleanup budget")
			}
		}
		err := callLease(c, parent, func(context.Context, Lease) error { t.Error("callback ran"); return nil }, quietRenewal)
		e := leaseFailure(t, err)
		if !errors.Is(err, context.DeadlineExceeded) || e.Release == nil || len(f.acks) != 0 {
			t.Fatal("cleanup timeout discarded recovery")
		}
	})
}

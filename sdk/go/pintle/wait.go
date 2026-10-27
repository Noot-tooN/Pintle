package pintle

import (
	"context"
	"fmt"
	v1 "github.com/Noot-tooN/Pintle/gen/go/pintle/v1"
	"time"
)

// WaitRequest is an immutable durable acquisition intent. Keep the complete
// request across retries/restarts. A transport error does not cancel it.
type WaitRequest struct {
	Acquisition AcquireRequest
	WaitTimeout time.Duration
}
type WaitState int

const (
	WaitUnspecified WaitState = iota
	Pending
	Cancelled
	WaitExpired
)

type WaitResult struct {
	Granted *GrantResult
	State   WaitState
}

func (s *Session) NewWaitAcquire(holder string, leaseTTL, waitTimeout time.Duration) (WaitRequest, error) {
	if waitTimeout < time.Millisecond || waitTimeout > time.Minute || waitTimeout%time.Millisecond != 0 {
		return WaitRequest{}, fmt.Errorf("wait timeout must be 1..60000 whole milliseconds")
	}
	r, err := s.NewAcquire(holder, leaseTTL)
	return WaitRequest{Acquisition: r, WaitTimeout: waitTimeout}, err
}
func wireWait(r WaitRequest) (*v1.AcquireRequest, error) {
	ms, err := ttl(r.Acquisition.TTL)
	if err != nil {
		return nil, err
	}
	if r.Acquisition.Attempt == nil || r.WaitTimeout < time.Millisecond || r.WaitTimeout > time.Minute || r.WaitTimeout%time.Millisecond != 0 {
		return nil, fmt.Errorf("session attempt and wait timeout 1..60000ms required")
	}
	a := r.Acquisition
	return &v1.AcquireRequest{Acquisition: &v1.TryAcquireRequest{Resource: resource(a.Resource), RequestId: a.RequestID, HolderRef: a.HolderRef, LeaseTtlMs: ms, Attempt: wireAttempt(a.Attempt)}, WaitTimeoutMs: uint32(r.WaitTimeout / time.Millisecond)}, nil
}
func decodeWait(r *v1.AcquireResponse) (*WaitResult, error) {
	if r == nil {
		return nil, fmt.Errorf("missing acquisition result")
	}
	switch o := r.Outcome.(type) {
	case *v1.AcquireResponse_Acquired:
		g, err := acquired(o.Acquired)
		if err != nil {
			return nil, err
		}
		if g.Contended {
			return nil, fmt.Errorf("blocking acquisition returned contention")
		}
		return &WaitResult{Granted: g}, nil
	case *v1.AcquireResponse_Waiting:
		if o.Waiting < v1.WaitState_WAIT_STATE_PENDING || o.Waiting > v1.WaitState_WAIT_STATE_EXPIRED {
			return nil, fmt.Errorf("invalid wait state")
		}
		return &WaitResult{State: WaitState(o.Waiting)}, nil
	default:
		return nil, fmt.Errorf("missing acquisition outcome")
	}
}

// ResumeAcquire makes one bounded unary call for the same server-owned intent.
// Pending is resumable; the server queue survives the response or disconnection.
func (c *Client) ResumeAcquire(ctx context.Context, r WaitRequest) (*WaitResult, error) {
	w, err := wireWait(r)
	if err != nil {
		return nil, err
	}
	res, err := c.rpc.Acquire(ctx, authorize(c, w))
	if err != nil {
		return nil, err
	}
	return matchedWait(res.Msg, r)
}

// Acquire waits for a terminal server decision, resuming only a returned Pending
// ticket. It never calls TryAcquire or arbitrates contention. On transport/context
// error, reconcile or CancelPending with the SAME request using a fresh context.
func (c *Client) Acquire(ctx context.Context, r WaitRequest) (*WaitResult, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		result, err := c.ResumeAcquire(ctx, r)
		if err != nil || result.State != Pending {
			return result, err
		}
	}
}

// CancelPending atomically cancels even an intent whose Acquire has not arrived.
// If grant won, returns that grant and does NOT release it. Reconcile ownership.
func (c *Client) CancelPending(ctx context.Context, r WaitRequest) (*WaitResult, error) {
	w, err := wireWait(r)
	if err != nil {
		return nil, err
	}
	res, err := c.rpc.CancelPending(ctx, authorize(c, &v1.CancelPendingRequest{Acquire: w}))
	if err != nil {
		return nil, err
	}
	return matchedWait(res.Msg.Result, r)
}

func matchedWait(response *v1.AcquireResponse, r WaitRequest) (*WaitResult, error) {
	result, err := decodeWait(response)
	if err == nil && result.Granted != nil {
		receipt := result.Granted.Receipt
		a := r.Acquisition
		if receipt.Resource != a.Resource || receipt.AcquisitionID != a.RequestID || receipt.HolderRef != a.HolderRef {
			return nil, fmt.Errorf("response does not match acquisition")
		}
	}
	return result, err
}

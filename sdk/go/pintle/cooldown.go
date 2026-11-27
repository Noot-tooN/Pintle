package pintle

import (
	"context"
	"errors"
	"fmt"
	"time"

	v1 "github.com/Noot-tooN/Pintle/gen/go/pintle/v1"
)

// CooldownStatus describes the decision for one logical attempt.
type CooldownStatus int

const (
	CooldownAdmitted CooldownStatus = iota + 1
	CoolingDown
	CooldownSuperseded
)

// CooldownDecision reports authority times. AdmittedAt is set only for a win.
// RetryAt is the earliest opportunity observed, without a reserved turn.
type CooldownDecision struct {
	Status     CooldownStatus
	AdmittedAt time.Time
	RetryAt    time.Time
}

// CooldownRecoveryError means an admission may have committed. Retry on this
// error to reconcile the original attempt with a new context.
type CooldownRecoveryError struct {
	cause   error
	client  *Client
	request *v1.TryAdmitRequest
}

func (e *CooldownRecoveryError) Error() string {
	return fmt.Sprintf("cooldown admission outcome unknown: %v", e.cause)
}
func (e *CooldownRecoveryError) Unwrap() error { return e.cause }

// Retry resends the same attempt once. A superseded result ends recovery.
func (e *CooldownRecoveryError) Retry(ctx context.Context) (CooldownDecision, error) {
	d, err := e.client.sendAdmission(ctx, e.request)
	if err != nil {
		return CooldownDecision{}, &CooldownRecoveryError{err, e.client, e.request}
	}
	return d, nil
}

// TryAdmit makes one logical attempt. The winning interval starts at admission,
// even if the caller exits before doing any work. Transport retries retain the
// original attempt. Another TryAdmit call starts a new attempt.
func (c *Client) TryAdmit(ctx context.Context, r Resource, interval time.Duration) (CooldownDecision, error) {
	if c.cooldownRPC == nil || !validResource(r) {
		return CooldownDecision{}, errors.New("TryAdmit requires a base client and valid resource")
	}
	if interval < time.Millisecond || interval > 24*time.Hour || interval%time.Millisecond != 0 {
		return CooldownDecision{}, errors.New("cooldown interval must be whole milliseconds in [1,86400000]")
	}
	prepared, err := c.cooldownRPC.GetCooldownWindow(ctx, authorize(c, &v1.GetCooldownWindowRequest{Resource: resource(r)}))
	if err != nil {
		return CooldownDecision{}, err
	}
	window, err := decimal(prepared.Msg.WindowId, false)
	if err != nil || window > maxAuthorityTime {
		return CooldownDecision{}, errors.New("invalid cooldown window ID")
	}
	id, err := NewRequestID()
	if err != nil {
		return CooldownDecision{}, err
	}
	req := &v1.TryAdmitRequest{Resource: resource(r), WindowId: prepared.Msg.WindowId, RequestId: id, IntervalMs: uint32(interval.Milliseconds())}
	d, err := c.sendAdmission(ctx, req)
	if ambiguousRPC(err) {
		recovery := &CooldownRecoveryError{err, c, req}
		if ctx.Err() == nil {
			return recovery.Retry(ctx)
		}
		return CooldownDecision{}, recovery
	}
	return d, err
}

func (c *Client) sendAdmission(ctx context.Context, req *v1.TryAdmitRequest) (CooldownDecision, error) {
	res, err := c.cooldownRPC.TryAdmit(ctx, authorize(c, req))
	if err != nil {
		return CooldownDecision{}, err
	}
	reply := res.Msg
	end := reply.RetryAtUnixMs
	if end > 0 && end <= maxAuthorityTime {
		d := CooldownDecision{RetryAt: time.UnixMilli(int64(end)).UTC()}
		switch reply.Status {
		case v1.CooldownStatus_COOLDOWN_STATUS_ADMITTED:
			start := reply.GetAdmittedAtUnixMs()
			if reply.AdmittedAtUnixMs != nil && start < end && end-start == uint64(req.IntervalMs) {
				d.Status = CooldownAdmitted
				d.AdmittedAt = time.UnixMilli(int64(start)).UTC()
				return d, nil
			}
		case v1.CooldownStatus_COOLDOWN_STATUS_COOLING_DOWN:
			if reply.AdmittedAtUnixMs == nil {
				d.Status = CoolingDown
				return d, nil
			}
		case v1.CooldownStatus_COOLDOWN_STATUS_SUPERSEDED:
			if reply.AdmittedAtUnixMs == nil {
				d.Status = CooldownSuperseded
				return d, nil
			}
		}
	}
	return CooldownDecision{}, errors.New("invalid admission response")
}

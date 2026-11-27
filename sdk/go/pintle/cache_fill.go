package pintle

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/Noot-tooN/Pintle/gen/go/pintle/v1"
)

// MaxFlightResultBytes is the maximum result shared inline by the authority.
const MaxFlightResultBytes = 4096
const flightLifetime = 30 * time.Second
const flightCleanupTimeout = 5 * time.Second

var (
	ErrFlightExpired            = errors.New("singleflight producer expired")
	ErrFlightFailed             = errors.New("singleflight fill failed")
	ErrFlightTooLarge           = errors.New("singleflight result exceeds the inline limit")
	errInvalidFlightObservation = errors.New("invalid singleflight observation")
)

// CacheFillRecoveryError retains the exact inputs to an unresolved RPC. Use the
// generated client for explicit recovery. Ticket and Completion are private.
type CacheFillRecoveryError struct {
	Cause      error
	Join       *v1.JoinFlightRequest
	Ticket     string
	Completion *v1.CompleteFlightRequest // nil for join or waiter cancellation
}

func (e *CacheFillRecoveryError) Error() string {
	return fmt.Sprintf("pintle cache fill recovery: %v", e.Cause)
}
func (e *CacheFillRecoveryError) Unwrap() error { return e.Cause }
func (e CacheFillRecoveryError) String() string {
	return "CacheFillRecoveryError{recovery data redacted}"
}
func (e CacheFillRecoveryError) GoString() string { return e.String() }

// CacheFill checks lookup before contacting Pintle and again as producer. On a
// miss, fill computes AND publishes the value before returning. Use an immutable
// versioned key or atomically compare generation at the destination. Each call
// invokes fill at most once. Both callbacks must honor ctx and join their work.
func (c *Client) CacheFill(ctx context.Context, r Resource, lookup func(context.Context) (value []byte, hit bool, err error), fill func(work context.Context, generation uint64) ([]byte, error)) ([]byte, error) {
	if c.flightRPC == nil || !validResource(r) || lookup == nil || fill == nil {
		return nil, errors.New("CacheFill requires a base client, valid resource, lookup and fill")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ctx, stop := context.WithTimeout(ctx, flightLifetime+flightCleanupTimeout)
	defer stop()
	if v, hit, err := lookup(ctx); err != nil || hit {
		return v, err
	}
	gen, err := c.flightRPC.GetFlightGeneration(ctx, authorize(c, &v1.GetFlightGenerationRequest{Resource: resource(r)}))
	if err != nil {
		return nil, err
	}
	n, err := decimal(gen.Msg.Generation, true)
	if err != nil {
		return nil, err
	}
	id, err := NewRequestID()
	if err != nil {
		return nil, err
	}
	attempt := &v1.JoinFlightRequest{Resource: resource(r), Generation: gen.Msg.Generation, RequestId: id}
	// A replay can return the original assignment. Never extend its local work budget.
	workDeadline := time.Now().Add(flightLifetime)
	joined, err := c.flightRPC.JoinFlight(ctx, authorize(c, attempt))
	if flightAmbiguous(err) {
		cleanup, done := flightCleanup(ctx)
		joined, err = c.flightRPC.JoinFlight(cleanup, authorize(c, attempt))
		done()
		if flightAmbiguous(err) {
			return nil, &CacheFillRecoveryError{Cause: errors.Join(ctx.Err(), err), Join: attempt}
		}
	}
	if err != nil {
		if connect.CodeOf(err) == connect.CodeFailedPrecondition && ctx.Err() == nil {
			if v, hit, readErr := lookup(ctx); hit || readErr != nil {
				return v, readErr
			}
		}
		return nil, errors.Join(ctx.Err(), err)
	}
	ticket := joined.Msg.Ticket
	raw, err := hex.DecodeString(ticket)
	if err != nil || len(raw) != 32 || hex.EncodeToString(raw) != ticket {
		return nil, &CacheFillRecoveryError{Cause: errors.New("invalid singleflight ticket"), Join: attempt}
	}
	if joined.Msg.Producer {
		return c.produceFill(ctx, attempt, ticket, n, workDeadline, lookup, fill)
	}
	v, err := c.awaitFill(ctx, attempt, ticket)
	if errors.Is(err, ErrFlightExpired) && ctx.Err() == nil {
		if cached, hit, readErr := lookup(ctx); hit || readErr != nil {
			return cached, readErr
		}
	}
	return v, err
}

func flightCleanup(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), flightCleanupTimeout)
}
func flightAmbiguous(err error) bool {
	if err == nil {
		return false
	}
	switch connect.CodeOf(err) {
	case connect.CodeUnknown, connect.CodeUnavailable, connect.CodeDeadlineExceeded, connect.CodeCanceled, connect.CodeInternal:
		return true
	}
	return false
}

// pending is distinct from a successful empty value.
func flightResult(o *v1.FlightObservation) (value []byte, pending bool, err error) {
	switch x := o.GetOutcome().(type) {
	case *v1.FlightObservation_Value:
		if len(x.Value) <= MaxFlightResultBytes {
			return x.Value, false, nil
		}
	case *v1.FlightObservation_Status:
		switch x.Status {
		case v1.FlightStatus_FLIGHT_STATUS_PENDING:
			return nil, true, nil
		case v1.FlightStatus_FLIGHT_STATUS_CANCELLED:
			return nil, false, context.Canceled
		case v1.FlightStatus_FLIGHT_STATUS_LEADER_EXPIRED:
			return nil, false, ErrFlightExpired
		case v1.FlightStatus_FLIGHT_STATUS_FILL_FAILED:
			return nil, false, ErrFlightFailed
		case v1.FlightStatus_FLIGHT_STATUS_RESULT_TOO_LARGE:
			return nil, false, ErrFlightTooLarge
		}
	}
	return nil, false, errInvalidFlightObservation
}

func (c *Client) produceFill(ctx context.Context, join *v1.JoinFlightRequest, ticket string, generation uint64, deadline time.Time, lookup func(context.Context) ([]byte, bool, error), fill func(context.Context, uint64) ([]byte, error)) (value []byte, err error) {
	work, stop := context.WithDeadline(ctx, deadline)
	defer stop()
	completion := &v1.CompleteFlightRequest{Resource: join.Resource, Ticket: ticket, Completion: &v1.CompleteFlightRequest_Failure{Failure: v1.FlightFailure_FLIGHT_FAILURE_FILL_FAILED}}
	// Also settle the failure during panic unwinding, without swallowing the panic.
	defer func() {
		verified, settleErr := c.completeFill(ctx, join, completion)
		if err == nil && settleErr == nil {
			value = verified
		} else {
			value = nil
			if !errors.Is(err, settleErr) {
				err = errors.Join(err, settleErr)
			}
		}
	}()
	if err = work.Err(); err != nil {
		return nil, err
	}
	var hit bool
	value, hit, err = lookup(work)
	if err == nil && !hit {
		if err = work.Err(); err == nil {
			value, err = fill(work, generation)
		}
	}
	if err == nil {
		err = work.Err()
	}
	if err != nil {
		return nil, err
	}
	if len(value) > MaxFlightResultBytes {
		completion.Completion = &v1.CompleteFlightRequest_Failure{Failure: v1.FlightFailure_FLIGHT_FAILURE_RESULT_TOO_LARGE}
		return nil, ErrFlightTooLarge
	}
	completion.Completion = &v1.CompleteFlightRequest_Value{Value: bytes.Clone(value)}
	return value, nil
}

// A completion is single-assignment. Reconcile only the exact payload, including
// the oneof discriminator for an empty value or an oversize marker.
func (c *Client) completeFill(ctx context.Context, join *v1.JoinFlightRequest, req *v1.CompleteFlightRequest) ([]byte, error) {
	cleanup, cancel := flightCleanup(ctx)
	defer cancel()
	res, err := c.flightRPC.CompleteFlight(cleanup, authorize(c, req))
	if flightAmbiguous(err) && cleanup.Err() == nil {
		res, err = c.flightRPC.CompleteFlight(cleanup, authorize(c, req))
	}
	if err == nil {
		value, pending, statusErr := flightResult(res.Msg.Observation)
		if errors.Is(statusErr, ErrFlightExpired) {
			return nil, statusErr
		}
		switch x := req.Completion.(type) {
		case *v1.CompleteFlightRequest_Value:
			if !pending && statusErr == nil && bytes.Equal(x.Value, value) {
				return value, nil
			}
		case *v1.CompleteFlightRequest_Failure:
			if x.Failure == v1.FlightFailure_FLIGHT_FAILURE_RESULT_TOO_LARGE && errors.Is(statusErr, ErrFlightTooLarge) {
				return nil, statusErr
			}
			if x.Failure == v1.FlightFailure_FLIGHT_FAILURE_FILL_FAILED && errors.Is(statusErr, ErrFlightFailed) {
				return nil, statusErr
			}
		}
		err = errors.New("completion reply does not match published outcome")
	}
	return nil, &CacheFillRecoveryError{Cause: err, Join: join, Ticket: req.Ticket, Completion: req}
}

func (c *Client) awaitFill(ctx context.Context, join *v1.JoinFlightRequest, ticket string) ([]byte, error) {
	q := &v1.ObserveFlightRequest{Resource: join.Resource, Ticket: ticket}
	delay := 100 * time.Millisecond
	var cause error
	for {
		if cause = waitRenewal(ctx, delay); cause != nil {
			break
		}
		observed, err := c.flightRPC.ObserveFlight(ctx, authorize(c, q))
		if err == nil {
			v, pending, statusErr := flightResult(observed.Msg.Observation)
			if errors.Is(statusErr, errInvalidFlightObservation) {
				cause = statusErr
				break
			}
			if !pending {
				return v, statusErr
			}
		} else if !flightAmbiguous(err) {
			cause = err
			break
		}
		delay = min(2*delay, 2*time.Second)
	}
	// Cancellation races completion at the authority. Its terminal result wins.
	cleanup, cancel := flightCleanup(ctx)
	defer cancel()
	request := &v1.CancelFlightRequest{Resource: join.Resource, Ticket: ticket}
	cancelled, err := c.flightRPC.CancelFlight(cleanup, authorize(c, request))
	if flightAmbiguous(err) && cleanup.Err() == nil {
		cancelled, err = c.flightRPC.CancelFlight(cleanup, authorize(c, request))
	}
	if err != nil {
		return nil, &CacheFillRecoveryError{Cause: errors.Join(cause, err), Join: join, Ticket: ticket}
	}
	v, pending, statusErr := flightResult(cancelled.Msg.Observation)
	if pending || errors.Is(statusErr, errInvalidFlightObservation) {
		return nil, &CacheFillRecoveryError{Cause: errors.Join(cause, errors.New("invalid waiter cancellation reply")), Join: join, Ticket: ticket}
	}
	if errors.Is(statusErr, context.Canceled) {
		return nil, cause
	}
	return v, statusErr
}

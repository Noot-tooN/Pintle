package pintle

import (
	"context"
	"fmt"
	v1 "pintle.local/spike/gen/go/pintle/v1"
	"strconv"
	"sync"
	"time"
)

// Attempt is part of a prepared request's immutable retry identity. Persist it
// with the payload for crash recovery. Never replace it after an uncertain RPC.
type Attempt struct {
	SessionID                     string
	Sequence, AcknowledgedThrough uint64
}

func wireAttempt(a *Attempt) *v1.RequestAttempt {
	if a == nil {
		return nil
	}
	return &v1.RequestAttempt{SessionId: a.SessionID, Sequence: strconv.FormatUint(a.Sequence, 10), AcknowledgedThrough: strconv.FormatUint(a.AcknowledgedThrough, 10)}
}

// Session manages resource-local sequence allocation. It never automatically
// acknowledges results or renews ownership. Use Acknowledge after the application
// has safely consumed/persisted the outcomes; Heartbeat maintains idle recovery.
type Session struct {
	client             *Client
	resource           Resource
	id                 string
	mu                 sync.Mutex
	next, acknowledged uint64
	expires            uint64
	window             uint64
}

func (c *Client) OpenSession(ctx context.Context, r Resource) (*Session, error) {
	res, err := c.rpc.OpenSession(ctx, authorize(c, &v1.OpenSessionRequest{Resource: resource(r)}))
	if err != nil {
		return nil, err
	}
	s := &Session{client: c, resource: r, next: 1}
	if err = s.update(res.Msg.Session); err != nil {
		return nil, err
	}
	return s, nil
}
func (s *Session) update(i *v1.RecoverySession) error {
	if i == nil || i.RequestWindow < 2 || i.RequestWindow > 32 {
		return fmt.Errorf("invalid recovery session")
	}
	id, e := strconv.ParseUint(i.SessionId, 10, 64)
	if e != nil || id == 0 || strconv.FormatUint(id, 10) != i.SessionId {
		return fmt.Errorf("invalid session ID")
	}
	ack, e := decimal(i.AcknowledgedThrough, false)
	if e != nil {
		return e
	}
	expires, e := decimal(i.ExpiresAtUnixMs, true)
	if e != nil || expires > maxAuthorityTime {
		return fmt.Errorf("invalid session expiry")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.id != "" && s.id != i.SessionId {
		return fmt.Errorf("session changed unexpectedly")
	}
	s.id = i.SessionId
	s.acknowledged = max(s.acknowledged, ack)
	s.expires = expires
	s.window = uint64(i.RequestWindow)
	return nil
}

// Acknowledge relinquishes historical results through a contiguous sequence.
// The server rejects gaps. An ACK never releases a lock. Retrying an ACK is safe.
func (s *Session) Acknowledge(ctx context.Context, through uint64) error {
	s.mu.Lock()
	id := s.id
	s.mu.Unlock()
	res, err := s.client.rpc.AcknowledgeSession(ctx, authorize(s.client, &v1.AcknowledgeSessionRequest{Resource: resource(s.resource), SessionId: id, AcknowledgedThrough: strconv.FormatUint(through, 10)}))
	if err != nil {
		return err
	}
	return s.update(res.Msg.Session)
}
func (s *Session) Heartbeat(ctx context.Context) error {
	s.mu.Lock()
	ack := s.acknowledged
	s.mu.Unlock()
	return s.Acknowledge(ctx, ack)
}
func (s *Session) attempt(release bool) (*Attempt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	limit := s.window
	if !release {
		limit--
	}
	if s.next == 0 || s.next <= s.acknowledged || s.next-s.acknowledged > limit {
		return nil, fmt.Errorf("request window full: resolve and acknowledge earlier requests")
	}
	a := &Attempt{s.id, s.next, s.acknowledged}
	s.next++
	return a, nil
}
func (s *Session) NewAcquire(holder string, duration time.Duration) (AcquireRequest, error) {
	if _, err := ttl(duration); err != nil {
		return AcquireRequest{}, err
	}
	r, err := NewAcquire(s.resource, holder, duration)
	if err != nil {
		return r, err
	}
	r.Attempt, err = s.attempt(false)
	return r, err
}
func (s *Session) NewRenew(l Lease, duration time.Duration) (RenewRequest, error) {
	if l.Resource != s.resource {
		return RenewRequest{}, fmt.Errorf("lease belongs to another resource")
	}
	if _, err := ttl(duration); err != nil {
		return RenewRequest{}, err
	}
	r, err := NewRenew(l, duration)
	if err != nil {
		return r, err
	}
	r.Attempt, err = s.attempt(false)
	return r, err
}
func (s *Session) NewRelease(l Lease) (ReleaseRequest, error) {
	if l.Resource != s.resource {
		return ReleaseRequest{}, fmt.Errorf("lease belongs to another resource")
	}
	r, err := NewRelease(l)
	if err != nil {
		return r, err
	}
	r.Attempt, err = s.attempt(true)
	return r, err
}
func (s *Session) Status(ctx context.Context, id string, a *Attempt) (*Status, error) {
	s.mu.Lock()
	sessionID := s.id
	s.mu.Unlock()
	if a == nil || a.SessionID != sessionID {
		return nil, fmt.Errorf("attempt belongs to another session")
	}
	return s.client.GetAttemptStatus(ctx, s.resource, id, a)
}

// AcknowledgeAttempt supports recovery from a saved request after a client
// restart. Call only after safely consuming all results through its sequence.
func (c *Client) AcknowledgeAttempt(ctx context.Context, r Resource, a Attempt) error {
	_, err := c.rpc.AcknowledgeSession(ctx, authorize(c, &v1.AcknowledgeSessionRequest{Resource: resource(r), SessionId: a.SessionID, AcknowledgedThrough: strconv.FormatUint(a.Sequence, 10)}))
	return err
}

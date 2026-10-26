//go:build (darwin || linux) && testhooks

package integration

import (
	"connectrpc.com/connect"
	"context"
	"fmt"
	"net/http"
	pintle "pintle.local/spike/sdk/go/pintle"
	"strings"
	"testing"
	"time"
)

func waitCall(r pintle.WaitRequest, cancel bool) func(context.Context, *pintle.Client) (any, error) {
	return func(ctx context.Context, c *pintle.Client) (any, error) {
		if cancel {
			return c.CancelPending(ctx, r)
		}
		return c.ResumeAcquire(ctx, r)
	}
}
func waitResult(t *testing.T, p *sessionPending) *pintle.WaitResult {
	t.Helper()
	if p.err != nil {
		t.Fatal(p.err)
	}
	return p.result.(*pintle.WaitResult)
}
func waitStatus(h *sessionHarness, t *testing.T, r pintle.WaitRequest, now uint64) *pintle.Status {
	p := h.do(t, now, func(ctx context.Context, c *pintle.Client) (any, error) {
		return c.GetAttemptStatus(ctx, r.Acquisition.Resource, r.Acquisition.RequestID, r.Acquisition.Attempt)
	})
	if p.err != nil {
		t.Fatal(p.err)
	}
	return p.result.(*pintle.Status)
}

// Complete the bounded unary hold through an explicit timer signal, not sleep.
func resumePending(h *sessionHarness, t *testing.T, r pintle.WaitRequest, now uint64) *sessionPending {
	call := fmt.Sprintf("session-%d", h.number+1)
	g := h.c.hold(call, "wait_registered")
	release(g, directive{Now: now, Timer: true})
	return h.do(t, now, waitCall(r, false))
}
func p2owner(h *sessionHarness, t *testing.T, key string) (*pintle.Session, *pintle.Lease) {
	s := h.open(t, key)
	req, _ := s.NewAcquire("owner", time.Second)
	g := grantSession(t, h.do(t, 1000, acquireSession(req)))
	return s, g.Current
}
func releaseCall(r pintle.ReleaseRequest) func(context.Context, *pintle.Client) (any, error) {
	return func(ctx context.Context, c *pintle.Client) (any, error) { return c.Release(ctx, r) }
}
func TestP2Deterministic(t *testing.T) {
	h := configuredSessionHarness(t, "P2_ENABLED:true")
	defer h.r.stop()
	t.Run("registration_release_gap", func(t *testing.T) {
		for _, releaseBeforeSubscribe := range []bool{false, true} {
			t.Run(fmt.Sprint(releaseBeforeSubscribe), func(t *testing.T) {
				s, l := p2owner(h, t, fmt.Sprint("gap", releaseBeforeSubscribe))
				req, _ := s.NewWaitAcquire("waiter", time.Second, 5*time.Second)
				rel, _ := s.NewRelease(*l)
				call := fmt.Sprintf("session-%d", h.number+1)
				registered := h.c.hold(call, "wait_registered")
				wake := h.c.hold(call, "wait_wake")
				release(wake, directive{Now: 1001})
				p := h.queue(t, 1001, waitCall(req, false))
				h.commit(t, p)
				release(p.after, directive{Now: 1001})
				arrived(t, registered)
				if releaseBeforeSubscribe {
					h.do(t, 1001, releaseCall(rel))
					release(registered, directive{Now: 1001})
				} else {
					release(registered, directive{Now: 1001})
					arrived(t, wake)
					h.do(t, 1001, releaseCall(rel))
				}
				h.wait(t, p)
				g := waitResult(t, p).Granted
				assertSame(t, g.Receipt.FencingToken, "2")
			})
		}
	})
	t.Run("cancel_release_commit_and_reply_orders", func(t *testing.T) {
		for _, cancelFirst := range []bool{false, true} {
			for _, replyFirst := range []bool{false, true} {
				t.Run(fmt.Sprintf("%t-%t", cancelFirst, replyFirst), func(t *testing.T) {
					s, l := p2owner(h, t, fmt.Sprintf("race-%t-%t", cancelFirst, replyFirst))
					req, _ := s.NewWaitAcquire("waiter", time.Second, 5*time.Second)
					assertSame(t, waitResult(t, resumePending(h, t, req, 1000)).State, pintle.Pending)
					rel, _ := s.NewRelease(*l)
					a := h.queue(t, 1001, waitCall(req, true))
					b := h.queue(t, 1001, releaseCall(rel))
					if cancelFirst {
						h.commit(t, a)
						h.commit(t, b)
					} else {
						h.commit(t, b)
						h.commit(t, a)
					}
					if replyFirst {
						h.finish(t, a)
						h.finish(t, b)
					} else {
						h.finish(t, b)
						h.finish(t, a)
					}
					got := waitResult(t, a)
					if cancelFirst {
						assertSame(t, got.State, pintle.Cancelled)
					} else {
						assertSame(t, got.Granted.Receipt.FencingToken, "2")
					}
					replay := waitResult(t, h.do(t, 1001, waitCall(req, false)))
					if cancelFirst {
						assertSame(t, replay.State, pintle.Cancelled)
					} else {
						assertSame(t, *replay.Granted.Receipt, *got.Granted.Receipt)
					}
				})
			}
		}
	})
	t.Run("deadline_edges_and_fifo", func(t *testing.T) {
		for _, now := range []uint64{1999, 2000, 2001} {
			t.Run(fmt.Sprint(now), func(t *testing.T) {
				s, l := p2owner(h, t, fmt.Sprint("deadline", now))
				req, _ := s.NewWaitAcquire("waiter", time.Second, time.Second)
				assertSame(t, waitResult(t, resumePending(h, t, req, 1000)).State, pintle.Pending)
				rel, _ := s.NewRelease(*l)
				h.do(t, now, releaseCall(rel))
				st := waitStatus(h, t, req, now)
				if now < 2000 {
					assertSame(t, st.Acquired.Receipt.FencingToken, "2")
				} else {
					assertSame(t, st.Waiting, pintle.WaitExpired)
				}
			})
		}
		s, l := p2owner(h, t, "fifo")
		a, _ := s.NewWaitAcquire("first", time.Second, 5*time.Second)
		b, _ := s.NewWaitAcquire("second", time.Second, 5*time.Second)
		resumePending(h, t, a, 1000)
		resumePending(h, t, b, 1000)
		rel, _ := s.NewRelease(*l)
		h.do(t, 1001, releaseCall(rel))
		first := waitStatus(h, t, a, 1001)
		assertSame(t, first.Acquired.Receipt.FencingToken, "2")
		assertSame(t, waitStatus(h, t, b, 1001).Waiting, pintle.Pending)
		barge, _ := s.NewAcquire("barge", time.Second)
		assertSame(t, grantSession(t, h.do(t, 1001, acquireSession(barge))).Contended, true)
		rel, _ = s.NewRelease(*first.Acquired.Current)
		h.do(t, 1002, releaseCall(rel))
		assertSame(t, waitStatus(h, t, b, 1002).Acquired.Receipt.FencingToken, "3")
	})
	t.Run("pending_restart_and_cancel_before_registration", func(t *testing.T) {
		s, l := p2owner(h, t, "restart")
		req, _ := s.NewWaitAcquire("waiter", time.Second, 5*time.Second)
		resumePending(h, t, req, 1000)
		h.r.stop()
		h.activation++
		h.r = start(t, h.root, h.state, h.config, h.activation, false, "P2_ENABLED:true", "PINTLE_TEST_SCHEDULER:"+h.c.server.URL)
		assertSame(t, waitStatus(h, t, req, 1001).Waiting, pintle.Pending)
		rel, _ := s.NewRelease(*l)
		h.do(t, 1001, releaseCall(rel))
		assertSame(t, waitStatus(h, t, req, 1001).Acquired.Receipt.FencingToken, "2")
		next, _ := s.NewWaitAcquire("cancel-first", time.Second, 5*time.Second)
		assertSame(t, waitResult(t, h.do(t, 1001, waitCall(next, true))).State, pintle.Cancelled)
		assertSame(t, waitResult(t, h.do(t, 1001, waitCall(next, false))).State, pintle.Cancelled)
		conflict := next
		conflict.WaitTimeout = 4 * time.Second
		p := h.do(t, 1001, waitCall(conflict, false))
		expectCode(t, p.err, connect.CodeAlreadyExists)
	})
}

func TestP2Rollback(t *testing.T) {
	h := configuredSessionHarness(t, "P2_ENABLED:true")
	defer h.r.stop()
	for _, operation := range []string{"register", "cancel", "release-grant"} {
		for _, kind := range []string{"sql", "error"} {
			t.Run(operation+"-"+kind, func(t *testing.T) {
				completed := false
				for position := 1; position <= 32; position++ {
					key := fmt.Sprintf("rollback-%s-%s-%d", operation, kind, position)
					s, l := p2owner(h, t, key)
					req, _ := s.NewWaitAcquire("waiter", time.Second, 5*time.Second)
					if operation != "register" {
						resumePending(h, t, req, 1000)
					}
					call := fmt.Sprintf("session-%d", h.number+1)
					g := h.c.hold(call, "wait_registered")
					release(g, directive{Now: 1000, Timer: true})
					f := waitCall(req, operation == "cancel")
					if operation == "release-grant" {
						rel, _ := s.NewRelease(*l)
						f = releaseCall(rel)
					}
					p := h.queue(t, 1000, f)
					release(p.after, directive{Now: 1000})
					release(p.before, directive{Now: 1000, Fault: object{"kind": kind, "after_write": position}})
					h.wait(t, p)
					if p.err == nil {
						completed = true
						t.Logf("verified rollback after each of %d writes", position-1)
						if position < 3 {
							t.Fatal("unexpected write count")
						}
						break
					}
					expectCode(t, p.err, connect.CodeUnavailable)
					q := h.do(t, 1000, func(ctx context.Context, c *pintle.Client) (any, error) {
						return c.GetAttemptStatus(ctx, req.Acquisition.Resource, req.Acquisition.RequestID, req.Acquisition.Attempt)
					})
					if operation == "register" {
						expectCode(t, q.err, connect.CodeNotFound)
					} else {
						if q.err != nil {
							t.Fatal(q.err)
						}
						assertSame(t, q.result.(*pintle.Status).Waiting, pintle.Pending)
					}
					owner := h.do(t, 1000, func(ctx context.Context, c *pintle.Client) (any, error) {
						return c.GetAttemptStatus(ctx, l.Resource, l.AcquisitionID, &pintle.Attempt{SessionID: req.Acquisition.Attempt.SessionID, Sequence: 1})
					})
					if owner.err != nil {
						t.Fatal(owner.err)
					}
					assertSame(t, owner.result.(*pintle.Status).Acquired.Current.FencingToken, "1")
				}
				if !completed {
					t.Fatal("write matrix exceeded bound")
				}
			})
		}
	}
}
func TestP2Alarm(t *testing.T) {
	for _, mode := range []string{"normal", "restart", "rollback"} {
		t.Run(mode, func(t *testing.T) {
			h := configuredSessionHarness(t, "P2_ENABLED:true", "PINTLE_TEST_REAL_ALARM:true")
			defer func() { h.r.stop() }()
			alarm := h.c.hold("alarm", "alarm_start")
			s, _ := p2owner(h, t, "alarm")
			req, _ := s.NewWaitAcquire("waiter", time.Second, 5*time.Second)
			resumePending(h, t, req, 1000)
			arrived(t, alarm)
			if mode == "restart" {
				h.r.stop()
				h.activation++
				alarm = h.c.hold("alarm", "alarm_start")
				h.r = start(t, h.root, h.state, h.config, h.activation, false, "P2_ENABLED:true", "PINTLE_TEST_SCHEDULER:"+h.c.server.URL, "PINTLE_TEST_REAL_ALARM:true")
				arrived(t, alarm)
			}
			if mode == "rollback" {
				retry := h.c.hold("alarm", "alarm_start")
				release(alarm, directive{Now: 2000, Fault: object{"kind": "sql", "after_write": 3}})
				arrived(t, retry)
				stats := sessionStats(t, h.root, h.state)
				assertSame(t, stats[0]["fence"], "1")
				assertSame(t, stats[0]["pending"], float64(1))
				alarm = retry
			}

			done := h.c.hold("alarm", "alarm_done")
			next := h.c.hold("alarm", "alarm_start")
			release(alarm, directive{Now: 2000})
			arrived(t, done)
			release(done, directive{Now: 2000})
			st := waitStatus(h, t, req, 2000)
			assertSame(t, st.Acquired.Receipt.FencingToken, "2")
			// The next alarm is held until process shutdown, keeping subsequent time explicit.
			_ = next
		})
	}
}

func TestP2CapacityAndIdle(t *testing.T) {
	h := configuredSessionHarness(t, "P2_ENABLED:true")
	defer h.r.stop()
	s, _ := p2owner(h, t, "capacity")
	type held struct {
		p    *sessionPending
		g    *gate
		r    pintle.WaitRequest
		call string
	}
	var all []held
	registrationStart := time.Now()
	for i := 0; i < 16; i++ {
		req, _ := s.NewWaitAcquire("waiter", time.Second, 5*time.Second)
		call := fmt.Sprintf("session-%d", h.number+1)
		g := h.c.hold(call, "wait_registered")
		p := h.queue(t, 1000, waitCall(req, false))
		h.commit(t, p)
		release(p.after, directive{Now: 1000})
		arrived(t, g)
		all = append(all, held{p, g, req, call})
	}
	// Subscriber admission rejects before any queue mutation, without using sleeps.
	overflow, _ := s.NewWaitAcquire("overflow", time.Second, 5*time.Second)
	call := "overflow"
	g := h.c.hold(call, "gateway_start")
	release(g, directive{Now: 1000})
	c := sdk(t, h.r.base, h.a, &http.Client{Transport: scheduledTransport{}, Timeout: 10 * time.Second})
	ctx := context.WithValue(context.Background(), callContext{}, call)
	_, err := c.ResumeAcquire(ctx, overflow)
	expectCode(t, err, connect.CodeResourceExhausted)
	t.Logf("16 waiter registration wall time (includes test checkpoints): %s", time.Since(registrationStart))
	stats := sessionStats(t, h.root, h.state)
	t.Logf("16 durable pending + 16 bounded HTTP subscriptions: %v", stats)
	// Allow each handler into its notification wait, then hold after its initial
	// durable recheck. No polling timer is present; unrelated objects cannot wake it.
	for _, x := range all {
		wake := h.c.hold(x.call, "wait_wake")
		release(x.g, directive{Now: 1000})
		arrived(t, wake)
		release(wake, directive{Now: 1000})
	}
	// Force a same-resource status notification to give every handler a completed
	// recheck, then use wait_wake gates to observe and park all subscribers.
	wakes := make([]*gate, len(all))
	for i, x := range all {
		wakes[i] = h.c.hold(x.call, "wait_wake")
	}
	waitStatus(h, t, all[0].r, 1000)
	for i := range all {
		arrived(t, wakes[i])
	}
	h.c.mu.Lock()
	before := len(h.c.events)
	h.c.mu.Unlock()
	for i := 0; i < 4; i++ {
		h.open(t, fmt.Sprint("unrelated", i))
	}
	h.c.mu.Lock()
	events := append([]string(nil), h.c.events[before:]...)
	h.c.mu.Unlock()
	for _, e := range events {
		for _, x := range all {
			if strings.Contains(e, x.call+"/") {
				t.Fatalf("unrelated object woke waiter: %s", e)
			}
		}
	}
	// Cancellation remains available at handler capacity and resolves each durable
	// request. Releasing notifications lets the held HTTP handlers return terminal.
	for _, x := range all {
		waitResult(t, h.do(t, 1000, waitCall(x.r, true)))
	}
	for _, g := range wakes {
		release(g, directive{Now: 1000})
	}
	for _, x := range all {
		h.wait(t, x.p)
		assertSame(t, waitResult(t, x.p).State, pintle.Cancelled)
	}
	assertSame(t, waitResult(t, resumePending(h, t, overflow, 1000)).State, pintle.Pending)
}
func TestP2UnknownCommit(t *testing.T) {
	for _, stage := range []string{"before_commit", "after_sync", "grant_after_sync"} {
		t.Run(stage, func(t *testing.T) {
			h := configuredSessionHarness(t, "P2_ENABLED:true")
			defer func() { h.r.stop() }()
			s, l := p2owner(h, t, "unknown")
			req, _ := s.NewWaitAcquire("waiter", time.Second, 5*time.Second)
			var p *sessionPending
			if stage == "grant_after_sync" {
				resumePending(h, t, req, 1000)
				rel, _ := s.NewRelease(*l)
				p = h.queue(t, 1001, releaseCall(rel))
				h.commit(t, p)
			} else {
				p = h.queue(t, 1001, waitCall(req, false))
				if stage == "after_sync" {
					h.commit(t, p)
				}
			}
			h.r.stop()
			h.wait(t, p)
			h.activation++
			h.r = start(t, h.root, h.state, h.config, h.activation, false, "P2_ENABLED:true", "PINTLE_TEST_SCHEDULER:"+h.c.server.URL)
			q := h.do(t, 1001, func(ctx context.Context, c *pintle.Client) (any, error) {
				return c.GetAttemptStatus(ctx, req.Acquisition.Resource, req.Acquisition.RequestID, req.Acquisition.Attempt)
			})
			if stage == "before_commit" {
				expectCode(t, q.err, connect.CodeNotFound)
				resumePending(h, t, req, 1001)
			} else if q.err != nil {
				t.Fatal(q.err)
			} else if stage == "after_sync" {
				assertSame(t, q.result.(*pintle.Status).Waiting, pintle.Pending)
			} else {
				assertSame(t, q.result.(*pintle.Status).Acquired.Receipt.FencingToken, "2")
			}
			if stage != "grant_after_sync" {
				rel, _ := s.NewRelease(*l)
				h.do(t, 1001, releaseCall(rel))
			}
			replay := waitResult(t, h.do(t, 1001, waitCall(req, false)))
			assertSame(t, replay.Granted.Receipt.FencingToken, "2")
		})
	}
}

func TestP2DuplicateAndDisconnect(t *testing.T) {
	h := configuredSessionHarness(t, "P2_ENABLED:true")
	defer h.r.stop()
	for _, disconnect := range []bool{false, true} {
		t.Run(fmt.Sprint(disconnect), func(t *testing.T) {
			s, l := p2owner(h, t, fmt.Sprint("duplicate", disconnect))
			req, _ := s.NewWaitAcquire("waiter", time.Second, 5*time.Second)
			var requests []*sessionPending
			var registered []*gate
			for i := 0; i < 2; i++ {
				call := fmt.Sprintf("session-%d", h.number+1)
				g := h.c.hold(call, "wait_registered")
				registered = append(registered, g)
				p := h.queue(t, 1001, waitCall(req, false))
				h.commit(t, p)
				release(p.after, directive{Now: 1001})
				arrived(t, g)
				requests = append(requests, p)
			}
			if disconnect {
				requests[0].cancel()
				h.wait(t, requests[0])
				if requests[0].err == nil {
					t.Fatal("cancelled HTTP call unexpectedly succeeded")
				}
			}
			rel, _ := s.NewRelease(*l)
			h.do(t, 1001, releaseCall(rel))
			for _, g := range registered {
				release(g, directive{Now: 1001})
			}
			h.wait(t, requests[1])
			got := waitResult(t, requests[1])
			assertSame(t, got.Granted.Receipt.FencingToken, "2")
			if !disconnect {
				h.wait(t, requests[0])
				assertSame(t, *waitResult(t, requests[0]).Granted.Receipt, *got.Granted.Receipt)
			}
			assertSame(t, waitStatus(h, t, req, 1001).Acquired.Receipt.FencingToken, "2")
		})
	}
}

func TestP2SDKResumesSameIntent(t *testing.T) {
	h := configuredSessionHarness(t, "P2_ENABLED:true")
	defer h.r.stop()
	s, l := p2owner(h, t, "resume")
	req, _ := s.NewWaitAcquire("waiter", time.Second, 5*time.Second)
	call := fmt.Sprintf("session-%d", h.number+1)
	registered := h.c.hold(call, "wait_registered")
	p := h.queue(t, 1000, func(ctx context.Context, c *pintle.Client) (any, error) { return c.Acquire(ctx, req) })
	h.commit(t, p)
	// First gateway dispatch already happened. Observe the SDK's second dispatch
	// after the first response's controlled hold timer returns Pending.
	resumed := h.c.hold(call, "gateway_start")
	release(p.after, directive{Now: 1000})
	arrived(t, registered)
	release(registered, directive{Now: 1000, Timer: true})
	arrived(t, resumed)
	rel, _ := s.NewRelease(*l)
	h.do(t, 1000, releaseCall(rel))
	release(resumed, directive{Now: 1000})
	h.wait(t, p)
	got := waitResult(t, p)
	assertSame(t, got.Granted.Receipt.FencingToken, "2")
	assertSame(t, waitStatus(h, t, req, 1000).Acquired.Receipt.FencingToken, "2")
}

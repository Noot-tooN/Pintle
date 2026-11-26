//go:build (darwin || linux) && testhooks

package integration

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"connectrpc.com/connect"
	pintle "github.com/Noot-tooN/Pintle/sdk/go/pintle"
)

func semaphoreCall(capacity uint32, f func(context.Context, *pintle.Client) (any, error)) func(context.Context, *pintle.Client) (any, error) {
	return func(ctx context.Context, c *pintle.Client) (any, error) {
		sem, err := c.Semaphore(capacity)
		if err != nil {
			return nil, err
		}
		return f(ctx, sem)
	}
}
func openSemaphore(t *testing.T, h *sessionHarness, key string, capacity uint32) *pintle.Session {
	t.Helper()
	p := h.do(t, 1000, semaphoreCall(capacity, func(ctx context.Context, c *pintle.Client) (any, error) {
		return c.OpenSession(ctx, pintle.Resource{Namespace: "test", Key: key})
	}))
	if p.err != nil {
		t.Fatal(p.err)
	}
	return p.result.(*pintle.Session)
}
func semaphoreAcquire(r pintle.AcquireRequest) func(context.Context, *pintle.Client) (any, error) {
	return semaphoreCall(r.Capacity, acquireSession(r))
}
func semaphoreStatus(t *testing.T, h *sessionHarness, r pintle.AcquireRequest, now uint64) *pintle.Status {
	t.Helper()
	p := h.do(t, now, semaphoreCall(r.Capacity, func(ctx context.Context, c *pintle.Client) (any, error) {
		return c.GetAttemptStatus(ctx, r.Resource, r.RequestID, r.Attempt)
	}))
	if p.err != nil {
		t.Fatal(p.err)
	}
	return p.result.(*pintle.Status)
}
func queueSemaphore(t *testing.T, h *sessionHarness, r pintle.WaitRequest, now uint64) *sessionPending {
	t.Helper()
	g := h.c.hold(fmt.Sprintf("session-%d", h.number+1), "wait_registered")
	release(g, directive{Now: now, Timer: true})
	return h.do(t, now, semaphoreCall(r.Acquisition.Capacity, waitCall(r, false)))
}
func TestP4SemaphoreSchedules(t *testing.T) {
	h := configuredSessionHarness(t, "P2_ENABLED:true")
	defer func() { h.r.stop() }()
	t.Run("all_three_contender_commit_orders", func(t *testing.T) {
		orders := [][]int{{0, 1, 2}, {0, 2, 1}, {1, 0, 2}, {1, 2, 0}, {2, 0, 1}, {2, 1, 0}}
		for _, order := range orders {
			t.Run(fmt.Sprint(order), func(t *testing.T) {
				s := openSemaphore(t, h, fmt.Sprint("orders", order), 2)
				var requests [3]pintle.AcquireRequest
				var calls [3]*sessionPending
				for i := range requests {
					var err error
					requests[i], err = s.NewAcquire(fmt.Sprint("worker", i), time.Second)
					if err != nil {
						t.Fatal(err)
					}
					calls[i] = h.queue(t, 1000, semaphoreAcquire(requests[i]))
				}
				for _, i := range order {
					h.commit(t, calls[i])
				}
				for i := len(order) - 1; i >= 0; i-- {
					h.finish(t, calls[order[i]])
				}
				a := grantSession(t, calls[order[0]])
				b := grantSession(t, calls[order[1]])
				third := grantSession(t, calls[order[2]])
				if a.Current == nil || b.Current == nil || !third.Contended {
					t.Fatal("capacity did not select first two commits")
				}
				assertSame(t, a.Current.FencingToken, "1")
				assertSame(t, b.Current.FencingToken, "2")
				if a.Current.PermitSlot == b.Current.PermitSlot || a.Current.LeaseID == b.Current.LeaseID {
					t.Fatal("aliased permits")
				}
				for _, i := range order {
					r := grantSession(t, h.do(t, 1000, semaphoreAcquire(requests[i])))
					if i == order[2] {
						if !r.Contended {
							t.Fatal("contended intent changed")
						}
					} else if !r.Replayed || r.Current == nil {
						t.Fatal("grant retry changed")
					}
				}
			})
		}
	})
	t.Run("duplicate_commit_reply_orders", func(t *testing.T) {
		for originalFirst := range 2 {
			s := openSemaphore(t, h, fmt.Sprint("duplicate", originalFirst), 2)
			r, _ := s.NewAcquire("worker", time.Second)
			a := h.queue(t, 1000, semaphoreAcquire(r))
			b := h.queue(t, 1000, semaphoreAcquire(r))
			order := []*sessionPending{a, b}
			if originalFirst == 0 {
				order = []*sessionPending{b, a}
			}
			h.commit(t, order[0])
			h.commit(t, order[1])
			h.finish(t, order[1])
			h.finish(t, order[0])
			ga, gb := grantSession(t, a), grantSession(t, b)
			if *ga.Current != *gb.Current || ga.Replayed == gb.Replayed {
				t.Fatal("duplicate consumed second permit")
			}
			changed := r
			changed.Capacity = 3
			expectCode(t, h.do(t, 1000, semaphoreAcquire(changed)).err, connect.CodeAlreadyExists)
			next, _ := s.NewAcquire("next", time.Second)
			g := grantSession(t, h.do(t, 1000, semaphoreAcquire(next)))
			assertSame(t, g.Current.FencingToken, "2")
		}
	})
	t.Run("expiry_fifo_batch_restart_and_slot_scoped_status", func(t *testing.T) {
		s := openSemaphore(t, h, "batch", 2)
		var owners [2]pintle.AcquireRequest
		var grants [2]*pintle.Lease
		for i := range owners {
			owners[i], _ = s.NewAcquire(fmt.Sprint("owner", i), 100*time.Millisecond)
			grants[i] = grantSession(t, h.do(t, 1000, semaphoreAcquire(owners[i]))).Current
		}
		var waits [2]pintle.WaitRequest
		for i := range waits {
			waits[i], _ = s.NewWaitAcquire(fmt.Sprint("waiter", i), time.Second, time.Second)
			assertSame(t, waitResult(t, queueSemaphore(t, h, waits[i], 1000)).State, pintle.Pending)
		}
		wrong, err := s.NewWaitAcquire("wrong-capacity", time.Second, 10*time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		wrong.Acquisition.Capacity = 3
		expectCode(t, queueSemaphore(t, h, wrong, 1000).err, connect.CodeFailedPrecondition)
		restartSessionHarness(h)
		expectCode(t, queueSemaphore(t, h, wrong, 1010).err, connect.CodeFailedPrecondition)
		barge, _ := s.NewAcquire("barge", time.Second)
		if !grantSession(t, h.do(t, 1100, semaphoreAcquire(barge))).Contended {
			t.Fatal("barged ahead of FIFO batch")
		}
		a := semaphoreStatus(t, h, waits[0].Acquisition, 1100).Acquired
		b := semaphoreStatus(t, h, waits[1].Acquisition, 1100).Acquired
		assertSame(t, a.Current.FencingToken, "3")
		assertSame(t, b.Current.FencingToken, "4")
		if a.Current.LeaseID == b.Current.LeaseID || a.Current.PermitSlot == b.Current.PermitSlot {
			t.Fatal("batch reused capability or slot")
		}
		for _, r := range owners {
			assertSame(t, semaphoreStatus(t, h, r, 1100).Acquired.State, pintle.Superseded)
		}
		rel, _ := s.NewRelease(*a.Current)
		p := h.do(t, 1100, semaphoreCall(2, releaseCall(rel)))
		if p.err != nil {
			t.Fatal(p.err)
		}
		assertSame(t, semaphoreStatus(t, h, waits[0].Acquisition, 1100).Acquired.State, pintle.Released)
		assertSame(t, semaphoreStatus(t, h, waits[1].Acquisition, 1100).Acquired.State, pintle.Active)
		stale, _ := s.NewRelease(*grants[0])
		expectCode(t, h.do(t, 1100, semaphoreCall(2, releaseCall(stale))).err, connect.CodePermissionDenied)
		resize, _ := s.NewAcquire("resize", time.Second)
		resize.Capacity = 3
		expectCode(t, h.do(t, 1100, semaphoreAcquire(resize)).err, connect.CodeFailedPrecondition)
		fresh, _ := s.NewAcquire("fresh", time.Second)
		g := grantSession(t, h.do(t, 1100, semaphoreAcquire(fresh)))
		assertSame(t, g.Current.PermitSlot, a.Current.PermitSlot)
		assertSame(t, g.Current.FencingToken, "5")
	})
	t.Run("renew_expiry_and_primitive_isolation", func(t *testing.T) {
		for _, now := range []uint64{1099, 1100, 1101} {
			key := fmt.Sprint("boundary", now)
			s := openSemaphore(t, h, key, 2)
			r, _ := s.NewAcquire("worker", 100*time.Millisecond)
			g := grantSession(t, h.do(t, 1000, semaphoreAcquire(r)))
			renew, _ := s.NewRenew(*g.Current, time.Second)
			p := h.do(t, now, semaphoreCall(2, func(ctx context.Context, c *pintle.Client) (any, error) { return c.Renew(ctx, renew) }))
			if now < 1100 {
				n := grantSession(t, p)
				assertSame(t, n.Current.PermitSlot, g.Current.PermitSlot)
				assertSame(t, n.Current.FencingToken, g.Current.FencingToken)
			} else {
				expectCode(t, p.err, connect.CodeFailedPrecondition)
			}
			m := h.open(t, key)
			mr, _ := m.NewAcquire("mutex", time.Second)
			mg := grantSession(t, h.do(t, 1000, acquireSession(mr)))
			assertSame(t, mg.Current.Capacity, uint32(0))
			assertSame(t, mg.Current.FencingToken, "1")
		}
	})
	t.Run("cancel_and_release_linearization", func(t *testing.T) {
		for cancelFirst := range 2 {
			s := openSemaphore(t, h, fmt.Sprint("cancel", cancelFirst), 2)
			var owner *pintle.Lease
			for range 2 {
				r, _ := s.NewAcquire("owner", time.Second)
				owner = grantSession(t, h.do(t, 1000, semaphoreAcquire(r))).Current
			}
			w, _ := s.NewWaitAcquire("waiting", time.Second, time.Second)
			assertSame(t, waitResult(t, queueSemaphore(t, h, w, 1000)).State, pintle.Pending)
			rel, _ := s.NewRelease(*owner)
			a := h.queue(t, 1001, semaphoreCall(2, waitCall(w, true)))
			b := h.queue(t, 1001, semaphoreCall(2, releaseCall(rel)))
			if cancelFirst == 1 {
				h.commit(t, a)
				h.commit(t, b)
			} else {
				h.commit(t, b)
				h.commit(t, a)
			}
			h.finish(t, a)
			h.finish(t, b)
			result := waitResult(t, a)
			if cancelFirst == 1 {
				assertSame(t, result.State, pintle.Cancelled)
			} else if result.Granted == nil || result.Granted.Current == nil {
				t.Fatal("cancel did not recover committed grant")
			}
		}
	})
}

func TestP4SemaphoreRollback(t *testing.T) {
	h := configuredSessionHarness(t, "P2_ENABLED:true")
	defer func() { h.r.stop() }()
	// Inject at every write until the first position beyond this transition.
	// Both real SQLite constraint errors and Rust errors must roll back grant,
	// fence, release and receipt together.
	for _, kind := range []string{"sql", "error"} {
		t.Run(kind, func(t *testing.T) {
			reachedEnd := false
			for position := 1; position <= 32; position++ {
				s := openSemaphore(t, h, fmt.Sprintf("rollback-%s-%d", kind, position), 2)
				var owner *pintle.Lease
				for range 2 {
					r, _ := s.NewAcquire("owner", time.Second)
					owner = grantSession(t, h.do(t, 1000, semaphoreAcquire(r))).Current
				}
				w, _ := s.NewWaitAcquire("waiter", time.Second, time.Second)
				waitResult(t, queueSemaphore(t, h, w, 1000))
				rel, _ := s.NewRelease(*owner)
				p := h.queue(t, 1001, semaphoreCall(2, releaseCall(rel)))
				release(p.after, directive{Now: 1001})
				release(p.before, directive{Now: 1001, Fault: object{"kind": kind, "after_write": position}})
				h.wait(t, p)
				if p.err == nil {
					reachedEnd = true
					t.Logf("rolled back all %d writes", position-1)
					break
				}
				expectCode(t, p.err, connect.CodeUnavailable)
				assertSame(t, semaphoreStatus(t, h, w.Acquisition, 1001).Waiting, pintle.Pending)
				retry := h.do(t, 1001, semaphoreCall(2, releaseCall(rel)))
				if retry.err != nil {
					t.Fatal(retry.err)
				}
				got := semaphoreStatus(t, h, w.Acquisition, 1001).Acquired
				assertSame(t, got.Current.FencingToken, "3")
			}
			if !reachedEnd {
				t.Fatal("rollback coverage incomplete")
			}
		})
	}
}

func TestP4SemaphoreAlarmAndPrecision(t *testing.T) {
	t.Run("durable_alarm_batch_after_restart", func(t *testing.T) {
		h := configuredSessionHarness(t, "P2_ENABLED:true", "PINTLE_TEST_REAL_ALARM:true")
		defer func() { h.r.stop() }()
		alarm := h.c.hold("alarm", "alarm_start")
		s := openSemaphore(t, h, "alarm", 2)
		for range 2 {
			r, _ := s.NewAcquire("owner", time.Second)
			grantSession(t, h.do(t, 1000, semaphoreAcquire(r)))
		}
		var waits [2]pintle.WaitRequest
		for i := range waits {
			waits[i], _ = s.NewWaitAcquire("waiter", time.Second, 5*time.Second)
			assertSame(t, waitResult(t, queueSemaphore(t, h, waits[i], 1000)).State, pintle.Pending)
		}
		arrived(t, alarm)
		h.r.stop()
		h.activation++
		alarm = h.c.hold("alarm", "alarm_start")
		h.r = start(t, h.root, h.state, h.config, h.activation, "P2_ENABLED:true", "PINTLE_TEST_SCHEDULER:"+h.c.server.URL, "PINTLE_TEST_REAL_ALARM:true")
		arrived(t, alarm)
		// Fail after the first grant has been written. The next alarm must
		// observe both original permits and both pending intents, then grant both.
		retry := h.c.hold("alarm", "alarm_start")
		release(alarm, directive{Now: 2000, Fault: object{"kind": "sql", "after_write": 7}})
		arrived(t, retry)
		for _, rows := range localRecords(t, h.root, h.state) {
			assertSame(t, rows["fence"], "2")
		}
		done := h.c.hold("alarm", "alarm_done")
		h.c.hold("alarm", "alarm_start")
		release(retry, directive{Now: 2000})
		arrived(t, done)
		release(done, directive{Now: 2000})
		a := semaphoreStatus(t, h, waits[0].Acquisition, 2000).Acquired.Current
		b := semaphoreStatus(t, h, waits[1].Acquisition, 2000).Acquired.Current
		assertSame(t, a.FencingToken, "3")
		assertSame(t, b.FencingToken, "4")
		if a.LeaseID == b.LeaseID {
			t.Fatal("alarm reused capability")
		}
	})
	for _, seed := range []uint64{(1 << 53) - 1, 1 << 53, (1 << 53) + 1, ^uint64(0)} {
		t.Run(strconv.FormatUint(seed, 10), func(t *testing.T) {
			h := configuredSessionHarness(t, "P2_ENABLED:true")
			defer func() { h.r.stop() }()
			s := openSemaphore(t, h, "precision", 2)
			h.r.stop()
			decimal := strconv.FormatUint(seed, 10)
			seedLocalCounters(t, h, decimal, decimal)
			restartSessionHarness(h)
			r, _ := s.NewAcquire("worker", time.Second)
			p := h.do(t, 1000, semaphoreAcquire(r))
			if seed == ^uint64(0) {
				expectCode(t, p.err, connect.CodeResourceExhausted)
				return
			}
			g := grantSession(t, p)
			assertSame(t, g.Current.FencingToken, strconv.FormatUint(seed+1, 10))
			assertSame(t, g.Current.Revision, strconv.FormatUint(seed+1, 10))
			restartSessionHarness(h)
			again := grantSession(t, h.do(t, 1000, semaphoreAcquire(r)))
			assertSame(t, *again.Receipt, *g.Receipt)
			assertSame(t, again.Replayed, true)
		})
	}
}

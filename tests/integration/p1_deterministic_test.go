//go:build (darwin || linux) && testhooks

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"connectrpc.com/connect"
	pintle "pintle.local/spike/sdk/go/pintle"
)

type callContext struct{}
type scheduledTransport struct{}

func (scheduledTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	copy := req.Clone(req.Context())
	copy.Header = req.Header.Clone()
	if call, ok := req.Context().Value(callContext{}).(string); ok {
		copy.Header.Set("X-Pintle-Test-Call", call)
	}
	return http.DefaultTransport.RoundTrip(copy)
}

type leaseJob struct {
	call, method, key, id string
	now                   uint64
	lease                 pintle.Lease
	ttl                   time.Duration
	before, after         *gate
	done                  chan struct{}
	grant                 *pintle.GrantResult
	released              *pintle.ReleaseResult
	status                *pintle.Status
	err                   error
}
type leaseHarness struct {
	t                         *testing.T
	c                         *controller
	root, state, config, a, b string
	r                         *runtime
	number, activation        int
	histories                 []object
}

func newLeaseHarness(t *testing.T) *leaseHarness {
	c := newController(t)
	root, state, config, a, b, r := p1setup(t, "PINTLE_TEST_SCHEDULER:"+c.server.URL)
	h := &leaseHarness{t: t, c: c, root: root, state: state, config: config, a: a, b: b, r: r}
	t.Cleanup(func() {
		c.mu.Lock()
		events := append([]string(nil), c.events...)
		c.mu.Unlock()
		data, _ := json.MarshalIndent(object{"histories": h.histories, "checkpoints": events}, "", "  ")
		if err := os.WriteFile(filepath.Join(filepath.Dir(state), "lease-histories.json"), data, 0600); err != nil {
			t.Error(err)
		}
	})
	return h
}
func (h *leaseHarness) queue(t *testing.T, method, key, id string, l pintle.Lease, ttl time.Duration, now uint64) *leaseJob {
	t.Helper()
	h.number++
	p := &leaseJob{call: fmt.Sprintf("lease-%d", h.number), method: method, key: key, id: id, now: now, lease: l, ttl: ttl, done: make(chan struct{})}
	begin := h.c.hold(p.call, "gateway_start")
	release(begin, directive{Now: now})
	p.before = h.c.hold(p.call, "before_commit")
	p.after = h.c.hold(p.call, "after_sync")
	c := sdk(t, h.r.base, h.a, &http.Client{Transport: scheduledTransport{}, Timeout: 30 * time.Second})
	ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), callContext{}, p.call), 60*time.Second)
	go func() {
		defer close(p.done)
		defer cancel()
		switch method {
		case "acquire":
			p.grant, p.err = c.TryAcquire(ctx, acq(key, id, ttl))
		case "renew":
			p.grant, p.err = c.Renew(ctx, pintle.RenewRequest{Lease: l, RequestID: id, TTL: ttl})
		case "release":
			p.released, p.err = c.Release(ctx, pintle.ReleaseRequest{Lease: l, RequestID: id})
		case "status":
			p.status, p.err = c.GetRequestStatus(ctx, pintle.Resource{Namespace: "test", Key: key}, id)
		default:
			panic(method)
		}
	}()
	arrived(t, p.before)
	return p
}
func (h *leaseHarness) commit(t *testing.T, p *leaseJob) {
	t.Helper()
	release(p.before, directive{Now: p.now})
	arrived(t, p.after)
}
func (h *leaseHarness) finish(t *testing.T, p *leaseJob) {
	t.Helper()
	release(p.after, directive{Now: p.now})
	h.wait(t, p)
}
func (h *leaseHarness) wait(t *testing.T, p *leaseJob) {
	t.Helper()
	select {
	case <-p.done:
	case <-time.After(15 * time.Second):
		t.Fatal("lease request watchdog")
	}
}
func (h *leaseHarness) do(t *testing.T, method, key, id string, l pintle.Lease, ttl time.Duration, now uint64) *leaseJob {
	t.Helper()
	p := h.queue(t, method, key, id, l, ttl, now)
	h.commit(t, p)
	h.finish(t, p)
	return p
}
func (h *leaseHarness) fresh(t *testing.T, key string, ttl time.Duration) *pintle.Lease {
	p := h.do(t, "acquire", key, "a", pintle.Lease{}, ttl, 1000)
	return requireLease(t, p.grant, p.err)
}
func (h *leaseHarness) restart() {
	h.r.stop()
	h.activation++
	h.r = start(h.t, h.root, h.state, h.config, h.activation, false, "P1_ENABLED:true", "PINTLE_TEST_SCHEDULER:"+h.c.server.URL)
}

// Independent small reference model for the controlled three-operation histories.
// It starts with lease A, then predicts Renew(A), Release(A), Acquire(B) in each
// chosen linearization order; response delivery order does not affect the state.
type lifecycleModel struct {
	owner                   string
	expiry, fence, revision uint64
}
type lifecycleExpected struct {
	kind                    string
	expiry, fence, revision uint64
	code                    connect.Code
}

func (m *lifecycleModel) step(method string, now uint64) lifecycleExpected {
	if method == "acquire" {
		if m.owner != "" && now < m.expiry {
			return lifecycleExpected{kind: "contended"}
		}
		m.owner = "b"
		m.expiry = now + 300
		m.fence++
		m.revision++
	} else {
		if m.owner == "" {
			return lifecycleExpected{kind: "error", code: connect.CodeFailedPrecondition}
		}
		if m.owner != "a" {
			return lifecycleExpected{kind: "error", code: connect.CodePermissionDenied}
		}
		if now >= m.expiry {
			return lifecycleExpected{kind: "error", code: connect.CodeFailedPrecondition}
		}
		m.revision++
		if method == "renew" {
			m.expiry = max(m.expiry, now+300)
		} else {
			m.owner = ""
			return lifecycleExpected{kind: "release", revision: m.revision}
		}
	}
	return lifecycleExpected{kind: "grant", expiry: m.expiry, fence: m.fence, revision: m.revision}
}
func verifyLifecycle(t *testing.T, p *leaseJob, e lifecycleExpected) {
	t.Helper()
	switch e.kind {
	case "error":
		expectCode(t, p.err, e.code)
	case "release":
		if p.err != nil {
			t.Fatal(p.err)
		}
		assertSame(t, p.released.Revision, fmt.Sprint(e.revision))
		assertSame(t, p.released.Replayed, false)
	case "contended":
		if p.err != nil {
			t.Fatal(p.err)
		}
		assertSame(t, p.grant.Contended, true)
	case "grant":
		l := requireLease(t, p.grant, p.err)
		assertSame(t, l.ExpiresAtUnixMS, e.expiry)
		assertSame(t, l.FencingToken, fmt.Sprint(e.fence))
		assertSame(t, l.Revision, fmt.Sprint(e.revision))
		assertSame(t, p.grant.Replayed, false)
	default:
		t.Fatal("invalid expected outcome")
	}
}
func TestP1Deterministic(t *testing.T) {
	h := newLeaseHarness(t)
	t.Run("expiry_edges", func(t *testing.T) {
		for _, method := range []string{"acquire", "renew", "release"} {
			for _, now := range []uint64{1099, 1100, 1101} {
				t.Run(fmt.Sprintf("%s-%d", method, now), func(t *testing.T) {
					key := fmt.Sprintf("edge-%s-%d", method, now)
					a := h.fresh(t, key, 100*time.Millisecond)
					p := h.do(t, method, key, "next", *a, 300*time.Millisecond, now)
					model := lifecycleModel{owner: "a", expiry: 1100, fence: 1, revision: 1}
					verifyLifecycle(t, p, model.step(method, now))
				})
			}
		}
	})
	t.Run("renew_release_acquire_all_orders", func(t *testing.T) {
		orders := [][3]int{{0, 1, 2}, {0, 2, 1}, {1, 0, 2}, {1, 2, 0}, {2, 0, 1}, {2, 1, 0}}
		for _, now := range []uint64{1099, 1100} {
			for i, order := range orders {
				for j, replies := range orders {
					t.Run(fmt.Sprintf("at%d-commit%d-reply%d", now, i, j), func(t *testing.T) {
						key := fmt.Sprintf("race-%d-%d-%d", now, i, j)
						a := h.fresh(t, key, 100*time.Millisecond)
						methods := []string{"renew", "release", "acquire"}
						ps := make([]*leaseJob, 3)
						for k, method := range methods {
							ps[k] = h.queue(t, method, key, method, *a, 300*time.Millisecond, now)
						}
						// All three real DO handlers are now paused before transaction entry.
						model := lifecycleModel{owner: "a", expiry: 1100, fence: 1, revision: 1}
						expected := make([]lifecycleExpected, 3)
						for _, k := range order {
							expected[k] = model.step(methods[k], now)
							h.commit(t, ps[k])
						}
						for _, k := range replies {
							h.finish(t, ps[k])
							verifyLifecycle(t, ps[k], expected[k])
						}
						// Save ordering and numeric expectations, never capabilities.
						h.histories = append(h.histories, object{"test": t.Name(), "now": now, "methods": methods, "commit_order": order, "response_order": replies, "expected": []object{
							{"kind": expected[0].kind, "code": expected[0].code.String(), "revision": expected[0].revision},
							{"kind": expected[1].kind, "code": expected[1].code.String(), "revision": expected[1].revision},
							{"kind": expected[2].kind, "code": expected[2].code.String(), "revision": expected[2].revision},
						}})
					})
				}
			}
		}
	})
	t.Run("renew_and_release_duplicate_ids", func(t *testing.T) {
		for _, method := range []string{"renew", "release"} {
			t.Run(method, func(t *testing.T) {
				key := "duplicate-" + method
				a := h.fresh(t, key, time.Second)
				first := h.queue(t, method, key, "same", *a, time.Second, 1050)
				second := h.queue(t, method, key, "same", *a, time.Second, 1050)
				h.commit(t, second)
				h.commit(t, first)
				h.finish(t, first)
				h.finish(t, second)
				if first.err != nil || second.err != nil {
					t.Fatalf("%v %v", first.err, second.err)
				}
				if method == "renew" {
					assertSame(t, *first.grant.Receipt, *second.grant.Receipt)
					assertSame(t, first.grant.Replayed, true)
					assertSame(t, second.grant.Replayed, false)
				} else {
					assertSame(t, first.released.Revision, second.released.Revision)
					assertSame(t, first.released.Replayed, true)
					assertSame(t, second.released.Replayed, false)
				}
			})
		}
	})
	t.Run("rollback_every_lifecycle_write", func(t *testing.T) {
		for _, method := range []string{"acquire", "renew", "release"} {
			for _, kind := range []string{"sql", "error"} {
				writes := 5
				if method == "acquire" {
					writes = 6
				}
				for n := 1; n <= writes; n++ {
					t.Run(fmt.Sprintf("%s-%s-%d", method, kind, n), func(t *testing.T) {
						key := fmt.Sprintf("rollback-%s-%s-%d", method, kind, n)
						var a pintle.Lease
						if method != "acquire" {
							a = *h.fresh(t, key, time.Second)
						}
						p := h.queue(t, method, key, "operation", a, time.Second, 1050)
						release(p.before, directive{Now: 1050, Fault: object{"kind": kind, "after_write": n}})
						h.wait(t, p)
						expectCode(t, p.err, connect.CodeUnavailable)
						retry := h.do(t, method, key, "operation", a, time.Second, 1050)
						if retry.err != nil {
							t.Fatal(retry.err)
						}
						if method == "release" {
							assertSame(t, retry.released.Replayed, false)
							assertSame(t, retry.released.Revision, "2")
						} else {
							assertSame(t, retry.grant.Replayed, false)
							assertSame(t, retry.grant.Receipt.FencingToken, "1")
							want := "2"
							if method == "acquire" {
								want = "1"
							}
							assertSame(t, retry.grant.Receipt.Revision, want)
						}
					})
				}
			}
		}
	})
	t.Run("unknown_acknowledgement_and_restart", func(t *testing.T) {
		for _, method := range []string{"acquire", "renew", "release"} {
			for _, stage := range []string{"before_commit", "after_sync"} {
				t.Run(method+"-"+stage, func(t *testing.T) {
					key := "restart-" + method + "-" + stage
					var a pintle.Lease
					if method != "acquire" {
						a = *h.fresh(t, key, time.Second)
					}
					p := h.queue(t, method, key, "operation", a, time.Second, 1050)
					if stage == "after_sync" {
						h.commit(t, p)
					}
					h.r.stop()
					h.wait(t, p)
					if p.err == nil {
						t.Fatal("expected connection loss")
					}
					release(p.before, directive{Now: 1050})
					release(p.after, directive{Now: 1050})
					h.restart()
					retry := h.do(t, method, key, "operation", a, time.Second, 1100)
					if retry.err != nil {
						t.Fatal(retry.err)
					}
					committed := stage == "after_sync"
					if method == "release" {
						assertSame(t, retry.released.Replayed, committed)
					} else {
						assertSame(t, retry.grant.Replayed, committed)
						want := uint64(2100)
						if committed {
							want = 2050
						}
						assertSame(t, retry.grant.Receipt.ExpiresAtUnixMS, want)
					}
				})
			}
		}
	})
	t.Run("deadline_and_acknowledgement_recovery", func(t *testing.T) {
		for _, method := range []string{"acquire", "renew", "release"} {
			for _, boundary := range []string{"deadline-before", "deadline-after", "ack-after"} {
				t.Run(method+"-"+boundary, func(t *testing.T) {
					key := "uncertain-" + method + "-" + boundary
					var a pintle.Lease
					if method != "acquire" {
						a = *h.fresh(t, key, 60*time.Second)
					}
					p := h.queue(t, method, key, "operation", a, 60*time.Second, 1050)
					committed := boundary != "deadline-before"
					if !committed {
						// The server caps RPC duration at 10,000ms; equality expires.
						release(p.before, directive{Now: 11050})
					} else {
						h.commit(t, p)
						// Durability has completed, but the acknowledgement is lost
						// or the response's deadline check now rejects it.
						responseTime := uint64(11050)
						if boundary == "ack-after" {
							responseTime = 1051 // Isolate acknowledgement loss from deadline expiry.
						}
						release(p.after, directive{Now: responseTime, Fail: boundary == "ack-after"})
					}
					h.wait(t, p)
					code := connect.CodeDeadlineExceeded
					if boundary == "ack-after" {
						code = connect.CodeUnavailable
					}
					expectCode(t, p.err, code)
					retry := h.do(t, method, key, "operation", a, 60*time.Second, 11051)
					if retry.err != nil {
						t.Fatal(retry.err)
					}
					if method == "release" {
						assertSame(t, retry.released.Replayed, committed)
						assertSame(t, retry.released.Revision, "2")
					} else {
						assertSame(t, retry.grant.Replayed, committed)
						want := uint64(71051)
						if committed {
							want = 61050
						}
						assertSame(t, retry.grant.Receipt.ExpiresAtUnixMS, want)
						assertSame(t, retry.grant.Receipt.FencingToken, "1")
					}
				})
			}
		}
	})
	t.Run("clock_regression_and_status_after_restart", func(t *testing.T) {
		key := "clock-regression"
		a := h.fresh(t, key, 100*time.Millisecond)
		observed := h.do(t, "status", key, "a", *a, 0, 1100)
		if observed.err != nil {
			t.Fatal(observed.err)
		}
		assertSame(t, observed.status.Acquired.State, pintle.Expired)
		h.restart()
		bad := h.do(t, "renew", key, "renew", *a, time.Second, 1099)
		expectCode(t, bad.err, connect.CodeUnavailable)
		next := h.do(t, "acquire", key, "b", *a, time.Second, 1100)
		l := requireLease(t, next.grant, next.err)
		assertSame(t, l.FencingToken, "2")
	})
	h.r.stop()
}

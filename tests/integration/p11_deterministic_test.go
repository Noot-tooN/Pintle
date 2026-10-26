//go:build (darwin || linux) && testhooks

package integration

import (
	"connectrpc.com/connect"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	pintle "pintle.local/spike/sdk/go/pintle"
	"testing"
	"time"
)

type sessionPending struct {
	cancel        context.CancelFunc
	before, after *gate
	done          chan struct{}
	result        any
	err           error
	now           uint64
}
type sessionHarness struct {
	t                         *testing.T
	c                         *controller
	root, state, config, a, b string
	r                         *runtime
	number, activation        int
}

func newSessionHarness(t *testing.T, vars ...string) *sessionHarness {
	return configuredSessionHarness(t, "P11_ENABLED:true", vars...)
}
func configuredSessionHarness(t *testing.T, mode string, vars ...string) *sessionHarness {
	c := newController(t)
	root, state, config, a, b, r := leaseSetup(t, mode, append([]string{"PINTLE_TEST_SCHEDULER:" + c.server.URL}, vars...)...)
	t.Cleanup(func() {
		c.mu.Lock()
		events := append([]string(nil), c.events...)
		c.mu.Unlock()
		data, err := json.MarshalIndent(events, "", "  ")
		if err == nil {
			err = os.WriteFile(filepath.Join(filepath.Dir(state), "session-checkpoints.json"), data, 0600)
		}
		if err != nil {
			t.Error(err)
		}
	})
	return &sessionHarness{t: t, c: c, root: root, state: state, config: config, a: a, b: b, r: r}
}
func (h *sessionHarness) queue(t *testing.T, now uint64, f func(context.Context, *pintle.Client) (any, error)) *sessionPending {
	h.number++
	call := fmt.Sprintf("session-%d", h.number)
	begin := h.c.hold(call, "gateway_start")
	release(begin, directive{Now: now})
	p := &sessionPending{before: h.c.hold(call, "before_commit"), after: h.c.hold(call, "after_sync"), done: make(chan struct{}), now: now}
	c := sdk(t, h.r.base, h.a, &http.Client{Transport: scheduledTransport{}, Timeout: 30 * time.Second})
	ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), callContext{}, call), 60*time.Second)
	p.cancel = cancel
	go func() { defer close(p.done); defer cancel(); p.result, p.err = f(ctx, c) }()
	arrived(t, p.before)
	return p
}
func (h *sessionHarness) commit(t *testing.T, p *sessionPending) {
	release(p.before, directive{Now: p.now})
	arrived(t, p.after)
}
func (h *sessionHarness) wait(t *testing.T, p *sessionPending) {
	t.Helper()
	select {
	case <-p.done:
	case <-time.After(15 * time.Second):
		t.Fatal("session request watchdog")
	}
}
func (h *sessionHarness) finish(t *testing.T, p *sessionPending) {
	release(p.after, directive{Now: p.now})
	h.wait(t, p)
}
func (h *sessionHarness) do(t *testing.T, now uint64, f func(context.Context, *pintle.Client) (any, error)) *sessionPending {
	p := h.queue(t, now, f)
	h.commit(t, p)
	h.finish(t, p)
	return p
}
func (h *sessionHarness) open(t *testing.T, key string) *pintle.Session {
	p := h.do(t, 1000, func(ctx context.Context, c *pintle.Client) (any, error) {
		return c.OpenSession(ctx, pintle.Resource{Namespace: "test", Key: key})
	})
	if p.err != nil {
		t.Fatal(p.err)
	}
	return p.result.(*pintle.Session)
}
func acquireSession(r pintle.AcquireRequest) func(context.Context, *pintle.Client) (any, error) {
	return func(ctx context.Context, c *pintle.Client) (any, error) { return c.TryAcquire(ctx, r) }
}
func grantSession(t *testing.T, p *sessionPending) *pintle.GrantResult {
	t.Helper()
	if p.err != nil {
		t.Fatal(p.err)
	}
	return p.result.(*pintle.GrantResult)
}
func TestP11Deterministic(t *testing.T) {
	h := newSessionHarness(t)
	t.Run("session_expiry_boundaries", func(t *testing.T) {
		for _, now := range []uint64{300999, 301000, 301001} {
			t.Run(fmt.Sprint(now), func(t *testing.T) {
				s := h.open(t, fmt.Sprint(now))
				req, _ := s.NewAcquire("worker", time.Second)
				p := h.do(t, now, acquireSession(req))
				if now < 301000 {
					grantSession(t, p)
				} else {
					expectCode(t, p.err, connect.CodeFailedPrecondition)
				}
			})
		}
	})
	t.Run("acknowledgement_versus_retry_orders", func(t *testing.T) {
		for _, ackFirst := range []bool{false, true} {
			for _, replyFirst := range []bool{false, true} {
				t.Run(fmt.Sprintf("ack%t-response%t", ackFirst, replyFirst), func(t *testing.T) {
					s := h.open(t, fmt.Sprintf("ack-%t-%t", ackFirst, replyFirst))
					req, _ := s.NewAcquire("worker", time.Second)
					original := grantSession(t, h.do(t, 1000, acquireSession(req)))
					// Piggyback ACK on sequence 2. It is contended and cannot release owner.
					next := req
					next.RequestID = "next"
					next.Attempt = &pintle.Attempt{SessionID: req.Attempt.SessionID, Sequence: 2, AcknowledgedThrough: 1}
					ack := h.queue(t, 1001, acquireSession(next))
					retry := h.queue(t, 1001, acquireSession(req))
					if ackFirst {
						h.commit(t, ack)
						h.commit(t, retry)
					} else {
						h.commit(t, retry)
						h.commit(t, ack)
					}
					if replyFirst {
						h.finish(t, retry)
						h.finish(t, ack)
					} else {
						h.finish(t, ack)
						h.finish(t, retry)
					}
					assertSame(t, grantSession(t, ack).Contended, true)
					if ackFirst {
						expectCode(t, retry.err, connect.CodeFailedPrecondition)
					} else {
						assertSame(t, *grantSession(t, retry).Receipt, *original.Receipt)
					}
				})
			}
		}
	})
	t.Run("every_write_rollback", func(t *testing.T) {
		for _, method := range []string{"acquire", "renew", "release"} {
			writes := 6
			if method != "acquire" {
				writes = 5
			}
			for _, kind := range []string{"sql", "error"} {
				for n := 1; n <= writes; n++ {
					t.Run(fmt.Sprintf("%s-%s-%d", method, kind, n), func(t *testing.T) {
						s := h.open(t, fmt.Sprintf("fault-%s-%s-%d", method, kind, n))
						req, _ := s.NewAcquire("worker", time.Second)
						operation := acquireSession(req)
						if method != "acquire" {
							g := grantSession(t, h.do(t, 1000, operation))
							l := *g.Current
							if method == "renew" {
								r, _ := s.NewRenew(l, time.Second)
								operation = func(ctx context.Context, c *pintle.Client) (any, error) { return c.Renew(ctx, r) }
							} else {
								r, _ := s.NewRelease(l)
								operation = func(ctx context.Context, c *pintle.Client) (any, error) { return c.Release(ctx, r) }
							}
						}
						p := h.queue(t, 1001, operation)
						release(p.before, directive{Now: 1001, Fault: object{"kind": kind, "after_write": n}})
						h.wait(t, p)
						expectCode(t, p.err, connect.CodeUnavailable)
						p = h.do(t, 1002, operation)
						if p.err != nil {
							t.Fatal(p.err)
						}
						if method == "release" {
							assertSame(t, p.result.(*pintle.ReleaseResult).Replayed, false)
						} else {
							g := grantSession(t, p)
							assertSame(t, g.Replayed, false)
							assertSame(t, g.Receipt.FencingToken, "1")
						}
					})
				}
			}
		}
	})
	t.Run("unknown_commit_restart", func(t *testing.T) {
		for _, committed := range []bool{false, true} {
			t.Run(fmt.Sprint(committed), func(t *testing.T) {
				s := h.open(t, fmt.Sprintf("restart-%t", committed))
				req, _ := s.NewAcquire("worker", time.Second)
				p := h.queue(t, 1001, acquireSession(req))
				if committed {
					h.commit(t, p)
				}
				h.r.stop()
				h.wait(t, p)
				if p.err == nil {
					t.Fatal("expected connection loss")
				}
				release(p.before, directive{Now: 1001})
				release(p.after, directive{Now: 1001})
				h.activation++
				h.r = start(h.t, h.root, h.state, h.config, h.activation, false, "P11_ENABLED:true", "PINTLE_TEST_SCHEDULER:"+h.c.server.URL)
				g := grantSession(t, h.do(t, 1002, acquireSession(req)))
				assertSame(t, g.Replayed, committed)
			})
		}
	})
	h.r.stop()
}

// Real workerd invokes DurableObject.alarm. Only its clock and admission are
// controlled; this never calls cleanup through a fake public HTTP endpoint.
func TestP11DeterministicAlarm(t *testing.T) {
	for _, mode := range []string{"normal", "rollback", "restart"} {
		t.Run(mode, func(t *testing.T) {
			h := newSessionHarness(t, "PINTLE_TEST_REAL_ALARM:true")
			startGate := h.c.hold("alarm", "alarm_start")
			doneGate := h.c.hold("alarm", "alarm_done")
			s := h.open(t, "alarm-resource")
			req, _ := s.NewAcquire("worker", time.Second)
			g := grantSession(t, h.do(t, 1000, acquireSession(req)))
			assertSame(t, g.Receipt.FencingToken, "1")
			arrived(t, startGate)
			before := sessionStats(t, h.root, h.state)
			assertSame(t, before[0]["sessions"], float64(1))
			if mode != "normal" {
				again := h.c.hold("alarm", "alarm_start")
				if mode == "rollback" {
					release(startGate, directive{Now: 301000, Fault: object{"kind": "error", "after_write": 2}})
				} else {
					h.r.stop()
					release(startGate, directive{Now: 301000})
					h.activation++
					h.r = start(h.t, h.root, h.state, h.config, h.activation, false, "P11_ENABLED:true", "PINTLE_TEST_SCHEDULER:"+h.c.server.URL, "PINTLE_TEST_REAL_ALARM:true")
				}
				arrived(t, again)
				intact := sessionStats(t, h.root, h.state)
				assertSame(t, intact[0]["sessions"], float64(1))
				assertSame(t, intact[0]["receipts"], float64(1))
				release(again, directive{Now: 301000})
			} else {
				release(startGate, directive{Now: 301000})
			}
			arrived(t, doneGate)
			after := sessionStats(t, h.root, h.state)
			assertSame(t, after[0]["sessions"], float64(0))
			assertSame(t, after[0]["receipts"], float64(0))
			assertSame(t, after[0]["fence"], "1")
			release(doneGate, directive{Now: 301000})
			p := h.do(t, 301000, acquireSession(req))
			expectCode(t, p.err, connect.CodeFailedPrecondition)
			h.r.stop()
		})
	}
}

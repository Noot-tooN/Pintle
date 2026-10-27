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

// Start without an implicit commit gate so the caller can control any boundary.
func launchMutation(t *testing.T, h *sessionHarness, call string, req pintle.AcquireRequest) *sessionPending {
	t.Helper()
	p := &sessionPending{done: make(chan struct{})}
	c := sdk(t, h.r.base, h.a, &http.Client{Transport: scheduledTransport{}, Timeout: 30 * time.Second})
	ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), callContext{}, call), 60*time.Second)
	p.cancel = cancel
	go func() { defer close(p.done); defer cancel(); p.result, p.err = c.TryAcquire(ctx, req) }()
	return p
}
func TestMutexSchedules(t *testing.T) {
	h := configuredSessionHarness(t, "P2_ENABLED:true")
	defer h.r.stop()
	t.Run("all_three_request_orders", func(t *testing.T) {
		orders := [][3]int{{0, 1, 2}, {0, 2, 1}, {1, 0, 2}, {1, 2, 0}, {2, 0, 1}, {2, 1, 0}}
		for _, mode := range []string{"distinct", "duplicate", "conflict"} {
			for ai, admit := range orders {
				for ri, reply := range orders {
					t.Run(fmt.Sprintf("%s-admit%d-reply%d", mode, ai, ri), func(t *testing.T) {
						s := h.open(t, fmt.Sprintf("%s-%d-%d", mode, ai, ri))
						req, _ := s.NewAcquire("worker-0", time.Minute)
						requests := []pintle.AcquireRequest{req, req, req}
						if mode == "distinct" {
							requests[1], _ = s.NewAcquire("worker-1", time.Minute)
							requests[2], _ = s.NewAcquire("worker-2", time.Minute)
						} else if mode == "conflict" {
							requests[1].HolderRef = "worker-1"
							requests[2].HolderRef = "worker-2"
						}
						pending := make([]*sessionPending, 3)
						for i := range pending {
							pending[i] = h.queue(t, 1001, acquireSession(requests[i]))
						}
						for _, i := range admit {
							h.commit(t, pending[i])
						}
						for _, i := range reply {
							h.finish(t, pending[i])
						}
						first := grantSession(t, pending[admit[0]])
						assertSame(t, first.Replayed, false)
						assertSame(t, first.Receipt.FencingToken, "1")
						for _, i := range admit[1:] {
							switch mode {
							case "distinct":
								assertSame(t, grantSession(t, pending[i]).Contended, true)
							case "duplicate":
								g := grantSession(t, pending[i])
								assertSame(t, g.Replayed, true)
								assertSame(t, *g.Receipt, *first.Receipt)
							case "conflict":
								expectCode(t, pending[i].err, connect.CodeAlreadyExists)
							}
						}
					})
				}
			}
		}
	})
	t.Run("deadline_boundaries", func(t *testing.T) {
		// The gateway caps the client's watchdog timeout to 10000 logical ms.
		for _, stage := range []string{"before_dispatch", "before_commit", "before_response", "gateway_response"} {
			for _, now := range []uint64{10999, 11000, 11001} {
				t.Run(fmt.Sprintf("%s-%d", stage, now), func(t *testing.T) {
					call := fmt.Sprintf("deadline-%s-%d", strings.ReplaceAll(stage, "_", "-"), now)
					s := h.open(t, call)
					req, _ := s.NewAcquire("worker", time.Minute)
					g := h.c.hold(call, stage)
					p := launchMutation(t, h, call, req)
					arrived(t, g)
					release(g, directive{Now: now})
					h.wait(t, p)
					if now < 11000 {
						grantSession(t, p)
					} else {
						expectCode(t, p.err, connect.CodeDeadlineExceeded)
					}
					committed := now < 11000 || stage == "before_response" || stage == "gateway_response"
					assertSame(t, grantSession(t, h.do(t, 11002, acquireSession(req))).Replayed, committed)
				})
			}
		}
	})
	t.Run("controlled_timer_races", func(t *testing.T) {
		for _, stage := range []string{"before_commit", "after_sync"} {
			for _, timeoutWins := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s-%t", stage, timeoutWins), func(t *testing.T) {
					call := fmt.Sprintf("timer-%s-%t", strings.ReplaceAll(stage, "_", "-"), timeoutWins)
					s := h.open(t, call)
					req, _ := s.NewAcquire("worker", time.Minute)
					begin := h.c.hold(call, "gateway_start")
					timer := h.c.hold(call, "deadline_timer")
					work := h.c.hold(call, stage)
					p := launchMutation(t, h, call, req)
					arrived(t, begin)
					release(begin, directive{Now: 1000, Timer: true})
					arrived(t, timer)
					arrived(t, work)
					if timeoutWins {
						release(timer, directive{Now: 11000})
						h.wait(t, p)
						expectCode(t, p.err, connect.CodeDeadlineExceeded)
						release(work, directive{Now: 11000})
					} else {
						release(work, directive{Now: 1000})
						h.wait(t, p)
						grantSession(t, p)
						release(timer, directive{Now: 11000})
					}
					assertSame(t, grantSession(t, h.do(t, 11002, acquireSession(req))).Replayed, !timeoutWins || stage == "after_sync")
				})
			}
		}
	})
	t.Run("failure_boundaries", func(t *testing.T) {
		for _, stage := range []string{"before_dispatch", "before_commit", "after_commit", "after_sync", "before_response", "gateway_response"} {
			t.Run(stage, func(t *testing.T) {
				call := "failure-" + strings.ReplaceAll(stage, "_", "-")
				s := h.open(t, call)
				req, _ := s.NewAcquire("worker", time.Minute)
				g := h.c.hold(call, stage)
				p := launchMutation(t, h, call, req)
				arrived(t, g)
				release(g, directive{Now: 1000, Fail: true})
				h.wait(t, p)
				expectCode(t, p.err, connect.CodeUnavailable)
				assertSame(t, grantSession(t, h.do(t, 1001, acquireSession(req))).Replayed, stage != "before_dispatch" && stage != "before_commit")
			})
		}
	})
}

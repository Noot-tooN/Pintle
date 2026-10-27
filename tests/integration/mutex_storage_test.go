//go:build (darwin || linux) && testhooks

package integration

import (
	"connectrpc.com/connect"
	"fmt"
	"strconv"
	"testing"
	"time"
)

func restartSessionHarness(h *sessionHarness) {
	h.r.stop()
	h.activation++
	h.r = start(h.t, h.root, h.state, h.config, h.activation, "P2_ENABLED:true", "PINTLE_TEST_SCHEDULER:"+h.c.server.URL)
}
func TestMutexStorage(t *testing.T) {
	t.Run("lossless_counters_and_exhaustion", func(t *testing.T) {
		for _, seed := range []uint64{(1 << 53) - 1, 1 << 53, (1 << 53) + 1, ^uint64(0) - 1, ^uint64(0)} {
			t.Run(strconv.FormatUint(seed, 10), func(t *testing.T) {
				h := configuredSessionHarness(t, "P2_ENABLED:true")
				defer func() { h.r.stop() }()
				s := h.open(t, "precision")
				h.r.stop()
				decimal := strconv.FormatUint(seed, 10)
				seedLocalCounters(t, h, decimal, decimal)
				restartSessionHarness(h)
				req, _ := s.NewAcquire("worker", time.Minute)
				p := h.do(t, 1001, acquireSession(req))
				if seed == ^uint64(0) {
					expectCode(t, p.err, connect.CodeResourceExhausted)
					for _, rows := range localRecords(t, h.root, h.state) {
						assertSame(t, rows["fence"], decimal)
						assertSame(t, rows["revision"], decimal)
						assertSame(t, rows["owner"], "")
					}
					return
				}
				g := grantSession(t, p)
				next := strconv.FormatUint(seed+1, 10)
				assertSame(t, g.Receipt.FencingToken, next)
				assertSame(t, g.Receipt.Revision, next)
				for _, rows := range localRecords(t, h.root, h.state) {
					assertSame(t, rows["fence"], next)
					assertSame(t, rows["revision"], next)
				}
				restartSessionHarness(h)
				again := grantSession(t, h.do(t, 1002, acquireSession(req)))
				assertSame(t, again.Replayed, true)
				assertSame(t, *again.Receipt, *g.Receipt)
				rel, _ := s.NewRelease(*g.Current)
				released := h.do(t, 1003, releaseCall(rel))
				if seed == ^uint64(0)-1 {
					expectCode(t, released.err, connect.CodeResourceExhausted)
					return
				}
				if released.err != nil {
					t.Fatal(released.err)
				}
				successor, _ := s.NewAcquire("successor", time.Minute)
				got := grantSession(t, h.do(t, 1004, acquireSession(successor)))
				assertSame(t, got.Receipt.FencingToken, strconv.FormatUint(seed+2, 10))
				assertSame(t, got.Receipt.Revision, strconv.FormatUint(seed+3, 10))
				historical := grantSession(t, h.do(t, 1004, acquireSession(req)))
				assertSame(t, *historical.Receipt, *g.Receipt)
				assertSame(t, historical.Current == nil, true)
			})
		}
	})
	t.Run("independent_counter_exhaustion", func(t *testing.T) {
		for _, pair := range [][2]string{{"18446744073709551615", "42"}, {"42", "18446744073709551615"}} {
			t.Run(pair[0]+"-"+pair[1], func(t *testing.T) {
				h := configuredSessionHarness(t, "P2_ENABLED:true")
				defer func() { h.r.stop() }()
				s := h.open(t, "exhaustion")
				h.r.stop()
				seedLocalCounters(t, h, pair[0], pair[1])
				restartSessionHarness(h)
				req, _ := s.NewAcquire("worker", time.Minute)
				expectCode(t, h.do(t, 1001, acquireSession(req)).err, connect.CodeResourceExhausted)
				for _, rows := range localRecords(t, h.root, h.state) {
					assertSame(t, rows["fence"], pair[0])
					assertSame(t, rows["revision"], pair[1])
					assertSame(t, rows["owner"], "")
				}
			})
		}
	})
	t.Run("complete_rollback", func(t *testing.T) {
		h := configuredSessionHarness(t, "P2_ENABLED:true")
		defer h.r.stop()
		for _, kind := range []string{"sql", "error"} {
			completed := false
			for n := 1; n <= 32; n++ {
				s := h.open(t, fmt.Sprintf("%s-%d", kind, n))
				req, _ := s.NewAcquire("worker", time.Minute)
				before := localRecords(t, h.root, h.state)
				p := h.queue(t, 1001, acquireSession(req))
				release(p.after, directive{Now: 1001})
				release(p.before, directive{Now: 1001, Fault: object{"kind": kind, "after_write": n}})
				h.wait(t, p)
				if p.err == nil {
					if n < 5 {
						t.Fatal("expected multi-record grant")
					}
					t.Logf("%s: all %d write positions rolled back", kind, n-1)
					completed = true
					break
				}
				expectCode(t, p.err, connect.CodeUnavailable)
				assertSame(t, localRecords(t, h.root, h.state), before)
				// An aborted intent reserves neither its sequence nor its payload.
				changed := req
				changed.HolderRef = "changed"
				got := grantSession(t, h.do(t, 1001, acquireSession(changed)))
				assertSame(t, got.Replayed, false)
				assertSame(t, got.Receipt.FencingToken, "1")
				expectCode(t, h.do(t, 1001, acquireSession(req)).err, connect.CodeAlreadyExists)
			}
			if !completed {
				t.Fatal("write count exceeded bound")
			}
		}
		for _, key := range []string{"clock", "session/0", "wait_counter", "owner", "fence", "revision"} {
			t.Run("read_"+key, func(t *testing.T) {
				s := h.open(t, "read-"+key)
				req, _ := s.NewAcquire("worker", time.Minute)
				before := localRecords(t, h.root, h.state)
				p := h.queue(t, 1001, acquireSession(req))
				release(p.before, directive{Now: 1001, Fault: object{"kind": "read", "key": key}})
				h.wait(t, p)
				expectCode(t, p.err, connect.CodeUnavailable)
				assertSame(t, localRecords(t, h.root, h.state), before)
				assertSame(t, grantSession(t, h.do(t, 1001, acquireSession(req))).Replayed, false)
			})
		}
	})
}

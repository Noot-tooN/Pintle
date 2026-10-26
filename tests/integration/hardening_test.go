//go:build (darwin || linux) && testhooks

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "pintle.local/spike/gen/go/pintle/v1"
	"pintle.local/spike/gen/go/pintle/v1/pintlev1connect"
)

type directive struct {
	Timer bool   `json:"timer,omitempty"`
	Now   uint64 `json:"now"`
	Fail  bool   `json:"fail,omitempty"`
	Fault object `json:"fault,omitempty"`
}
type gate struct {
	reached chan struct{}
	release chan directive
}
type controller struct {
	mu     sync.Mutex
	gates  map[string]*gate
	events []string
	times  map[string]uint64
	stop   chan struct{}
	server *httptest.Server
}

func newController(t *testing.T) *controller {
	c := &controller{gates: map[string]*gate{}, times: map[string]uint64{}, stop: make(chan struct{})}
	c.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/")
		c.mu.Lock()
		c.events = append(c.events, "arrive "+name)
		g := c.gates[name]
		if g != nil {
			delete(c.gates, name)
			close(g.reached)
		}
		call := strings.SplitN(name, "/", 2)[0]
		now := c.times[call]
		if now == 0 {
			now = 1000
		}
		c.mu.Unlock()
		d := directive{Now: now}
		if g != nil {
			select {
			case d = <-g.release:
			case <-c.stop:
				http.Error(w, "test stopped", 503)
				return
			case <-r.Context().Done():
				return
			}
		}
		c.mu.Lock()
		c.times[call] = d.Now
		c.events = append(c.events, fmt.Sprintf("release %s now=%d fail=%t fault=%v", name, d.Now, d.Fail, d.Fault))
		c.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(d)
	}))
	t.Cleanup(func() { close(c.stop); c.server.Close() })
	return c
}
func (c *controller) hold(call, stage string) *gate {
	c.mu.Lock()
	defer c.mu.Unlock()
	name := call + "/" + stage
	if c.gates[name] != nil {
		panic("duplicate gate " + name)
	}
	g := &gate{reached: make(chan struct{}), release: make(chan directive, 1)}
	c.gates[name] = g
	return g
}
func arrived(t *testing.T, g *gate) {
	t.Helper()
	select {
	case <-g.reached:
	case <-time.After(15 * time.Second):
		t.Fatal("checkpoint not reached (watchdog, not schedule)")
	}
}
func release(g *gate, d directive) { g.release <- d }

type pendingCall struct {
	done     chan struct{}
	response *connect.Response[v1.TryAcquireResponse]
	err      error
	op       historyOp
	cancel   context.CancelFunc
}
type hardHarness struct {
	t                          *testing.T
	control                    *controller
	runtime                    *runtime
	root, state, config, token string
	activation                 int
	sequence                   atomic.Uint64
	number                     int
	transcripts                []object
}

func (h *hardHarness) launch(id, owner string) (string, *pendingCall) {
	h.number++
	call := fmt.Sprintf("call-%d", h.number)
	return call, &pendingCall{done: make(chan struct{}), op: historyOp{ID: id, Owner: owner, Kind: "grant"}}
}
func (h *hardHarness) send(call, key string, p *pendingCall) {
	c := pintlev1connect.NewLockServiceClient(client, h.runtime.base)
	// A generous wall-clock watchdog bounds a broken harness. Logical time and
	// checkpoint releases determine every tested outcome.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	p.cancel = cancel
	p.op.Call = h.sequence.Add(1)
	go func() {
		defer close(p.done)
		defer cancel()
		req := connect.NewRequest(rpcRequest(key, p.op.ID, p.op.Owner))
		req.Header().Set("Authorization", "Bearer "+h.token)
		req.Header().Set("X-Pintle-Test-Call", call)
		p.response, p.err = c.TryAcquire(ctx, req)
		p.op.Return = h.sequence.Add(1)
	}()
}
func (h *hardHarness) finish(t *testing.T, p *pendingCall) historyOp {
	t.Helper()
	select {
	case <-p.done:
	case <-time.After(15 * time.Second):
		p.cancel()
		t.Fatal("RPC did not finish (watchdog)")
	}
	if p.err != nil {
		switch connect.CodeOf(p.err) {
		case connect.CodeAlreadyExists:
			p.op.Result.Kind = "conflict"
		case connect.CodeResourceExhausted:
			p.op.Result.Kind = "exhausted"
		case connect.CodeUnavailable, connect.CodeDeadlineExceeded, connect.CodeCanceled, connect.CodeUnknown:
			p.op.Result.Kind = "unknown"
		default:
			t.Fatalf("unexpected RPC error: %v", p.err)
		}
	} else if g := p.response.Msg.GetAcquired(); g != nil {
		fence, err := strconv.ParseUint(g.FencingToken, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		revision, err := strconv.ParseUint(g.Revision, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		assertSame(t, g.RequestId, p.op.ID)
		assertSame(t, g.HolderRef, p.op.Owner)
		p.op.Result = historyResult{Kind: "granted", Fence: fence, Revision: revision, Replayed: p.response.Msg.Replayed}
	} else if p.response.Msg.GetContended() != nil {
		p.op.Result.Kind = "contended"
	} else {
		t.Fatal("missing outcome")
	}
	return p.op
}
func (h *hardHarness) retry(t *testing.T, key, id, owner string) historyOp {
	call, p := h.launch(id, owner)
	h.send(call, key, p)
	return h.finish(t, p)
}
func (h *hardHarness) check(t *testing.T, history ...historyOp) {
	t.Helper()
	h.transcripts = append(h.transcripts, object{"test": t.Name(), "history": history})
	if !linearizable(history) {
		t.Fatalf("nonlinearizable history: %+v", history)
	}
}
func (h *hardHarness) restart() {
	h.runtime.stop()
	h.activation++
	h.runtime = start(h.t, h.root, h.state, h.config, h.activation, true, "P0B_ENABLED:true", "PINTLE_TEST_SCHEDULER:"+h.control.server.URL)
}
func TestDeterministicHardening(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	run, err := os.MkdirTemp(filepath.Join(root, "test-results"), "hardening-")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("checkpoint traces, histories and runtime logs: %s", run)
	c := newController(t)
	h := &hardHarness{t: t, control: c, root: root, state: filepath.Join(run, "state"), config: protocolConfig(t, root, run), token: token(t)}
	auth, _ := json.Marshal([]object{{"token": h.token, "tenant": "tenant-a", "subject": "alice", "namespaces": []string{"test"}}})
	vars := filepath.Join(run, ".dev.vars")
	if err := os.WriteFile(vars, []byte("PINTLE_TEST_CREDENTIALS='"+string(auth)+"'\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Remove(vars) })
	h.runtime = start(t, root, h.state, h.config, 0, true, "P0B_ENABLED:true", "PINTLE_TEST_SCHEDULER:"+c.server.URL)
	t.Cleanup(func() {
		c.mu.Lock()
		events := append([]string(nil), c.events...)
		c.mu.Unlock()
		data, _ := json.MarshalIndent(object{"checkpoints": events, "histories": h.transcripts}, "", "  ")
		if err := os.WriteFile(filepath.Join(run, "histories.json"), data, 0600); err != nil {
			t.Error(err)
		}
	})
	t.Run("all_three_request_admission_orders", func(t *testing.T) {
		orders := [][3]int{{0, 1, 2}, {0, 2, 1}, {1, 0, 2}, {1, 2, 0}, {2, 0, 1}, {2, 1, 0}}
		for _, mode := range []string{"distinct", "duplicate", "conflicting"} {
			for n, order := range orders {
				for m, replies := range orders {
					t.Run(fmt.Sprintf("%s-admit%d-reply%d", mode, n, m), func(t *testing.T) {
						key := fmt.Sprintf("schedule-%s-%d-%d", mode, n, m)
						var ps [3]*pendingCall
						var before, after [3]*gate
						for i := 0; i < 3; i++ {
							id := fmt.Sprintf("id-%d", i)
							owner := "alice"
							if mode != "distinct" {
								id = "same"
							}
							if mode == "conflicting" {
								owner = fmt.Sprintf("owner-%d", i)
							}
							call, p := h.launch(id, owner)
							ps[i] = p
							before[i] = c.hold(call, "before_commit")
							after[i] = c.hold(call, "after_sync")
							h.send(call, key, p)
						}
						for _, g := range before {
							arrived(t, g)
						} // all handlers are actually in the DO
						for _, i := range order {
							release(before[i], directive{Now: 1000})
							arrived(t, after[i])
						}
						history := make([]historyOp, 3)
						// Force return order too, with acknowledgements rather than sleeps.
						for _, i := range replies {
							release(after[i], directive{Now: 1000})
							history[i] = h.finish(t, ps[i])
						}
						assertSame(t, history[order[0]].Result.Replayed, false)
						assertSame(t, history[order[0]].Result.Kind, "granted")
						for _, i := range order[1:] {
							want := "contended"
							if mode == "duplicate" {
								want = "granted"
								assertSame(t, history[i].Result.Replayed, true)
							}
							if mode == "conflicting" {
								want = "conflict"
							}
							assertSame(t, history[i].Result.Kind, want)
						}
						h.check(t, history...)
					})
				}
			}
		}
	})
	t.Run("deadline_boundaries_without_sleep", func(t *testing.T) {
		// Generated Go client sends its watchdog timeout; adapter caps it at
		// 10000ms. Logical initial time=1000, so the exact boundary is 11000.
		for _, stage := range []string{"before_dispatch", "before_commit", "before_response", "gateway_response"} {
			for _, now := range []uint64{10999, 11000, 11001} {
				t.Run(fmt.Sprintf("%s-%d", stage, now), func(t *testing.T) {
					key := fmt.Sprintf("deadline-%s-%d", stage, now)
					call, p := h.launch("id", "alice")
					g := c.hold(call, stage)
					h.send(call, key, p)
					arrived(t, g)
					release(g, directive{Now: now})
					first := h.finish(t, p)
					committed := now < 11000 || stage == "before_response" || stage == "gateway_response"
					if now < 11000 {
						assertSame(t, first.Result.Kind, "granted")
					} else {
						expectCode(t, p.err, connect.CodeDeadlineExceeded)
					}
					retry := h.retry(t, key, "id", "alice")
					assertSame(t, retry.Result.Replayed, committed)
					h.check(t, first, retry)
				})
			}
		}
	})
	t.Run("controlled_timer_races", func(t *testing.T) {
		for _, stage := range []string{"before_commit", "after_sync"} {
			for _, timeoutWins := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s-timeout-%t", stage, timeoutWins), func(t *testing.T) {
					key := fmt.Sprintf("timer-%s-%t", stage, timeoutWins)
					call, p := h.launch("id", "alice")
					start := c.hold(call, "gateway_start")
					timer := c.hold(call, "deadline_timer")
					work := c.hold(call, stage)
					h.send(call, key, p)
					arrived(t, start)
					release(start, directive{Now: 1000, Timer: true})
					arrived(t, timer)
					arrived(t, work)
					var first historyOp
					if timeoutWins {
						release(timer, directive{Now: 11000})
						first = h.finish(t, p)
						expectCode(t, p.err, connect.CodeDeadlineExceeded)
						release(work, directive{Now: 11000})
					} else {
						release(work, directive{Now: 1000})
						first = h.finish(t, p)
						assertSame(t, first.Result.Kind, "granted")
						release(timer, directive{Now: 11000})
					}
					retry := h.retry(t, key, "id", "alice")
					assertSame(t, retry.Result.Replayed, !timeoutWins || stage == "after_sync")
					h.check(t, first, retry)
				})
			}
		}
	})
	t.Run("failure_and_rollback_matrix", func(t *testing.T) {
		for _, stage := range []string{"before_dispatch", "before_commit", "after_commit", "after_sync", "before_response", "gateway_response"} {
			t.Run(stage, func(t *testing.T) {
				key := "fail-" + stage
				call, p := h.launch("id", "alice")
				g := c.hold(call, stage)
				h.send(call, key, p)
				arrived(t, g)
				release(g, directive{Now: 1000, Fail: true})
				first := h.finish(t, p)
				expectCode(t, p.err, connect.CodeUnavailable)
				retry := h.retry(t, key, "id", "alice")
				assertSame(t, retry.Result.Replayed, stage != "before_dispatch" && stage != "before_commit")
				h.check(t, first, retry)
			})
		}
		for i, readKey := range []string{"request/id", "owner", "fence", "revision"} {
			t.Run("read-"+readKey, func(t *testing.T) {
				key := fmt.Sprintf("fail-read-%d", i)
				call, p := h.launch("id", "alice")
				g := c.hold(call, "before_commit")
				h.send(call, key, p)
				arrived(t, g)
				release(g, directive{Now: 1000, Fault: object{"kind": "read", "key": readKey}})
				first := h.finish(t, p)
				expectCode(t, p.err, connect.CodeUnavailable)
				assertSame(t, len(snapshot(t, h.runtime.base, key)), 0)
				retry := h.retry(t, key, "id", "alice")
				assertSame(t, retry.Result.Replayed, false)
				h.check(t, first, retry)
			})
		}
		for _, kind := range []string{"sql", "error"} {
			for position := 1; position <= 4; position++ {
				t.Run(fmt.Sprintf("%s-write-%d", kind, position), func(t *testing.T) {
					key := fmt.Sprintf("fail-%s-%d", kind, position)
					call, p := h.launch("id", "alice")
					g := c.hold(call, "before_commit")
					h.send(call, key, p)
					arrived(t, g)
					release(g, directive{Now: 1000, Fault: object{"kind": kind, "after_write": position}})
					first := h.finish(t, p)
					expectCode(t, p.err, connect.CodeUnavailable)
					assertSame(t, len(snapshot(t, h.runtime.base, key)), 0)
					retry := h.retry(t, key, "id", "alice")
					assertSame(t, retry.Result.Replayed, false)
					h.check(t, first, retry)
				})
			}
		}
	})
	t.Run("aborted_id_is_not_reserved", func(t *testing.T) {
		key := "aborted-reuse"
		call, p := h.launch("same", "alice")
		g := c.hold(call, "before_commit")
		h.send(call, key, p)
		arrived(t, g)
		release(g, directive{Now: 1000, Fault: object{"kind": "sql", "after_write": 4}})
		aborted := h.finish(t, p)
		expectCode(t, p.err, connect.CodeUnavailable)
		winner := h.retry(t, key, "same", "bob")
		assertSame(t, winner.Result.Replayed, false)
		conflict := h.retry(t, key, "same", "alice")
		assertSame(t, conflict.Result.Kind, "conflict")
		h.check(t, aborted, winner, conflict)
	})
	t.Run("historical_replay_after_successor", func(t *testing.T) {
		key := "historical"
		first := h.retry(t, key, "old", "alice")
		vacate := historyOp{Call: h.sequence.Add(1), Kind: "vacate", Result: historyResult{Kind: "vacated"}}
		mustCall(t, h.runtime.base, "local-a", "test", key, object{"op": "vacate"}, 200)
		vacate.Return = h.sequence.Add(1)
		successor := h.retry(t, key, "new", "bob")
		before := snapshot(t, h.runtime.base, key)
		old := h.retry(t, key, "old", "alice")
		assertSame(t, old.Result.Replayed, true)
		assertSame(t, snapshot(t, h.runtime.base, key), before)
		assertSame(t, successor.Result.Fence, uint64(2))
		h.check(t, first, vacate, successor, old)
	})
	t.Run("cancel_and_restart_at_known_boundaries", func(t *testing.T) {
		for _, stage := range []string{"before_commit", "after_sync"} {
			for _, action := range []string{"cancel", "restart"} {
				t.Run(stage+"-"+action, func(t *testing.T) {
					key := stage + "-" + action
					call, p := h.launch("id", "alice")
					g := c.hold(call, stage)
					h.send(call, key, p)
					arrived(t, g)
					if action == "restart" {
						h.runtime.stop()
					} else {
						p.cancel()
					}
					first := h.finish(t, p)
					if p.err == nil {
						t.Fatal("expected interrupted response")
					}
					// Cancelled calls can still commit. Expire a precommit request
					// explicitly; post-sync cancellation must preserve its receipt.
					if action == "cancel" {
						release(g, directive{Now: 11000})
					} else {
						release(g, directive{Now: 1000})
						h.restart()
					}
					retry := h.retry(t, key, "id", "alice")
					assertSame(t, retry.Result.Replayed, stage == "after_sync")
					h.check(t, first, retry)
				})
			}
		}
	})
	h.runtime.stop()
}

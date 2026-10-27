package hosted

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	pintle "github.com/Noot-tooN/Pintle/sdk/go/pintle"
)

const capacityRequestLimit = 600
const capacityResourceLimit = 6

type capacityWire struct {
	base                                http.RoundTripper
	slots                               chan struct{}
	attempts                            atomic.Int32
	stopped                             atomic.Bool
	mu                                  sync.Mutex
	sent, inflight, peak, networkErrors int
	statuses                            map[int]int
	colos                               map[string]int
}

func newCapacityWire(base http.RoundTripper) *capacityWire {
	return &capacityWire{base: base, slots: make(chan struct{}, 16), statuses: map[int]int{}, colos: map[string]int{}}
}
func (w *capacityWire) RoundTrip(r *http.Request) (*http.Response, error) {
	if w.stopped.Load() || w.attempts.Add(1) > capacityRequestLimit {
		return nil, errors.New("capacity run stopped or RPC budget exceeded")
	}
	select {
	case w.slots <- struct{}{}:
	case <-r.Context().Done():
		return nil, r.Context().Err()
	}
	defer func() { <-w.slots; w.mu.Lock(); w.inflight--; w.mu.Unlock() }()
	w.mu.Lock()
	w.sent++
	w.inflight++
	w.peak = max(w.peak, w.inflight)
	w.mu.Unlock()
	res, err := w.base.RoundTrip(r)
	if err == nil {
		body, e := io.ReadAll(io.LimitReader(res.Body, 16385))
		res.Body.Close()
		if e != nil {
			err = e
		} else if len(body) > 16384 {
			err = errors.New("capacity response too large")
		} else {
			res.Body = io.NopCloser(bytes.NewReader(body))
		}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if err != nil {
		w.networkErrors++
		return nil, err
	}
	w.statuses[res.StatusCode]++
	if parts := strings.Split(res.Header.Get("CF-Ray"), "-"); len(parts) == 2 && len(parts[1]) == 3 {
		w.colos[parts[1]]++
	}
	if res.StatusCode >= 500 {
		w.stopped.Store(true)
	}
	return res, nil
}

type CapacityReport struct {
	StartedAt     string                    `json:"started_at"`
	FinishedAt    string                    `json:"finished_at"`
	Attempts      int32                     `json:"rpc_attempts"`
	Sent          int                       `json:"rpc_sent"`
	Peak          int                       `json:"peak_inflight"`
	Resources     int                       `json:"resources"`
	NetworkErrors int                       `json:"network_errors"`
	Statuses      map[int]int               `json:"http_statuses"`
	Colos         map[string]int            `json:"gateway_colos"`
	Samples       map[string]latencySummary `json:"latency"`
	Passed        bool                      `json:"passed"`
}
type latencySummary struct {
	Samples int     `json:"samples"`
	P50     float64 `json:"p50_ms"`
	P95     float64 `json:"p95_ms"`
	P99     float64 `json:"p99_ms"`
	Max     float64 `json:"max_ms"`
}

func summarize(samples []float64) latencySummary {
	if len(samples) == 0 {
		return latencySummary{}
	}
	sorted := append([]float64(nil), samples...)
	sort.Float64s(sorted)
	q := func(p float64) float64 { return sorted[int(math.Ceil(float64(len(sorted))*p))-1] }
	return latencySummary{len(sorted), q(.5), q(.95), q(.99), sorted[len(sorted)-1]}
}

type renewalFault struct {
	client   *http.Client
	mode     string
	ack      chan struct{}
	injected chan struct{}
	hit      atomic.Bool
}

func (f *renewalFault) Do(r *http.Request) (*http.Response, error) {
	res, err := f.client.Do(r)
	if err != nil {
		return nil, err
	}
	if strings.HasSuffix(r.URL.Path, "/Renew") && res.StatusCode == 200 && f.mode != "normal" && f.hit.CompareAndSwap(false, true) {
		res.Body.Close()
		if f.injected != nil {
			close(f.injected)
		}
		if f.mode == "delay" {
			<-r.Context().Done()
			return nil, r.Context().Err()
		}
		return nil, errors.New("test discarded committed renewal reply")
	}
	if strings.HasSuffix(r.URL.Path, "/AcknowledgeSession") && res.StatusCode == 200 {
		select {
		case f.ack <- struct{}{}:
		default:
		}
	}
	return res, nil
}

// RunCapacity is shared with local Wrangler. Barriers observe admission/ACK/Lost;
// concurrent winners are never prescribed. Latency is measured, not asserted.
func RunCapacity(t *testing.T, endpoint, token string, save func(CapacityReport)) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	w := newCapacityWire(http.DefaultTransport)
	httpClient := &http.Client{Transport: w, Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	c, e := pintle.New(endpoint, token, httpClient)
	require(t, e)
	id, e := pintle.NewRequestID()
	require(t, e)
	report := CapacityReport{StartedAt: time.Now().UTC().Format(time.RFC3339Nano), Samples: map[string]latencySummary{}}
	defer func() {
		w.stopped.Store(true)
		cancel()
		w.mu.Lock()
		report.Attempts = w.attempts.Load()
		report.Sent = w.sent
		report.Peak = w.peak
		report.NetworkErrors = w.networkErrors
		report.Statuses = make(map[int]int)
		for k, v := range w.statuses {
			report.Statuses[k] = v
		}
		report.Colos = make(map[string]int)
		for k, v := range w.colos {
			report.Colos[k] = v
		}
		w.mu.Unlock()
		report.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
		report.Passed = !t.Failed()
		save(report)
	}()
	resource := func(name string) pintle.Resource {
		report.Resources++
		if report.Resources > capacityResourceLimit {
			t.Fatal("resource budget exceeded")
		}
		return pintle.Resource{Namespace: "staging", Key: "capacity-" + id + "-" + name}
	}
	open := func(r pintle.Resource) *pintle.Session { s, e := c.OpenSession(ctx, r); require(t, e); return s }
	grant := func(s *pintle.Session) pintle.Lease {
		r, e := s.NewAcquire("owner", time.Minute)
		require(t, e)
		g, e := c.TryAcquire(ctx, r)
		require(t, e)
		return *active(t, g)
	}
	release := func(s *pintle.Session, l pintle.Lease) {
		r, e := s.NewRelease(l)
		require(t, e)
		_, e = c.Release(ctx, r)
		require(t, e)
	}

	t.Log("resilience: cancel a held HTTP request after observed durable Pending admission")
	s := open(resource("disconnect"))
	owner := grant(s)
	wait, e := s.NewWaitAcquire("disconnected", time.Minute, time.Minute)
	require(t, e)
	waitCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { _, err := c.ResumeAcquire(waitCtx, wait); done <- err }()
	admitted := false
	for i := 0; i < 20; i++ {
		st, err := c.GetAttemptStatus(ctx, wait.Acquisition.Resource, wait.Acquisition.RequestID, wait.Acquisition.Attempt)
		if err == nil {
			if st.Waiting != pintle.Pending {
				t.Fatal("waiter granted before release")
			}
			admitted = true
			break
		}
		code(t, err, connect.CodeNotFound)
		select {
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if !admitted {
		t.Fatal("Pending admission not observed")
	}
	stop()
	select {
	case err := <-done:
		code(t, err, connect.CodeCanceled)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	st, e := c.GetAttemptStatus(ctx, wait.Acquisition.Resource, wait.Acquisition.RequestID, wait.Acquisition.Attempt)
	require(t, e)
	if st.Waiting != pintle.Pending {
		t.Fatal("disconnect cancelled durable intent")
	}
	release(s, owner)
	hand, e := c.ResumeAcquire(ctx, wait)
	require(t, e)
	next := *active(t, hand.Granted)
	greater(t, next.FencingToken, owner.FencingToken)
	again, e := c.ResumeAcquire(ctx, wait)
	require(t, e)
	sameReceipt(t, hand.Granted.Receipt, again.Granted.Receipt)
	release(s, next)

	r := resource("maintenance")
	for _, mode := range []string{"normal", "drop", "delay"} {
		t.Logf("maintenance: %s renewal response", mode)
		s := open(r)
		owner := grant(s)
		fault := &renewalFault{client: httpClient, mode: mode, ack: make(chan struct{}, 1)}
		managed, e := pintle.New(endpoint, token, fault)
		require(t, e)
		m, e := managed.Maintain(ctx, owner, 3*time.Second)
		require(t, e)
		// Stop in every failure path; no renewal goroutine outlives the request budget.
		func() {
			defer m.Stop()
			if mode == "normal" {
				select {
				case <-fault.ack:
				case <-m.Lost():
					t.Fatal("maintenance lost before ACK")
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			} else {
				select {
				case <-m.Lost():
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				if !fault.hit.Load() || m.Err() == nil || m.Recovery() == nil {
					t.Fatal("uncertain renewal lost its recovery envelope")
				}
				recovery := m.Recovery()
				status, e := c.GetAttemptStatus(ctx, r, recovery.RequestID, recovery.Attempt)
				require(t, e)
				active(t, status.Renewed)
				replay, e := c.Renew(ctx, *recovery)
				require(t, e)
				sameReceipt(t, status.Renewed.Receipt, replay.Receipt)
				if replay.Receipt.FencingToken != owner.FencingToken {
					t.Fatal("renewal advanced fence")
				}
				require(t, c.AcknowledgeAttempt(ctx, r, *recovery.Attempt))
			}
		}()
		select {
		case <-m.Lost():
		default:
			t.Fatal("Stop did not close Lost")
		}
		competitor, e := s.NewAcquire("competitor", time.Second)
		require(t, e)
		busy, e := c.TryAcquire(ctx, competitor)
		require(t, e)
		if !busy.Contended {
			t.Fatal("Stop/Lost implicitly released ownership")
		}
		release(s, owner)
	}

	t.Log("WAN samples: sequential grants and hot-resource contention at 1, 4, 8 clients")
	s = open(resource("grants"))
	samples := []float64{}
	for i := 0; i < 32; i++ {
		req, e := s.NewAcquire("sample", time.Minute)
		require(t, e)
		at := time.Now()
		g, e := c.TryAcquire(ctx, req)
		samples = append(samples, float64(time.Since(at).Microseconds())/1000)
		require(t, e)
		active(t, g)
		rel, e := s.NewRelease(*g.Current)
		require(t, e)
		_, e = c.Release(ctx, rel)
		require(t, e)
		require(t, s.Acknowledge(ctx, rel.Attempt.Sequence))
	}
	report.Samples["uncontended_grant"] = summarize(samples)
	s = open(resource("hot"))
	owner = grant(s)
	for _, n := range []int{1, 4, 8} {
		renew, e := s.NewRenew(owner, time.Minute)
		require(t, e)
		g, e := c.Renew(ctx, renew)
		require(t, e)
		owner = *active(t, g)
		samples = nil
		for batch := 0; batch < 12; batch++ {
			requests := make([]pintle.AcquireRequest, n)
			for i := range requests {
				requests[i], e = s.NewAcquire("contender", time.Second)
				require(t, e)
			}
			type outcome struct {
				g   *pintle.GrantResult
				err error
				ms  float64
			}
			results := make(chan outcome, n)
			start := make(chan struct{})
			for _, req := range requests {
				go func() {
					<-start
					at := time.Now()
					g, e := c.TryAcquire(ctx, req)
					results <- outcome{g, e, float64(time.Since(at).Microseconds()) / 1000}
				}()
			}
			close(start)
			for range requests {
				o := <-results
				require(t, o.err)
				if !o.g.Contended {
					t.Fatal("contender became owner")
				}
				samples = append(samples, o.ms)
			}
			require(t, s.Acknowledge(ctx, requests[len(requests)-1].Attempt.Sequence))
		}
		report.Samples[fmt.Sprintf("contended_%d_clients", n)] = summarize(samples)
	}
	release(s, owner)

	t.Log("capacity: 16 durable pending waiters; overflow rejects and cancellation restores capacity")
	r = resource("waiters")
	s = open(r)
	owner = grant(s)
	ws := open(r)
	waits := make([]pintle.WaitRequest, 16)
	for i := range waits {
		waits[i], e = ws.NewWaitAcquire("idle", time.Minute, time.Minute)
		require(t, e)
	}
	type waited struct {
		r   *pintle.WaitResult
		err error
		ms  float64
	}
	waitedResults := make(chan waited, 16)
	start := make(chan struct{})
	for _, req := range waits {
		go func() {
			<-start
			at := time.Now()
			v, e := c.ResumeAcquire(ctx, req)
			waitedResults <- waited{v, e, float64(time.Since(at).Microseconds()) / 1000}
		}()
	}
	close(start)
	samples = nil
	for range waits {
		o := <-waitedResults
		require(t, o.err)
		if o.r.State != pintle.Pending {
			t.Fatal("expected bounded Pending response")
		}
		samples = append(samples, o.ms)
	}
	report.Samples["idle_pending_16_clients"] = summarize(samples)
	overflow, e := ws.NewWaitAcquire("overflow", time.Minute, time.Minute)
	require(t, e)
	_, e = c.ResumeAcquire(ctx, overflow)
	code(t, e, connect.CodeResourceExhausted)
	cancelled, e := c.CancelPending(ctx, waits[0])
	require(t, e)
	if cancelled.State != pintle.Cancelled {
		t.Fatal("cancel failed")
	}
	admittedResult, e := c.ResumeAcquire(ctx, overflow)
	require(t, e)
	if admittedResult.State != pintle.Pending {
		t.Fatal("capacity did not recover")
	}
	waits[0] = overflow
	cancelledResults := make(chan waited, 16)
	for _, req := range waits {
		go func() { v, e := c.CancelPending(ctx, req); cancelledResults <- waited{r: v, err: e} }()
	}
	for range waits {
		o := <-cancelledResults
		require(t, o.err)
		if o.r.State != pintle.Cancelled {
			t.Fatal("waiter unexpectedly granted during cleanup")
		}
	}
	release(s, owner)

	t.Log("capacity: 15 normal sessions plus reserved owner recovery slot, then explicit exhaustion")
	r = resource("sessions")
	var first *pintle.Session
	for i := 0; i < 15; i++ {
		s = open(r)
		if i == 0 {
			first = s
		}
	}
	_, e = c.OpenSession(ctx, r)
	code(t, e, connect.CodeResourceExhausted)
	owner = grant(first)
	recovery := open(r)
	_, e = c.OpenSession(ctx, r)
	code(t, e, connect.CodeResourceExhausted)
	release(recovery, owner)
	t.Log("PASS: bounded resilience and capacity scenarios; all known live owners released and waiters cancelled")
}

func CapacityJSON(r CapacityReport) []byte { b, _ := json.MarshalIndent(r, "", "  "); return b }

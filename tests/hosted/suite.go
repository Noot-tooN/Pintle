// Package hosted contains bounded public-API smoke checks shared by local
// Wrangler integration and the explicitly enabled hosted staging test.
package hosted

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	pintle "github.com/Noot-tooN/Pintle/sdk/go/pintle"
)

func validateTarget(endpoint, token string) error {
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "https" || !regexp.MustCompile(`^pintle-staging\.[a-z0-9][a-z0-9-]*\.workers\.dev$`).MatchString(u.Host) || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return errors.New("endpoint must be the HTTPS pintle-staging.<account-subdomain>.workers.dev origin")
	}
	if !regexp.MustCompile(`^[A-Za-z0-9]{32,256}$`).MatchString(token) {
		return errors.New("staging token must contain 32..256 ASCII alphanumeric characters")
	}
	return nil
}

type transport struct {
	calls           atomic.Int32
	dropNextAcquire atomic.Bool
}

func (b *transport) RoundTrip(r *http.Request) (*http.Response, error) {
	if b.calls.Add(1) > 96 {
		return nil, errors.New("hosted smoke request budget exceeded")
	}
	res, err := http.DefaultTransport.RoundTrip(r)
	if err == nil && r.URL.Path == "/pintle.v1.LockService/TryAcquire" && res.StatusCode == 200 && b.dropNextAcquire.CompareAndSwap(true, false) {
		// Consume the real committed reply, then hide it from the SDK. No server hook.
		_, readErr := io.Copy(io.Discard, io.LimitReader(res.Body, 16385))
		res.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		return nil, errors.New("test discarded committed response")
	}
	return res, err
}
func require(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func code(t *testing.T, err error, want connect.Code) {
	t.Helper()
	if err == nil || connect.CodeOf(err) != want {
		t.Fatalf("expected %s, got %v", want, err)
	}
}
func active(t *testing.T, g *pintle.GrantResult) *pintle.Lease {
	t.Helper()
	if g == nil || g.Receipt == nil || g.Current == nil || g.State != pintle.Active || g.Contended {
		t.Fatal("expected active lease")
	}
	return g.Current
}
func sameReceipt(t *testing.T, a, b *pintle.Lease) {
	t.Helper()
	if a == nil || b == nil || *a != *b {
		t.Fatal("immutable receipt changed")
	}
}
func greater(t *testing.T, a, b string) {
	t.Helper()
	x, e := strconv.ParseUint(a, 10, 64)
	require(t, e)
	y, e := strconv.ParseUint(b, 10, 64)
	require(t, e)
	if x <= y {
		t.Fatal("fencing token did not advance")
	}
}

// Run only targets explicitly provided by its caller. TestStaging validates a
// staging HTTPS origin; the local integration caller supplies its own loopback
// runtime. Five unique resource names prevent cross-run interference. This is
// bounded smoke evidence, not controlled scheduling or hosted failover proof.
func Run(t *testing.T, endpoint, token string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	wire := &transport{}
	httpClient := &http.Client{Transport: wire, Timeout: 12 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	c, err := pintle.New(endpoint, token, httpClient)
	require(t, err)
	runID, err := pintle.NewRequestID()
	require(t, err)
	open := func(t *testing.T, name string) *pintle.Session {
		t.Helper()
		s, err := c.OpenSession(ctx, pintle.Resource{Namespace: "staging", Key: "smoke-" + runID + "-" + name})
		require(t, err)
		return s
	}
	releaseLease := func(t *testing.T, s *pintle.Session, l pintle.Lease) {
		t.Helper()
		r, err := s.NewRelease(l)
		require(t, err)
		_, err = c.Release(ctx, r)
		require(t, err)
	}
	started := time.Now()
	defer func() {
		t.Logf("public-API smoke: %d transport attempts, elapsed %s; at most five new resource names", wire.calls.Load(), time.Since(started))
	}()

	t.Run("authentication", func(t *testing.T) {
		bad, err := pintle.New(endpoint, "invalid", httpClient)
		require(t, err)
		_, err = bad.OpenSession(ctx, pintle.Resource{Namespace: "staging", Key: "unauthorized"})
		code(t, err, connect.CodeUnauthenticated)
	})
	t.Run("lifecycle_replay_and_ack", func(t *testing.T) {
		s := open(t, "lifecycle")
		req, err := s.NewAcquire("owner", time.Minute)
		require(t, err)
		g, err := c.TryAcquire(ctx, req)
		require(t, err)
		owner := *active(t, g)
		replay, err := c.TryAcquire(ctx, req)
		require(t, err)
		sameReceipt(t, g.Receipt, replay.Receipt)
		if !replay.Replayed {
			t.Fatal("acquisition retry was not replayed")
		}
		changed := req
		changed.HolderRef = "changed"
		_, err = c.TryAcquire(ctx, changed)
		code(t, err, connect.CodeAlreadyExists)
		other, err := s.NewAcquire("contender", time.Minute)
		require(t, err)
		busy, err := c.TryAcquire(ctx, other)
		require(t, err)
		if !busy.Contended {
			t.Fatal("second owner granted")
		}
		renew, err := s.NewRenew(owner, time.Minute)
		require(t, err)
		renewed, err := c.Renew(ctx, renew)
		require(t, err)
		active(t, renewed)
		if renewed.Receipt.FencingToken != owner.FencingToken {
			t.Fatal("renew advanced fence")
		}
		renewedAgain, err := c.Renew(ctx, renew)
		require(t, err)
		sameReceipt(t, renewed.Receipt, renewedAgain.Receipt)
		if !renewedAgain.Replayed {
			t.Fatal("renew retry was not replayed")
		}
		rel, err := s.NewRelease(*renewed.Current)
		require(t, err)
		released, err := c.Release(ctx, rel)
		require(t, err)
		releaseReplay, err := c.Release(ctx, rel)
		require(t, err)
		if !releaseReplay.Replayed || releaseReplay.Revision != released.Revision {
			t.Fatal("release replay changed")
		}
		next, err := s.NewAcquire("successor", time.Minute)
		require(t, err)
		successor, err := c.TryAcquire(ctx, next)
		require(t, err)
		active(t, successor)
		greater(t, successor.Receipt.FencingToken, owner.FencingToken)
		stale, err := s.NewRenew(owner, time.Minute)
		require(t, err)
		_, err = c.Renew(ctx, stale)
		code(t, err, connect.CodePermissionDenied)
		old, err := s.Status(ctx, req.RequestID, req.Attempt)
		require(t, err)
		sameReceipt(t, old.Acquired.Receipt, g.Receipt)
		if old.Acquired.Current != nil {
			t.Fatal("historical receipt authorized current ownership")
		}
		// Every sequence through the stale renewal has a terminal result.
		require(t, s.Acknowledge(ctx, stale.Attempt.Sequence))
		_, err = s.Status(ctx, req.RequestID, req.Attempt)
		code(t, err, connect.CodeFailedPrecondition)
		releaseLease(t, s, *successor.Current)
	})
	t.Run("concurrent_acquisitions_and_duplicates", func(t *testing.T) {
		s := open(t, "concurrent")
		requests := make([]pintle.AcquireRequest, 4)
		for i := range requests {
			requests[i], err = s.NewAcquire(fmt.Sprint("worker-", i), time.Minute)
			require(t, err)
		}
		type result struct {
			index int
			grant *pintle.GrantResult
			err   error
		}
		race := func(reqs []pintle.AcquireRequest) []result {
			start := make(chan struct{})
			done := make(chan result, len(reqs))
			for i, req := range reqs {
				go func() { <-start; g, e := c.TryAcquire(ctx, req); done <- result{i, g, e} }()
			}
			close(start)
			all := make([]result, 0, len(reqs))
			for range reqs {
				all = append(all, <-done)
			}
			return all
		}
		winner := -1
		var grant *pintle.GrantResult
		for _, r := range race(requests) {
			require(t, r.err)
			if !r.grant.Contended {
				active(t, r.grant)
				if winner != -1 {
					t.Fatal("multiple grants")
				}
				winner = r.index
				grant = r.grant
			}
		}
		if winner < 0 {
			t.Fatal("no grant")
		}
		duplicate := []pintle.AcquireRequest{requests[winner], requests[winner], requests[winner], requests[winner]}
		for _, r := range race(duplicate) {
			require(t, r.err)
			sameReceipt(t, grant.Receipt, r.grant.Receipt)
			if !r.grant.Replayed {
				t.Fatal("duplicate was executed again")
			}
		}
		releaseLease(t, s, *grant.Current)
	})
	t.Run("pending_cancellation_and_handoff", func(t *testing.T) {
		s := open(t, "waiting")
		req, err := s.NewAcquire("owner", time.Minute)
		require(t, err)
		g, err := c.TryAcquire(ctx, req)
		require(t, err)
		owner := *active(t, g)
		cancelled, err := s.NewWaitAcquire("cancelled", time.Minute, time.Minute)
		require(t, err)
		// A full bounded response confirms registration without guessing admission time.
		pending, err := c.ResumeAcquire(ctx, cancelled)
		require(t, err)
		if pending.State != pintle.Pending {
			t.Fatal("expected retained pending intent")
		}
		gone, err := c.CancelPending(ctx, cancelled)
		require(t, err)
		if gone.State != pintle.Cancelled {
			t.Fatal("pending cancellation failed")
		}
		gone, err = c.ResumeAcquire(ctx, cancelled)
		require(t, err)
		if gone.State != pintle.Cancelled {
			t.Fatal("cancelled intent rejoined queue")
		}
		wait, err := s.NewWaitAcquire("waiter", time.Minute, time.Minute)
		require(t, err)
		type outcome struct {
			result *pintle.WaitResult
			err    error
		}
		done := make(chan outcome, 1)
		waitCtx, stop := context.WithCancel(ctx)
		defer stop()
		go func() { r, e := c.Acquire(waitCtx, wait); done <- outcome{r, e} }()
		// Public status is the admission barrier. Polling is test observation only;
		// the SDK's Acquire still uses server-owned waits, never TryAcquire loops.
		admitted := false
		for i := 0; i < 20; i++ {
			state, e := s.Status(ctx, wait.Acquisition.RequestID, wait.Acquisition.Attempt)
			if e == nil {
				if state.Waiting != pintle.Pending {
					t.Fatal("waiter granted before owner released")
				}
				admitted = true
				break
			}
			if connect.CodeOf(e) != connect.CodeNotFound {
				t.Fatal(e)
			}
			timer := time.NewTimer(100 * time.Millisecond)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				t.Fatal(ctx.Err())
			}
		}
		if !admitted {
			t.Fatal("waiter registration not observed within bounded attempts")
		}
		releaseLease(t, s, owner)
		select {
		case out := <-done:
			require(t, out.err)
			active(t, out.result.Granted)
			greater(t, out.result.Granted.Receipt.FencingToken, owner.FencingToken)
			replay, e := c.ResumeAcquire(ctx, wait)
			require(t, e)
			sameReceipt(t, replay.Granted.Receipt, out.result.Granted.Receipt)
			releaseLease(t, s, *out.result.Granted.Current)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	})
	t.Run("committed_response_loss", func(t *testing.T) {
		s := open(t, "response-loss")
		req, err := s.NewAcquire("owner", time.Minute)
		require(t, err)
		wire.dropNextAcquire.Store(true)
		_, err = c.TryAcquire(ctx, req)
		if err == nil || wire.dropNextAcquire.Load() {
			t.Fatal("did not discard successful upstream response")
		}
		status, err := s.Status(ctx, req.RequestID, req.Attempt)
		require(t, err)
		active(t, status.Acquired)
		retry, err := c.TryAcquire(ctx, req)
		require(t, err)
		sameReceipt(t, status.Acquired.Receipt, retry.Receipt)
		if !retry.Replayed {
			t.Fatal("unknown commit retry granted anew")
		}
		releaseLease(t, s, *retry.Current)
	})
	t.Run("expiry_handoff", func(t *testing.T) {
		s := open(t, "expiry")
		req, err := s.NewAcquire("owner", time.Second)
		require(t, err)
		g, err := c.TryAcquire(ctx, req)
		require(t, err)
		owner := *active(t, g)
		wait, err := s.NewWaitAcquire("successor", time.Minute, 10*time.Second)
		require(t, err)
		got, err := c.Acquire(ctx, wait)
		require(t, err)
		active(t, got.Granted)
		greater(t, got.Granted.Receipt.FencingToken, owner.FencingToken)
		releaseLease(t, s, *got.Granted.Current)
	})
}

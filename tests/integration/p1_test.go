//go:build darwin || linux

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	pintle "pintle.local/spike/sdk/go/pintle"
)

func p1setup(t *testing.T, variables ...string) (string, string, string, string, string, *runtime) {
	return leaseSetup(t, "P1_ENABLED:true", variables...)
}
func leaseSetup(t *testing.T, mode string, variables ...string) (string, string, string, string, string, *runtime) {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	run, err := os.MkdirTemp(filepath.Join(root, "test-results"), "p1-")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("P1 runtime state: %s", run)
	a, b := token(t), token(t)
	auth, _ := json.Marshal([]object{{"token": a, "tenant": "tenant-a", "subject": "alice", "namespaces": []string{"test"}}, {"token": b, "tenant": "tenant-a", "subject": "bob", "namespaces": []string{"test"}}})
	vars := filepath.Join(run, ".dev.vars")
	if err := os.WriteFile(vars, []byte("PINTLE_TEST_CREDENTIALS='"+string(auth)+"'\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Remove(vars) })
	config := protocolConfig(t, root, run)
	state := filepath.Join(run, "state")
	r := start(t, root, state, config, 0, false, append([]string{mode}, variables...)...)
	return root, state, config, a, b, r
}
func sdk(t *testing.T, base, credential string, h connect.HTTPClient) *pintle.Client {
	t.Helper()
	c, err := pintle.New(base, credential, h)
	if err != nil {
		t.Fatal(err)
	}
	return c
}
func acq(key, id string, ttl time.Duration) pintle.AcquireRequest {
	return pintle.AcquireRequest{Resource: pintle.Resource{Namespace: "test", Key: key}, RequestID: id, HolderRef: "worker", TTL: ttl}
}
func requireLease(t *testing.T, g *pintle.GrantResult, err error) *pintle.Lease {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	if g == nil || g.Receipt == nil || g.Current == nil || g.State != pintle.Active {
		t.Fatalf("not an active grant: state/result %v", g)
	}
	return g.Current
}
func TestP1SDK(t *testing.T) {
	root, state, config, a, b, r := p1setup(t)
	c := sdk(t, r.base, a, client)
	other := sdk(t, r.base, b, client)
	ctx := context.Background()
	t.Run("lifecycle_and_immutable_recovery", func(t *testing.T) {
		request, err := pintle.NewAcquire(pintle.Resource{Namespace: "test", Key: "lifecycle"}, "worker", 30*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		first, err := c.TryAcquire(ctx, request)
		l := requireLease(t, first, err)
		assertSame(t, l.FencingToken, "1")
		renewedRequest, err := pintle.NewRenew(*l, 60*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		result, err := c.Renew(ctx, renewedRequest)
		updated := requireLease(t, result, err)
		assertSame(t, updated.FencingToken, "1")
		assertSame(t, updated.Revision, "2")
		replay, err := c.TryAcquire(ctx, request)
		requireLease(t, replay, err)
		assertSame(t, replay.Replayed, true)
		assertSame(t, *replay.Receipt, *first.Receipt)
		assertSame(t, *replay.Current, *updated)
		_, err = other.Renew(ctx, renewedRequest)
		expectCode(t, err, connect.CodePermissionDenied)
		wrong := renewedRequest
		wrong.RequestID = "wrong"
		wrong.Lease.LeaseID = token(t)
		_, err = c.Renew(ctx, wrong)
		expectCode(t, err, connect.CodePermissionDenied)
		releaseRequest, err := pintle.NewRelease(*updated)
		if err != nil {
			t.Fatal(err)
		}
		released, err := c.Release(ctx, releaseRequest)
		if err != nil {
			t.Fatal(err)
		}
		assertSame(t, released.Revision, "3")
		status, err := c.GetRequestStatus(ctx, request.Resource, request.RequestID)
		if err != nil {
			t.Fatal(err)
		}
		assertSame(t, status.Acquired.State, pintle.Released)
		assertSame(t, status.Acquired.Current, (*pintle.Lease)(nil))
		next, err := other.TryAcquire(ctx, acq("lifecycle", "successor", 30*time.Second))
		successor := requireLease(t, next, err)
		assertSame(t, successor.FencingToken, "2")
		again, err := c.Release(ctx, releaseRequest)
		if err != nil {
			t.Fatal(err)
		}
		assertSame(t, again.Replayed, true)
		assertSame(t, again.Revision, "3")
		oldRenew := renewedRequest
		oldRenew.RequestID = "stale"
		_, err = c.Renew(ctx, oldRenew)
		expectCode(t, err, connect.CodePermissionDenied)
		status, err = c.GetRequestStatus(ctx, request.Resource, request.RequestID)
		if err != nil {
			t.Fatal(err)
		}
		assertSame(t, status.Acquired.State, pintle.Superseded)
		_, err = other.GetRequestStatus(ctx, request.Resource, request.RequestID)
		expectCode(t, err, connect.CodePermissionDenied)
		_, err = c.GetRequestStatus(ctx, request.Resource, "unknown")
		expectCode(t, err, connect.CodeNotFound)
	})
	t.Run("concurrent_sdk_requests", func(t *testing.T) {
		var wg sync.WaitGroup
		gate := make(chan struct{})
		results := make(chan *pintle.GrantResult, 24)
		errors := make(chan error, 24)
		request := acq("sdk-retries", "same", 30*time.Second)
		for i := 0; i < 24; i++ {
			wg.Add(1)
			go func() { defer wg.Done(); <-gate; res, err := c.TryAcquire(ctx, request); results <- res; errors <- err }()
		}
		close(gate)
		wg.Wait()
		close(results)
		close(errors)
		for err := range errors {
			if err != nil {
				t.Fatal(err)
			}
		}
		fresh := 0
		var capability string
		for res := range results {
			l := requireLease(t, res, nil)
			if capability == "" {
				capability = l.LeaseID
			}
			assertSame(t, l.LeaseID, capability)
			assertSame(t, l.FencingToken, "1")
			if !res.Replayed {
				fresh++
			}
		}
		assertSame(t, fresh, 1)
	})
	t.Run("distinct_request_contention", func(t *testing.T) {
		var wg sync.WaitGroup
		gate := make(chan struct{})
		type result struct {
			grant *pintle.GrantResult
			err   error
		}
		results := make(chan result, 24)
		for i := 0; i < 24; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-gate
				g, err := c.TryAcquire(ctx, acq("sdk-contention", fmt.Sprintf("attempt-%d", i), 60*time.Second))
				results <- result{g, err}
			}(i)
		}
		close(gate)
		wg.Wait()
		close(results)
		winners, contended := 0, 0
		for result := range results {
			if result.err != nil {
				t.Fatal(result.err)
			}
			if result.grant.Contended {
				contended++
				continue
			}
			l := requireLease(t, result.grant, nil)
			assertSame(t, l.FencingToken, "1")
			assertSame(t, result.grant.Replayed, false)
			winners++
		}
		assertSame(t, winners, 1)
		assertSame(t, contended, 23)
	})
	t.Run("lost_response_then_status_and_restart", func(t *testing.T) {
		observed := make(chan struct{}, 1)
		proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			upstream, err := http.NewRequestWithContext(req.Context(), req.Method, r.base+req.URL.Path, req.Body)
			if err != nil {
				http.Error(w, "request", 500)
				return
			}
			upstream.Header = req.Header.Clone()
			res, err := client.Do(upstream)
			if err != nil {
				http.Error(w, "upstream", 500)
				return
			}
			defer res.Body.Close()
			_, err = io.Copy(io.Discard, res.Body)
			if err != nil || res.StatusCode != 200 {
				http.Error(w, "upstream response", 500)
				return
			}
			observed <- struct{}{}
			conn, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				conn.Close()
			}
		}))
		defer proxy.Close()
		lost := sdk(t, proxy.URL, a, client)
		request := acq("lost", "lost-id", 60*time.Second)
		_, err := lost.TryAcquire(ctx, request)
		if err == nil {
			t.Fatal("expected lost response")
		}
		select {
		case <-observed:
		case <-time.After(5 * time.Second):
			t.Fatal("upstream did not acknowledge")
		}
		before, err := c.GetRequestStatus(ctx, request.Resource, request.RequestID)
		if err != nil {
			t.Fatal(err)
		}
		r.stop()
		r = start(t, root, state, config, 1, false, "P1_ENABLED:true")
		c = sdk(t, r.base, a, client)
		replay, err := c.TryAcquire(ctx, request)
		requireLease(t, replay, err)
		assertSame(t, replay.Replayed, true)
		assertSame(t, *replay.Receipt, *before.Acquired.Receipt)
		_, err = c.Release(ctx, pintle.ReleaseRequest{Lease: *replay.Current, RequestID: "cleanup"})
		if err != nil {
			t.Fatal(err)
		}
	})
	r.stop()
}

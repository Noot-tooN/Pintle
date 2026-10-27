package hosted

import (
	"context"
	"net/http"
	"testing"
	"time"

	"connectrpc.com/connect"
	pintle "github.com/Noot-tooN/Pintle/sdk/go/pintle"
)

// LifecycleControl changes only the runtime/configuration around the same
// authority history. Local integration supplies process restarts; staging uses
// the guarded Just recipes. The returned URL allows a new local runtime port.
type LifecycleControl struct {
	Replace func() string
	Rotate  func(newOnly bool) string
	// Optional observational barrier for eventually propagated hosted deployment.
	Replaced func() bool
}

type LifecycleState struct {
	Client  *pintle.Client
	Session *pintle.Session
	Request pintle.AcquireRequest
}

// RunLifecycle exercises one resource with immutable attempts across replacement
// and key rotation. No schedule depends on which concurrent request wins.
func RunLifecycle(t *testing.T, endpoint, oldKey, newKey string, httpClient *http.Client, ctl LifecycleControl) LifecycleState {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	makeClient := func(key string) *pintle.Client {
		c, e := pintle.New(endpoint, key, httpClient)
		require(t, e)
		return c
	}
	old := makeClient(oldKey)
	id, e := pintle.NewRequestID()
	require(t, e)
	s, e := old.OpenSession(ctx, pintle.Resource{Namespace: "staging", Key: "lifecycle-" + id})
	require(t, e)
	acquire := func(c *pintle.Client, holder string) (pintle.AcquireRequest, *pintle.GrantResult) {
		r, e := s.NewAcquire(holder, time.Minute)
		require(t, e)
		g, e := c.TryAcquire(ctx, r)
		require(t, e)
		active(t, g)
		return r, g
	}
	release := func(c *pintle.Client, l pintle.Lease) {
		r, e := s.NewRelease(l)
		require(t, e)
		_, e = c.Release(ctx, r)
		require(t, e)
	}
	pending := func(c *pintle.Client, holder string) pintle.WaitRequest {
		r, e := s.NewWaitAcquire(holder, time.Minute, time.Minute)
		require(t, e)
		w, e := c.ResumeAcquire(ctx, r)
		require(t, e)
		if w.State != pintle.Pending {
			t.Fatal("expected observed Pending before runtime/configuration change")
		}
		return r
	}
	verifyPending := func(c *pintle.Client, r pintle.WaitRequest) {
		st, e := c.GetAttemptStatus(ctx, r.Acquisition.Resource, r.Acquisition.RequestID, r.Acquisition.Attempt)
		require(t, e)
		if st.Waiting != pintle.Pending {
			t.Fatal("admitted pending intent did not survive replacement/rotation")
		}
	}
	handoff := func(c *pintle.Client, r pintle.WaitRequest, prior pintle.Lease) pintle.Lease {
		release(c, prior)
		w, e := c.ResumeAcquire(ctx, r)
		require(t, e)
		l := *active(t, w.Granted)
		greater(t, l.FencingToken, prior.FencingToken)
		replay, e := c.ResumeAcquire(ctx, r)
		require(t, e)
		sameReceipt(t, w.Granted.Receipt, replay.Granted.Receipt)
		release(c, l)
		return l
	}

	t.Log("replacement: preserve owner, immutable receipt and admitted FIFO waiter")
	original, g := acquire(old, "before-replacement")
	waiter := pending(old, "after-replacement")
	endpoint = ctl.Replace()
	old = makeClient(oldKey)
	eventually(t, ctx, func() bool {
		replay, err := old.TryAcquire(ctx, original)
		require(t, err)
		sameReceipt(t, g.Receipt, replay.Receipt)
		active(t, replay)
		if !replay.Replayed {
			t.Fatal("replacement re-executed acquisition")
		}
		return ctl.Replaced == nil || ctl.Replaced()
	})
	verifyPending(old, waiter)
	prior := handoff(old, waiter, *g.Current)

	t.Log("rotation: overlap keys, preserve identity, remove old key, recover admitted work")
	request, grant := acquire(old, "before-rotation")
	greater(t, grant.Receipt.FencingToken, prior.FencingToken)
	admitted := pending(old, "admitted-before-revocation")
	endpoint = ctl.Rotate(false)
	fresh := makeClient(newKey)
	old = makeClient(oldKey)
	// Propagation probes repeat the same attempt, never allocate replacement sessions.
	eventually(t, ctx, func() bool {
		r, err := fresh.TryAcquire(ctx, request)
		if err != nil {
			if connect.CodeOf(err) == connect.CodeUnauthenticated {
				return false
			}
			require(t, err)
		}
		sameReceipt(t, grant.Receipt, r.Receipt)
		active(t, r)
		return true
	})
	r, e := old.TryAcquire(ctx, request)
	require(t, e)
	sameReceipt(t, grant.Receipt, r.Receipt)
	_, e = fresh.OpenSession(ctx, pintle.Resource{Namespace: "forbidden", Key: "no-authority"})
	code(t, e, connect.CodePermissionDenied)
	endpoint = ctl.Rotate(true)
	fresh = makeClient(newKey)
	old = makeClient(oldKey)
	eventually(t, ctx, func() bool {
		r, err := old.TryAcquire(ctx, request)
		if err != nil {
			code(t, err, connect.CodeUnauthenticated)
			return true
		}
		sameReceipt(t, grant.Receipt, r.Receipt)
		return false
	})
	// Check a second admission surface after observing revocation.
	_, e = old.GetAttemptStatus(ctx, request.Resource, request.RequestID, request.Attempt)
	code(t, e, connect.CodeUnauthenticated)
	recovered, e := fresh.GetAttemptStatus(ctx, request.Resource, request.RequestID, request.Attempt)
	require(t, e)
	sameReceipt(t, grant.Receipt, recovered.Acquired.Receipt)
	verifyPending(fresh, admitted)
	renew, e := s.NewRenew(*grant.Current, time.Minute)
	require(t, e)
	renewed, e := fresh.Renew(ctx, renew)
	require(t, e)
	active(t, renewed)
	if renewed.Receipt.FencingToken != grant.Receipt.FencingToken {
		t.Fatal("rotation changed fencing token")
	}
	again, e := fresh.Renew(ctx, renew)
	require(t, e)
	sameReceipt(t, renewed.Receipt, again.Receipt)
	handoff(fresh, admitted, *renewed.Current)
	// Session is used only for preparing requests after rotation. Its original
	// client's key is revoked; recovery calls must explicitly use the new client.
	return LifecycleState{fresh, s, request}
}

func eventually(t *testing.T, ctx context.Context, probe func() bool) {
	t.Helper()
	for i := 0; i < 20; i++ {
		if probe() {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Second):
		}
	}
	t.Fatal("credential propagation did not converge within 20 bounded probes")
}

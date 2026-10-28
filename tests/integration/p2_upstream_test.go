//go:build (darwin || linux) && testhooks

package integration

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	pintle "github.com/Noot-tooN/Pintle/sdk/go/pintle"
)

func TestP2ExpiredQueueHead(t *testing.T) {
	h := configuredSessionHarness(t, "P2_ENABLED:true")
	defer h.r.stop()
	for _, now := range []uint64{600999, 601000, 601001} {
		t.Run(fmt.Sprint(now), func(t *testing.T) {
			key := fmt.Sprint("expired-head-", now)
			head := h.open(t, key)
			seed, err := head.NewAcquire("old-result", time.Second)
			if err != nil {
				t.Fatal(err)
			}
			grantSession(t, h.do(t, 1000, acquireSession(seed)))
			// Retain the initial receipt until its 600000-ms recovery limit.
			for _, at := range []uint64{250000, 500000} {
				p := h.do(t, at, func(ctx context.Context, _ *pintle.Client) (any, error) {
					return nil, head.Heartbeat(ctx)
				})
				if p.err != nil {
					t.Fatal(p.err)
				}
			}
			open := func() *pintle.Session {
				p := h.do(t, 600000, func(ctx context.Context, c *pintle.Client) (any, error) {
					return c.OpenSession(ctx, seed.Resource)
				})
				if p.err != nil {
					t.Fatal(p.err)
				}
				return p.result.(*pintle.Session)
			}
			owner, tail := open(), open()
			a, err := owner.NewAcquire("live-owner", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			lease := grantSession(t, h.do(t, 600000, acquireSession(a))).Current
			first, err := head.NewWaitAcquire("head", time.Second, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			second, err := tail.NewWaitAcquire("tail", time.Second, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			for _, request := range []pintle.WaitRequest{first, second} {
				assertSame(t, waitResult(t, resumePending(h, t, request, 600000)).State, pintle.Pending)
			}
			r, err := owner.NewRelease(*lease)
			if err != nil {
				t.Fatal(err)
			}
			p := h.do(t, now, releaseCall(r))
			if p.err != nil {
				t.Fatal(p.err)
			}
			if now < 601000 {
				assertSame(t, waitStatus(h, t, first, now).Acquired.Current.FencingToken, "3")
				assertSame(t, waitStatus(h, t, second, now).Waiting, pintle.Pending)
			} else {
				grant := waitStatus(h, t, second, now).Acquired
				assertSame(t, grant.Current.FencingToken, "3") // no grant/fence for the expired head
				p := h.do(t, now, waitCall(first, false))
				expectCode(t, p.err, connect.CodeFailedPrecondition)
				assertSame(t, *waitStatus(h, t, second, now).Acquired.Current, *grant.Current)
			}
		})
	}
}

// Every RPC gets an explicit authority time. Response loss follows a fully read
// real committed reply; ACK is an observation barrier, never a timed sleep.
type controlledMaintenanceHTTP struct {
	controller *controller
	now        *atomic.Uint64
	number     atomic.Uint64
	name       string
	dropRenew  bool
	ack        chan struct{}
}

func (w *controlledMaintenanceHTTP) Do(req *http.Request) (*http.Response, error) {
	call := fmt.Sprintf("maintenance-%s-%d", w.name, w.number.Add(1))
	release(w.controller.hold(call, "gateway_start"), directive{Now: w.now.Load()})
	r := req.Clone(req.Context())
	r.Header.Set("X-Pintle-Test-Call", call)
	res, err := client.Do(r)
	if err != nil {
		return nil, err
	}
	body, err := io.ReadAll(io.LimitReader(res.Body, 16385))
	res.Body.Close()
	if err != nil {
		return nil, err
	}
	if len(body) > 16384 {
		return nil, fmt.Errorf("response exceeds unary limit")
	}
	if res.StatusCode == 200 && w.dropRenew && strings.HasSuffix(r.URL.Path, "/Renew") {
		return nil, fmt.Errorf("discarded committed renewal response")
	}
	res.Body = io.NopCloser(bytes.NewReader(body))
	if res.StatusCode == 200 && strings.HasSuffix(r.URL.Path, "/AcknowledgeSession") {
		select {
		case w.ack <- struct{}{}:
		default:
		}
	}
	return res, nil
}

func TestP2MaintenanceSuccessorRecovery(t *testing.T) {
	h := configuredSessionHarness(t, "P2_ENABLED:true")
	defer h.r.stop()
	s := h.open(t, "maintenance-generations")
	a, err := s.NewAcquire("old-owner", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	oldLease := *grantSession(t, h.do(t, 1000, acquireSession(a))).Current
	var now atomic.Uint64
	now.Store(1000)
	oldClient := sdk(t, h.r.base, h.a, &controlledMaintenanceHTTP{controller: h.c, now: &now, name: "old", dropRenew: true})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	old, err := oldClient.Maintain(ctx, oldLease, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Stop()
	await := func(ch <-chan struct{}) {
		t.Helper()
		select {
		case <-ch:
		case <-time.After(15 * time.Second):
			t.Fatal("maintenance watchdog")
		}
	}
	await(old.Lost())
	recovery := old.Recovery()
	if old.Err() == nil || recovery == nil {
		t.Fatal("missing lost-renewal recovery")
	}
	old.Stop()

	// The old renewal committed at 1000 and expires exactly at 61000.
	now.Store(61000)
	next, err := s.NewAcquire("successor", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	freshLease := *grantSession(t, h.do(t, 61000, acquireSession(next))).Current
	assertSame(t, freshLease.FencingToken, "2")
	ack := make(chan struct{}, 1)
	freshClient := sdk(t, h.r.base, h.a, &controlledMaintenanceHTTP{controller: h.c, now: &now, name: "fresh", ack: ack})
	fresh, err := freshClient.Maintain(ctx, freshLease, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Stop()
	await(ack)
	assertSame(t, fresh.Latest().LeaseID, freshLease.LeaseID)
	assertSame(t, fresh.Latest().FencingToken, "2")
	// Reconcile the old generation after the successor's renewal. Historical
	// success must not become current ownership or mutate the successor.
	late := grantSession(t, h.do(t, 61000, func(ctx context.Context, c *pintle.Client) (any, error) { return c.Renew(ctx, *recovery) }))
	assertSame(t, late.Replayed, true)
	assertSame(t, late.Receipt.FencingToken, "1")
	assertSame(t, late.State, pintle.Superseded)
	if late.Current != nil {
		t.Fatal("old receipt presented as current ownership")
	}
	stale, err := s.NewRenew(oldLease, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	p := h.do(t, 61000, func(ctx context.Context, c *pintle.Client) (any, error) { return c.Renew(ctx, stale) })
	expectCode(t, p.err, connect.CodePermissionDenied)
	old.Stop()
	await(old.Lost())
	select {
	case <-fresh.Lost():
		t.Fatal("old recovery stopped successor")
	default:
	}
	fresh.Stop()
	status := h.do(t, 61000, func(ctx context.Context, c *pintle.Client) (any, error) {
		return c.GetAttemptStatus(ctx, next.Resource, next.RequestID, next.Attempt)
	})
	if status.err != nil {
		t.Fatal(status.err)
	}
	assertSame(t, *status.result.(*pintle.Status).Acquired.Current, fresh.Latest())
	rel, err := s.NewRelease(freshLease)
	if err != nil {
		t.Fatal(err)
	}
	p = h.do(t, 61000, releaseCall(rel))
	if p.err != nil {
		t.Fatal(p.err)
	}
}

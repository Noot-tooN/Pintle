//go:build darwin || linux

package integration

import (
	"bytes"
	"connectrpc.com/connect"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	pintle "pintle.local/spike/sdk/go/pintle"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func staticKey(token, tenant, subject string) object {
	return object{"token": token, "tenant": tenant, "subject": subject, "namespaces": []string{"test"}, "not_before_ms": "0", "expires_at_ms": "9007199254740991"}
}
func writeStatic(t *testing.T, state string, keys []object) {
	t.Helper()
	data, err := json.Marshal(keys)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(filepath.Dir(state), ".dev.vars"), []byte("PINTLE_CREDENTIALS='"+string(data)+"'\n"), 0600); err != nil {
		t.Fatal(err)
	}
}
func staticSetup(t *testing.T, keys []object, vars ...string) (string, string, string, *runtime) {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	run, err := os.MkdirTemp(filepath.Join(root, "test-results"), "p3-")
	if err != nil {
		t.Fatal(err)
	}
	state := filepath.Join(run, "state")
	writeStatic(t, state, keys)
	t.Cleanup(func() { os.Remove(filepath.Join(run, ".dev.vars")) })
	config := protocolConfig(t, root, run)
	r := start(t, root, state, config, 0, false, append([]string{"P2_ENABLED:true", "PINTLE_AUTH_MODE:static"}, vars...)...)
	t.Logf("P3 local state: %s", run)
	return root, state, config, r
}
func TestP3StaticAuthAndTransport(t *testing.T) {
	a, b, rotated, otherTenant := token(t), token(t), token(t), token(t)
	keys := []object{staticKey(a, "tenant-a", "alice"), staticKey(rotated, "tenant-a", "alice"), staticKey(b, "tenant-a", "bob"), staticKey(otherTenant, "tenant-b", "alice")}
	root, state, config, r := staticSetup(t, keys, "PINTLE_METRICS:true")
	defer func() { r.stop() }()
	ctx := context.Background()
	c := sdk(t, r.base, a, client)
	resource := pintle.Resource{Namespace: "test", Key: "private-resource"}
	s, err := c.OpenSession(ctx, resource)
	if err != nil {
		t.Fatal(err)
	}
	req, _ := s.NewAcquire("worker", 60*time.Second)
	g, err := c.TryAcquire(ctx, req)
	lease := requireLease(t, g, err)
	same := sdk(t, r.base, rotated, client)
	replay, err := same.TryAcquire(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	assertSame(t, *replay.Receipt, *g.Receipt)
	_, err = sdk(t, r.base, b, client).GetAttemptStatus(ctx, resource, req.RequestID, req.Attempt)
	expectCode(t, err, connect.CodePermissionDenied)
	_, err = sdk(t, r.base, otherTenant, client).TryAcquire(ctx, req)
	expectCode(t, err, connect.CodeFailedPrecondition)
	_, err = c.OpenSession(ctx, pintle.Resource{Namespace: "forbidden", Key: "key"})
	expectCode(t, err, connect.CodePermissionDenied)
	t.Run("all_rpc_transport_rejections", func(t *testing.T) {
		for _, method := range []string{"Acquire", "CancelPending", "TryAcquire", "Renew", "Release", "GetRequestStatus", "OpenSession", "AcknowledgeSession"} {
			for _, test := range []struct {
				name    string
				headers map[string]string
				body    []byte
				status  int
			}{
				{"missing-version", map[string]string{"Connect-Protocol-Version": ""}, nil, 400},
				{"unsupported-codec", map[string]string{"Content-Type": "application/json"}, nil, 415},
				{"unsupported-compression", map[string]string{"Content-Encoding": "gzip"}, nil, 501},
				{"bad-timeout", map[string]string{"Connect-Timeout-Ms": "-1"}, nil, 400},
				{"oversize", nil, bytes.Repeat([]byte("x"), 8193), 429},
				{"malformed", nil, []byte{0xff}, 400},
			} {
				t.Run(method+"/"+test.name, func(t *testing.T) {
					status, h, data := rawRPC(t, r.base, a, "POST", "/pintle.v1.LockService/"+method, bytes.NewReader(test.body), test.headers)
					assertSame(t, status, test.status)
					assertSame(t, h.Get("Cache-Control"), "no-store")
					if strings.Contains(string(data), a) || strings.Contains(string(data), lease.LeaseID) {
						t.Fatal("secret leaked in wire error")
					}
				})
			}
		}
	})
	// Rotation removes the old key while retaining tenant/subject and SQLite history.
	r.stop()
	writeStatic(t, state, keys[1:])
	r = start(t, root, state, config, 1, false, "P2_ENABLED:true", "PINTLE_AUTH_MODE:static", "PINTLE_METRICS:true")
	_, err = sdk(t, r.base, a, client).OpenSession(ctx, resource)
	expectCode(t, err, connect.CodeUnauthenticated)
	replay, err = sdk(t, r.base, rotated, client).TryAcquire(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	assertSame(t, *replay.Receipt, *g.Receipt)
	r.stop()
	entries, err := os.ReadFile(filepath.Join(filepath.Dir(state), "wrangler-debug-0.log"))
	if err != nil {
		t.Fatal(err)
	}
	metrics := 0
	for _, line := range strings.Split(string(entries), "\n") {
		start := strings.Index(line, `{"duration_ms"`)
		if start < 0 || !strings.Contains(line, `"event":"pintle.rpc"`) {
			continue
		}
		var event object
		if err = json.Unmarshal([]byte(line[start:]), &event); err != nil {
			t.Fatal(err)
		}
		metrics++
		if len(event) != 4 {
			t.Fatalf("unexpected metric fields: %v", event)
		}
		for _, secret := range []string{a, b, rotated, otherTenant, req.RequestID, resource.Key, lease.LeaseID} {
			if strings.Contains(line, secret) {
				t.Fatal("sensitive metric label")
			}
		}
	}
	if metrics < 8 {
		t.Fatal("missing bounded metrics")
	}
	t.Logf("validated %d sanitized RPC metric events", metrics)
}
func TestP3StaticConfigurationFailsClosed(t *testing.T) {
	a := token(t)
	key := staticKey(a, "tenant", "alice")
	for _, kind := range []string{"expired", "not-yet-valid", "missing-expiry", "duplicate", "missing-static-secret", "invalid-mode"} {
		t.Run(kind, func(t *testing.T) {
			k := object{}
			for field, v := range key {
				k[field] = v
			}
			keys := []object{k}
			expected := connect.CodeUnavailable
			if kind == "expired" {
				k["expires_at_ms"] = "1"
				expected = connect.CodeUnauthenticated
			}
			if kind == "not-yet-valid" {
				k["not_before_ms"] = "9007199254740000"
				expected = connect.CodeUnauthenticated
			}
			if kind == "missing-expiry" {
				delete(k, "expires_at_ms")
			}
			if kind == "duplicate" {
				keys = append(keys, k)
			}
			root, state, config, r := staticSetup(t, keys)
			defer func() { r.stop() }()
			if kind == "missing-static-secret" || kind == "invalid-mode" {
				r.stop()
				mode := "static"
				if kind == "missing-static-secret" {
					raw, _ := json.Marshal([]object{key})
					os.WriteFile(filepath.Join(filepath.Dir(state), ".dev.vars"), []byte("PINTLE_TEST_CREDENTIALS='"+string(raw)+"'\n"), 0600)
				} else {
					mode = "unexpected"
				}
				r = start(t, root, state, config, 1, false, "P2_ENABLED:true", "PINTLE_AUTH_MODE:"+mode)
			}
			_, err := sdk(t, r.base, a, client).OpenSession(context.Background(), pintle.Resource{Namespace: "test", Key: "key"})
			expectCode(t, err, expected)
		})
	}
}

// Local latency evidence is deliberately separate from deterministic correctness
// assertions. There are no timing thresholds and no sleeps selecting winners.
func TestP3LocalLatency(t *testing.T) {
	a := token(t)
	_, state, _, r := staticSetup(t, []object{staticKey(a, "tenant", "alice")})
	defer r.stop()
	c := sdk(t, r.base, a, client)
	ctx := context.Background()
	report := object{"scope": "local Wrangler; no WAN or production claim"}
	for _, clients := range []int{1, 8} {
		s, err := c.OpenSession(ctx, pintle.Resource{Namespace: "test", Key: fmt.Sprint("hot-key-", clients)})
		if err != nil {
			t.Fatal(err)
		}
		first, _ := s.NewAcquire("owner", 60*time.Second)
		g, err := c.TryAcquire(ctx, first)
		requireLease(t, g, err)
		var durations []float64
		var mu sync.Mutex
		for batch := 0; batch < 12; batch++ {
			var wg sync.WaitGroup
			gate := make(chan struct{})
			requests := make([]pintle.AcquireRequest, clients)
			for i := range requests {
				requests[i], err = s.NewAcquire("contender", time.Second)
				if err != nil {
					t.Fatal(err)
				}
			}
			for _, req := range requests {
				wg.Add(1)
				go func(req pintle.AcquireRequest) {
					defer wg.Done()
					<-gate
					start := time.Now()
					result, e := c.TryAcquire(ctx, req)
					elapsed := float64(time.Since(start).Microseconds()) / 1000
					if e != nil || !result.Contended {
						t.Errorf("unexpected contention result: %v", e)
						return
					}
					mu.Lock()
					durations = append(durations, elapsed)
					mu.Unlock()
				}(req)
			}
			close(gate)
			wg.Wait()
			if err = s.Acknowledge(ctx, requests[len(requests)-1].Attempt.Sequence); err != nil {
				t.Fatal(err)
			}
		}
		sort.Float64s(durations)
		if len(durations) != 12*clients {
			t.Fatal("incomplete latency sample")
		}
		quantile := func(p float64) float64 { return durations[int(math.Ceil(float64(len(durations))*p))-1] }
		report[strconv.Itoa(clients)+"_clients"] = object{"samples": len(durations), "p50_ms": quantile(.50), "p95_ms": quantile(.95), "p99_ms": quantile(.99), "max_ms": durations[len(durations)-1]}
	}
	for _, clients := range []int{1, 8} {
		sessions := make([]*pintle.Session, clients)
		for i := range sessions {
			var err error
			sessions[i], err = c.OpenSession(ctx, pintle.Resource{Namespace: "test", Key: fmt.Sprintf("grant-%d-%d", clients, i)})
			if err != nil {
				t.Fatal(err)
			}
		}
		var samples []float64
		var mu sync.Mutex
		for batch := 0; batch < 12; batch++ {
			var wg sync.WaitGroup
			gate := make(chan struct{})
			for _, s := range sessions {
				wg.Add(1)
				go func(s *pintle.Session) {
					defer wg.Done()
					req, err := s.NewAcquire("worker", time.Second)
					if err != nil {
						t.Error(err)
						return
					}
					<-gate
					start := time.Now()
					g, err := c.TryAcquire(ctx, req)
					elapsed := float64(time.Since(start).Microseconds()) / 1000
					if err != nil || g.Current == nil {
						t.Errorf("grant failed: %v", err)
						return
					}
					rel, err := s.NewRelease(*g.Current)
					if err != nil {
						t.Error(err)
						return
					}
					if _, err = c.Release(ctx, rel); err != nil {
						t.Error(err)
						return
					}
					if err = s.Acknowledge(ctx, rel.Attempt.Sequence); err != nil {
						t.Error(err)
						return
					}
					mu.Lock()
					samples = append(samples, elapsed)
					mu.Unlock()
				}(s)
			}
			close(gate)
			wg.Wait()
		}
		if len(samples) != 12*clients {
			t.Fatal("incomplete grant sample")
		}
		sort.Float64s(samples)
		q := func(p float64) float64 { return samples[int(math.Ceil(float64(len(samples))*p))-1] }
		report[fmt.Sprint(clients, "_independent_grant_clients")] = object{"samples": len(samples), "p50_ms": q(.5), "p95_ms": q(.95), "p99_ms": q(.99), "max_ms": samples[len(samples)-1]}
	}

	data, _ := json.MarshalIndent(report, "", "  ")
	if err := os.WriteFile(filepath.Join(filepath.Dir(state), "latency.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("local latency evidence: %s", data)
}

type maintenanceHTTP struct {
	lostRenew bool
	ack       chan struct{}
}

func (h maintenanceHTTP) Do(r *http.Request) (*http.Response, error) {
	response, err := client.Do(r)
	if err != nil {
		return nil, err
	}
	if h.lostRenew && strings.HasSuffix(r.URL.Path, "/Renew") {
		response.Body.Close()
		return nil, fmt.Errorf("simulated lost renewal response")
	}
	if h.ack != nil && strings.HasSuffix(r.URL.Path, "/AcknowledgeSession") {
		select {
		case h.ack <- struct{}{}:
		default:
		}
	}
	return response, nil
}
func TestP3MaintenanceRuntime(t *testing.T) {
	a := token(t)
	_, _, _, r := staticSetup(t, []object{staticKey(a, "tenant", "alice")})
	defer r.stop()
	ctx := context.Background()
	c := sdk(t, r.base, a, client)
	for _, loss := range []bool{false, true} {
		t.Run(fmt.Sprint(loss), func(t *testing.T) {
			resource := pintle.Resource{Namespace: "test", Key: fmt.Sprint("maintain-", loss)}
			s, err := c.OpenSession(ctx, resource)
			if err != nil {
				t.Fatal(err)
			}
			req, _ := s.NewAcquire("worker", 60*time.Second)
			g, err := c.TryAcquire(ctx, req)
			l := requireLease(t, g, err)
			ack := make(chan struct{}, 1)
			managed := sdk(t, r.base, a, maintenanceHTTP{lostRenew: loss, ack: ack})
			m, err := managed.Maintain(ctx, *l, 60*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer m.Stop()
			if loss {
				select {
				case <-m.Lost():
				case <-time.After(10 * time.Second):
					t.Fatal("Lost watchdog")
				}
				recovery := m.Recovery()
				if recovery == nil || m.Err() == nil {
					t.Fatal("unknown renewal lost its attempt")
				}
				status, err := c.GetAttemptStatus(ctx, resource, recovery.RequestID, recovery.Attempt)
				if err != nil {
					t.Fatal(err)
				}
				assertSame(t, status.Renewed.Current.FencingToken, l.FencingToken)
				if err = c.AcknowledgeAttempt(ctx, resource, *recovery.Attempt); err != nil {
					t.Fatal(err)
				}
			} else {
				select {
				case <-ack:
				case <-time.After(10 * time.Second):
					t.Fatal("renewal ACK watchdog")
				}
				assertSame(t, m.Latest().Revision, "2")
			}
			m.Stop()
			competitor, _ := s.NewAcquire("competitor", time.Second)
			other, err := c.TryAcquire(ctx, competitor)
			if err != nil {
				t.Fatal(err)
			}
			assertSame(t, other.Contended, true)
			rel, _ := s.NewRelease(*l)
			if _, err = c.Release(ctx, rel); err != nil {
				t.Fatal(err)
			}
		})
	}
}

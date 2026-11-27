//go:build (darwin || linux) && !testhooks

package integration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"google.golang.org/protobuf/proto"
	"io"
	"net/http"
	"path"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	v1 "github.com/Noot-tooN/Pintle/gen/go/pintle/v1"
	"github.com/Noot-tooN/Pintle/gen/go/pintle/v1/pintlev1connect"
	"github.com/Noot-tooN/Pintle/sdk/go/pintle"
)

type cacheHTTP func(*http.Request) (*http.Response, error)

func (f cacheHTTP) Do(r *http.Request) (*http.Response, error) { return f(r) }
func cacheMiss(context.Context) ([]byte, bool, error)          { return nil, false, nil }

func TestManagedCacheFill(t *testing.T) {
	key := token(t)
	_, _, _, r := staticSetup(t, []object{staticKey(key, "tenant", "cache")}, "PINTLE_SINGLEFLIGHT_ENABLED:true")
	defer r.stop()
	raw := pintlev1connect.NewSingleflightServiceClient(client, r.base)
	for _, completionWins := range []bool{false, true} {
		t.Run(fmt.Sprintf("lost_waiter_join/completion_%t", completionWins), func(t *testing.T) {
			resource := pintle.Resource{Namespace: "test", Key: t.Name()}
			wire := &v1.ResourceId{Namespace: resource.Namespace, Key: resource.Key}
			leader, err := raw.JoinFlight(t.Context(), flightRequest(key, &v1.JoinFlightRequest{Resource: wire, Generation: "1", RequestId: "leader"}))
			if err != nil || !leader.Msg.Producer {
				t.Fatal("failed to establish producer", err)
			}
			complete := func() {
				t.Helper()
				_, err := raw.CompleteFlight(t.Context(), flightRequest(key, &v1.CompleteFlightRequest{Resource: wire, Ticket: leader.Msg.Ticket, Completion: &v1.CompleteFlightRequest_Value{Value: []byte("won")}}))
				if err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			original := new(v1.JoinFlightResponse)
			recovered := new(v1.JoinFlightResponse)
			joins, cancels := 0, 0
			c := sdk(t, r.base, key, cacheHTTP(func(req *http.Request) (*http.Response, error) {
				switch path.Base(req.URL.Path) {
				case "JoinFlight":
					joins++
					if joins == 1 {
						d := discardFlightReply{wire: original}
						_, err := d.Do(req)
						if !d.seen {
							t.Fatal("join did not commit", err)
						}
						cancel()
						if completionWins {
							complete()
						}
						return nil, io.ErrUnexpectedEOF
					}
					res, err := client.Do(req)
					if err == nil && res.StatusCode == 200 {
						b, e := io.ReadAll(res.Body)
						res.Body.Close()
						if e != nil {
							return nil, e
						}
						res.Body = io.NopCloser(bytes.NewReader(b))
						if e := proto.Unmarshal(b, recovered); e != nil {
							return nil, e
						}
					}
					return res, err
				case "CancelFlight":
					cancels++
				}
				return client.Do(req)
			}))
			val, err := c.CacheFill(ctx, resource, cacheMiss, func(context.Context, uint64) ([]byte, error) { t.Fatal("waiter ran fill"); return nil, nil })
			if completionWins {
				if err != nil || string(val) != "won" {
					t.Fatal("lost winning completion", err)
				}
			} else {
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("got %v", err)
				}
				state, err := raw.ObserveFlight(t.Context(), flightRequest(key, &v1.ObserveFlightRequest{Resource: wire, Ticket: original.Ticket}))
				if err != nil || state.Msg.Observation.GetStatus() != v1.FlightStatus_FLIGHT_STATUS_CANCELLED {
					t.Fatal("orphaned waiter", err)
				}
				complete()
			}
			if original.Producer || original.Ticket == "" || recovered.Ticket != original.Ticket || joins != 2 || cancels != 1 {
				t.Fatal("did not reconcile exact waiter")
			}
		})
	}

	t.Run("unknown_completion_preserves_exact_request", func(t *testing.T) {
		resource := pintle.Resource{Namespace: "test", Key: t.Name()}
		drops := 0
		c := sdk(t, r.base, key, cacheHTTP(func(req *http.Request) (*http.Response, error) {
			if path.Base(req.URL.Path) == "CompleteFlight" {
				drops++
				d := discardFlightReply{wire: new(v1.CompleteFlightResponse)}
				return d.Do(req)
			}
			return client.Do(req)
		}))
		fills := 0
		_, err := c.CacheFill(t.Context(), resource, cacheMiss, func(context.Context, uint64) ([]byte, error) { fills++; return []byte("recover"), nil })
		var recovery *pintle.CacheFillRecoveryError
		if !errors.As(err, &recovery) || recovery.Join == nil || recovery.Ticket == "" || recovery.Completion == nil || drops != 2 || fills != 1 {
			t.Fatal("missing exact completion recovery")
		}
		result, err := raw.CompleteFlight(t.Context(), flightRequest(key, recovery.Completion))
		if err != nil || string(result.Msg.Observation.GetValue()) != "recover" {
			t.Fatal("explicit recovery failed", err)
		}
	})
	t.Run("producer_panic_settles_waiters", func(t *testing.T) {
		resource := pintle.Resource{Namespace: "test", Key: t.Name()}
		wire := &v1.ResourceId{Namespace: resource.Namespace, Key: resource.Key}
		var waiter *v1.JoinFlightResponse
		c := sdk(t, r.base, key, client)
		func() {
			defer func() {
				if recover() != "origin panicked" {
					t.Error("lost callback panic")
				}
			}()
			_, _ = c.CacheFill(t.Context(), resource, cacheMiss, func(context.Context, uint64) ([]byte, error) {
				res, err := raw.JoinFlight(t.Context(), flightRequest(key, &v1.JoinFlightRequest{Resource: wire, Generation: "1", RequestId: "waiting"}))
				if err != nil {
					t.Fatal(err)
				}
				waiter = res.Msg
				panic("origin panicked")
			})
		}()
		if waiter == nil {
			t.Fatal("missing waiter")
		}
		observation, err := raw.ObserveFlight(t.Context(), flightRequest(key, &v1.ObserveFlightRequest{Resource: wire, Ticket: waiter.Ticket}))
		if err != nil || observation.Msg.Observation.GetStatus() != v1.FlightStatus_FLIGHT_STATUS_FILL_FAILED {
			t.Fatal("panic left waiter pending", err)
		}
	})
	for _, size := range []int{0, 4096, 4097} {
		t.Run(fmt.Sprintf("lost_producer_completion/%d", size), func(t *testing.T) {
			resource := pintle.Resource{Namespace: "test", Key: t.Name()}
			callbacks, completions := 0, 0
			c := sdk(t, r.base, key, cacheHTTP(func(req *http.Request) (*http.Response, error) {
				if path.Base(req.URL.Path) == "CompleteFlight" {
					completions++
					if size > 4096 && req.ContentLength > 512 {
						t.Errorf("oversize payload sent to authority")
					}
					if completions == 1 {
						d := discardFlightReply{wire: new(v1.CompleteFlightResponse)}
						res, err := d.Do(req)
						if !d.seen {
							t.Fatal("completion failed before reply loss", err)
						}
						return res, err
					}
				}
				return client.Do(req)
			}))
			want := bytes.Repeat([]byte{0xff}, size)
			val, err := c.CacheFill(t.Context(), resource, cacheMiss, func(context.Context, uint64) ([]byte, error) { callbacks++; return want, nil })
			if size > 4096 {
				if !errors.Is(err, pintle.ErrFlightTooLarge) {
					t.Fatal(err)
				}
			} else if err != nil || !bytes.Equal(val, want) {
				t.Fatal("lost completion changed value", err)
			}
			if callbacks != 1 || completions != 2 {
				t.Fatal("incorrect producer replay counts", callbacks, completions)
			}
		})
	}
	t.Run("producer_rechecks_cache", func(t *testing.T) {
		reads := 0
		c := sdk(t, r.base, key, client)
		v, err := c.CacheFill(t.Context(), pintle.Resource{Namespace: "test", Key: "recheck"}, func(context.Context) ([]byte, bool, error) {
			reads++
			return []byte("published-between-reads"), reads == 2, nil
		}, func(context.Context, uint64) ([]byte, error) {
			t.Fatal("read origin after cache filled")
			return nil, nil
		})
		if reads != 2 || err != nil || string(v) != "published-between-reads" {
			t.Fatal("producer cache recheck failed", err)
		}
	})
	for _, hold := range []time.Duration{0, time.Second} {
		t.Run("ten_cache_misses/"+hold.String(), func(t *testing.T) {
			const callers = 10
			resource := pintle.Resource{Namespace: "test", Key: "ten-misses-" + hold.String()}
			var mu sync.Mutex
			counts := map[string]int{}
			joined := make(chan struct{})
			// Hold the producer until every distinct caller has joined this generation.
			transport := cacheHTTP(func(req *http.Request) (*http.Response, error) {
				res, err := client.Do(req)
				mu.Lock()
				defer mu.Unlock()
				m := path.Base(req.URL.Path)
				counts[m]++
				if m == "JoinFlight" && counts[m] == callers {
					close(joined)
				}
				return res, err
			})
			cache := immutableValue{}
			var reads atomic.Int32
			start := make(chan struct{})
			results := make(chan error, callers)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			for range callers {
				c := sdk(t, r.base, key, transport)
				go func() {
					<-start
					v, err := c.CacheFill(ctx, resource, cache.lookup, func(work context.Context, generation uint64) ([]byte, error) {
						reads.Add(1)
						select {
						case <-joined:
						case <-work.Done():
							return nil, work.Err()
						}
						// Simulated origin latency for the bounded RPC measurement.
						select {
						case <-time.After(hold):
						case <-work.Done():
							return nil, work.Err()
						}
						cache.store([]byte("same"))
						return []byte("same"), nil
					})
					if err == nil && string(v) != "same" {
						err = errors.New("different result")
					}
					results <- err
				}()
			}
			close(start)
			for range callers {
				if err := <-results; err != nil {
					t.Error(err)
				}
			}
			mu.Lock()
			defer mu.Unlock()
			if reads.Load() != 1 || counts["GetFlightGeneration"] != callers || counts["JoinFlight"] != callers || counts["CompleteFlight"] != 1 || counts["ObserveFlight"] < callers-1 || counts["CancelFlight"] != 0 {
				t.Fatal("unexpected cache fill counts", reads.Load(), counts)
			}
			t.Logf("10 distinct callers, %d origin read, RPC counts %v", reads.Load(), counts)
		})
	}
}

type immutableValue struct {
	mu    sync.Mutex
	value []byte
}

func (c *immutableValue) lookup(context.Context) ([]byte, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.value, c.value != nil, nil
}
func (c *immutableValue) store(v []byte) { c.mu.Lock(); defer c.mu.Unlock(); c.value = v }

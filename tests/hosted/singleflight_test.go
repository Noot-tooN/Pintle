//go:build darwin || linux

package hosted

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Noot-tooN/Pintle/sdk/go/pintle"
)

// All callers in the parent share the existing 96-request transport budget.
// A successful ObserveFlight proves the waiter has a ticket before we proceed.
type flightProbe struct {
	client                     *pintle.Client
	observed                   chan struct{}
	cancelled                  atomic.Int32
	holdObservationUntilCancel bool
}

func stagingFlightClient(t *testing.T, budget *transport) *flightProbe {
	t.Helper()
	var keys []stagingCredential
	require(t, json.Unmarshal(privateFile(t, "../../.hermit/staging/credentials.json"), &keys))
	if len(keys) != 1 || len(keys[0].Namespaces) != 1 || keys[0].Namespaces[0] != "staging" {
		t.Fatal("one staging-only credential required")
	}
	endpoint := os.Getenv("PINTLE_STAGING_URL")
	require(t, validateTarget(endpoint, keys[0].Token))
	p := &flightProbe{observed: make(chan struct{})}
	var once sync.Once
	noRedirect := func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	h := &http.Client{Timeout: 10 * time.Second, CheckRedirect: noRedirect, Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		resp, err := budget.RoundTrip(r)
		if err == nil && resp.StatusCode == http.StatusOK {
			switch path.Base(r.URL.Path) {
			case "ObserveFlight":
				once.Do(func() { close(p.observed) })
				if p.holdObservationUntilCancel {
					// Delay delivery of an actual pending response so the parent can
					// order cancellation after the producer's confirmed completion.
					<-r.Context().Done()
					resp.Body.Close()
					return nil, r.Context().Err()
				}
			case "CancelFlight":
				p.cancelled.Add(1)
			}
		}
		return resp, err
	})}
	var err error
	p.client, err = pintle.NewWithHTTPClient(endpoint, keys[0].Token, h)
	require(t, err)
	return p
}

type flightValue struct {
	value []byte
	err   error
}
type flightStore string

func (s flightStore) lookup(ctx context.Context) ([]byte, bool, error) {
	value, err := os.ReadFile(string(s))
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	return value, err == nil, err
}

// Each scenario has one writer. Publish the complete value in one rename.
func (s flightStore) publish(value []byte) error {
	if err := os.WriteFile(string(s)+".tmp", value, 0600); err != nil {
		return err
	}
	return os.Rename(string(s)+".tmp", string(s))
}

func (p *flightProbe) wait(ctx context.Context, r pintle.Resource, store flightStore) <-chan flightValue {
	result := make(chan flightValue, 1)
	go func() {
		value, err := p.client.CacheFill(ctx, r, store.lookup, unexpectedFlightFill)
		result <- flightValue{value, err}
	}()
	return result
}

func unexpectedFlightFill(context.Context, uint64) ([]byte, error) {
	return nil, errors.New("unexpected second origin fill")
}
func awaitFlightSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for hosted admission")
	}
}

func TestStagingSingleflight(t *testing.T) {
	if os.Getenv("PINTLE_RUN_HOSTED_SINGLEFLIGHT") != "1" {
		t.Skip("requires explicit singleflight staging run")
	}
	id, err := pintle.NewRequestID()
	require(t, err)
	budget := &transport{}
	for _, size := range []int{4096, 4097} {
		t.Run(fmt.Sprintf("inline_%d", size), func(t *testing.T) {
			r := pintle.Resource{Namespace: "staging", Key: fmt.Sprintf("flight-%s-%d", id, size)}
			store := flightStore(filepath.Join(t.TempDir(), "value"))
			value := bytes.Repeat([]byte("x"), size)
			producer := stagingFlightClient(t, budget)
			entered, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer unblock()
			result := make(chan flightValue, 1)
			go func() {
				v, err := producer.client.CacheFill(t.Context(), r, store.lookup, func(ctx context.Context, _ uint64) ([]byte, error) {
					close(entered)
					select {
					case <-release:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
					return value, store.publish(value)
				})
				result <- flightValue{v, err}
			}()
			awaitFlightSignal(t, entered)
			waiter := stagingFlightClient(t, budget)
			survivor := waiter.wait(t.Context(), r, store)
			awaitFlightSignal(t, waiter.observed)
			var checkLateCancellation func()
			if size == 4096 {
				cancelled := stagingFlightClient(t, budget)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				gone := cancelled.wait(ctx, r, store)
				awaitFlightSignal(t, cancelled.observed)
				cancel()
				got := <-gone
				if !errors.Is(got.err, context.Canceled) || got.value != nil || cancelled.cancelled.Load() != 1 {
					t.Fatalf("waiter cancellation: %v, successful CancelFlight calls %d", got.err, cancelled.cancelled.Load())
				}

				late := stagingFlightClient(t, budget)
				late.holdObservationUntilCancel = true
				lateCtx, stop := context.WithCancel(t.Context())
				defer stop()
				lateResult := late.wait(lateCtx, r, store)
				awaitFlightSignal(t, late.observed)
				checkLateCancellation = func() {
					stop()
					got := <-lateResult
					if got.err != nil || !bytes.Equal(got.value, value) || late.cancelled.Load() != 1 {
						t.Fatalf("completion before cancellation: %v, successful CancelFlight calls %d", got.err, late.cancelled.Load())
					}
					t.Log("cancellation before completion detached one waiter; cancellation after completion returned the committed value")
				}
			}
			unblock()
			for _, who := range []<-chan flightValue{result, survivor} {
				got := <-who
				if size == 4096 {
					if got.err != nil || !bytes.Equal(got.value, value) {
						t.Fatalf("inline delivery: length %d, error %v", len(got.value), got.err)
					}
				} else if !errors.Is(got.err, pintle.ErrFlightTooLarge) || got.value != nil {
					t.Fatalf("oversize delivery: length %d, error %v", len(got.value), got.err)
				}
			}
			if checkLateCancellation != nil {
				checkLateCancellation()
			}
			previous := budget.calls.Load()
			got, err := producer.client.CacheFill(t.Context(), r, store.lookup, unexpectedFlightFill)
			if err != nil || !bytes.Equal(got, value) || budget.calls.Load() != previous {
				t.Fatalf("repeat cache hit: length %d, error %v", len(got), err)
			}
			t.Logf("one origin fill, %d bytes in application cache, repeat hit used zero RPCs", size)
		})
	}
	for _, mode := range []string{"before_publish", "after_publish"} {
		t.Run("producer_crash_"+mode, func(t *testing.T) {
			r := pintle.Resource{Namespace: "staging", Key: "flight-" + id + "-" + mode}
			store := flightStore(filepath.Join(t.TempDir(), "value"))
			ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
			defer cancel()
			child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestStagingSingleflightProducer$")
			child.Env = append(os.Environ(), "PINTLE_FLIGHT_CHILD="+mode, "PINTLE_FLIGHT_KEY="+r.Key, "PINTLE_FLIGHT_FILE="+string(store))
			in, err := child.StdinPipe()
			require(t, err)
			defer in.Close()
			out, err := child.StdoutPipe()
			require(t, err)
			require(t, child.Start())
			defer func() {
				if child.ProcessState == nil {
					_ = child.Process.Kill()
					_ = child.Wait()
				}
			}()
			lines := bufio.NewScanner(out)
			expect := func(want string) {
				t.Helper()
				if !lines.Scan() || lines.Text() != want {
					t.Fatalf("producer failed to reach %s", want)
				}
			}
			expect("admitted")
			start := time.Now()
			waiter := stagingFlightClient(t, budget)
			result := waiter.wait(ctx, r, store)
			awaitFlightSignal(t, waiter.observed)
			_, err = in.Write([]byte{1})
			require(t, err)
			expect("kill")
			require(t, child.Process.Kill())
			var killed *exec.ExitError
			if err := child.Wait(); !errors.As(err, &killed) || killed.ProcessState.ExitCode() != -1 {
				t.Fatalf("producer did not die by signal: %v", err)
			}
			got := <-result
			if mode == "after_publish" {
				if got.err != nil || string(got.value) != "published" {
					t.Fatalf("cache recovery failed: %v", got.err)
				}
			} else {
				if !errors.Is(got.err, pintle.ErrFlightExpired) || got.value != nil {
					t.Fatalf("expected expired flight, got %v", got.err)
				}
				v, err := waiter.client.CacheFill(ctx, r, store.lookup, func(_ context.Context, generation uint64) ([]byte, error) {
					if generation != 2 {
						return nil, fmt.Errorf("unexpected successor generation %d", generation)
					}
					return []byte("retry"), store.publish([]byte("retry"))
				})
				if err != nil || string(v) != "retry" {
					t.Fatalf("explicit retry failed: %v", err)
				}
			}
			t.Logf("producer killed %s; waiter outcome verified after %v", mode, time.Since(start).Round(time.Millisecond))
		})
	}
	t.Logf("hosted RPCs: %d parent plus two admissions of two calls each in child processes", budget.calls.Load())
}

func TestStagingSingleflightProducer(t *testing.T) {
	mode := os.Getenv("PINTLE_FLIGHT_CHILD")
	if os.Getenv("PINTLE_RUN_HOSTED_SINGLEFLIGHT") != "1" || (mode != "before_publish" && mode != "after_publish") {
		t.Skip("staging crash-test subprocess only")
	}
	budget := &transport{}
	client := stagingFlightClient(t, budget).client
	r := pintle.Resource{Namespace: "staging", Key: os.Getenv("PINTLE_FLIGHT_KEY")}
	store := flightStore(os.Getenv("PINTLE_FLIGHT_FILE"))
	_, err := client.CacheFill(t.Context(), r, store.lookup, func(ctx context.Context, generation uint64) ([]byte, error) {
		if generation != 1 || budget.calls.Load() != 2 {
			return nil, errors.New("unexpected producer admission")
		}
		fmt.Println("admitted")
		var gate [1]byte
		if _, err := io.ReadFull(os.Stdin, gate[:]); err != nil {
			return nil, err
		}
		if mode == "after_publish" {
			if err := store.publish([]byte("published")); err != nil {
				return nil, err
			}
		}
		fmt.Println("kill")
		_, err := io.ReadFull(os.Stdin, gate[:])
		return nil, err
	})
	t.Fatalf("producer returned before being killed: %v", err)
}

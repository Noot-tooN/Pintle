//go:build (darwin || linux) && testhooks

package integration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Noot-tooN/Pintle/sdk/go/pintle"
)

// Both OS processes share this immutable, versioned file. Link publishes a
// complete candidate atomically and refuses to replace another writer's entry.
type immutableFillCache struct{ dir string }

func (c immutableFillCache) lookup(context.Context) ([]byte, bool, error) {
	b, err := os.ReadFile(filepath.Join(c.dir, "value"))
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	return b, err == nil, err
}
func (c immutableFillCache) origin(value string) error {
	f, err := os.OpenFile(filepath.Join(c.dir, "origin-reads"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(f, value)
	return errors.Join(err, f.Close())
}
func (c immutableFillCache) publish(value string) error {
	f, err := os.CreateTemp(c.dir, "candidate-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	_, err = f.WriteString(value)
	if err = errors.Join(err, f.Close()); err != nil {
		return err
	}
	return os.Link(f.Name(), filepath.Join(c.dir, "value"))
}

func TestCacheFillChild(t *testing.T) {
	mode := os.Getenv("PINTLE_CACHE_FILL_CHILD")
	if mode == "" {
		t.Skip("subprocess only")
	}
	cache := immutableFillCache{os.Getenv("PINTLE_CACHE_FILL_DIR")}
	c := sdk(t, os.Getenv("PINTLE_CACHE_FILL_ENDPOINT"), os.Getenv("PINTLE_CACHE_FILL_KEY"), client)
	resource := pintle.Resource{Namespace: "test", Key: mode}
	_, err := c.CacheFill(t.Context(), resource, cache.lookup, func(_ context.Context, generation uint64) ([]byte, error) {
		if generation != 1 {
			return nil, fmt.Errorf("unexpected producer generation %d", generation)
		}
		if err := cache.origin("original"); err != nil {
			return nil, err
		}
		if err := os.WriteFile(filepath.Join(cache.dir, "computing"), nil, 0600); err != nil {
			return nil, err
		}
		var signal [1]byte
		if _, err := io.ReadFull(os.Stdin, signal[:]); err != nil {
			return nil, err
		}
		if err := cache.publish("original"); err != nil {
			return nil, err
		}
		if err := os.WriteFile(filepath.Join(cache.dir, "published"), nil, 0600); err != nil {
			return nil, err
		}
		// Parent kills this process before the callback can return to the SDK.
		_, err := io.ReadFull(os.Stdin, signal[:])
		return []byte("original"), err
	})
	if mode != "stale" || !errors.Is(err, os.ErrExist) || !errors.Is(err, pintle.ErrFlightExpired) {
		t.Fatalf("unexpected child outcome: %v", err)
	}
}

func TestSingleflightCacheFillProcesses(t *testing.T) {
	key := token(t)
	ctrl := newController(t)
	_, _, _, r := staticSetup(t, []object{staticKey(key, "test", "app")}, "PINTLE_SINGLEFLIGHT_ENABLED:true", "PINTLE_TEST_SCHEDULER:"+ctrl.server.URL)
	defer r.stop()
	target, err := url.Parse(r.base)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"crash", "stale"} {
		t.Run(mode, func(t *testing.T) {
			cache := immutableFillCache{t.TempDir()}
			var now atomic.Uint64
			now.Store(1000)
			observed := make(chan struct{})
			var once sync.Once
			proxy := httputil.NewSingleHostReverseProxy(target)
			proxy.ModifyResponse = func(response *http.Response) error {
				if filepath.Base(response.Request.URL.Path) == "ObserveFlight" && response.StatusCode == 200 {
					once.Do(func() { close(observed) })
				}
				return nil
			}
			gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				ts := now.Load()
				name := fmt.Sprintf("fill-%s-%d", mode, ts)
				ctrl.mu.Lock()
				ctrl.times[name] = ts
				ctrl.mu.Unlock()
				req.Header.Set("X-Pintle-Test-Call", name)
				proxy.ServeHTTP(w, req)
			}))
			defer gateway.Close()
			c := sdk(t, gateway.URL, key, client)
			resource := pintle.Resource{Namespace: "test", Key: mode}
			cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestCacheFillChild$")
			cmd.Env = append(os.Environ(), "PINTLE_CACHE_FILL_CHILD="+mode, "PINTLE_CACHE_FILL_DIR="+cache.dir, "PINTLE_CACHE_FILL_ENDPOINT="+gateway.URL, "PINTLE_CACHE_FILL_KEY="+key)
			input, err := cmd.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			var output bytes.Buffer
			cmd.Stdout = &output
			cmd.Stderr = &output
			if err = cmd.Start(); err != nil {
				t.Fatal(err)
			}
			exited := make(chan struct{})
			var exitErr error
			go func() { exitErr = cmd.Wait(); close(exited) }()
			defer func() { input.Close(); _ = cmd.Process.Kill(); <-exited }()
			waitFile := func(name string) {
				t.Helper()
				deadline := time.Now().Add(10 * time.Second)
				for time.Now().Before(deadline) {
					if _, err := os.Stat(filepath.Join(cache.dir, name)); err == nil {
						return
					}
					select {
					case <-exited:
						t.Fatalf("child exited: %v %s", exitErr, output.String())
					case <-time.After(10 * time.Millisecond):
					}
				}
				t.Fatalf("child never reached %s", name)
			}
			release := func() {
				t.Helper()
				if _, err := input.Write([]byte{1}); err != nil {
					t.Fatal(err)
				}
			}
			waitFile("computing")
			if mode == "crash" {
				result := make(chan error, 1)
				go func() {
					v, err := c.CacheFill(t.Context(), resource, cache.lookup, func(context.Context, uint64) ([]byte, error) {
						_ = cache.origin("unexpected")
						return nil, errors.New("waiter repeated the fill")
					})
					if err == nil && string(v) != "original" {
						err = errors.New("waiter lost cached value")
					}
					result <- err
				}()
				select {
				case <-observed:
				case <-time.After(10 * time.Second):
					t.Fatal("second process did not attach")
				}
				release()
				waitFile("published")
				if err := cmd.Process.Kill(); err != nil {
					t.Fatal(err)
				}
				<-exited
				if exitErr == nil {
					t.Fatal("producer exited normally")
				}
				now.Store(31000)
				select {
				case err := <-result:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(10 * time.Second):
					t.Fatal("waiter did not recover")
				}
			} else {
				now.Store(31000)
				v, err := c.CacheFill(t.Context(), resource, cache.lookup, func(_ context.Context, g uint64) ([]byte, error) {
					if g != 2 {
						return nil, fmt.Errorf("successor generation %d", g)
					}
					if err := cache.origin("fresh"); err != nil {
						return nil, err
					}
					return []byte("fresh"), cache.publish("fresh")
				})
				if err != nil || string(v) != "fresh" {
					t.Fatal("successor failed", err)
				}
				release()
				select {
				case <-exited:
					if exitErr != nil {
						t.Fatal(exitErr, output.String())
					}
				case <-time.After(10 * time.Second):
					t.Fatal("stale producer did not finish")
				}
			}
			reads, err := os.ReadFile(filepath.Join(cache.dir, "origin-reads"))
			if err != nil {
				t.Fatal(err)
			}
			got, hit, err := cache.lookup(t.Context())
			if err != nil || !hit {
				t.Fatal("missing cache entry", err)
			}
			if mode == "crash" {
				assertSame(t, string(reads), "original\n")
				assertSame(t, string(got), "original")
			} else {
				assertSame(t, string(reads), "original\nfresh\n")
				assertSame(t, string(got), "fresh")
			}
		})
	}
}

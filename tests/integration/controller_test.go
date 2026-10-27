//go:build (darwin || linux) && testhooks

package integration

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
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

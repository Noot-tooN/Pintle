//go:build (darwin || linux) && !testhooks

package integration

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"github.com/Noot-tooN/Pintle/tests/hosted"
	"strings"
	"testing"
	"time"
)

func TestHostedSuiteLocally(t *testing.T) {
	credential := token(t)
	key := staticKey(credential, "staging-local", "smoke")
	key["namespaces"] = []string{"staging"}
	_, _, _, r := staticSetup(t, []object{key})
	defer r.stop()
	hosted.Run(t, r.base, credential)
}

func TestHostedLifecycleLocally(t *testing.T) {
	old, rotated := token(t), token(t)
	key := staticKey(old, "staging-local", "smoke")
	key["namespaces"] = []string{"staging"}
	newKey := staticKey(rotated, "staging-local", "smoke")
	newKey["namespaces"] = []string{"staging"}
	root, state, config, r := staticSetup(t, []object{key}, "PINTLE_METRICS:true")
	defer func() { r.stop() }()
	activation := 0
	restart := func() string {
		r.stop()
		activation++
		r = start(t, root, state, config, activation, "P2_ENABLED:true", "PINTLE_AUTH_MODE:static", "PINTLE_METRICS:true")
		return r.base
	}
	ctl := hosted.LifecycleControl{
		Replace: restart,
		Rotate: func(newOnly bool) string {
			keys := []object{key, newKey}
			if newOnly {
				keys = []object{newKey}
			}
			r.stop()
			writeStatic(t, state, keys)
			return restart()
		},
	}
	hosted.RunLifecycle(t, r.base, old, rotated, &http.Client{Timeout: 15 * time.Second}, ctl)
	r.stop()
	instances := map[string]bool{}
	for i := 0; i <= activation; i++ {
		log, err := os.ReadFile(filepath.Join(filepath.Dir(state), fmt.Sprintf("wrangler-debug-%d.log", i)))
		if err != nil {
			t.Fatal(err)
		}
		seen := map[string]bool{}
		for _, line := range strings.Split(string(log), "\n") {
			pos := strings.Index(line, `{"event":"pintle.authority"`)
			if pos < 0 {
				continue
			}
			var event map[string]any
			if err := json.Unmarshal([]byte(line[pos:]), &event); err != nil {
				t.Fatal(err)
			}
			if len(event) != 4 {
				t.Fatal("unexpected authority metric fields")
			}
			id, ok := event["instance"].(string)
			if !ok || len(id) != 64 {
				t.Fatal("missing diagnostic instance marker")
			}
			for _, secret := range []string{old, rotated, "lifecycle-"} {
				if strings.Contains(line, secret) {
					t.Fatal("sensitive authority metric")
				}
			}
			seen[id] = true
		}
		if len(seen) != 1 {
			t.Fatal("expected one stable authority instance per runtime")
		}
		for id := range seen {
			if instances[id] {
				t.Fatal("instance marker survived process replacement")
			}
			instances[id] = true
		}
	}
}

func TestHostedCapacityLocally(t *testing.T) {
	credential := token(t)
	key := staticKey(credential, "staging-local", "capacity")
	key["namespaces"] = []string{"staging"}
	_, _, _, r := staticSetup(t, []object{key})
	defer r.stop()
	hosted.RunCapacity(t, r.base, credential, func(report hosted.CapacityReport) { t.Logf("local rehearsal: %s", hosted.CapacityJSON(report)) })
}

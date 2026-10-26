//go:build darwin || linux

package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

type object map[string]any
type row struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}
type grant struct {
	Request struct {
		RequestID string `json:"request_id"`
		Owner     string `json:"owner"`
	} `json:"request"`
	Fence    string `json:"fence"`
	Revision string `json:"revision"`
}
type outcome struct {
	Outcome  string `json:"outcome"`
	Grant    grant  `json:"grant"`
	Replayed bool   `json:"replayed"`
}

var client = &http.Client{Timeout: 15 * time.Second}

func call(base, tenant, namespace, key string, operation object) ([]byte, int, error) {
	body, err := json.Marshal(object{"namespace": namespace, "key": key, "operation": operation})
	if err != nil {
		return nil, 0, err
	}
	req, err := http.NewRequest("POST", base+"/spike", bytes.NewReader(body))
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+tenant)
	req.Header.Set("Content-Type", "application/json")
	res, err := client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer res.Body.Close()
	data, err := io.ReadAll(res.Body)
	return data, res.StatusCode, err
}
func mustCall(t *testing.T, base, tenant, ns, key string, op object, status int) []byte {
	t.Helper()
	data, got, err := call(base, tenant, ns, key, op)
	if err != nil || got != status {
		t.Fatalf("%v: status %d wanted %d, err %v, body %s", op, got, status, err, data)
	}
	return data
}
func acquire(id, owner string) object {
	return object{"op": "grant", "request": object{"request_id": id, "owner": owner}}
}
func decode(t *testing.T, data []byte) outcome {
	t.Helper()
	var out outcome
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("%s: %v", data, err)
	}
	return out
}
func snapshot(t *testing.T, base, key string) []row {
	t.Helper()
	data := mustCall(t, base, "local-a", "test", key, object{"op": "snapshot"}, 200)
	var rows []row
	if err := json.Unmarshal(data, &rows); err != nil {
		t.Fatal(err)
	}
	return rows
}
func assertSame(t *testing.T, got, want any) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, wanted %#v", got, want)
	}
}

// Check every logical record against the acknowledged grant, not only row count.
func assertCommitted(t *testing.T, base, key string, expected grant) {
	t.Helper()
	records := map[string]string{}
	for _, r := range snapshot(t, base, key) {
		records[r.Key] = r.Value
	}
	assertSame(t, records["fence"], expected.Fence)
	assertSame(t, records["revision"], expected.Revision)
	for _, key := range []string{"owner", "request/" + expected.Request.RequestID} {
		var stored grant
		if err := json.Unmarshal([]byte(records[key]), &stored); err != nil {
			t.Fatal(err)
		}
		assertSame(t, stored, expected)
	}
}

type runtime struct {
	cmd  *exec.Cmd
	done chan error
	base string
	log  *os.File
}

func freePort(t *testing.T) string {
	t.Helper()
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	defer l.Close()
	return strconv.Itoa(l.Addr().(*net.TCPAddr).Port)
}
func start(t *testing.T, root, state, config string, activation int, enabled bool, variables ...string) *runtime {
	t.Helper()
	port := freePort(t)
	args := []string{filepath.Join(root, "node_modules/wrangler/bin/wrangler.js"), "dev", "--config", config, "--local", "--ip", "127.0.0.1", "--port", port, "--inspector-port", "0", "--persist-to", state, "--log-level", "info"}
	if enabled {
		args = append(args, "--var", "LOCAL_SPIKE:true")
	}
	for _, variable := range variables {
		args = append(args, "--var", variable)
	}
	ready := enabled
	for _, variable := range variables {
		if variable == "P0B_ENABLED:true" || variable == "P1_ENABLED:true" || variable == "P11_ENABLED:true" || variable == "P2_ENABLED:true" {
			ready = true
		}
	}
	cmd := exec.Command(filepath.Join(root, "bin/node"), args...)
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "WRANGLER_SEND_METRICS=false", "CI=true", "WRANGLER_LOG_PATH="+filepath.Join(filepath.Dir(state), fmt.Sprintf("wrangler-debug-%d.log", activation)))
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	log, err := os.Create(filepath.Join(filepath.Dir(state), fmt.Sprintf("wrangler-%d.log", activation)))
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stdout = log
	cmd.Stderr = log
	if err = cmd.Start(); err != nil {
		log.Close()
		t.Fatal(err)
	}
	r := &runtime{cmd: cmd, done: make(chan error, 1), base: "http://127.0.0.1:" + port, log: log}
	go func() { r.done <- cmd.Wait() }()
	t.Cleanup(r.stop)
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-r.done:
			data, _ := os.ReadFile(log.Name())
			t.Fatalf("wrangler exited: %v\n%s", err, data)
		default:
		}
		res, err := client.Get(r.base + "/health")
		if err == nil {
			io.Copy(io.Discard, res.Body)
			res.Body.Close()
			if (ready && res.StatusCode == 200) || (!ready && res.StatusCode == 403) {
				return r
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	data, _ := os.ReadFile(log.Name())
	t.Fatalf("wrangler startup timed out\n%s", data)
	return nil
}
func (r *runtime) stop() {
	if r.cmd == nil {
		return
	}
	syscall.Kill(-r.cmd.Process.Pid, syscall.SIGKILL)
	select {
	case <-r.done:
	case <-time.After(5 * time.Second):
	}
	r.log.Close()
	r.cmd = nil
}

// Child test processes wait for a parent barrier, then issue exactly one HTTP
// request. The parent verifies contention between separate Go client processes.
func TestClientProcess(t *testing.T) {
	if os.Getenv("PINTLE_CHILD") == "" {
		t.Skip("used by concurrency test")
	}
	if _, err := io.ReadFull(os.Stdin, make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	data, status, err := call(os.Getenv("PINTLE_URL"), "local-a", "test", os.Getenv("PINTLE_KEY"), acquire(os.Getenv("PINTLE_ID"), "process"))
	if err != nil || status != 200 {
		t.Fatalf("status %d: %v %s", status, err, data)
	}
	if err := os.WriteFile(os.Getenv("PINTLE_OUTPUT"), data, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestP0(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	results := filepath.Join(root, "test-results")
	if err = os.MkdirAll(results, 0755); err != nil {
		t.Fatal(err)
	}
	run, err := os.MkdirTemp(results, "p0-")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("runtime logs and SQLite state: %s", run)
	// Derive config from the real configuration; only omit its build hook because
	// just test-runtime has built the actual Wasm. All bindings/migrations stay.
	raw, err := os.ReadFile(filepath.Join(root, "wrangler.jsonc"))
	if err != nil {
		t.Fatal(err)
	}
	// JSONC's sole comment is removed before standard decoding.
	lines := strings.Split(string(raw), "\n")
	for i, line := range lines {
		if strings.HasPrefix(strings.TrimSpace(line), "//") {
			lines[i] = ""
		}
	}
	var config object
	if err = json.Unmarshal([]byte(strings.Join(lines, "\n")), &config); err != nil {
		t.Fatal(err)
	}
	delete(config, "build")
	config["main"] = filepath.Join(root, "crates/cloudflare/build/worker/shim.mjs")
	data, _ := json.MarshalIndent(config, "", "  ")
	configFile := filepath.Join(run, "wrangler.json")
	if err = os.WriteFile(configFile, data, 0600); err != nil {
		t.Fatal(err)
	}
	state := filepath.Join(run, "state")
	r := start(t, root, state, configFile, 0, false)
	mustCall(t, r.base, "local-a", "test", "disabled", acquire("a", "alice"), 403)
	r.stop()
	r = start(t, root, state, configFile, 1, true)

	t.Run("atomic_rollback_every_write", func(t *testing.T) {
		for _, kind := range []string{"sql", "error"} {
			for position := 1; position <= 4; position++ {
				key := fmt.Sprintf("rollback-%s-%d", kind, position)
				mustCall(t, r.base, "local-a", "test", key, object{"op": "seed", "fence": "9007199254740992", "revision": "31"}, 200)
				before := snapshot(t, r.base, key)
				op := acquire("failed", "alice")
				op["fault"] = object{"kind": kind, "after_write": position}
				failure := mustCall(t, r.base, "local-a", "test", key, op, 500)
				expectedFailure := "UNIQUE constraint failed"
				if kind == "error" {
					expectedFailure = "injected Rust callback error"
				}
				if !strings.Contains(string(failure), expectedFailure) {
					t.Fatalf("unexpected failure: %s", failure)
				}
				assertSame(t, snapshot(t, r.base, key), before)
				out := decode(t, mustCall(t, r.base, "local-a", "test", key, acquire("failed", "alice"), 200))
				assertSame(t, out.Outcome, "granted")
				assertSame(t, out.Replayed, false)
				assertSame(t, out.Grant.Fence, "9007199254740993")
				assertSame(t, out.Grant.Revision, "32")
				assertCommitted(t, r.base, key, out.Grant)
				assertSame(t, len(snapshot(t, r.base, key)), 4)
			}
		}
		// An empty authority must remain completely empty on a failed first grant.
		op := acquire("empty", "alice")
		op["fault"] = object{"kind": "sql", "after_write": 3}
		mustCall(t, r.base, "local-a", "test", "empty-rollback", op, 500)
		assertSame(t, len(snapshot(t, r.base, "empty-rollback")), 0)
	})
	t.Run("concurrent_client_processes", func(t *testing.T) {
		key := "process-race"
		var previous uint64
		for round := 0; round < 3; round++ {
			const n = 12
			cmds := make([]*exec.Cmd, n)
			pipes := make([]io.WriteCloser, n)
			outputs := make([]string, n)
			logs := make([]bytes.Buffer, n)
			for i := 0; i < n; i++ {
				outputs[i] = filepath.Join(run, fmt.Sprintf("client-%d-%d.json", round, i))
				cmd := exec.Command(os.Args[0], "-test.run=^TestClientProcess$", "-test.timeout=30s")
				cmd.Env = append(os.Environ(), "PINTLE_CHILD=1", "PINTLE_URL="+r.base, "PINTLE_KEY="+key, "PINTLE_ID="+fmt.Sprintf("r%d-c%d", round, i), "PINTLE_OUTPUT="+outputs[i])
				cmd.Stdout = &logs[i]
				cmd.Stderr = &logs[i]
				pipes[i], err = cmd.StdinPipe()
				if err != nil {
					t.Fatal(err)
				}
				if err = cmd.Start(); err != nil {
					t.Fatal(err)
				}
				cmds[i] = cmd
			}
			for _, p := range pipes {
				p.Write([]byte{1})
				p.Close()
			}
			winners := 0
			for i, cmd := range cmds {
				if err = cmd.Wait(); err != nil {
					t.Fatalf("client %d: %v %s", i, err, logs[i].String())
				}
				data, err := os.ReadFile(outputs[i])
				if err != nil {
					t.Fatal(err)
				}
				out := decode(t, data)
				switch out.Outcome {
				case "granted":
					winners++
					assertSame(t, out.Replayed, false)
					token, err := strconv.ParseUint(out.Grant.Fence, 10, 64)
					if err != nil || token <= previous {
						t.Fatalf("token regression %s after %d", out.Grant.Fence, previous)
					}
					previous = token
				case "contended":
				default:
					t.Fatalf("unexpected outcome: %s", data)
				}
			}
			assertSame(t, winners, 1)
			mustCall(t, r.base, "local-a", "test", key, object{"op": "vacate"}, 200)
		}
		assertSame(t, previous, uint64(3))
	})
	t.Run("concurrent_same_id_and_payload_conflict", func(t *testing.T) {
		const n = 24
		responses := make(chan []byte, n)
		errs := make(chan error, n)
		var wg sync.WaitGroup
		barrier := make(chan struct{})
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-barrier
				data, status, err := call(r.base, "local-a", "test", "dedupe", acquire("stable", "alice"))
				if err != nil || status != 200 {
					errs <- fmt.Errorf("%d %v %s", status, err, data)
					return
				}
				responses <- data
			}()
		}
		close(barrier)
		wg.Wait()
		close(responses)
		close(errs)
		for err := range errs {
			t.Fatal(err)
		}
		fresh := 0
		for data := range responses {
			out := decode(t, data)
			assertSame(t, out.Outcome, "granted")
			assertSame(t, out.Grant.Fence, "1")
			if !out.Replayed {
				fresh++
			}
		}
		assertSame(t, fresh, 1)
		before := snapshot(t, r.base, "dedupe")
		out := decode(t, mustCall(t, r.base, "local-a", "test", "dedupe", acquire("stable", "bob"), 200))
		assertSame(t, out.Outcome, "request_id_conflict")
		assertSame(t, snapshot(t, r.base, "dedupe"), before)
	})
	t.Run("response_lost_after_commit", func(t *testing.T) {
		observed := make(chan outcome, 1)
		proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			data, status, err := call(r.base, "local-a", "test", "lost-response", acquire("stable", "alice"))
			if err != nil || status != 200 {
				http.Error(w, "upstream failure", 502)
				return
			}
			var out outcome
			if json.Unmarshal(data, &out) != nil {
				http.Error(w, "bad JSON", 502)
				return
			}
			observed <- out
			conn, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				conn.Close()
			}
		}))
		defer proxy.Close()
		_, _, err := call(proxy.URL, "local-a", "test", "ignored", acquire("stable", "alice"))
		if err == nil {
			t.Fatal("expected connection loss")
		}
		var committed outcome
		select {
		case committed = <-observed:
		case <-time.After(5 * time.Second):
			t.Fatal("no confirmed upstream commit")
		}
		out := decode(t, mustCall(t, r.base, "local-a", "test", "lost-response", acquire("stable", "alice"), 200))
		assertSame(t, out.Replayed, true)
		assertSame(t, out.Grant, committed.Grant)
		assertSame(t, len(snapshot(t, r.base, "lost-response")), 4)
	})
	t.Run("precision_boundaries_and_overflow", func(t *testing.T) {
		key := "precision"
		const initial uint64 = 9007199254740990
		mustCall(t, r.base, "local-a", "test", key, object{"op": "seed", "fence": strconv.FormatUint(initial, 10), "revision": "9007199254740991"}, 200)
		for i := uint64(1); i <= 5; i++ {
			out := decode(t, mustCall(t, r.base, "local-a", "test", key, acquire(fmt.Sprintf("p%d", i), "alice"), 200))
			assertSame(t, out.Grant.Fence, strconv.FormatUint(initial+i, 10))
			assertSame(t, out.Grant.Revision, strconv.FormatUint(9007199254740990+2*i, 10))
			if i < 5 {
				mustCall(t, r.base, "local-a", "test", key, object{"op": "vacate"}, 200)
			}
		}
		for _, field := range []string{"fence", "revision"} {
			key := "overflow-" + field
			op := object{"op": "seed", "fence": "0", "revision": "0"}
			op[field] = "18446744073709551615"
			mustCall(t, r.base, "local-a", "test", key, op, 200)
			before := snapshot(t, r.base, key)
			out := decode(t, mustCall(t, r.base, "local-a", "test", key, acquire("exhausted", "alice"), 200))
			assertSame(t, out.Outcome, "exhausted")
			assertSame(t, snapshot(t, r.base, key), before)
		}
		mustCall(t, r.base, "local-a", "test", "max-u64", object{"op": "seed", "fence": "18446744073709551614", "revision": "0"}, 200)
		out := decode(t, mustCall(t, r.base, "local-a", "test", "max-u64", acquire("last", "alice"), 200))
		assertSame(t, out.Grant.Fence, "18446744073709551615")
		mustCall(t, r.base, "local-a", "test", "max-u64", object{"op": "vacate"}, 200)
		before := snapshot(t, r.base, "max-u64")
		out = decode(t, mustCall(t, r.base, "local-a", "test", "max-u64", acquire("too-far", "alice"), 200))
		assertSame(t, out.Outcome, "exhausted")
		assertSame(t, snapshot(t, r.base, "max-u64"), before)
		// A JSON number cannot silently enter the string-encoded counter path.
		mustCall(t, r.base, "local-a", "test", "unsafe-number", object{"op": "seed", "fence": 9007199254740992, "revision": "0"}, 400)
	})
	t.Run("tenant_and_tuple_isolation", func(t *testing.T) {
		for _, tenant := range []string{"local-a", "local-b"} {
			out := decode(t, mustCall(t, r.base, tenant, "test", "shared", acquire("a", "alice"), 200))
			assertSame(t, out.Replayed, false)
			assertSame(t, out.Grant.Fence, "1")
		}
		for _, scope := range [][2]string{{"a:b", "c"}, {"a", "b:c"}} {
			out := decode(t, mustCall(t, r.base, "local-a", scope[0], scope[1], acquire("a", "alice"), 200))
			assertSame(t, out.Outcome, "granted")
		}
		mustCall(t, r.base, "untrusted", "test", "shared", acquire("b", "mallory"), 401)
	})
	t.Run("kill_restart_persistence_and_replay", func(t *testing.T) {
		keys := []string{"precision", "max-u64", "dedupe", "lost-response", "empty-rollback", "overflow-revision"}
		before := map[string][]row{}
		for _, key := range keys {
			before[key] = snapshot(t, r.base, key)
		}
		for activation := 2; activation <= 3; activation++ {
			r.stop()
			r = start(t, root, state, configFile, activation, true)
			for _, key := range keys {
				assertSame(t, snapshot(t, r.base, key), before[key])
			}
			out := decode(t, mustCall(t, r.base, "local-a", "test", "precision", acquire("p5", "alice"), 200))
			assertSame(t, out.Replayed, true)
			assertSame(t, out.Grant.Fence, "9007199254740995")
			out = decode(t, mustCall(t, r.base, "local-a", "test", "lost-response", acquire("stable", "alice"), 200))
			assertSame(t, out.Replayed, true)
		}
		mustCall(t, r.base, "local-a", "test", "precision", object{"op": "vacate"}, 200)
		out := decode(t, mustCall(t, r.base, "local-a", "test", "precision", acquire("after-restart", "bob"), 200))
		assertSame(t, out.Grant.Fence, "9007199254740996")
		current := snapshot(t, r.base, "precision")
		out = decode(t, mustCall(t, r.base, "local-a", "test", "precision", acquire("p1", "alice"), 200))
		assertSame(t, out.Replayed, true)
		assertSame(t, out.Grant.Fence, "9007199254740991")
		assertSame(t, snapshot(t, r.base, "precision"), current)
	})
	r.stop()
}

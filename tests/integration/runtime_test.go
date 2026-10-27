//go:build darwin || linux

package integration

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"syscall"
	"testing"
	"time"
)

type object map[string]any

var client = &http.Client{Timeout: 15 * time.Second}

func assertSame(t *testing.T, got, want any) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, wanted %#v", got, want)
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
func start(t *testing.T, root, state, config string, activation int, variables ...string) *runtime {
	t.Helper()
	port := freePort(t)
	args := []string{filepath.Join(root, "node_modules/wrangler/bin/wrangler.js"), "dev", "--config", config, "--local", "--ip", "127.0.0.1", "--port", port, "--inspector-port", "0", "--persist-to", state, "--log-level", "info"}
	for _, variable := range variables {
		args = append(args, "--var", variable)
	}
	ready := false
	for _, variable := range variables {
		if variable == "P1_ENABLED:true" || variable == "P11_ENABLED:true" || variable == "P2_ENABLED:true" {
			ready = true
		}
	}
	cmd := exec.Command(filepath.Join(root, "bin/node"), args...)
	cmd.Dir = root
	// The startup banner otherwise checks npm when Wrangler's update cache expires.
	cmd.Env = append(os.Environ(), "WRANGLER_SEND_METRICS=false", "WRANGLER_HIDE_BANNER=true", "CI=true", "WRANGLER_LOG_PATH="+filepath.Join(filepath.Dir(state), fmt.Sprintf("wrangler-debug-%d.log", activation)))
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
			if (ready && res.StatusCode == 200) || (!ready && res.StatusCode == 404) {
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

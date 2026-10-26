//go:build (darwin || linux) && testhooks

package integration

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	pintle "pintle.local/spike/sdk/go/pintle"
	"strings"
	"testing"
	"time"
)

type processTransport struct{ role string }

func (p processTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("x-pintle-test-call", "process-"+p.role)
	return http.DefaultTransport.RoundTrip(r)
}
func TestP2ProcessHelper(t *testing.T) {
	role := os.Getenv("PINTLE_P2_CHILD")
	if role == "" {
		t.Skip("child process helper")
	}
	c := sdk(t, os.Getenv("PINTLE_P2_ENDPOINT"), os.Getenv("PINTLE_P2_CREDENTIAL"), &http.Client{Transport: processTransport{role}, Timeout: 20 * time.Second})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, err := c.OpenSession(ctx, pintle.Resource{Namespace: "test", Key: "two-processes"})
	if err != nil {
		t.Fatal(err)
	}
	if role == "owner" {
		req, _ := s.NewAcquire("owner", time.Second)
		g, err := c.TryAcquire(ctx, req)
		l := requireLease(t, g, err)
		fmt.Println("PINTLE owned " + l.FencingToken)
		scanner := bufio.NewScanner(os.Stdin)
		if !scanner.Scan() || scanner.Text() != "release" {
			t.Fatal("missing release signal")
		}
		rel, _ := s.NewRelease(*l)
		if _, err = c.Release(ctx, rel); err != nil {
			t.Fatal(err)
		}
		fmt.Println("PINTLE released")
	} else {
		req, _ := s.NewWaitAcquire("waiter", time.Second, 5*time.Second)
		r, err := c.Acquire(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		l := requireLease(t, r.Granted, nil)
		replay, err := c.Acquire(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		assertSame(t, *replay.Granted.Receipt, *r.Granted.Receipt)
		fmt.Println("PINTLE acquired " + l.FencingToken)
	}
}
func TestP2TwoProcesses(t *testing.T) {
	h := configuredSessionHarness(t, "P2_ENABLED:true")
	defer h.r.stop()
	type child struct {
		cmd   *exec.Cmd
		in    io.WriteCloser
		lines chan string
		done  chan error
	}
	startChild := func(role string) *child {
		cmd := exec.Command(os.Args[0], "-test.run=^TestP2ProcessHelper$", "-test.v")
		cmd.Env = append(os.Environ(), "PINTLE_P2_CHILD="+role, "PINTLE_P2_ENDPOINT="+h.r.base, "PINTLE_P2_CREDENTIAL="+h.a)
		in, err := cmd.StdinPipe()
		if err != nil {
			t.Fatal(err)
		}
		out, err := cmd.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		cmd.Stderr = cmd.Stdout
		if err = cmd.Start(); err != nil {
			t.Fatal(err)
		}
		c := &child{cmd, in, make(chan string, 16), make(chan error, 1)}
		go func() {
			scan := bufio.NewScanner(out)
			for scan.Scan() {
				c.lines <- scan.Text()
			}
			close(c.lines)
			c.done <- cmd.Wait()
		}()
		t.Cleanup(func() { in.Close(); _ = cmd.Process.Kill() })
		return c
	}
	read := func(c *child, want string) {
		t.Helper()
		timer := time.NewTimer(15 * time.Second)
		defer timer.Stop()
		for {
			select {
			case line, ok := <-c.lines:
				if !ok {
					t.Fatal("child exited before " + want)
				}
				if strings.HasPrefix(line, "PINTLE ") {
					assertSame(t, line, want)
					return
				}
			case <-timer.C:
				t.Fatal("child output watchdog")
			}
		}
	}
	owner := startChild("owner")
	read(owner, "PINTLE owned 1")
	registered := h.c.hold("process-waiter", "wait_registered")
	waiter := startChild("waiter")
	arrived(t, registered)
	fmt.Fprintln(owner.in, "release")
	read(owner, "PINTLE released")
	release(registered, directive{Now: 1000})
	read(waiter, "PINTLE acquired 2")
	for _, c := range []*child{owner, waiter} {
		select {
		case err := <-c.done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(15 * time.Second):
			t.Fatal("child exit watchdog")
		}
	}
}

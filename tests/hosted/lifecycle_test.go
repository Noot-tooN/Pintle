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
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	pintle "github.com/Noot-tooN/Pintle/sdk/go/pintle"
)

type stagingCredential struct {
	Token      string   `json:"token"`
	Tenant     string   `json:"tenant"`
	Subject    string   `json:"subject"`
	Namespaces []string `json:"namespaces"`
	NotBefore  string   `json:"not_before_ms,omitempty"`
	Expires    string   `json:"expires_at_ms"`
}

type authorityEvent struct {
	Event     string `json:"event"`
	Instance  string `json:"instance"`
	Operation string `json:"operation"`
	Status    int    `json:"status"`
}

// Only this strict allowlist escapes the tail process. Raw request metadata,
// headers, exceptions and unrelated log lines are never persisted or printed.
func parseAuthority(line string) (authorityEvent, bool) {
	var e authorityEvent
	start := strings.IndexByte(line, '{')
	if start < 0 {
		return e, false
	}
	raw := []byte(line[start:])
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || len(fields) != 4 || json.Unmarshal(raw, &e) != nil {
		return e, false
	}
	return e, e.Event == "pintle.authority" && regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(e.Instance) && (e.Operation == "fetch" || e.Operation == "alarm") && e.Status >= 100 && e.Status <= 599
}

type authorityTail struct {
	events chan authorityEvent
	done   chan struct{}
	stop   func()
}

func startTail(t *testing.T, root string) *authorityTail {
	t.Helper()
	cmd := exec.Command(filepath.Join(root, "bin/just"), "staging-tail")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "WRANGLER_WRITE_LOGS=false", "WRANGLER_HIDE_BANNER=true", "NO_COLOR=1")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	reader, writer := io.Pipe()
	cmd.Stdout = writer
	cmd.Stderr = writer
	trace := &authorityTail{events: make(chan authorityEvent, 256), done: make(chan struct{})}
	ready := make(chan struct{}, 1)
	require(t, cmd.Start())
	go func() { _ = cmd.Wait(); writer.Close(); close(trace.done) }()
	go func() {
		scanner := bufio.NewScanner(reader)
		scanner.Buffer(make([]byte, 4096), 256*1024)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.Contains(line, "Connected to pintle-staging, waiting for logs") {
				select {
				case ready <- struct{}{}:
				default:
				}
			}
			if event, ok := parseAuthority(line); ok {
				select {
				case trace.events <- event:
				case <-trace.done:
					return
				}
			}
		}
	}()
	trace.stop = sync.OnceFunc(func() {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGINT)
		select {
		case <-trace.done:
		case <-time.After(5 * time.Second):
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			<-trace.done
		}
		reader.Close()
	})
	t.Cleanup(trace.stop)
	select {
	case <-ready:
	case <-trace.done:
		t.Fatal("staging tail exited before readiness; raw logs discarded")
	case <-time.After(30 * time.Second):
		t.Fatal("staging tail readiness timeout")
	}
	return trace
}
func (tr *authorityTail) next(ctx context.Context, operation string) (authorityEvent, error) {
	for {
		select {
		case e := <-tr.events:
			if e.Status >= 500 {
				return e, fmt.Errorf("authority %s returned status %d", e.Operation, e.Status)
			}
			if e.Operation == operation {
				return e, nil
			}
		case <-tr.done:
			return authorityEvent{}, errors.New("staging tail exited; lifecycle evidence incomplete")
		case <-ctx.Done():
			return authorityEvent{}, ctx.Err()
		}
	}
}

type observedTransport struct {
	budget transport
	trace  *authorityTail
	last   string
}

func (w *observedTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	res, err := w.budget.RoundTrip(r)
	if err != nil {
		return res, err
	}
	// These statuses are known to reach the authority in these scenarios. Auth
	// rejection is at the gateway and deliberately produces no authority event.
	if res.StatusCode == 200 || res.StatusCode == 400 || res.StatusCode == 409 || res.StatusCode == 412 {
		// Drain the bounded unary response before awaiting post-invocation logs.
		// Waiting inside RoundTrip with an unread streaming body can prevent the
		// platform from finishing the invocation that emits those logs.
		if err := bufferResponse(res); err != nil {
			return nil, err
		}
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		e, err := w.trace.next(ctx, "fetch")
		if err != nil {
			res.Body.Close()
			return nil, fmt.Errorf("authority fetch telemetry after HTTP %d: %w", res.StatusCode, err)
		}
		w.last = e.Instance
	}
	return res, nil
}

func bufferResponse(res *http.Response) error {
	defer res.Body.Close()
	body, err := io.ReadAll(io.LimitReader(res.Body, 16385))
	if err != nil {
		return fmt.Errorf("read unary response: %w", err)
	}
	if len(body) > 16384 {
		return errors.New("hosted unary response exceeded 16 KiB")
	}
	res.Body = io.NopCloser(bytes.NewReader(body))
	return nil
}

func TestStagingLifecycle(t *testing.T) {
	if os.Getenv("PINTLE_RUN_HOSTED_LIFECYCLE") != "1" {
		t.Skip("remote lifecycle changes require explicit just test-staging-lifecycle")
	}
	root, e := filepath.Abs("../..")
	require(t, e)
	endpoint := os.Getenv("PINTLE_STAGING_URL")
	path := filepath.Join(root, ".hermit/staging/credentials.json")
	raw, e := os.ReadFile(path)
	require(t, e)
	var keys []stagingCredential
	require(t, json.Unmarshal(raw, &keys))
	if len(keys) != 1 || len(keys[0].Namespaces) != 1 || keys[0].Namespaces[0] != "staging" {
		t.Fatal("expected one existing staging-only credential")
	}
	require(t, validateTarget(endpoint, keys[0].Token))
	if !regexp.MustCompile(`^[a-f0-9]{32}$`).MatchString(os.Getenv("CLOUDFLARE_ACCOUNT_ID")) {
		t.Fatal("explicit Cloudflare account required")
	}
	var expiry int64
	_, e = fmt.Sscan(keys[0].Expires, &expiry)
	require(t, e)
	if time.Until(time.UnixMilli(expiry)) < 15*time.Minute {
		t.Fatal("existing credential needs at least 15 minutes remaining")
	}
	next := keys[0]
	next.Token, e = pintle.NewRequestID()
	require(t, e)
	// Rotation does not silently extend the existing authorization window.
	candidate := filepath.Join(root, ".hermit/staging/rotation-candidate.json")
	data, e := json.Marshal([]stagingCredential{next})
	require(t, e)
	file, e := os.OpenFile(candidate, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	require(t, e)
	_, e = file.Write(data)
	require(t, e)
	require(t, file.Close())
	t.Log("New credential prepared privately; expiry unchanged. A failed run retains both recovery files.")
	runJust := func(recipe string, input []byte) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, filepath.Join(root, "bin/just"), recipe)
		cmd.Dir = root
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM) }
		cmd.WaitDelay = 5 * time.Second
		cmd.Env = append(os.Environ(), "WRANGLER_WRITE_LOGS=false", "WRANGLER_HIDE_BANNER=true")
		cmd.Stdin = bytes.NewReader(input)
		output, err := cmd.CombinedOutput()
		// Defensive redaction also covers errors from a future Wrangler version.
		safe := strings.ReplaceAll(strings.ReplaceAll(string(output), keys[0].Token, "[REDACTED]"), next.Token, "[REDACTED]")
		t.Logf("%s:\n%s", recipe, safe)
		require(t, err)
	}
	runJust("staging-origin", nil)
	runJust("staging-deploy", nil)
	trace := startTail(t, root)
	wire := &observedTransport{trace: trace}
	defer func() { t.Logf("Lifecycle client transport attempts: %d (cap 96)", wire.budget.calls.Load()) }()
	httpClient := &http.Client{Transport: wire, Timeout: 40 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	var beforeReplacement string
	ctl := LifecycleControl{
		Replace:  func() string { beforeReplacement = wire.last; runJust("staging-deploy", nil); return endpoint },
		Replaced: func() bool { return wire.last != beforeReplacement },
		Rotate: func(newOnly bool) string {
			updated := []stagingCredential{keys[0], next}
			if newOnly {
				updated = []stagingCredential{next}
			}
			body, err := json.Marshal(updated)
			require(t, err)
			runJust("staging-secret", body)
			// A failed upload leaves the candidate for reconciliation rather than
			// assuming rollback or reactivating a key that may already be revoked.
			temp := path + ".next"
			require(t, os.WriteFile(temp, body, 0600))
			require(t, os.Rename(temp, path))
			if newOnly {
				require(t, os.Remove(candidate))
			}
			return endpoint
		},
	}
	state := RunLifecycle(t, endpoint, keys[0].Token, next.Token, httpClient, ctl)
	t.Log("Verified changed authority instance across redeployment")
	// A tail that spans several code/secret versions stopped delivering authority
	// events in hosted rehearsals. Use a fresh observer for the independent alarm
	// phase, before any alarm test work is admitted. Attaching tail can itself
	// replace instances; the idle comparison starts only after this attachment.
	trace.stop()
	trace = startTail(t, root)
	wire.trace = trace
	t.Log("Fresh tail attached after rotation, before alarm admissions")
	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Minute)
	defer cancel()
	s, c := state.Session, state.Client
	owner, e := s.NewAcquire("alarm-owner", 20*time.Second)
	require(t, e)
	g, e := c.TryAcquire(ctx, owner)
	require(t, e)
	active(t, g)
	wait, e := s.NewWaitAcquire("alarm-successor", time.Minute, time.Minute)
	require(t, e)
	pending, e := c.ResumeAcquire(ctx, wait)
	require(t, e)
	if pending.State != pintle.Pending {
		t.Fatal("expected Pending before quiet alarm window")
	}
	t.Log("No client RPCs until a successful hosted alarm is observed")
	alarmCtx, stop := context.WithTimeout(ctx, 45*time.Second)
	event, e := trace.next(alarmCtx, "alarm")
	stop()
	require(t, e)
	if event.Status != 200 {
		t.Fatal("alarm did not complete successfully")
	}
	st, e := c.GetAttemptStatus(ctx, wait.Acquisition.Resource, wait.Acquisition.RequestID, wait.Acquisition.Attempt)
	require(t, e)
	successor := *active(t, st.Acquired)
	greater(t, successor.FencingToken, g.Receipt.FencingToken)
	replay, e := c.ResumeAcquire(ctx, wait)
	require(t, e)
	sameReceipt(t, st.Acquired.Receipt, replay.Granted.Receipt)
	release, e := s.NewRelease(successor)
	require(t, e)
	_, e = c.Release(ctx, release)
	require(t, e)
	beforeIdle := wire.last
	t.Log("Idle cleanup: no client RPCs for the five-minute session-expiry alarm; waiting at most 6 minutes")
	// No live owner/waiter remains; the next alarm is session reclamation.
	// This single-resource rehearsal must run without other staging traffic.
	alarmCtx, stop = context.WithTimeout(ctx, 6*time.Minute)
	event, e = trace.next(alarmCtx, "alarm")
	stop()
	require(t, e)
	if event.Status != 200 {
		t.Fatal("idle alarm did not complete successfully")
	}
	if event.Instance == beforeIdle {
		t.Fatal("no observed instance replacement during idle window")
	}
	t.Log("Observed new authority instance on idle alarm, with no intervening client RPC")
	_, e = c.TryAcquire(ctx, state.Request)
	code(t, e, connect.CodeFailedPrecondition)
	fresh, e := c.OpenSession(ctx, state.Request.Resource)
	require(t, e)
	req, e := fresh.NewAcquire("after-session-expiry", time.Minute)
	require(t, e)
	after, e := c.TryAcquire(ctx, req)
	require(t, e)
	active(t, after)
	greater(t, after.Receipt.FencingToken, successor.FencingToken)
	greater(t, req.Attempt.SessionID, state.Request.Attempt.SessionID)
	rel, e := fresh.NewRelease(*after.Current)
	require(t, e)
	_, e = c.Release(ctx, rel)
	require(t, e)
	t.Logf("PASS: alarm progress, idle instance replacement, expired retry rejection, session/fence continuity; %d transport attempts; one new resource", wire.budget.calls.Load())
}

func TestAuthorityLogSanitization(t *testing.T) {
	good := `{"event":"pintle.authority","instance":"` + strings.Repeat("a", 64) + `","operation":"alarm","status":200}`
	if _, ok := parseAuthority("  (log) " + good); !ok {
		t.Fatal("valid metric rejected")
	}
	for _, bad := range []string{`Authorization: secret`, strings.Replace(good, `"status":200`, `"status":200,"token":"secret"`, 1), strings.Replace(good, `"alarm"`, `"secret"`, 1), strings.Replace(good, strings.Repeat("a", 64), "secret", 1)} {
		if _, ok := parseAuthority(bad); ok {
			t.Fatal("unsafe log accepted")
		}
	}
}

func TestBufferHostedResponse(t *testing.T) {
	for _, size := range []int{0, 16384, 16385} {
		body := &trackedBody{Reader: strings.NewReader(strings.Repeat("x", size))}
		res := &http.Response{Body: body}
		err := bufferResponse(res)
		if !body.closed {
			t.Fatal("upstream response was not closed before telemetry observation")
		}
		if size > 16384 {
			if err == nil {
				t.Fatal("oversized response accepted")
			}
			continue
		}
		require(t, err)
		got, err := io.ReadAll(res.Body)
		require(t, err)
		if len(got) != size {
			t.Fatal("buffering changed response")
		}
	}
}

type trackedBody struct {
	io.Reader
	closed bool
}

func (b *trackedBody) Close() error { b.closed = true; return nil }

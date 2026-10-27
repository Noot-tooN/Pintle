//go:build darwin || linux

package hosted

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	pintle "github.com/Noot-tooN/Pintle/sdk/go/pintle"
)

type persistedEvent struct {
	authorityEvent
	ID   string
	Time int64
}

type logQuery struct {
	client         *http.Client
	account, token string
	from           int64
	calls          int
}

// Queries the management API only: observing an alarm never invokes the Worker.
// Keep the full time window on each poll: ingestion can be delayed/out of order.
func (q *logQuery) snapshot(ctx context.Context) ([]persistedEvent, error) {
	if q.calls >= 90 {
		return nil, fmt.Errorf("persisted-log query budget exceeded")
	}
	q.calls++
	payload := map[string]any{"queryId": "pintle-alarm-rehearsal", "dry": true, "view": "events", "limit": 200,
		"timeframe":  map[string]int64{"from": q.from, "to": time.Now().UnixMilli()},
		"parameters": map[string]any{"filters": []any{map[string]string{"key": "$metadata.service", "operation": "eq", "type": "string", "value": "pintle-staging"}}}}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", "https://api.cloudflare.com/client/v4/accounts/"+q.account+"/workers/observability/telemetry/query", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+q.token)
	req.Header.Set("Content-Type", "application/json")
	res, err := q.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("persisted-log transport failed")
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return nil, fmt.Errorf("persisted-log query HTTP %d", res.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(res.Body, 2*1024*1024+1))
	if err != nil {
		return nil, fmt.Errorf("persisted-log response read failed")
	}
	if len(data) > 2*1024*1024 {
		return nil, fmt.Errorf("persisted-log response too large")
	}
	return decodePersisted(data, q.from)
}

// Unknown/raw provider metadata is discarded. Never print API bodies or errors.
func decodePersisted(data []byte, from int64) ([]persistedEvent, error) {
	var envelope struct {
		Success bool `json:"success"`
		Result  struct {
			Events struct {
				Count  int `json:"count"`
				Events []struct {
					Source    json.RawMessage `json:"source"`
					Timestamp int64           `json:"timestamp"`
					Metadata  struct {
						ID      string `json:"id"`
						Service string `json:"service"`
					} `json:"$metadata"`
				} `json:"events"`
			} `json:"events"`
		} `json:"result"`
	}
	if json.Unmarshal(data, &envelope) != nil || !envelope.Success {
		return nil, fmt.Errorf("invalid persisted-log response")
	}
	rows := envelope.Result.Events
	if rows.Count >= 200 || len(rows.Events) >= 200 {
		return nil, fmt.Errorf("log window truncated or unexpected staging traffic")
	}
	result := []persistedEvent{}
	seen := map[string]persistedEvent{}
	for _, row := range rows.Events {
		if row.Metadata.Service != "pintle-staging" {
			return nil, fmt.Errorf("unexpected telemetry service")
		}
		var kind struct {
			Event string `json:"event"`
		}
		if json.Unmarshal(row.Source, &kind) != nil {
			return nil, fmt.Errorf("invalid telemetry source")
		}
		if kind.Event != "pintle.authority" {
			continue
		}
		event, ok := parseAuthority(string(row.Source))
		if !ok || row.Metadata.ID == "" || row.Timestamp < from {
			return nil, fmt.Errorf("invalid authority telemetry")
		}
		if event.Status != 200 {
			return nil, fmt.Errorf("authority telemetry status %d", event.Status)
		}
		e := persistedEvent{event, row.Metadata.ID, row.Timestamp}
		if prior, ok := seen[e.ID]; ok {
			if prior != e {
				return nil, fmt.Errorf("conflicting telemetry event")
			}
			continue
		}
		seen[e.ID] = e
		result = append(result, e)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Time == result[j].Time {
			return result[i].ID < result[j].ID
		}
		return result[i].Time < result[j].Time
	})
	return result, nil
}

func (q *logQuery) until(t *testing.T, ctx context.Context, predicate func([]persistedEvent) bool) []persistedEvent {
	t.Helper()
	for {
		events, err := q.snapshot(ctx)
		require(t, err)
		if predicate(events) {
			return events
		}
		timer := time.NewTimer(5 * time.Second)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			t.Fatal("persisted telemetry observation deadline exceeded")
		}
	}
}
func eventsOf(events []persistedEvent, operation string) []persistedEvent {
	var out []persistedEvent
	for _, e := range events {
		if e.Operation == operation {
			out = append(out, e)
		}
	}
	return out
}
func privateFile(t *testing.T, path string) []byte {
	t.Helper()
	st, e := os.Stat(path)
	require(t, e)
	if !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 {
		t.Fatal("credential file must be private and regular")
	}
	data, e := os.ReadFile(path)
	require(t, e)
	return data
}

func TestStagingAlarms(t *testing.T) {
	if os.Getenv("PINTLE_RUN_HOSTED_ALARMS") != "1" {
		t.Skip("requires explicit just test-staging-alarms")
	}
	root, e := filepath.Abs("../..")
	require(t, e)
	account := os.Getenv("CLOUDFLARE_ACCOUNT_ID")
	if !regexp.MustCompile(`^[a-f0-9]{32}$`).MatchString(account) {
		t.Fatal("explicit account required")
	}
	var keys []stagingCredential
	require(t, json.Unmarshal(privateFile(t, filepath.Join(root, ".hermit/staging/credentials.json")), &keys))
	if len(keys) != 1 || len(keys[0].Namespaces) != 1 || keys[0].Namespaces[0] != "staging" {
		t.Fatal("expected one staging-only application key")
	}
	require(t, validateTarget(os.Getenv("PINTLE_STAGING_URL"), keys[0].Token))
	var expiry int64
	_, e = fmt.Sscan(keys[0].Expires, &expiry)
	require(t, e)
	if time.Until(time.UnixMilli(expiry)) < 15*time.Minute {
		t.Fatal("application credential needs 15 minutes remaining")
	}
	logsToken := strings.TrimSpace(string(privateFile(t, filepath.Join(root, ".hermit/staging/observability-token"))))
	if logsToken == "" {
		t.Fatal("missing observability token")
	}
	noRedirect := func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	q := &logQuery{client: &http.Client{Timeout: 15 * time.Second, CheckRedirect: noRedirect}, account: account, token: logsToken, from: time.Now().Add(-time.Second).UnixMilli()}
	initial, e := q.snapshot(ctx)
	require(t, e)
	if len(initial) != 0 {
		t.Fatal("staging must be quiet before the isolated alarm rehearsal")
	}
	wire := &transport{}
	defer func() {
		t.Logf("Bounded usage: %d client RPC attempts (cap 96), %d log queries (cap 90), one resource", wire.calls.Load(), q.calls)
	}()
	c, e := pintle.New(os.Getenv("PINTLE_STAGING_URL"), keys[0].Token, &http.Client{Timeout: 15 * time.Second, Transport: wire, CheckRedirect: noRedirect})
	require(t, e)
	id, e := pintle.NewRequestID()
	require(t, e)
	resource := pintle.Resource{Namespace: "staging", Key: "alarms-" + id}
	s, e := c.OpenSession(ctx, resource)
	require(t, e)
	owner, e := s.NewAcquire("alarm-owner", time.Minute)
	require(t, e)
	g, e := c.TryAcquire(ctx, owner)
	require(t, e)
	active(t, g)
	wait, e := s.NewWaitAcquire("alarm-successor", time.Minute, time.Minute)
	require(t, e)
	pending, e := c.ResumeAcquire(ctx, wait)
	require(t, e)
	if pending.State != pintle.Pending {
		t.Fatal("expected observed Pending admission")
	}
	t.Log("Pending admitted; no client RPCs until the persisted expiry alarm is observed")
	alarmCtx, stop := context.WithTimeout(ctx, 90*time.Second)
	observed := q.until(t, alarmCtx, func(events []persistedEvent) bool {
		return len(eventsOf(events, "fetch")) >= 3 && len(eventsOf(events, "alarm")) >= 1
	})
	stop()
	fetches, alarms := eventsOf(observed, "fetch"), eventsOf(observed, "alarm")
	if len(fetches) != 3 || len(alarms) != 1 || alarms[0].Time <= fetches[2].Time {
		t.Fatal("unexpected traffic or alarm order")
	}
	st, e := c.GetAttemptStatus(ctx, resource, wait.Acquisition.RequestID, wait.Acquisition.Attempt)
	require(t, e)
	successor := *active(t, st.Acquired)
	greater(t, successor.FencingToken, g.Receipt.FencingToken)
	// The grant timestamp derived from its immutable TTL must belong to the
	// observed alarm, not a later request-driven transition (allow log timestamp skew).
	grantAt := int64(successor.ExpiresAtUnixMS) - time.Minute.Milliseconds()
	if grantAt < int64(g.Receipt.ExpiresAtUnixMS) || grantAt > alarms[0].Time+1000 {
		t.Fatal("successor grant does not match expiry alarm")
	}
	replay, e := c.ResumeAcquire(ctx, wait)
	require(t, e)
	sameReceipt(t, st.Acquired.Receipt, replay.Granted.Receipt)
	rel, e := s.NewRelease(successor)
	require(t, e)
	_, e = c.Release(ctx, rel)
	require(t, e)
	baselineCtx, stop := context.WithTimeout(ctx, 45*time.Second)
	observed = q.until(t, baselineCtx, func(events []persistedEvent) bool { return len(eventsOf(events, "fetch")) >= 6 })
	stop()
	fetches = eventsOf(observed, "fetch")
	if len(fetches) != 6 {
		t.Fatal("unexpected RPC traffic before idle window")
	}
	beforeIdle := fetches[5]
	t.Logf("Alarm-only grant verified; no client RPCs during idle cleanup (last fetch %s)", time.UnixMilli(beforeIdle.Time).UTC().Format(time.RFC3339Nano))
	idleCtx, stop := context.WithTimeout(ctx, 6*time.Minute)
	observed = q.until(t, idleCtx, func(events []persistedEvent) bool { return len(eventsOf(events, "alarm")) >= 2 })
	stop()
	fetches, alarms = eventsOf(observed, "fetch"), eventsOf(observed, "alarm")
	if len(fetches) != 6 || len(alarms) != 2 {
		t.Fatal("unexpected traffic during idle observation")
	}
	idle := alarms[1]
	if idle.Time-beforeIdle.Time < 290000 || idle.Instance == beforeIdle.Instance {
		t.Fatal("idle alarm did not demonstrate a quiet interval and authority reconstruction")
	}
	t.Logf("Persisted idle alarm: new authority instance after %d ms without client RPCs", idle.Time-beforeIdle.Time)
	_, e = c.TryAcquire(ctx, owner)
	code(t, e, connect.CodeFailedPrecondition)
	fresh, e := c.OpenSession(ctx, resource)
	require(t, e)
	req, e := fresh.NewAcquire("after-idle", time.Minute)
	require(t, e)
	after, e := c.TryAcquire(ctx, req)
	require(t, e)
	active(t, after)
	greater(t, after.Receipt.FencingToken, successor.FencingToken)
	greater(t, req.Attempt.SessionID, owner.Attempt.SessionID)
	finalRelease, e := fresh.NewRelease(*after.Current)
	require(t, e)
	_, e = c.Release(ctx, finalRelease)
	require(t, e)
	t.Log("PASS: alarm-only handoff, idle reconstruction, expired-session rejection and monotonic session/fencing history")
}

func TestPersistedEventDecoding(t *testing.T) {
	row := func(id string, at int64, operation string) map[string]any {
		return map[string]any{
			"source":    map[string]any{"event": "pintle.authority", "instance": strings.Repeat("a", 64), "operation": operation, "status": 200},
			"timestamp": at, "$metadata": map[string]any{"id": id, "service": "pintle-staging", "requestHeaders": "must-not-escape"}}
	}
	encode := func(rows []map[string]any, count int) []byte {
		b, e := json.Marshal(map[string]any{"success": true, "result": map[string]any{"events": map[string]any{"count": count, "events": rows}}})
		require(t, e)
		return b
	}
	earlier, later := row("first", 100, "fetch"), row("second", 200, "alarm")
	events, e := decodePersisted(encode([]map[string]any{later, earlier, earlier}, 3), 100)
	require(t, e)
	if len(events) != 2 || events[0].ID != "first" || events[1].Operation != "alarm" {
		t.Fatal("out-of-order or duplicate events mishandled")
	}
	out, e := json.Marshal(events)
	require(t, e)
	if strings.Contains(string(out), "must-not-escape") {
		t.Fatal("raw provider metadata escaped")
	}
	// A later poll can contain an earlier event that arrived late; no cursor drops it.
	first, e := decodePersisted(encode([]map[string]any{later}, 1), 100)
	require(t, e)
	if len(first) != 1 || len(events) != 2 {
		t.Fatal("delayed snapshot lost an event")
	}
	for _, name := range []string{"truncated", "wrong-service", "old-event", "extra-field", "error-status", "invalid-instance", "conflicting-id", "malformed", "api-failure"} {
		t.Run(name, func(t *testing.T) {
			r := row("id", 100, "fetch")
			rows := []map[string]any{r}
			count := 1
			switch name {
			case "truncated":
				count = 200
			case "wrong-service":
				r["$metadata"].(map[string]any)["service"] = "other-worker"
			case "old-event":
				r["timestamp"] = 99
			case "extra-field":
				r["source"].(map[string]any)["credential"] = "must-not-escape"
			case "error-status":
				r["source"].(map[string]any)["status"] = 500
			case "invalid-instance":
				r["source"].(map[string]any)["instance"] = "must-not-escape"
			case "conflicting-id":
				rows = append(rows, row("id", 101, "alarm"))
			}
			b := encode(rows, count)
			if name == "malformed" {
				b = []byte("must-not-escape")
			}
			if name == "api-failure" {
				b = []byte(`{"success":false,"errors":["must-not-escape"]}`)
			}
			_, e := decodePersisted(b, 100)
			if e == nil {
				t.Fatal("invalid observation accepted")
			}
			if strings.Contains(e.Error(), "must-not-escape") {
				t.Fatal("raw payload escaped through error")
			}
		})
	}
}

func TestPersistedQueryBudget(t *testing.T) {
	q := &logQuery{calls: 90}
	if _, e := q.snapshot(context.Background()); e == nil {
		t.Fatal("query budget did not fail before network access")
	}
}

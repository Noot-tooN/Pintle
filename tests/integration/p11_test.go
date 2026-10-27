//go:build darwin || linux

package integration

import (
	"connectrpc.com/connect"
	"context"
	"encoding/json"
	"os/exec"
	"path/filepath"
	pintle "pintle.local/spike/sdk/go/pintle"
	"strconv"
	"sync"
	"testing"
	"time"
)

// Read-only local SQLite evidence via Node's bundled sqlite API. No test endpoint
// is exposed by the service, and no extra package/global runtime is needed.
func sessionStats(t *testing.T, root, state string) []object {
	t.Helper()
	script := `import{DatabaseSync}from'node:sqlite';import{readdirSync}from'node:fs';import{join}from'node:path';
 const out=[];function walk(p){for(const e of readdirSync(p,{withFileTypes:true})){const f=join(p,e.name);if(e.isDirectory())walk(f);else if(e.name.endsWith('.sqlite')){const db=new DatabaseSync(f,{readOnly:true});if(db.prepare("SELECT count(*) AS n FROM sqlite_master WHERE name='records'").get().n){const rows=db.prepare("SELECT key,value FROM records").all();let sessions=0,receipts=0,pending=0;for(const r of rows){if(r.key.startsWith('session/')&&r.value!=='null'){sessions++;const entries=Object.values(JSON.parse(r.value).entries);receipts+=entries.length;pending+=entries.filter(c=>c.waiting?.state==="Pending").length;}}out.push({rows:rows.length,sessions,receipts,pending,bytes:rows.reduce((n,r)=>n+r.key.length+r.value.length,0),allocated:db.prepare('PRAGMA page_count').get().page_count*db.prepare('PRAGMA page_size').get().page_size,fence:rows.find(r=>r.key==='fence')?.value??'0'});}db.close();}}}walk(process.argv[1]);console.log(JSON.stringify(out));`
	cmd := exec.Command(filepath.Join(root, "bin/node"), "--no-warnings", "--input-type=module", "-e", script, state)
	data, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	var result []object
	if err = json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	return result
}
func TestP11SDK(t *testing.T) {
	root, state, config, a, b, r := leaseSetup(t, "P11_ENABLED:true")
	c := sdk(t, r.base, a, client)
	ctx := context.Background()
	t.Run("bounded_history_and_explicit_ack", func(t *testing.T) {
		resource := pintle.Resource{Namespace: "test", Key: "recycled"}
		s, err := c.OpenSession(ctx, resource)
		if err != nil {
			t.Fatal(err)
		}
		request, err := s.NewAcquire("worker", 60*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		g, err := c.TryAcquire(ctx, request)
		l := requireLease(t, g, err)
		status, err := s.Status(ctx, request.RequestID, request.Attempt)
		if err != nil {
			t.Fatal(err)
		}
		assertSame(t, status.Acquired.Receipt.FencingToken, l.FencingToken)
		if err = s.Heartbeat(ctx); err != nil {
			t.Fatal(err)
		}
		if err = s.Acknowledge(ctx, request.Attempt.Sequence); err != nil {
			t.Fatal(err)
		}
		_, err = c.TryAcquire(ctx, request)
		expectCode(t, err, connect.CodeFailedPrecondition)
		for i := 0; i < 1100; i++ {
			renew, err := s.NewRenew(*l, 60*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			g, err = c.Renew(ctx, renew)
			l = requireLease(t, g, err)
			if err = s.Acknowledge(ctx, renew.Attempt.Sequence); err != nil {
				t.Fatal(err)
			}
			if i == 31 || i == 1099 {
				stats := sessionStats(t, root, state)
				t.Logf("retained SQLite evidence at renewal %d: %v", i+1, stats)
				assertSame(t, len(stats), 1)
				assertSame(t, stats[0]["receipts"], float64(0))
				assertSame(t, stats[0]["rows"], float64(6))
			}
		}
		assertSame(t, l.FencingToken, "1")
		assertSame(t, l.Revision, "1101")
		release, err := s.NewRelease(*l)
		if err != nil {
			t.Fatal(err)
		}
		released, err := c.Release(ctx, release)
		if err != nil {
			t.Fatal(err)
		}
		assertSame(t, released.Revision, "1102")
		if err = s.Acknowledge(ctx, release.Attempt.Sequence); err != nil {
			t.Fatal(err)
		}
		next, err := s.NewAcquire("worker", 60*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		g, err = c.TryAcquire(ctx, next)
		l = requireLease(t, g, err)
		assertSame(t, l.FencingToken, "2")
	})
	t.Run("concurrent_duplicate_and_conflict", func(t *testing.T) {
		s, err := c.OpenSession(ctx, pintle.Resource{Namespace: "test", Key: "concurrent"})
		if err != nil {
			t.Fatal(err)
		}
		req, err := s.NewAcquire("worker", 60*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		gate := make(chan struct{})
		results := make(chan *pintle.GrantResult, 16)
		errs := make(chan error, 16)
		for i := 0; i < 16; i++ {
			wg.Add(1)
			go func() { defer wg.Done(); <-gate; g, e := c.TryAcquire(ctx, req); results <- g; errs <- e }()
		}
		close(gate)
		wg.Wait()
		close(results)
		close(errs)
		for e := range errs {
			if e != nil {
				t.Fatal(e)
			}
		}
		fresh := 0
		for g := range results {
			if !g.Replayed {
				fresh++
			}
		}
		assertSame(t, fresh, 1)
		changed := req
		changed.TTL = time.Second
		_, err = c.TryAcquire(ctx, changed)
		expectCode(t, err, connect.CodeAlreadyExists)
		_, err = sdk(t, r.base, b, client).TryAcquire(ctx, req)
		expectCode(t, err, connect.CodePermissionDenied)
	})
	t.Run("restart_preserves_unacknowledged_and_ack_boundary", func(t *testing.T) {
		resource := pintle.Resource{Namespace: "test", Key: "restart"}
		s, err := c.OpenSession(ctx, resource)
		if err != nil {
			t.Fatal(err)
		}
		req, _ := s.NewAcquire("worker", 60*time.Second)
		g, err := c.TryAcquire(ctx, req)
		before := requireLease(t, g, err)
		r.stop()
		r = start(t, root, state, config, 1, "P11_ENABLED:true")
		c = sdk(t, r.base, a, client)
		g, err = c.TryAcquire(ctx, req)
		after := requireLease(t, g, err)
		assertSame(t, *after, *before)
		assertSame(t, g.Replayed, true)
		// Persist ACK via the generated client-independent prepared metadata.
		next := req
		next.RequestID = "next"
		next.Attempt = &pintle.Attempt{SessionID: req.Attempt.SessionID, Sequence: 2, AcknowledgedThrough: 1}
		g, err = c.TryAcquire(ctx, next)
		if err != nil {
			t.Fatal(err)
		}
		assertSame(t, g.Contended, true)
		r.stop()
		r = start(t, root, state, config, 2, "P11_ENABLED:true")
		c = sdk(t, r.base, a, client)
		_, err = c.TryAcquire(ctx, req)
		expectCode(t, err, connect.CodeFailedPrecondition)
		status, err := c.GetAttemptStatus(ctx, resource, next.RequestID, next.Attempt)
		if err != nil {
			t.Fatal(err)
		}
		assertSame(t, status.Acquired.Contended, true)
		if err = c.AcknowledgeAttempt(ctx, resource, *next.Attempt); err != nil {
			t.Fatal(err)
		}
		_, err = c.GetAttemptStatus(ctx, resource, next.RequestID, next.Attempt)
		expectCode(t, err, connect.CodeFailedPrecondition)
		assertSame(t, strconv.FormatUint(after.ExpiresAtUnixMS, 10), strconv.FormatUint(before.ExpiresAtUnixMS, 10))
	})
	r.stop()
}

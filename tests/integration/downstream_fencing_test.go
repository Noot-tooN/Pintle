//go:build (darwin || linux) && !testhooks

package integration

import (
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	pintle "github.com/Noot-tooN/Pintle/sdk/go/pintle"
)

// Set PINTLE_TEST_POSTGRES_CONTAINER to a disposable local PostgreSQL container.
// The test creates its own database and uses a fresh connection for each write.
func TestDownstreamPostgresFencing(t *testing.T) {
	container := os.Getenv("PINTLE_TEST_POSTGRES_CONTAINER")
	if container == "" {
		t.Skip("requires a disposable PostgreSQL container")
	}
	sql := func(t *testing.T, db, query string, vars ...string) string {
		t.Helper()
		args := []string{"exec", "-i", container, "psql", "-XqAt", "-U", "postgres", "-d", db, "-v", "ON_ERROR_STOP=1"}
		for _, v := range vars {
			args = append(args, "-v", v)
		}
		cmd := exec.CommandContext(t.Context(), "docker", args...)
		cmd.Stdin = strings.NewReader(query)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("psql: %v: %s", err, out)
		}
		return strings.TrimSpace(string(out))
	}
	id, err := pintle.NewRequestID()
	if err != nil {
		t.Fatal(err)
	}
	db := "fence_test_" + id[:24]
	sql(t, "postgres", "CREATE DATABASE "+db)
	defer sql(t, "postgres", "DROP DATABASE "+db)
	sql(t, db, `CREATE TABLE protected_work (
		tenant text, namespace text, primitive text, resource text, slot integer,
		fence numeric(20,0) NOT NULL, payload text NOT NULL,
		PRIMARY KEY (tenant, namespace, primitive, resource, slot)
	)`)

	const tenant = "pilot-test"
	credential := token(t)
	_, _, _, r := staticSetup(t, []object{staticKey(credential, tenant, "writer")})
	defer r.stop()
	mutex := sdk(t, r.base, credential, client)
	semaphore, err := mutex.Semaphore(2)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		client *pintle.Client
	}{{"lock", mutex}, {"semaphore", semaphore}} {
		t.Run(tc.name, func(t *testing.T) {
			session, err := tc.client.OpenSession(t.Context(), pintle.Resource{Namespace: "test", Key: "protected-work"})
			if err != nil {
				t.Fatal(err)
			}
			grant := func(holder string, ttl time.Duration) *pintle.Lease {
				t.Helper()
				req, err := session.NewAcquire(holder, ttl)
				if err != nil {
					t.Fatal(err)
				}
				result, err := tc.client.TryAcquire(t.Context(), req)
				return requireLease(t, result, err)
			}
			scope := func(l *pintle.Lease) []string {
				return []string{"tenant=" + tenant, "ns=" + l.Resource.Namespace, "primitive=" + tc.name,
					"resource=" + l.Resource.Key, "slot=" + strconv.FormatUint(uint64(l.PermitSlot), 10)}
			}
			write := func(l *pintle.Lease, payload, want string) {
				t.Helper()
				vars := append(scope(l), "fence="+l.FencingToken, "payload="+payload)
				got := sql(t, db, `INSERT INTO protected_work VALUES
					(:'tenant', :'ns', :'primitive', :'resource', :'slot', :'fence', :'payload')
					ON CONFLICT (tenant, namespace, primitive, resource, slot) DO UPDATE
					SET fence = excluded.fence, payload = excluded.payload
					WHERE protected_work.fence <= excluded.fence
					RETURNING payload`, vars...)
				assertSame(t, got, want)
			}
			checkStored := func(l *pintle.Lease, payload string) {
				t.Helper()
				got := sql(t, db, `SELECT fence::text || '/' || payload FROM protected_work
					WHERE (tenant, namespace, primitive, resource, slot) =
					(:'tenant', :'ns', :'primitive', :'resource', :'slot')`, scope(l)...)
				assertSame(t, got, l.FencingToken+"/"+payload)
			}
			var peer *pintle.Lease
			if tc.name == "semaphore" {
				peer = grant("peer", time.Minute)
				write(peer, "peer-first", "peer-first")
			}
			old := grant("old", 2*time.Second)
			write(old, "first", "first")
			time.Sleep(time.Until(time.UnixMilli(int64(old.ExpiresAtUnixMS)).Add(50 * time.Millisecond)))
			next := grant("successor", time.Minute)
			assertSame(t, next.PermitSlot, old.PermitSlot)
			if peer != nil && peer.PermitSlot == old.PermitSlot {
				t.Fatal("concurrent grants share a slot")
			}

			// Expiry and regrant alone cannot advance the destination's fence.
			write(old, "before-successor-write", "before-successor-write")
			write(next, "successor-write", "successor-write")
			write(old, "late", "")
			checkStored(next, "successor-write")
			t.Logf("slot %d: fence %s replaced %s; late write affected zero rows", next.PermitSlot, next.FencingToken, old.FencingToken)
			if peer != nil {
				peerFence, err := strconv.ParseUint(peer.FencingToken, 10, 64)
				nextFence, nextErr := strconv.ParseUint(next.FencingToken, 10, 64)
				if err != nil || nextErr != nil || peerFence >= nextFence {
					t.Fatal("expected the still-active peer to have a lower fence than the successor")
				}
				write(peer, "peer-again", "peer-again")
				checkStored(peer, "peer-again")
				t.Logf("slot %d: concurrent peer still wrote with fence %s", peer.PermitSlot, peer.FencingToken)
			}
		})
	}
}

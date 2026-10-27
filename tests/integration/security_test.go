//go:build darwin || linux

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/Noot-tooN/Pintle/gen/go/pintle/v1"
	"github.com/Noot-tooN/Pintle/gen/go/pintle/v1/pintlev1connect"
	pintle "github.com/Noot-tooN/Pintle/sdk/go/pintle"
)

// A capability and holder label alone cannot cross the authenticated principal
// boundary. The attacker knows the full request/lease here, not merely its ID.
func TestSecurityP2PrincipalBoundaries(t *testing.T) {
	a, b := token(t), token(t)
	_, _, _, runtime := staticSetup(t, []object{staticKey(a, "tenant", "alice"), staticKey(b, "tenant", "bob")})
	defer runtime.stop()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	resource := pintle.Resource{Namespace: "test", Key: "principal-boundary"}
	alice, bob := sdk(t, runtime.base, a, client), sdk(t, runtime.base, b, client)
	ownerSession, err := alice.OpenSession(ctx, resource)
	if err != nil {
		t.Fatal(err)
	}
	acquire, err := ownerSession.NewAcquire("bob", time.Minute) // holder label is not identity
	if err != nil {
		t.Fatal(err)
	}
	g, err := alice.TryAcquire(ctx, acquire)
	lease := *requireLease(t, g, err)
	attackerSession, err := bob.OpenSession(ctx, resource)
	if err != nil {
		t.Fatal(err)
	}

	_, err = bob.TryAcquire(ctx, acquire)
	expectCode(t, err, connect.CodePermissionDenied)
	_, err = bob.GetAttemptStatus(ctx, resource, acquire.RequestID, acquire.Attempt)
	expectCode(t, err, connect.CodePermissionDenied)
	err = bob.AcknowledgeAttempt(ctx, resource, *acquire.Attempt)
	expectCode(t, err, connect.CodePermissionDenied)

	renew, err := attackerSession.NewRenew(lease, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	_, err = bob.Renew(ctx, renew)
	expectCode(t, err, connect.CodePermissionDenied)
	// Use Alice's actual saved envelope, so trusting this forged principal would
	// disclose her committed grant instead of merely failing on Bob's session.
	request := connect.NewRequest(&v1.TryAcquireRequest{
		Resource:  &v1.ResourceId{Namespace: resource.Namespace, Key: resource.Key},
		RequestId: acquire.RequestID, HolderRef: acquire.HolderRef, LeaseTtlMs: 60000,
		Attempt: &v1.RequestAttempt{SessionId: acquire.Attempt.SessionID, Sequence: fmt.Sprint(acquire.Attempt.Sequence), AcknowledgedThrough: fmt.Sprint(acquire.Attempt.AcknowledgedThrough)},
	})
	request.Header().Set("Authorization", "Bearer "+b)
	request.Header().Set("X-Pintle-Subject", "alice")
	_, err = pintlev1connect.NewLockServiceClient(client, runtime.base).TryAcquire(ctx, request)
	expectCode(t, err, connect.CodePermissionDenied)
	release, err := attackerSession.NewRelease(lease)
	if err != nil {
		t.Fatal(err)
	}
	_, err = bob.Release(ctx, release)
	expectCode(t, err, connect.CodePermissionDenied)

	wait, err := ownerSession.NewWaitAcquire("waiting", time.Minute, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	_, err = bob.CancelPending(ctx, wait)
	expectCode(t, err, connect.CodePermissionDenied)
	// The real principal can settle the unsent intent; attacker cancellation did not write it.
	cancelled, err := alice.CancelPending(ctx, wait)
	if err != nil || cancelled.State != pintle.Cancelled {
		t.Fatalf("owner cancellation: %v", err)
	}
	status, err := alice.GetAttemptStatus(ctx, resource, acquire.RequestID, acquire.Attempt)
	if err != nil {
		t.Fatal(err)
	}
	assertSame(t, status.Acquired.State, pintle.Active)
	assertSame(t, *status.Acquired.Current, lease)
	code, _, _ := rawRPC(t, runtime.base, b, "POST", "/internal/p1/Renew", nil, map[string]string{"X-Pintle-Subject": "alice"})
	assertSame(t, code, 404)
	release, err = ownerSession.NewRelease(lease)
	if err != nil {
		t.Fatal(err)
	}
	_, err = alice.Release(ctx, release)
	if err != nil {
		t.Fatal(err)
	}
	if err = ownerSession.Acknowledge(ctx, release.Attempt.Sequence); err != nil {
		t.Fatal(err)
	}
}

func TestSecurityConfigurationBoundaries(t *testing.T) {
	t.Run("missing_auth_mode_never_selects_test_secret", func(t *testing.T) {
		credential := token(t)
		root, state, config, r := staticSetup(t, []object{staticKey(credential, "tenant", "alice")})
		r.stop()
		// Remove any explicit fixture mode from the local configuration.
		data, err := os.ReadFile(config)
		if err != nil {
			t.Fatal(err)
		}
		var cfg object
		if err = json.Unmarshal(data, &cfg); err != nil {
			t.Fatal(err)
		}
		delete(cfg, "vars")
		data, err = json.Marshal(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(config, data, 0600); err != nil {
			t.Fatal(err)
		}
		key := staticKey(credential, "tenant", "alice")
		delete(key, "expires_at_ms")
		data, err = json.Marshal([]object{key})
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(filepath.Dir(state), ".dev.vars"), []byte("PINTLE_TEST_CREDENTIALS='"+string(data)+"'\n"), 0600); err != nil {
			t.Fatal(err)
		}
		r = start(t, root, state, config, 1, "P2_ENABLED:true")
		defer r.stop()
		_, err = sdk(t, r.base, credential, client).OpenSession(context.Background(), pintle.Resource{Namespace: "test", Key: "private"})
		expectCode(t, err, connect.CodeUnavailable)
		// The default still accepts properly configured static credentials.
		r.stop()
		writeStatic(t, state, []object{staticKey(credential, "tenant", "alice")})
		r = start(t, root, state, config, 2, "P2_ENABLED:true")
		defer r.stop()
		if _, err = sdk(t, r.base, credential, client).OpenSession(context.Background(), pintle.Resource{Namespace: "test", Key: "private"}); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("removed_fixture_routes_stay_absent", func(t *testing.T) {
		credential := token(t)
		_, _, _, r := staticSetup(t, []object{staticKey(credential, "tenant", "alice")}, "LOCAL_SPIKE:true", "P0B_ENABLED:true")
		defer r.stop()
		for _, operation := range []string{"seed", "snapshot", "vacate", "grant"} {
			status, _, _ := rawRPC(t, r.base, "local-a", "POST", "/spike", strings.NewReader(`{"namespace":"test","key":"fixture","operation":{"op":"`+operation+`"}}`), nil)
			assertSame(t, status, 404)
		}
		status, _, _ := rawRPC(t, r.base, credential, "POST", "/internal/p0b/try-acquire", nil, nil)
		assertSame(t, status, 404)
		// Obsolete variables cannot interfere with the actual service either.
		_, err := sdk(t, r.base, credential, client).OpenSession(context.Background(), pintle.Resource{Namespace: "test", Key: "available"})
		if err != nil {
			t.Fatal(err)
		}
	})
}

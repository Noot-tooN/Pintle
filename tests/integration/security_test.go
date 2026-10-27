//go:build darwin || linux

package integration

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"connectrpc.com/connect"
	pintle "pintle.local/spike/sdk/go/pintle"
)

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

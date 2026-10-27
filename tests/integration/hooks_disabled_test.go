//go:build (darwin || linux) && !testhooks

package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"connectrpc.com/connect"
	v1 "github.com/Noot-tooN/Pintle/gen/go/pintle/v1/pintlev1connect"
)

// This runs against the normal Wasm before the instrumented build. An accidental
// test-hooks build reaches the poison controller and fails the RPC assertion.
func TestDefaultBuildIgnoresTestController(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	run, err := os.MkdirTemp(filepath.Join(root, "test-results"), "hooks-disabled-")
	if err != nil {
		t.Fatal(err)
	}
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"now":1000,"fail":true}`))
	}))
	defer server.Close()
	credential := token(t)
	auth, _ := json.Marshal([]object{{"token": credential, "tenant": "tenant-a", "subject": "alice", "namespaces": []string{"test"}}})
	vars := filepath.Join(run, ".dev.vars")
	if err := os.WriteFile(vars, []byte("PINTLE_TEST_CREDENTIALS='"+string(auth)+"'\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Remove(vars) })
	r := start(t, root, filepath.Join(run, "state"), protocolConfig(t, root, run), 0, "P1_ENABLED:true", "PINTLE_TEST_SCHEDULER:"+server.URL)
	defer r.stop()
	req := connect.NewRequest(rpcRequest("disabled-hooks", "id", "alice"))
	req.Header().Set("Authorization", "Bearer "+credential)
	req.Header().Set("X-Pintle-Test-Call", "poison")
	response, err := v1.NewLockServiceClient(client, r.base).TryAcquire(context.Background(), req)
	receipt(t, response, err)
	assertSame(t, hits.Load(), int32(0))
}

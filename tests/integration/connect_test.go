//go:build darwin || linux

package integration

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	v1 "pintle.local/spike/gen/go/pintle/v1"
	"pintle.local/spike/gen/go/pintle/v1/pintlev1connect"
)

func rpcRequest(key, id, holder string) *v1.TryAcquireRequest {
	return &v1.TryAcquireRequest{Resource: &v1.ResourceId{Namespace: "test", Key: key}, RequestId: id, HolderRef: holder}
}
func rpc(t *testing.T, c pintlev1connect.LockServiceClient, token string, msg *v1.TryAcquireRequest) (*connect.Response[v1.TryAcquireResponse], error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := connect.NewRequest(msg)
	req.Header().Set("Authorization", "Bearer "+token)
	return c.TryAcquire(ctx, req)
}
func expectCode(t *testing.T, err error, code connect.Code) {
	t.Helper()
	if err == nil || connect.CodeOf(err) != code {
		t.Fatalf("expected %s, got %v", code, err)
	}
}
func receipt(t *testing.T, res *connect.Response[v1.TryAcquireResponse], err error) *v1.GrantReceipt {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	if res.Msg.GetAcquired() == nil {
		t.Fatalf("not acquired: %v", res.Msg)
	}
	return res.Msg.GetAcquired()
}
func token(t *testing.T) string {
	t.Helper()
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b[:])
}
func protocolConfig(t *testing.T, root, run string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, "wrangler.jsonc"))
	if err != nil {
		t.Fatal(err)
	}
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
	data, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(run, "wrangler.json")
	if err = os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}
func rawRPC(t *testing.T, base, token, method, path string, body io.Reader, headers map[string]string) (int, http.Header, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, base+path, body)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/proto")
	req.Header.Set("Connect-Protocol-Version", "1")
	for k, v := range headers {
		if v == "" {
			req.Header.Del(k)
		} else {
			req.Header.Set(k, v)
		}
	}
	res, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	data, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	return res.StatusCode, res.Header, data
}
func TestP0b(t *testing.T) {
	// All restarts belong to the suite, including those initiated by subtests.
	runtimeOwner := t
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	results := filepath.Join(root, "test-results")
	if err = os.MkdirAll(results, 0755); err != nil {
		t.Fatal(err)
	}
	run, err := os.MkdirTemp(results, "p0b-")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("runtime logs and SQLite state: %s", run)
	config := protocolConfig(t, root, run)
	state := filepath.Join(run, "state")
	// Fixture setup only. The tested public RPC runs with /spike disabled.
	r := start(runtimeOwner, root, state, config, 0, true)
	for key, fence := range map[string]string{"rpc-precision": "9007199254740992", "rpc-max": "18446744073709551614", "rpc-exhausted": "18446744073709551615"} {
		mustCall(t, r.base, "local-a", "test", key, object{"op": "seed", "fence": fence, "revision": "9007199254740992"}, 200)
	}
	r.stop()
	r = start(runtimeOwner, root, state, config, 1, false, "P0B_ENABLED:true")
	c := pintlev1connect.NewLockServiceClient(client, r.base)
	_, err = rpc(t, c, "missing", rpcRequest("missing-config", "a", "alice"))
	expectCode(t, err, connect.CodeUnavailable)
	r.stop()
	a, b, other, rotated := token(t), token(t), token(t), token(t)
	credentials := []object{
		{"token": a, "tenant": "tenant-a", "subject": "alice", "namespaces": []string{"test", "n", strings.Repeat("n", 64)}},
		{"token": b, "tenant": "tenant-b", "subject": "alice", "namespaces": []string{"test", "n", strings.Repeat("n", 64)}},
		{"token": other, "tenant": "tenant-a", "subject": "bob", "namespaces": []string{"test", "n", strings.Repeat("n", 64)}},
		{"token": rotated, "tenant": "tenant-a", "subject": "alice", "namespaces": []string{"test", "n", strings.Repeat("n", 64)}},
	}
	auth, err := json.Marshal(credentials)
	if err != nil {
		t.Fatal(err)
	}
	vars := filepath.Join(run, ".dev.vars")
	if err = os.WriteFile(vars, []byte("PINTLE_TEST_CREDENTIALS='"+string(auth)+"'\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Remove(vars) })
	r = start(runtimeOwner, root, state, config, 2, false, "P0B_ENABLED:true")
	c = pintlev1connect.NewLockServiceClient(client, r.base)
	const path = pintlev1connect.LockServiceTryAcquireProcedure
	t.Run("generated_protobuf_roundtrip_replay_and_contention", func(t *testing.T) {
		msg := rpcRequest("rpc-roundtrip", "id-1", "worker-1")
		res, err := rpc(t, c, a, msg)
		first := receipt(t, res, err)
		assertSame(t, first.RequestId, "id-1")
		assertSame(t, first.HolderRef, "worker-1")
		assertSame(t, first.FencingToken, "1")
		assertSame(t, first.Revision, "1")
		if !proto.Equal(first.Resource, msg.Resource) {
			t.Fatal("resource mismatch")
		}
		assertSame(t, res.Msg.Replayed, false)
		assertSame(t, res.Header().Get("Content-Type"), "application/proto")
		assertSame(t, res.Header().Get("Cache-Control"), "no-store")
		res, err = rpc(t, c, a, msg)
		again := receipt(t, res, err)
		if !proto.Equal(first, again) || !res.Msg.Replayed {
			t.Fatal("unstable replay")
		}
		res, err = rpc(t, c, a, rpcRequest("rpc-roundtrip", "id-2", "worker-2"))
		if err != nil || res.Msg.GetContended() == nil {
			t.Fatalf("expected contended: %v %v", res, err)
		}
		_, err = rpc(t, c, a, rpcRequest("rpc-roundtrip", "id-1", "changed"))
		expectCode(t, err, connect.CodeAlreadyExists)
		unknown := rpcRequest("rpc-unknown-field", "id", "worker")
		unknown.ProtoReflect().SetUnknown(protowire.AppendString(protowire.AppendTag(nil, 100, protowire.BytesType), "future field"))
		res, err = rpc(t, c, a, unknown)
		receipt(t, res, err)
	})
	t.Run("auth_namespace_and_principal_isolation", func(t *testing.T) {
		_, err := rpc(t, c, "invalid", rpcRequest("rpc-auth", "id", "worker"))
		expectCode(t, err, connect.CodeUnauthenticated)
		msg := rpcRequest("rpc-auth", "id", "worker")
		msg.Resource.Namespace = "private"
		_, err = rpc(t, c, a, msg)
		expectCode(t, err, connect.CodePermissionDenied)
		for _, credential := range []string{a, b} {
			res, err := rpc(t, c, credential, rpcRequest("rpc-shared", "id", "worker"))
			g := receipt(t, res, err)
			assertSame(t, g.FencingToken, "1")
			assertSame(t, res.Msg.Replayed, false)
		}
		_, err = rpc(t, c, other, rpcRequest("rpc-shared", "id", "worker"))
		expectCode(t, err, connect.CodeAlreadyExists)
		res, err := rpc(t, c, rotated, rpcRequest("rpc-shared", "id", "worker"))
		receipt(t, res, err)
		assertSame(t, res.Msg.Replayed, true)
		req := connect.NewRequest(rpcRequest("rpc-shared", "id", "worker"))
		req.Header().Set("Authorization", "Bearer "+a)
		req.Header().Set("X-Pintle-Subject", "mallory")
		req.Header().Set("X-Pintle-Tenant", "tenant-b")
		req.Header().Set("X-Pintle-Deadline-Ms", "0")
		res, err = c.TryAcquire(context.Background(), req)
		receipt(t, res, err)
		assertSame(t, res.Msg.Replayed, true)
		mustCall(t, r.base, "local-a", "test", "rpc-shared", object{"op": "vacate"}, 403)
		status, _, _ := rawRPC(t, r.base, a, "POST", "/internal/p0b/try-acquire", bytes.NewReader(nil), nil)
		assertSame(t, status, 403)
	})
	t.Run("exact_tokens_over_protobuf", func(t *testing.T) {
		for key, want := range map[string]string{"rpc-precision": "9007199254740993", "rpc-max": "18446744073709551615"} {
			res, err := rpc(t, c, a, rpcRequest(key, "id", "worker"))
			g := receipt(t, res, err)
			assertSame(t, g.FencingToken, want)
			assertSame(t, g.Revision, "9007199254740993")
			res, err = rpc(t, c, a, rpcRequest(key, "id", "worker"))
			assertSame(t, receipt(t, res, err).FencingToken, want)
			assertSame(t, res.Msg.Replayed, true)
		}
		_, err := rpc(t, c, a, rpcRequest("rpc-exhausted", "id", "worker"))
		expectCode(t, err, connect.CodeResourceExhausted)
	})
	t.Run("generated_client_validation_and_codec_errors", func(t *testing.T) {
		cases := []*v1.TryAcquireRequest{{}, rpcRequest("", "id", "w"), rpcRequest("key", "", "w"), rpcRequest("key", "id", ""), rpcRequest(strings.Repeat("é", 65), "id", "w"), rpcRequest("key", strings.Repeat("a", 129), "w")}
		for _, msg := range cases {
			_, err := rpc(t, c, a, msg)
			expectCode(t, err, connect.CodeInvalidArgument)
		}
		jsonClient := pintlev1connect.NewLockServiceClient(client, r.base, connect.WithProtoJSON())
		_, err := rpc(t, jsonClient, a, rpcRequest("rpc-json", "id", "w"))
		expectCode(t, err, connect.CodeUnimplemented)
		compressed := pintlev1connect.NewLockServiceClient(client, r.base, connect.WithSendGzip(), connect.WithCompressMinBytes(0))
		_, err = rpc(t, compressed, a, rpcRequest("rpc-gzip", "id", "w"))
		expectCode(t, err, connect.CodeUnimplemented)
	})
	t.Run("wire_status_error_envelopes_and_limits", func(t *testing.T) {
		valid, _ := proto.Marshal(rpcRequest("rpc-wire", "id", "worker"))
		cases := []struct {
			name, method, path string
			body               []byte
			headers            map[string]string
			status             int
			code               string
		}{
			{"missing-version", "POST", path, valid, map[string]string{"Connect-Protocol-Version": ""}, 400, "invalid_argument"},
			{"bad-version", "POST", path, valid, map[string]string{"Connect-Protocol-Version": "2"}, 400, "invalid_argument"},
			{"bad-timeout", "POST", path, valid, map[string]string{"Connect-Timeout-Ms": "1e3"}, 400, "invalid_argument"},
			{"long-timeout", "POST", path, valid, map[string]string{"Connect-Timeout-Ms": "12345678901"}, 400, "invalid_argument"},
			{"negative-timeout", "POST", path, valid, map[string]string{"Connect-Timeout-Ms": "-1"}, 400, "invalid_argument"},
			{"expired", "POST", path, valid, map[string]string{"Connect-Timeout-Ms": "0"}, 504, "deadline_exceeded"},
			{"malformed-protobuf", "POST", path, []byte{0xff}, nil, 400, "invalid_argument"},
			{"unsupported-codec", "POST", path, valid, map[string]string{"Content-Type": "application/json"}, 415, "unimplemented"},
			{"streaming", "POST", path, valid, map[string]string{"Content-Type": "application/connect+proto"}, 415, "unimplemented"},
			{"grpc", "POST", path, valid, map[string]string{"Content-Type": "application/grpc"}, 415, "unimplemented"},
			{"get", "GET", path, nil, nil, 405, "unimplemented"},
			{"unknown-method", "POST", "/pintle.v1.LockService/Unknown", valid, nil, 501, "unimplemented"},
			{"oversized", "POST", path, bytes.Repeat([]byte{0}, 8193), nil, 429, "resource_exhausted"},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				status, h, data := rawRPC(t, r.base, a, tc.method, tc.path, bytes.NewReader(tc.body), tc.headers)
				assertSame(t, status, tc.status)
				assertSame(t, h.Get("Content-Type"), "application/json")
				assertSame(t, h.Get("Cache-Control"), "no-store")
				var e struct {
					Code string `json:"code"`
				}
				if err := json.Unmarshal(data, &e); err != nil {
					t.Fatalf("%s: %v", data, err)
				}
				assertSame(t, e.Code, tc.code)
			})
		}
		// No Content-Length: enforce the same cap while consuming a chunked body.
		status, _, _ := rawRPC(t, r.base, a, "POST", path, io.LimitReader(bytes.NewReader(bytes.Repeat([]byte{0}, 8193)), 8193), nil)
		assertSame(t, status, 429)
		res, err := rpc(t, c, a, rpcRequest("rpc-wire", "id", "worker"))
		receipt(t, res, err)
		assertSame(t, res.Msg.Replayed, false)
	})
	t.Run("exact_public_field_and_body_boundaries", func(t *testing.T) {
		cases := []struct {
			field, value string
			valid        bool
		}{
			{"namespace", "", false}, {"namespace", "n", true}, {"namespace", strings.Repeat("n", 64), true}, {"namespace", strings.Repeat("n", 65), false},
			{"namespace", "a/b", false}, {"namespace", "é", false},
			{"key", "", false}, {"key", strings.Repeat("é", 64), true}, {"key", strings.Repeat("é", 65), false}, {"key", "bad\x00key", false},
			{"request_id", strings.Repeat("i", 128), true}, {"request_id", strings.Repeat("i", 129), false}, {"request_id", "a/b", false},
			{"holder_ref", strings.Repeat("h", 64), true}, {"holder_ref", strings.Repeat("h", 65), false}, {"holder_ref", "a/b", false},
			{"holder_ref", "a._:-1", true},
		}
		for i, tc := range cases {
			t.Run(fmt.Sprintf("%s-%d", tc.field, i), func(t *testing.T) {
				msg := rpcRequest(fmt.Sprintf("boundary-%d", i), "id", "worker")
				switch tc.field {
				case "namespace":
					msg.Resource.Namespace = tc.value
				case "key":
					msg.Resource.Key = tc.value
				case "request_id":
					msg.RequestId = tc.value
				case "holder_ref":
					msg.HolderRef = tc.value
				}
				res, err := rpc(t, c, a, msg)
				if tc.valid {
					receipt(t, res, err)
				} else {
					expectCode(t, err, connect.CodeInvalidArgument)
				}
			})
		}
		for _, chunked := range []bool{false, true} {
			for _, size := range []int{8191, 8192, 8193} {
				t.Run(fmt.Sprintf("bytes-%d-chunked-%t", size, chunked), func(t *testing.T) {
					msg := rpcRequest(fmt.Sprintf("bytes-%d-%t", size, chunked), "id", "worker")
					data, err := proto.Marshal(msg)
					if err != nil {
						t.Fatal(err)
					}
					// Unknown field 100: two-byte tag and two-byte length prefix.
					padding := size - len(data) - 4
					data = protowire.AppendBytes(protowire.AppendTag(data, 100, protowire.BytesType), make([]byte, padding))
					assertSame(t, len(data), size)
					var body io.Reader = bytes.NewReader(data)
					if chunked {
						body = io.LimitReader(body, int64(size))
					}
					status, _, _ := rawRPC(t, r.base, a, "POST", path, body, nil)
					if size <= 8192 {
						assertSame(t, status, 200)
					} else {
						assertSame(t, status, 429)
					}
					res, err := rpc(t, c, a, msg)
					receipt(t, res, err)
					assertSame(t, res.Msg.Replayed, size <= 8192)
				})
			}
		}
	})
	t.Run("generated_clients_concurrent_grants_and_retries", func(t *testing.T) {
		for _, sameID := range []bool{false, true} {
			const n = 16
			key := fmt.Sprintf("rpc-race-%v", sameID)
			barrier := make(chan struct{})
			var wg sync.WaitGroup
			type result struct {
				res *connect.Response[v1.TryAcquireResponse]
				err error
			}
			results := make(chan result, n)
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					<-barrier
					id := fmt.Sprintf("id-%d", i)
					if sameID {
						id = "same"
					}
					req := connect.NewRequest(rpcRequest(key, id, "worker"))
					req.Header().Set("Authorization", "Bearer "+a)
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					res, err := c.TryAcquire(ctx, req)
					results <- result{res, err}
				}(i)
			}
			close(barrier)
			wg.Wait()
			close(results)
			fresh, acquired, contended := 0, 0, 0
			for result := range results {
				if result.err != nil {
					t.Fatal(result.err)
				}
				if g := result.res.Msg.GetAcquired(); g != nil {
					acquired++
					assertSame(t, g.FencingToken, "1")
					if !result.res.Msg.Replayed {
						fresh++
					}
				} else if result.res.Msg.GetContended() != nil {
					contended++
				} else {
					t.Fatal("missing outcome")
				}
			}
			assertSame(t, fresh, 1)
			if sameID {
				assertSame(t, acquired, n)
			} else {
				assertSame(t, contended, n-1)
			}
		}
	})
	t.Run("generated_client_response_loss_and_restart", func(t *testing.T) {
		committed := make(chan []byte, 1)
		proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			upstream, err := http.NewRequest("POST", r.base+req.URL.Path, req.Body)
			if err != nil {
				http.Error(w, "request", 502)
				return
			}
			upstream.Header = req.Header.Clone()
			res, err := client.Do(upstream)
			if err != nil {
				http.Error(w, "upstream", 502)
				return
			}
			defer res.Body.Close()
			data, err := io.ReadAll(res.Body)
			if err != nil || res.StatusCode != 200 {
				http.Error(w, "upstream response", 502)
				return
			}
			committed <- data
			conn, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				conn.Close()
			}
		}))
		defer proxy.Close()
		lostClient := pintlev1connect.NewLockServiceClient(client, proxy.URL)
		msg := rpcRequest("rpc-lost", "id", "worker")
		_, err := rpc(t, lostClient, a, msg)
		if err == nil {
			t.Fatal("expected lost response")
		}
		var original v1.TryAcquireResponse
		select {
		case data := <-committed:
			if err := proto.Unmarshal(data, &original); err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("no upstream receipt")
		}
		r.stop()
		r = start(runtimeOwner, root, state, config, 3, false, "P0B_ENABLED:true")
		c = pintlev1connect.NewLockServiceClient(client, r.base)
		res, err := rpc(t, c, a, msg)
		receipt(t, res, err)
		assertSame(t, res.Msg.Replayed, true)
		if !proto.Equal(res.Msg.GetAcquired(), original.GetAcquired()) {
			t.Fatal("lost-response replay changed")
		}
		res, err = rpc(t, c, a, rpcRequest("rpc-precision", "id", "worker"))
		assertSame(t, receipt(t, res, err).FencingToken, "9007199254740993")
		assertSame(t, res.Msg.Replayed, true)
	})
	t.Run("already_cancelled_client", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		req := connect.NewRequest(rpcRequest("cancelled", "id", "worker"))
		req.Header().Set("Authorization", "Bearer "+a)
		_, err := c.TryAcquire(ctx, req)
		expectCode(t, err, connect.CodeCanceled)
	})
	t.Run("real_timer_stalled_upload_smoke", func(t *testing.T) {
		address := strings.TrimPrefix(r.base, "http://")
		conn, err := net.DialTimeout("tcp", address, 2*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		if err = conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
			t.Fatal(err)
		}
		// Begin a chunked body but deliberately never terminate it. A raw
		// connection avoids net/http waiting for its own blocked body writer.
		_, err = fmt.Fprintf(conn, "POST %s HTTP/1.1\r\nHost: %s\r\nAuthorization: Bearer %s\r\nContent-Type: application/proto\r\nConnect-Protocol-Version: 1\r\nConnect-Timeout-Ms: 30\r\nTransfer-Encoding: chunked\r\n\r\n1\r\n%c\r\n", path, address, a, byte(10))
		if err != nil {
			t.Fatal(err)
		}
		res, err := http.ReadResponse(bufio.NewReader(conn), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		assertSame(t, res.StatusCode, 504)
	})

	r.stop()
}

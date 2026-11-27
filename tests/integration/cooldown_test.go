//go:build (darwin || linux) && !testhooks

package integration

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/url"
	"path"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/Noot-tooN/Pintle/gen/go/pintle/v1"
	"github.com/Noot-tooN/Pintle/gen/go/pintle/v1/pintlev1connect"
	"github.com/Noot-tooN/Pintle/sdk/go/pintle"
	"google.golang.org/protobuf/proto"
)

func TestCooldownRecovery(t *testing.T) {
	const enable = "PINTLE_COOLDOWN_ENABLED:true"
	for _, scenario := range []string{"admitted", "denied", "superseded"} {
		t.Run(scenario, func(t *testing.T) {
			a, b, other := token(t), token(t), token(t)
			root, state, config, r := staticSetup(t, []object{staticKey(a, "one", "alice"), staticKey(b, "one", "bob"), staticKey(other, "two", "alice")}, enable)
			defer func() { r.stop() }()
			ctx := t.Context()
			res := pintle.Resource{Namespace: "test", Key: t.Name()}
			interval := time.Millisecond
			if scenario == "denied" {
				initial, err := sdk(t, r.base, b, client).TryAdmit(ctx, res, 24*time.Hour)
				if err != nil || initial.Status != pintle.CooldownAdmitted {
					t.Fatal("initial winner", initial, err)
				}
			}
			var original *v1.TryAdmitRequest
			var lost v1.TryAdmitResponse
			var successor pintle.CooldownDecision
			reads, sends := 0, 0
			transport := cacheHTTP(func(req *http.Request) (*http.Response, error) {
				// Wrangler allocates a new loopback port after restart.
				target, err := url.Parse(r.base)
				if err != nil {
					return nil, err
				}
				req = req.Clone(req.Context())
				req.URL.Host = target.Host
				req.Host = target.Host
				if path.Base(req.URL.Path) == "GetCooldownWindow" {
					reads++
					return client.Do(req)
				}
				sends++
				body, err := io.ReadAll(req.Body)
				if err != nil {
					return nil, err
				}
				req.Body.Close()
				req.Body = io.NopCloser(bytes.NewReader(body))
				sent := new(v1.TryAdmitRequest)
				if err := proto.Unmarshal(body, sent); err != nil {
					t.Fatal(err)
				}
				if sends > 1 {
					if !proto.Equal(original, sent) {
						t.Fatal("retry changed the logical attempt")
					}
					if scenario == "denied" && sends == 2 {
						return nil, io.ErrUnexpectedEOF
					}
					return client.Do(req)
				}
				original = sent
				drop := discardFlightReply{wire: &lost}
				_, err = drop.Do(req)
				if !drop.seen {
					t.Fatal("failed to discard committed reply", err)
				}
				r.stop()
				r = start(t, root, state, config, 1, "P2_ENABLED:true", "PINTLE_AUTH_MODE:static", enable)
				if scenario == "superseded" {
					successor, err = sdk(t, r.base, b, client).TryAdmit(ctx, res, 24*time.Hour)
					if err != nil || successor.Status != pintle.CooldownAdmitted {
						t.Fatal("successor failed", successor, err)
					}
				}
				return nil, io.ErrUnexpectedEOF
			})
			d, err := sdk(t, r.base, a, transport).TryAdmit(ctx, res, interval)
			if scenario == "denied" {
				var unresolved *pintle.CooldownRecoveryError
				if !errors.As(err, &unresolved) {
					t.Fatal("missing recovery handle", err)
				}
				d, err = unresolved.Retry(ctx)
			}
			if err != nil || reads != 1 || sends < 2 {
				t.Fatal("replay failed", d, err, reads, sends)
			}
			switch scenario {
			case "admitted":
				if lost.Status != v1.CooldownStatus_COOLDOWN_STATUS_ADMITTED || d.Status != pintle.CooldownAdmitted || uint64(d.AdmittedAt.UnixMilli()) != lost.GetAdmittedAtUnixMs() || uint64(d.RetryAt.UnixMilli()) != lost.RetryAtUnixMs {
					t.Fatal("expired winner did not recover its timestamps", d)
				}
				raw := pintlev1connect.NewCooldownServiceClient(client, r.base)
				changed := proto.Clone(original).(*v1.TryAdmitRequest)
				changed.IntervalMs++
				_, err = raw.TryAdmit(ctx, flightRequest(a, changed))
				expectCode(t, err, connect.CodeAlreadyExists)
				denied, err := raw.TryAdmit(ctx, flightRequest(b, original))
				if err != nil || denied.Msg.Status != v1.CooldownStatus_COOLDOWN_STATUS_COOLING_DOWN {
					t.Fatal("winner identity crossed subjects", err)
				}
			case "denied":
				if lost.Status != v1.CooldownStatus_COOLDOWN_STATUS_COOLING_DOWN || d.Status != pintle.CoolingDown || uint64(d.RetryAt.UnixMilli()) != lost.RetryAtUnixMs {
					t.Fatal("lost denial changed", d)
				}
			case "superseded":
				if lost.Status != v1.CooldownStatus_COOLDOWN_STATUS_ADMITTED || d.Status != pintle.CooldownSuperseded || d.RetryAt != successor.RetryAt {
					t.Fatal("late retry displaced successor", d, successor)
				}
			}
			// The same key in another authenticated tenant is independent.
			separate, err := sdk(t, r.base, other, client).TryAdmit(ctx, res, time.Second)
			if err != nil || separate.Status != pintle.CooldownAdmitted {
				t.Fatal("tenant isolation", separate, err)
			}
		})
	}
}

func TestCooldownValidation(t *testing.T) {
	a := token(t)
	_, _, _, r := staticSetup(t, []object{staticKey(a, "one", "alice")}, "PINTLE_COOLDOWN_ENABLED:true")
	defer r.stop()
	ctx := t.Context()

	raw := pintlev1connect.NewCooldownServiceClient(client, r.base)
	resource := &v1.ResourceId{Namespace: "test", Key: "bounds"}
	for _, ms := range []uint32{0, 86400001} {
		_, err := raw.TryAdmit(ctx, flightRequest(a, &v1.TryAdmitRequest{Resource: resource, WindowId: "0", RequestId: "bad", IntervalMs: ms}))
		expectCode(t, err, connect.CodeInvalidArgument)
	}
	resource.Namespace = "forbidden"
	_, err := raw.GetCooldownWindow(ctx, flightRequest(a, &v1.GetCooldownWindowRequest{Resource: resource}))
	expectCode(t, err, connect.CodePermissionDenied)
}

func TestCooldownConcurrentAdmissions(t *testing.T) {
	key := token(t)
	_, _, _, r := staticSetup(t, []object{staticKey(key, "one", "worker")}, "PINTLE_COOLDOWN_ENABLED:true")
	defer r.stop()
	c := sdk(t, r.base, key, client)
	resource := pintle.Resource{Namespace: "test", Key: "search-index"}
	decisions := make([]pintle.CooldownDecision, 12)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range decisions {
		wg.Go(func() {
			<-start
			decision, err := c.TryAdmit(t.Context(), resource, time.Duration(i+1)*time.Minute)
			if err != nil {
				t.Error(err)
				return
			}
			decisions[i] = decision
		})
	}
	close(start)
	wg.Wait()
	winners := 0
	for i, d := range decisions {
		if d.Status == pintle.CooldownAdmitted {
			winners++
			if d.RetryAt.Sub(d.AdmittedAt) != time.Duration(i+1)*time.Minute {
				t.Fatal("wrong winner interval", d)
			}
		} else if d.Status != pintle.CoolingDown {
			t.Fatal("unexpected decision", d)
		}
		if !d.RetryAt.Equal(decisions[0].RetryAt) {
			t.Fatal("callers observed different windows")
		}
	}
	if winners != 1 {
		t.Fatalf("got %d winners", winners)
	}
}

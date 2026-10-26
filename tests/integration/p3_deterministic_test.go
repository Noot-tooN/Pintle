//go:build (darwin || linux) && testhooks

package integration

import (
	"connectrpc.com/connect"
	"context"
	"fmt"
	"net/http"
	pintle "pintle.local/spike/sdk/go/pintle"
	"testing"
	"time"
)

func TestP3CredentialTimeBoundaries(t *testing.T) {
	controller := newController(t)
	a := token(t)
	key := staticKey(a, "tenant", "alice")
	key["not_before_ms"] = "1000"
	key["expires_at_ms"] = "2000"
	_, _, _, r := staticSetup(t, []object{key}, "PINTLE_TEST_SCHEDULER:"+controller.server.URL)
	defer r.stop()
	c := sdk(t, r.base, a, &http.Client{Transport: scheduledTransport{}, Timeout: 10 * time.Second})
	for _, now := range []uint64{999, 1000, 1999, 2000, 2001} {
		t.Run(fmt.Sprint(now), func(t *testing.T) {
			call := fmt.Sprint("credential-", now)
			start := controller.hold(call, "gateway_start")
			release(start, directive{Now: now})
			ctx := context.WithValue(context.Background(), callContext{}, call)
			_, err := c.OpenSession(ctx, pintle.Resource{Namespace: "test", Key: call})
			if now >= 1000 && now < 2000 {
				if err != nil {
					t.Fatal(err)
				}
			} else {
				expectCode(t, err, connect.CodeUnauthenticated)
			}
		})
	}
	// Authorization is evaluated at gateway admission. Expiration does not undo
	// an already-authorized bounded in-flight operation or a durable waiter.
	call := "credential-inflight"
	start := controller.hold(call, "gateway_start")
	release(start, directive{Now: 1999})
	dispatch := controller.hold(call, "before_dispatch")
	release(dispatch, directive{Now: 2000})
	ctx := context.WithValue(context.Background(), callContext{}, call)
	if _, err := c.OpenSession(ctx, pintle.Resource{Namespace: "test", Key: call}); err != nil {
		t.Fatal(err)
	}
}

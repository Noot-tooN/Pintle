//go:build (darwin || linux) && testhooks

package integration

import (
	"testing"

	"connectrpc.com/connect"
	v1 "github.com/Noot-tooN/Pintle/gen/go/pintle/v1"
	"github.com/Noot-tooN/Pintle/gen/go/pintle/v1/pintlev1connect"
	"google.golang.org/protobuf/proto"
)

func TestCooldownStorage(t *testing.T) {
	ctrl := newController(t)
	auth := token(t)
	root, state, _, r := staticSetup(t, []object{staticKey(auth, "one", "alice")}, "PINTLE_COOLDOWN_ENABLED:true", "PINTLE_TEST_SCHEDULER:"+ctrl.server.URL)
	defer r.stop()
	rpc := pintlev1connect.NewCooldownServiceClient(client, r.base)
	res := &v1.ResourceId{Namespace: "test", Key: "index"}
	admit := func(label, id, window string, now uint64, fail bool) (*v1.TryAdmitResponse, error) {
		req := flightRequest(auth, &v1.TryAdmitRequest{Resource: res, WindowId: window, RequestId: id, IntervalMs: 10})
		req.Header().Set("X-Pintle-Test-Call", label)
		release(ctrl.hold(label, "gateway_start"), directive{Now: now})
		if fail {
			release(ctrl.hold(label, "before_commit"), directive{Now: now, Fault: object{"kind": "sql", "after_write": 1}})
		}
		reply, err := rpc.TryAdmit(t.Context(), req)
		if err != nil {
			return nil, err
		}
		return reply.Msg, nil
	}
	win, err := admit("win", "first", "0", 1000, false)
	if err != nil || win.Status != v1.CooldownStatus_COOLDOWN_STATUS_ADMITTED || win.RetryAtUnixMs != 1010 {
		t.Fatal("missing first admission", win, err)
	}
	denied, err := admit("early", "other", "0", 1009, false)
	if err != nil || denied.Status != v1.CooldownStatus_COOLDOWN_STATUS_COOLING_DOWN || denied.RetryAtUnixMs != 1010 {
		t.Fatal("early admission", denied, err)
	}
	before := localRecords(t, root, state)
	_, err = admit("rollback", "next", "1010", 1010, true)
	expectCode(t, err, connect.CodeUnavailable)
	assertSame(t, localRecords(t, root, state), before)
	replay, err := admit("recover", "first", "0", 1010, false)
	if err != nil || !proto.Equal(win, replay) {
		t.Fatal("rollback displaced winner", replay, err)
	}
	next, err := admit("boundary", "next", "1010", 1010, false)
	if err != nil || next.Status != v1.CooldownStatus_COOLDOWN_STATUS_ADMITTED || next.RetryAtUnixMs != 1020 {
		t.Fatal("exact boundary admission", next, err)
	}
}

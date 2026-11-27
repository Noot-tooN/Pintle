//go:build (darwin || linux) && testhooks

package integration

import (
	"connectrpc.com/connect"
	v1 "github.com/Noot-tooN/Pintle/gen/go/pintle/v1"
	"github.com/Noot-tooN/Pintle/gen/go/pintle/v1/pintlev1connect"
	"strconv"
	"testing"
)

func TestSingleflightStorage(t *testing.T) {
	ctrl := newController(t)
	token := token(t)
	root, state, _, r := staticSetup(t, []object{staticKey(token, "test", "writer")}, "PINTLE_SINGLEFLIGHT_ENABLED:true", "PINTLE_TEST_SCHEDULER:"+ctrl.server.URL)
	defer r.stop()
	sf := pintlev1connect.NewSingleflightServiceClient(client, r.base)
	ctx := t.Context()
	resource := &v1.ResourceId{Namespace: "test", Key: "rollback"}
	call := 0
	scheduled := func(req connect.AnyRequest, now uint64, fault object) {
		call++
		name := "flight-" + strconv.Itoa(call)
		req.Header().Set("X-Pintle-Test-Call", name)
		release(ctrl.hold(name, "gateway_start"), directive{Now: now})
		if fault != nil {
			release(ctrl.hold(name, "before_commit"), directive{Now: now, Fault: fault})
		}
	}
	join := func(id, gen string, now uint64, fault object) (*connect.Response[v1.JoinFlightResponse], error) {
		q := flightRequest(token, &v1.JoinFlightRequest{Resource: resource, Generation: gen, RequestId: id})
		scheduled(q, now, fault)
		return sf.JoinFlight(ctx, q)
	}
	// A failed first write rolls back table initialization, generation and receipt.
	fault := object{"kind": "sql", "after_write": 1}
	_, err := join("first", "1", 1000, fault)
	expectCode(t, err, connect.CodeUnavailable)
	leader, err := join("first", "1", 1000, nil)
	if err != nil || !leader.Msg.Producer {
		t.Fatal("failed initial grant survived rollback", err)
	}
	before := localRecords(t, root, state)
	_, err = join("retry", "1", 1001, fault)
	expectCode(t, err, connect.CodeUnavailable)
	assertSame(t, localRecords(t, root, state), before)
	waiter, err := join("retry", "1", 1001, nil)
	if err != nil || waiter.Msg.Producer {
		t.Fatal("missing waiter", err)
	}
	complete := func(now uint64, fault object) (*connect.Response[v1.CompleteFlightResponse], error) {
		q := flightRequest(token, &v1.CompleteFlightRequest{Resource: resource, Ticket: leader.Msg.Ticket, Completion: &v1.CompleteFlightRequest_Value{Value: []byte("result")}})
		scheduled(q, now, fault)
		return sf.CompleteFlight(ctx, q)
	}
	before = localRecords(t, root, state)
	_, err = complete(1002, object{"kind": "error", "after_write": 1})
	expectCode(t, err, connect.CodeUnavailable)
	assertSame(t, localRecords(t, root, state), before)
	out, err := complete(31000, nil)
	if err != nil || out.Msg.Observation.GetStatus() != v1.FlightStatus_FLIGHT_STATUS_LEADER_EXPIRED {
		t.Fatal("deadline completion revived producer", err)
	}
	old, err := join("retry", "1", 90999, nil)
	if err != nil || old.Msg.Ticket != waiter.Msg.Ticket {
		t.Fatal("receipt lost before boundary", err)
	}
	_, err = join("retry", "1", 91000, nil)
	expectCode(t, err, connect.CodeFailedPrecondition)
	_, err = join("retry", "2", 90999, nil)
	expectCode(t, err, connect.CodeUnavailable)
	next, err := join("retry", "2", 91000, nil)
	if err != nil || !next.Msg.Producer || next.Msg.Ticket == leader.Msg.Ticket {
		t.Fatal("fresh generation failed", err)
	}
	_, err = complete(91000, nil)
	expectCode(t, err, connect.CodeNotFound)
}

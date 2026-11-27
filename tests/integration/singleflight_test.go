//go:build darwin || linux

package integration

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"sync"
	"testing"

	"connectrpc.com/connect"
	v1 "github.com/Noot-tooN/Pintle/gen/go/pintle/v1"
	"github.com/Noot-tooN/Pintle/gen/go/pintle/v1/pintlev1connect"
	"google.golang.org/protobuf/proto"
)

func flightRequest[T any](key string, msg *T) *connect.Request[T] {
	req := connect.NewRequest(msg)
	req.Header().Set("Authorization", "Bearer "+key)
	return req
}

// Observe the committed reply at the transport boundary, then deliver a network
// error to the generated client. The caller must recover using its original input.
type discardFlightReply struct {
	wire proto.Message
	seen bool
}

func (d *discardFlightReply) Do(r *http.Request) (*http.Response, error) {
	res, err := client.Do(r)
	if err != nil || res.StatusCode != 200 {
		return res, err
	}
	defer res.Body.Close()
	b, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	if err = proto.Unmarshal(b, d.wire); err != nil {
		return nil, err
	}
	d.seen = true
	return nil, errors.New("discarded successful flight reply")
}

func TestSingleflightRecovery(t *testing.T) {
	a, b, otherTenant := token(t), token(t), token(t)
	keys := []object{staticKey(a, "one", "alice"), staticKey(b, "one", "bob"), staticKey(otherTenant, "two", "alice")}
	const enable = "PINTLE_SINGLEFLIGHT_ENABLED:true"
	root, state, config, r := staticSetup(t, keys, enable)
	defer func() { r.stop() }()
	activation := 0
	restart := func() {
		r.stop()
		activation++
		r = start(t, root, state, config, activation, "P2_ENABLED:true", "PINTLE_AUTH_MODE:static", enable)
	}
	service := func() pintlev1connect.SingleflightServiceClient {
		return pintlev1connect.NewSingleflightServiceClient(client, r.base)
	}
	ctx := t.Context()
	resource := &v1.ResourceId{Namespace: "test", Key: "catalog:v7:blue-shirt"}
	get := func(key string) string {
		t.Helper()
		res, err := service().GetFlightGeneration(ctx, flightRequest(key, &v1.GetFlightGenerationRequest{Resource: resource}))
		if err != nil {
			t.Fatal(err)
		}
		return res.Msg.Generation
	}
	join := func(key string, msg *v1.JoinFlightRequest) *v1.JoinFlightResponse {
		t.Helper()
		res, err := service().JoinFlight(ctx, flightRequest(key, msg))
		if err != nil {
			t.Fatal(err)
		}
		return res.Msg
	}
	observe := func(key, ticket string) *v1.FlightObservation {
		t.Helper()
		res, err := service().ObserveFlight(ctx, flightRequest(key, &v1.ObserveFlightRequest{Resource: resource, Ticket: ticket}))
		if err != nil {
			t.Fatal(err)
		}
		return res.Msg.Observation
	}
	gen := get(a)
	attempt := &v1.JoinFlightRequest{Resource: resource, Generation: gen, RequestId: "cache-miss"}
	issued := new(v1.JoinFlightResponse)
	dropped := &discardFlightReply{wire: issued}
	_, err := pintlev1connect.NewSingleflightServiceClient(dropped, r.base).JoinFlight(ctx, flightRequest(a, attempt))
	if err == nil || !dropped.seen {
		t.Fatalf("expected loss after committed admission: %v", err)
	}
	// Same request ID on an independent subject has its own membership.
	waiter := join(b, attempt)
	if waiter.Producer || waiter.Ticket == issued.Ticket {
		t.Fatal("waiter aliased producer")
	}
	restart()
	producer := join(a, attempt)
	if !proto.Equal(producer, issued) || !producer.Producer {
		t.Fatal("lost admission did not recover the original producer ticket")
	}
	if !proto.Equal(waiter, join(b, attempt)) {
		t.Fatal("waiter changed across restart")
	}
	if observe(b, waiter.Ticket).GetStatus() != v1.FlightStatus_FLIGHT_STATUS_PENDING {
		t.Fatal("waiter lost pending fill")
	}

	value := bytes.Repeat([]byte{0, 0xff}, 2048)
	completion := &v1.CompleteFlightRequest{Resource: resource, Ticket: producer.Ticket, Completion: &v1.CompleteFlightRequest_Value{Value: value}}
	_, err = service().CompleteFlight(ctx, flightRequest(b, completion))
	expectCode(t, err, connect.CodeNotFound)
	wrongRole := proto.Clone(completion).(*v1.CompleteFlightRequest)
	wrongRole.Ticket = waiter.Ticket
	_, err = service().CompleteFlight(ctx, flightRequest(b, wrongRole))
	expectCode(t, err, connect.CodePermissionDenied)
	_, err = service().ObserveFlight(ctx, flightRequest(otherTenant, &v1.ObserveFlightRequest{Resource: resource, Ticket: producer.Ticket}))
	expectCode(t, err, connect.CodeNotFound)
	_, err = service().ObserveFlight(ctx, flightRequest(a, &v1.ObserveFlightRequest{Resource: &v1.ResourceId{Namespace: "test", Key: "different"}, Ticket: producer.Ticket}))
	expectCode(t, err, connect.CodeNotFound)
	_, err = service().GetFlightGeneration(ctx, flightRequest(a, &v1.GetFlightGenerationRequest{Resource: &v1.ResourceId{Namespace: "forbidden", Key: "same"}}))
	expectCode(t, err, connect.CodePermissionDenied)
	recorded := new(v1.CompleteFlightResponse)
	dropped = &discardFlightReply{wire: recorded}
	_, err = pintlev1connect.NewSingleflightServiceClient(dropped, r.base).CompleteFlight(ctx, flightRequest(a, completion))
	if err == nil || !dropped.seen {
		t.Fatalf("expected loss after committed completion: %v", err)
	}
	restart()
	nextGen := get(b)
	if nextGen == gen {
		t.Fatal("generation reused")
	}
	next := join(b, &v1.JoinFlightRequest{Resource: resource, Generation: nextGen, RequestId: "next"})
	if !next.Producer {
		t.Fatal("missing successor")
	}
	if !proto.Equal(join(a, attempt), producer) {
		t.Fatal("replay created a new ticket")
	}
	replay, err := service().CompleteFlight(ctx, flightRequest(a, completion))
	if err != nil || !proto.Equal(recorded, replay.Msg) || !bytes.Equal(observe(b, waiter.Ticket).GetValue(), value) {
		t.Fatal("lost completion did not recover the original bytes", err)
	}
	conflict := proto.Clone(completion).(*v1.CompleteFlightRequest)
	conflict.Completion = &v1.CompleteFlightRequest_Value{Value: []byte("different")}
	_, err = service().CompleteFlight(ctx, flightRequest(a, conflict))
	expectCode(t, err, connect.CodeAlreadyExists)
	expiredJoin := &v1.JoinFlightRequest{Resource: resource, Generation: gen, RequestId: "unadmitted"}
	_, err = service().JoinFlight(ctx, flightRequest(a, expiredJoin))
	expectCode(t, err, connect.CodeFailedPrecondition)
	if observe(b, next.Ticket).GetStatus() != v1.FlightStatus_FLIGHT_STATUS_PENDING {
		t.Fatal("old completion changed successor")
	}
	late, err := service().CancelFlight(ctx, flightRequest(b, &v1.CancelFlightRequest{Resource: resource, Ticket: waiter.Ticket}))
	if err != nil || !bytes.Equal(late.Msg.Observation.GetValue(), value) {
		t.Fatal("late cancellation displaced completion", err)
	}

	// Concurrent duplicate admissions all consume one waiter slot.
	concurrent := &v1.JoinFlightRequest{Resource: resource, Generation: nextGen, RequestId: "concurrent"}
	var wg sync.WaitGroup
	tickets := make([]string, 20)
	for i := range tickets {
		wg.Go(func() {
			reply, err := service().JoinFlight(ctx, flightRequest(a, concurrent))
			if err != nil {
				t.Error(err)
				return
			}
			tickets[i] = reply.Msg.Ticket
		})
	}
	wg.Wait()
	for _, id := range tickets {
		if id == "" || id != tickets[0] {
			t.Fatal("duplicate admission changed ticket")
		}
	}
	cancellation := &v1.CancelFlightRequest{Resource: resource, Ticket: tickets[0]}
	cancelled := new(v1.CancelFlightResponse)
	dropped = &discardFlightReply{wire: cancelled}
	_, err = pintlev1connect.NewSingleflightServiceClient(dropped, r.base).CancelFlight(ctx, flightRequest(a, cancellation))
	if err == nil || !dropped.seen {
		t.Fatalf("expected lost cancellation reply: %v", err)
	}
	restart()
	if join(a, concurrent).Ticket != tickets[0] || observe(a, tickets[0]).GetStatus() != v1.FlightStatus_FLIGHT_STATUS_CANCELLED {
		t.Fatal("restart revived cancelled waiter")
	}
	re, err := service().CancelFlight(ctx, flightRequest(a, cancellation))
	if err != nil || !proto.Equal(re.Msg, cancelled) {
		t.Fatal("cancellation replay changed", err)
	}

	large := &v1.CompleteFlightRequest{Resource: resource, Ticket: next.Ticket, Completion: &v1.CompleteFlightRequest_Value{Value: make([]byte, 8193)}}
	_, err = service().CompleteFlight(ctx, flightRequest(b, large))
	expectCode(t, err, connect.CodeResourceExhausted)
	if observe(b, next.Ticket).GetStatus() != v1.FlightStatus_FLIGHT_STATUS_PENDING {
		t.Fatal("transport rejection changed fill")
	}
	live := join(a, &v1.JoinFlightRequest{Resource: resource, Generation: nextGen, RequestId: "watch-size"})
	large.Completion = &v1.CompleteFlightRequest_Failure{Failure: v1.FlightFailure_FLIGHT_FAILURE_RESULT_TOO_LARGE}
	out, err := service().CompleteFlight(ctx, flightRequest(b, large))
	if err != nil || out.Msg.Observation.GetStatus() != v1.FlightStatus_FLIGHT_STATUS_RESULT_TOO_LARGE || !proto.Equal(out.Msg.Observation, observe(a, live.Ticket)) {
		t.Fatal("oversize marker did not settle attached waiter", err)
	}
	// A present empty bytes field is a successful value; an absent completion is invalid.
	// A 4097-byte value below the transport limit must settle the generation.
	for _, size := range []int{0, 4097} {
		g := get(a)
		owner := join(a, &v1.JoinFlightRequest{Resource: resource, Generation: g, RequestId: "size"})
		msg := &v1.CompleteFlightRequest{Resource: resource, Ticket: owner.Ticket}
		_, err := service().CompleteFlight(ctx, flightRequest(a, msg))
		expectCode(t, err, connect.CodeInvalidArgument)
		msg.Completion = &v1.CompleteFlightRequest_Value{Value: make([]byte, size)}
		res, err := service().CompleteFlight(ctx, flightRequest(a, msg))
		if err != nil {
			t.Fatal(err)
		}
		if size == 0 {
			if _, ok := res.Msg.Observation.Outcome.(*v1.FlightObservation_Value); !ok || len(res.Msg.Observation.GetValue()) != 0 {
				t.Fatal("empty value lost its oneof presence")
			}
		} else if res.Msg.Observation.GetStatus() != v1.FlightStatus_FLIGHT_STATUS_RESULT_TOO_LARGE {
			t.Fatal("missing inline oversize result")
		}
	}

}

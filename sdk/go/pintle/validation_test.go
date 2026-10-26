package pintle

import (
	"context"
	"fmt"
	"google.golang.org/protobuf/proto"
	"net/http"
	"net/http/httptest"
	v1 "pintle.local/spike/gen/go/pintle/v1"
	"strings"
	"testing"
)

func wireLease() *v1.GrantReceipt {
	return &v1.GrantReceipt{Resource: &v1.ResourceId{Namespace: "test", Key: "key"}, RequestId: "acquire", HolderRef: "worker", LeaseId: strings.Repeat("a", 64), FencingToken: "1", Revision: "2", ExpiresAtUnixMs: "10000"}
}
func TestMalformedLeaseObservationsFailClosed(t *testing.T) {
	for _, field := range []string{"fence", "revision", "expiry", "holder", "id", "resource", "capability", "time-bound"} {
		t.Run(field, func(t *testing.T) {
			receipt := wireLease()
			current := proto.Clone(receipt).(*v1.GrantReceipt)
			switch field {
			case "fence":
				current.FencingToken = "2"
			case "revision":
				current.Revision = "1"
			case "expiry":
				current.ExpiresAtUnixMs = "9999"
			case "holder":
				current.HolderRef = "other"
			case "id":
				current.RequestId = "other"
			case "resource":
				current.Resource.Key = "other"
			case "capability":
				current.LeaseId = strings.Repeat("b", 64)
			case "time-bound":
				current.ExpiresAtUnixMs = "9007199254740992"
			}
			if _, err := grant(receipt, false, &v1.LeaseObservation{State: v1.LeaseState_LEASE_STATE_ACTIVE, Current: current}); err == nil {
				t.Fatal("inconsistent observation accepted")
			}
		})
	}
	if _, err := acquired(nil); err == nil {
		t.Fatal("nil response accepted")
	}
}
func TestEndpointPolicyAndNoRedirect(t *testing.T) {
	for _, endpoint := range []string{"http://example.com", "https://user:pass@example.com", "https://example.com?q=secret", "https://example.com/#secret"} {
		if _, err := New(endpoint, "token", nil); err == nil {
			t.Fatal("unsafe endpoint accepted")
		}
	}
	if _, err := New("https://example.com", "bad\nheader", nil); err == nil {
		t.Fatal("header injection accepted")
	}
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("redirect followed"); w.WriteHeader(500) }))
	defer destination.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, destination.URL, http.StatusTemporaryRedirect)
	}))
	defer origin.Close()
	c, err := New(origin.URL, "test-secret", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.OpenSession(context.Background(), Resource{"test", "key"}); err == nil {
		t.Fatal("redirect accepted")
	}
}
func TestResponseSizeAndSessionValidation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/proto")
		fmt.Fprint(w, strings.Repeat("x", 16385))
	}))
	defer server.Close()
	c, _ := New(server.URL, "token", nil)
	if _, err := c.OpenSession(context.Background(), Resource{"test", "key"}); err == nil {
		t.Fatal("oversized response accepted")
	}
	for _, value := range []string{"01", "-1", "18446744073709551616"} {
		s := &Session{next: 1}
		if err := s.update(&v1.RecoverySession{SessionId: "1", AcknowledgedThrough: value, ExpiresAtUnixMs: "1000", RequestWindow: 32}); err == nil {
			t.Fatal("noncanonical ACK accepted")
		}
	}
	s := &Session{next: 1}
	if err := s.update(&v1.RecoverySession{SessionId: "1", AcknowledgedThrough: "0", ExpiresAtUnixMs: "9007199254740992", RequestWindow: 32}); err == nil {
		t.Fatal("unsafe time accepted")
	}
}

func TestClientStringRedactsCredential(t *testing.T) {
	secret := strings.Repeat("a", 64)
	c, err := New("https://example.com", secret, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{fmt.Sprint(c), fmt.Sprintf("%+v", c), fmt.Sprintf("%#v", c)} {
		if strings.Contains(value, secret) {
			t.Fatal("credential logged")
		}
	}
}

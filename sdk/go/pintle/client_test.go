package pintle

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

type noNetwork struct{ t *testing.T }

func (n noNetwork) Do(*http.Request) (*http.Response, error) {
	n.t.Fatal("invalid TTL reached transport")
	return nil, fmt.Errorf("unexpected request")
}
func TestTTLValidationAndStablePreparedRequests(t *testing.T) {
	c, err := New("http://localhost:8787", "local-test", noNetwork{t})
	if err != nil {
		t.Fatal(err)
	}
	for _, ttl := range []time.Duration{0, 99 * time.Millisecond, 100*time.Millisecond + time.Nanosecond, 60*time.Second + time.Millisecond} {
		a, err := NewAcquire(Resource{"test", "key"}, "worker", ttl)
		if err != nil {
			t.Fatal(err)
		}
		id := a.RequestID
		if len(id) != 64 {
			t.Fatal("request ID length")
		}
		for i := 0; i < 2; i++ {
			if _, err := c.TryAcquire(context.Background(), a); err == nil {
				t.Fatal("invalid TTL accepted")
			}
			if a.RequestID != id {
				t.Fatal("request identity changed")
			}
		}
	}
}
func TestLeaseStringRedactsCapability(t *testing.T) {
	l := Lease{Resource: Resource{"test", "key"}, LeaseID: strings.Repeat("a", 64), FencingToken: "42"}
	for _, s := range []string{fmt.Sprint(l), fmt.Sprintf("%+v", &l), fmt.Sprintf("%#v", l), fmt.Sprintf("%#v", &l)} {
		if strings.Contains(s, l.LeaseID) {
			t.Fatal("capability logged")
		}
	}
}

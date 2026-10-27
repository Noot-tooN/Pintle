package hosted

import (
	"net/http"
	"os"
	"testing"
)

func TestStaging(t *testing.T) {
	if os.Getenv("PINTLE_RUN_HOSTED") != "1" {
		t.Skip("remote tests require explicit just test-staging invocation")
	}
	endpoint, token := os.Getenv("PINTLE_STAGING_URL"), os.Getenv("PINTLE_STAGING_TOKEN")
	if err := validateTarget(endpoint, token); err != nil {
		t.Fatal(err)
	}
	Run(t, endpoint, token)
}

func TestTargetValidation(t *testing.T) {
	token := "0123456789abcdef0123456789abcdef"
	for _, url := range []string{"https://pintle-staging.example.workers.dev", "https://pintle-staging.example.workers.dev/"} {
		if err := validateTarget(url, token); err != nil {
			t.Fatal(err)
		}
	}
	for _, url := range []string{"", "http://pintle-staging.example.workers.dev", "https://production.example.workers.dev", "https://pintle-staging.example.workers.dev.evil.test", "https://secret@pintle-staging.example.workers.dev", "https://pintle-staging.example.workers.dev/path", "https://pintle-staging.example.workers.dev?key=x", "https://pintle-staging.example.workers.dev#fragment", "https://pintle-staging.example.workers.dev:443", "http://127.0.0.1:8787"} {
		if validateTarget(url, token) == nil {
			t.Fatalf("accepted disallowed endpoint %q", url)
		}
	}
	for _, token := range []string{"", "short", "0123456789abcdef0123456789abcdef\n"} {
		if validateTarget("https://pintle-staging.example.workers.dev", token) == nil {
			t.Fatal("accepted invalid token")
		}
	}
}

// Exceeding the budget must fail before opening any connection.
func TestRequestBudget(t *testing.T) {
	wire := &transport{}
	wire.calls.Store(96)
	req, err := http.NewRequest("POST", "https://invalid.invalid", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = wire.RoundTrip(req)
	if err == nil || err.Error() != "hosted smoke request budget exceeded" {
		t.Fatalf("budget did not reject: %v", err)
	}
}

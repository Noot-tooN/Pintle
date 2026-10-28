// Command mutex demonstrates public SDK operations without external side effects.
package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pintle "github.com/Noot-tooN/Pintle/sdk/go/pintle"
)

func main() {
	mode := flag.String("mode", "lifecycle", "lifecycle, cancel, recover, with-lease, or with-fencing")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	if err := run(ctx, *mode); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, mode string) error {
	if mode != "lifecycle" && mode != "cancel" && mode != "recover" && mode != "with-lease" && mode != "with-fencing" {
		return errors.New("mode must be lifecycle, cancel, recover, with-lease, or with-fencing")
	}
	endpoint, token := os.Getenv("PINTLE_ENDPOINT"), os.Getenv("PINTLE_TOKEN")
	if endpoint == "" || token == "" {
		return errors.New("set PINTLE_ENDPOINT and PINTLE_TOKEN; see examples/README.md")
	}
	c, err := pintle.New(endpoint, token, nil)
	if err != nil {
		return err
	}
	key, err := pintle.NewRequestID()
	if err != nil {
		return err
	}
	namespace := os.Getenv("PINTLE_NAMESPACE")
	if namespace == "" {
		namespace = "test"
	}
	r := pintle.Resource{Namespace: namespace, Key: "example-" + key}
	if mode == "with-lease" || mode == "with-fencing" {
		if err := managedExample(ctx, c, r, mode == "with-fencing"); err != nil {
			return err
		}
		fmt.Printf("PASS %s\n", mode)
		return nil
	}
	s, err := c.OpenSession(ctx, r)
	if err != nil {
		return fmt.Errorf("open session: %w", err)
	}
	a, err := s.NewAcquire("example", time.Minute)
	if err != nil {
		return err
	}
	// Retain a unchanged. A transport error can mean a committed grant; do not
	// create another request ID to retry it. The recover example shows that boundary.
	g, err := c.TryAcquire(ctx, a)
	if err != nil {
		return fmt.Errorf("acquire uncertain; retain the prepared request: %w", err)
	}
	lease, err := active(g)
	if err != nil {
		return err
	}
	// No application writes occur here. Real work must have its downstream
	// resource atomically validate this fencing token; Lost cannot stop a paused process.
	fmt.Printf("acquired fence=%s (no external writes)\n", lease.FencingToken)
	switch mode {
	case "lifecycle":
		err = lifecycle(ctx, c, s, lease)
	case "cancel":
		err = cancellation(ctx, c, s, lease)
	case "recover":
		err = recovery(ctx, c, s, lease, endpoint, token)
	}
	if err != nil {
		return err
	}
	fmt.Printf("PASS %s\n", mode)
	return nil
}

func active(g *pintle.GrantResult) (pintle.Lease, error) {
	if g == nil || g.Contended || g.State != pintle.Active || g.Current == nil {
		return pintle.Lease{}, errors.New("no current lease; historical receipt is not ownership")
	}
	return *g.Current, nil
}

// Complete the release before relinquishing historical results. If Release is
// uncertain, retain this same request and reconcile it; ACK would discard evidence.
func release(ctx context.Context, c *pintle.Client, s *pintle.Session, lease pintle.Lease) error {
	r, err := s.NewRelease(lease)
	if err != nil {
		return err
	}
	if _, err = c.Release(ctx, r); err != nil {
		return fmt.Errorf("release uncertain; retain the prepared request: %w", err)
	}
	return s.Acknowledge(ctx, r.Attempt.Sequence)
}

func lifecycle(ctx context.Context, c *pintle.Client, s *pintle.Session, lease pintle.Lease) error {
	r, err := s.NewRenew(lease, time.Minute)
	if err != nil {
		return err
	}
	g, err := c.Renew(ctx, r)
	if err != nil {
		return fmt.Errorf("renew uncertain; retain the prepared request: %w", err)
	}
	lease, err = active(g)
	if err != nil {
		return err
	}
	fmt.Println("renewed; fencing token preserved")
	return release(ctx, c, s, lease)
}

func cancellation(ctx context.Context, c *pintle.Client, s *pintle.Session, owner pintle.Lease) error {
	w, err := s.NewWaitAcquire("waiting", time.Minute, time.Minute)
	if err != nil {
		return err
	}
	// Deterministic example: cancellation precedes network admission. Cancellation
	// after admission uses the same CancelPending call; the durable intent may survive.
	call, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = c.Acquire(call, w); !errors.Is(err, context.Canceled) {
		return errors.New("expected cancelled caller context")
	}
	settled, err := c.CancelPending(ctx, w) // fresh, live context; same immutable intent
	if err != nil {
		return fmt.Errorf("cancellation uncertain; retain the wait request: %w", err)
	}
	if settled.State != pintle.Cancelled {
		return errors.New("expected cancellation while owner is held")
	}
	again, err := c.ResumeAcquire(ctx, w) // even delayed admission cannot revive it
	if err != nil || again.State != pintle.Cancelled {
		return errors.New("cancelled intent did not remain cancelled")
	}
	if err = release(ctx, c, s, owner); err != nil {
		return err
	}
	// Now demonstrate the other ordering: a committed grant wins cancellation.
	w, err = s.NewWaitAcquire("grant-wins", time.Minute, time.Minute)
	if err != nil {
		return err
	}
	if _, err = c.Acquire(ctx, w); err != nil {
		return err
	}
	settled, err = c.CancelPending(ctx, w)
	if err != nil {
		return err
	}
	granted, err := active(settled.Granted)
	if err != nil {
		return err
	}
	fmt.Println("cancellation before admission stays cancelled; cancellation after grant requires Release")
	return release(ctx, c, s, granted)
}

// dropRenewal is example-only fault injection after a real successful response.
// It does not change the server or the SDK's production transport.
type dropRenewal struct {
	client  *http.Client
	dropped atomic.Bool
}

func (d *dropRenewal) Do(r *http.Request) (*http.Response, error) {
	res, err := d.client.Do(r)
	if err != nil {
		return nil, err
	}
	if strings.HasSuffix(r.URL.Path, "/Renew") && res.StatusCode == 200 && d.dropped.CompareAndSwap(false, true) {
		_, err = io.Copy(io.Discard, io.LimitReader(res.Body, 16385))
		res.Body.Close()
		if err != nil {
			return nil, err
		}
		return nil, errors.New("example discarded the committed renewal reply")
	}
	return res, nil
}

func recovery(ctx context.Context, c *pintle.Client, s *pintle.Session, lease pintle.Lease, endpoint, token string) error {
	fault := &dropRenewal{client: &http.Client{Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
	managed, err := pintle.New(endpoint, token, fault)
	if err != nil {
		return err
	}
	m, err := managed.Maintain(ctx, lease, time.Minute)
	if err != nil {
		return err
	}
	defer m.Stop()
	select {
	case <-m.Lost(): // stop all protected work immediately; this example has none
	case <-ctx.Done():
		return ctx.Err()
	}
	if !fault.dropped.Load() || m.Err() == nil {
		return errors.New("expected lost renewal reply")
	}
	r := m.Recovery()
	if r == nil {
		return errors.New("missing exact renewal recovery request")
	}
	status, err := c.GetAttemptStatus(ctx, lease.Resource, r.RequestID, r.Attempt)
	if err != nil {
		return fmt.Errorf("cannot establish renewal outcome: %w", err)
	}
	if _, err = active(status.Renewed); err != nil {
		return err
	}
	replayed, err := c.Renew(ctx, *r) // exact payload/session/sequence, no new identity
	if err != nil {
		return err
	}
	if replayed.Receipt == nil || *replayed.Receipt != *status.Renewed.Receipt || !replayed.Replayed {
		return errors.New("renewal replay changed its receipt")
	}
	if err = c.AcknowledgeAttempt(ctx, lease.Resource, *r.Attempt); err != nil {
		return err
	}
	m.Stop() // stops renewal; does not release; Lost stays permanently closed
	fmt.Println("Lost observed; same-attempt status/replay recovered the committed renewal")
	return release(ctx, c, s, lease)
}

// The immutable document's hash is a cache entry: redundant calculation and
// overwriting it with the same answer are harmless, so fencing is unnecessary.
func managedExample(ctx context.Context, c *pintle.Client, r pintle.Resource, fenced bool) error {
	options := pintle.LeaseOptions{TTL: 30 * time.Second}
	if !fenced {
		var cached [32]byte
		err := c.WithLease(ctx, r, options, func(work context.Context, _ pintle.Lease) error {
			if err := work.Err(); err != nil {
				return err
			}
			cached = sha256.Sum256([]byte("immutable document version 1"))
			return nil
		})
		if err != nil {
			return err
		}
		fmt.Printf("cached immutable document hash=%x; duplicate calculation is harmless\n", cached)
		return nil
	}

	// This is the application's destination, not Pintle's coordination store.
	// It is intentionally in-memory for a runnable example with no extra infra.
	inventory := inventoryStore{rows: make(map[string]inventoryRow)}
	var oldToken string
	for _, count := range []int{10, 8} {
		err := c.WithLease(ctx, r, options, func(work context.Context, lease pintle.Lease) error {
			if oldToken == "" {
				oldToken = lease.FencingToken
			}
			return inventory.save(work, "blue-shirt", count, lease.FencingToken)
		})
		if err != nil {
			return err
		}
	}
	// Model a delayed request from the first owner arriving after newer work.
	if err := inventory.save(ctx, "blue-shirt", 10, oldToken); !errors.Is(err, errStaleFence) {
		return errors.New("expected the old inventory write to be rejected")
	}
	fmt.Println("newer inventory stays at 8; delayed stock=10 write rejected")
	return nil
}

var errStaleFence = errors.New("stale fencing token")

type inventoryRow struct {
	stock int
	fence uint64
}
type inventoryStore struct {
	mu   sync.Mutex
	rows map[string]inventoryRow
}

// The fence check and data change share one atomic critical section. In a real
// database integration they must share a transaction/conditional write. Equal
// tokens allow multiple writes by one lease; they do not deduplicate effects.
func (s *inventoryStore) save(ctx context.Context, product string, stock int, token string) error {
	fence, err := strconv.ParseUint(token, 10, 64)
	if err != nil || fence == 0 {
		return errors.New("invalid fencing token")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if fence < s.rows[product].fence {
		return errStaleFence
	}
	s.rows[product] = inventoryRow{stock: stock, fence: fence}
	return nil
}

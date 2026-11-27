// Command cooldown shows two workers reacting to the same stale search index.
package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Noot-tooN/Pintle/sdk/go/pintle"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	client, err := pintle.New(os.Getenv("PINTLE_ENDPOINT"), os.Getenv("PINTLE_TOKEN"))
	if err != nil {
		return err
	}
	namespace := os.Getenv("PINTLE_NAMESPACE")
	if namespace == "" {
		namespace = "test"
	}
	tenant, err := pintle.NewRequestID() // A fresh example tenant on each run.
	if err != nil {
		return err
	}
	key := pintle.Resource{Namespace: namespace, Key: "search-index/" + tenant}

	// Both workers have already noticed the same source revision.
	const revision = 2
	index := &searchIndex{}
	for _, worker := range []string{"worker A", "worker B"} {
		d, err := client.TryAdmit(ctx, key, 30*time.Second)
		if err != nil {
			return err
		}
		switch d.Status {
		case pintle.CooldownAdmitted:
			documents := map[string]string{"guide": "search index", "notes": "search tips"}
			if index.refresh(revision, documents) {
				fmt.Printf("%s published revision %d\n", worker, revision)
			}
		case pintle.CoolingDown:
			fmt.Printf("%s can try again at %s\n", worker, d.RetryAt.Format(time.RFC3339Nano))
		case pintle.CooldownSuperseded:
			fmt.Printf("%s will recheck index freshness after a successor's admission\n", worker)
		}
	}
	return nil
}

// The application owns this index. A production destination must perform the
// revision comparison and publication atomically across all its writers.
type searchIndex struct {
	mu       sync.Mutex
	revision int
	postings map[string][]string
}

func (s *searchIndex) refresh(revision int, documents map[string]string) bool {
	s.mu.Lock()
	current := s.revision
	s.mu.Unlock()
	if current >= revision {
		return false
	}

	postings := make(map[string][]string)
	for id, text := range documents {
		for _, word := range strings.Fields(text) {
			postings[word] = append(postings[word], id)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// A newer build could have finished while we were indexing.
	if s.revision >= revision {
		return false
	}
	s.postings, s.revision = postings, revision
	return true
}

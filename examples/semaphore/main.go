// Command semaphore bounds concurrent cooperative cache jobs without external infrastructure.
package main

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	pintle "github.com/Noot-tooN/Pintle/sdk/go/pintle"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	if err := run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run(ctx context.Context) error {
	c, err := pintle.New(os.Getenv("PINTLE_ENDPOINT"), os.Getenv("PINTLE_TOKEN"))
	if err != nil {
		return err
	}
	const poolCapacity = 2
	const jobs = 6
	sem, err := c.Semaphore(pintle.SemaphoreOptions{Capacity: poolCapacity})
	if err != nil {
		return err
	}
	id, err := pintle.NewRequestID()
	if err != nil {
		return err
	}
	namespace := os.Getenv("PINTLE_NAMESPACE")
	if namespace == "" {
		namespace = "test"
	}
	resource := pintle.Resource{Namespace: namespace, Key: "render-pool-" + id}
	// Six jobs share two permits. Channels hold the first two callbacks until
	// both have entered; there are no sleeps deciding admission or overlap.
	entered := make(chan struct{}, jobs)
	finish := make(chan struct{})
	results := make(chan error, jobs)
	var active atomic.Int32
	for range jobs {
		go func() {
			results <- sem.WithPermit(ctx, resource, pintle.LeaseOptions{}, func(work context.Context, p pintle.Lease) error {
				n := active.Add(1)
				defer active.Add(-1)
				if n > poolCapacity || p.Capacity != poolCapacity || p.PermitSlot < 1 || p.PermitSlot > poolCapacity {
					return fmt.Errorf("invalid permit admission")
				}
				entered <- struct{}{}
				select {
				case <-finish:
				case <-work.Done():
					return work.Err()
				}
				// Cooperative, disposable work (e.g. rendering a cache candidate).
				// No irreversible external writes here. For fencing, a destination
				// must atomically guard each Resource + PermitSlot with FencingToken.
				return work.Err()
			})
		}()
	}
	var first error
	for range poolCapacity {
		select {
		case <-entered:
		case <-ctx.Done():
			first = ctx.Err()
		}
	}
	close(finish)
	// Join every callback and its bounded SDK cleanup before exiting.
	for range jobs {
		if err := <-results; first == nil && err != nil {
			first = err
		}
	}
	if first != nil {
		return first
	}
	fmt.Println("PASS semaphore: six jobs, two simultaneous permits, renewal and release")
	return nil
}

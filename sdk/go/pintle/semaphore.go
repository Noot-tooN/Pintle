package pintle

import (
	"context"
	"fmt"
)

// MaxSemaphoreCapacity bounds authoritative slot storage and work per mutation.
const MaxSemaphoreCapacity uint32 = 64

// SemaphoreOptions configures the pool used by a semaphore client.
type SemaphoreOptions struct {
	// Capacity is the total number of permits for each resource (1..64).
	// Each acquisition takes one permit. The first grant fixes the resource's capacity.
	Capacity uint32
}

// Semaphore returns a reusable client for pools with the given capacity.
// Creating the client makes no network request. Acquiring a permit on a
// resource with a different established capacity returns an error.
// Mutex and semaphore resource names occupy separate coordination namespaces.
// The returned client's session, acquisition and lease methods operate on permits.
// Use WithPermit for scoped work and keep the original client for exclusive locks.
func (c *Client) Semaphore(options SemaphoreOptions) (*Client, error) {
	capacity := options.Capacity
	if capacity == 0 || capacity > MaxSemaphoreCapacity {
		return nil, fmt.Errorf("semaphore capacity must be in 1..64")
	}
	if c.semaphoreRPC == nil {
		return nil, fmt.Errorf("client has no semaphore transport")
	}
	return &Client{rpc: c.semaphoreRPC, semaphoreRPC: c.semaphoreRPC, credential: c.credential, capacity: capacity, leaseSessions: &leaseSessionCache{}}, nil
}

// WithPermit holds one expiring semaphore permit while fn runs, automatically
// renewing it and cancelling the callback context on ownership loss. Cleanup
// and recovery errors follow WithLease's contract. Cancellation is cooperative;
// optional destination fencing must compare tokens within the same permit slot.
func (c *Client) WithPermit(ctx context.Context, r Resource, options LeaseOptions, fn func(context.Context, Lease) error) error {
	if c.capacity == 0 {
		return fmt.Errorf("WithPermit requires a semaphore client")
	}
	return c.WithLease(ctx, r, options, fn)
}

func validPermit(slot, capacity uint32) bool {
	return slot == 0 && capacity == 0 || capacity >= 1 && capacity <= MaxSemaphoreCapacity && slot >= 1 && slot <= capacity
}
func (c *Client) checkLease(l Lease) error {
	if l.Capacity != c.capacity {
		return fmt.Errorf("lease belongs to another primitive or capacity")
	}
	return validateOwnership(l)
}

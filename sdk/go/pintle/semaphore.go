package pintle

import (
	"context"
	"fmt"
)

// MaxSemaphoreCapacity bounds authoritative slot storage and work per mutation.
const MaxSemaphoreCapacity uint32 = 64

// Semaphore returns a client bound to the semaphore service and expected
// capacity (1..64). Keep and reuse it, including for recovery. The first
// successful grant fixes each resource's capacity; mismatches fail explicitly.
// Mutex and semaphore resource names occupy separate coordination namespaces.
// The returned client supports the same session, acquisition and lease methods.
func (c *Client) Semaphore(capacity uint32) (*Client, error) {
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

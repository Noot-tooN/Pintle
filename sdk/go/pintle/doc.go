// Package pintle provides server-owned distributed mutex operations over binary
// unary Connect. It does not arbitrate contention in the client.
//
// Create a Client with New. WithLease manages scoped callback acquisition, renewal,
// cooperative cancellation and bounded cleanup; fencing remains destination-enforced.
// For explicit recovery/persistence, OpenSession for one Resource. Prepare operations
// with Session.NewAcquire, NewWaitAcquire, NewRenew or NewRelease. Retain each
// request unchanged across retries, including its Attempt. Package-level request
// constructors omit sessions and are for the earlier focused protocol modes, not
// the current service. Client and Session methods support concurrent callers;
// callers own request immutability and acknowledgement ordering.
//
// A nil RPC error is not necessarily ownership: inspect Contended, State and
// Current. Receipt is historical. Cancellation or timeout does not undo a grant
// or cancel durable waiting intent. Reconcile with the same request; CancelPending
// may return a grant that requires explicit Release. Acknowledge only after safely
// consuming every result through that sequence; ACK never releases ownership.
//
// Maintain opts into renewal; its Lost signal is permanent and Stop never releases
// the lease. Pause protected work on Lost and retain Recovery for reconciliation.
// Downstream systems must atomically enforce fencing for safe external effects.
// Fencing tokens are exact decimal strings, not floating-point numbers.
//
// See README.md in this package and examples/mutex for the supported API contract,
// installation, finite recovery windows and runnable lifecycle/cancellation/recovery
// examples. This unpublished v0 API is not yet frozen.
package pintle

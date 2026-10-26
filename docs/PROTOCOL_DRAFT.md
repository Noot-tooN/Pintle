# Provisional public protocol — semantic draft, NOT frozen

This is an **illustrative contract design**, not a ready-to-compile final `.proto`. Do not silently publish it as a stable `v1`; local Workers/Wasm transport integration is verified, but release gates remain open. The source of truth for behavior is `PROJECT_SPEC.md` + `docs/GUARANTEES.md`.

**Implemented through P2:** the unpublished compiled schema is
[`proto/pintle/v1/lock.proto`](../proto/pintle/v1/lock.proto). Generated Go Connect
clients and the Go SDK interoperate with the Rust/Wasm Fetch adapter locally for
TryAcquire, Renew, Release, and GetRequestStatus. See [p1-results.md](p1-results.md)
and [ADR-008](adr/ADR-008-p1-lease-lifecycle.md) for exact semantics, including
immutable receipts, current observations, TTL bounds, and request retention.
P1.1 additionally implements OpenSession and AcknowledgeSession. Lifecycle and
status requests require RequestAttempt (decimal session ID, sequence, and ACK
boundary). See [ADR-009](adr/ADR-009-bounded-recovery-sessions.md) for bounded
recovery, terminal contention replay, explicit acknowledgement, and expiry.
Acknowledgements reclaim results and do not release locks. Recovery is no longer
indefinite. Legacy P1 remains a separate capped regression mode.
The sketches below describe a broader future service and are not the implemented
wire schema. P2 implements Acquire, CancelPending, and Pending/Cancelled/Expired
status using [ADR-010](adr/ADR-010-server-owned-bounded-acquire.md). AcquireRequest
contains the immutable session acquisition plus wait_timeout_ms. CancelPending
wraps that SAME request; AcquireResponse returns a retained grant or WaitState.
Pending is a bounded HTTP response, not a new client-side contention attempt.
Passing local integration and Buf compatibility does not freeze/publish v1.

## RPCs (server-side authoritative)

```text
LockService.TryAcquire(request) -> Acquired | Contended | ReplayedTerminal
LockService.Acquire(request)    -> Acquired | Deadline/Cancelled/Uncertain
LockService.Renew(request)      -> Lease | LeaseLost
LockService.Release(request)    -> Released | AlreadyReleased | LeaseLost
LockService.GetRequestStatus(id)-> Pending | Granted(lease) | Cancelled | Expired | NotFound
LockService.CancelPending(id)   -> Cancelled | AlreadyGranted(lease reference) | Terminal
```

`GetRequestStatus` recovers retained terminal attempts in P1.1 (including Contended); legacy P1 recovers successful mutation receipts only. `CancelPending` and pending/cancelled/expired states are implemented in P2. `Acquire` uses bounded unary waiting with resumable durable server-owned state. The logical service operation remains server-owned even if one transport connection ends.

## Potential messages (sketch; no stability promise)

```proto
syntax = "proto3";
package pintle.v1;

message ResourceId {
  string namespace = 1;
  string key = 2;
}

message AcquireLockRequest {
  ResourceId resource = 1;
  string request_id = 2;         // Stable across retries of same logical request
  uint64 requested_lease_ms = 3;
  uint64 max_wait_ms = 4;        // 0 for try_acquire semantics if shared request type
}

message Lease {
  ResourceId resource = 1;
  string lease_id = 2;           // Opaque, unguessable owner capability
  string fencing_token = 3;     // Decimal/text canonical until exact uint64 path verified
  string revision = 4;          // Distinct from fencing generation
  uint64 expires_at_unix_ms = 5; // Informational; client clocks cannot establish ownership
}

message Contended {
  string observed_revision = 1;
  uint64 retry_not_before_unix_ms = 2;
}

message TryAcquireLockResponse {
  oneof outcome {
    Lease acquired = 1;
    Contended contended = 2;
  }
}
```

In a final protocol, choose representation using protobuf scalar `uint64` or string deliberately. Standard ProtoJSON encodes uint64 as decimal strings; Rust/JS Wasm conversion must be exact for internal fencing semantics, not just network messages. Tenant identity should come from authentication and must not be spoofable by user-supplied `namespace` alone. `lease_id` may not be the sole needed bearer capability; renew/release auth design is open.

## Error model

At minimum distinguish `INVALID_ARGUMENT`, `UNAUTHENTICATED`, `PERMISSION_DENIED`, `CONTENDED` (as outcome rather than fatal error for TryAcquire), `DEADLINE_EXCEEDED`, `CANCELLED`, `LEASE_LOST`, `NOT_OWNER`, `REQUEST_ID_CONFLICT`, `UNAVAILABLE`, `COMMIT_UNKNOWN`, `UNSUPPORTED_CAPABILITY`, and `RESOURCE_EXHAUSTED`. Use Connect/gRPC standard error mapping where possible without losing semantic distinctions. Avoid treating network timeout as proof that no grant occurred.

## Compatibility policy

- Version Protobuf package (`pintle.v1`), generate clients, lint/breaking-change check with Buf.
- Treat `Acquire`/`TryAcquire` as separate operations in service layer and public SDK even if request fields share types.
- Cross-backend capabilities are negotiated/declared rather than silently weakened.
- Keep resource key canonicalization and tenant isolation stable; no user-controlled raw object IDs.
- Connection lifetimes, wait ticket retention, idempotency retention and max TTLs should be a server-declared contract with documented bounds.

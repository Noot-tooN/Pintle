# ADR-008: P1 lease lifecycle, authority time, and recovery

Status: accepted for the unpublished local P1 implementation (2026-10-26).
Production clock behavior, operational receipt retention, and authority migration
remain open. This extends ADR-005/006/007; it does not implement P2 waiting.

## Decision

The authoritative algorithm is `crates/core/src/lease.rs`, using only the generic
`Transaction` get/put boundary, a supplied authority-time sample, and a supplied
fresh capability. Cloudflare routing, authentication, clock access, randomness,
Protobuf, deadlines, and durability acknowledgement live in the adapter. There
is no PostgreSQL database or second coordination store: the same DO SQLite
transaction stores owner, expiry, fencing counter, revision, clock high-water
mark, receipt count, and successful-operation receipt.

The time sample is taken inside the synchronous transaction callback. No await,
network call, or test checkpoint occurs inside a transition. A fresh acquisition
performs six writes; renewal/release perform five. An error from storage or
record validation aborts the transaction. Expected business rejections are return
values: they may advance the clock high-water mark, but do not change owner,
fence, revision, receipt count, or receipts. The adapter waits for `storage.sync()`
before acknowledging success. Loss of acknowledgement remains an unknown outcome.

## Lease and ownership rules

- TTL is a whole number of milliseconds in [100, 60000]. Zero is invalid.
- The lease is active iff `authority_now < expires_at`; equality is expired.
- Expiry is lazy: no alarm is required for nonblocking acquisition. A new request
  can replace an expired owner atomically.
- Each distinct grant increments a checked u64 fencing counter. Renew and
  Release leave that counter unchanged; every successful mutation increments a
  checked u64 revision. Neither counter wraps or resets. Wire/storage values are
  canonical decimal strings, including above 2^53.
- Each fresh grant receives 256 random bits from Web Crypto `getRandomValues`.
  Renew/Release require that capability and the original authenticated subject.
  Capability matching for ownership uses `subtle` constant-time comparison.
  Holder labels are not credentials. Capability values must not be logged.
- Renew uses `max(previous_expiry, authority_now + requested_TTL)`, so a shorter
  requested TTL cannot shorten an existing lease. It cannot revive an expired,
  released, or superseded lease.
- Release requires an active matching owner and writes a null owner. A replay
  of an earlier successful Release returns its original revision and never
  clears a successor. A fresh Release ID after release/expiry fails explicitly.

## Time model and its limits

The adapter currently supplies Workers `Date::now()` in Unix milliseconds. Time
must fit in 2^53−1; expiry addition is checked. Every valid operation that reaches
state evaluation, including status, contention, and ownership rejection,
persists a clock high-water mark. A smaller subsequent time sample fails closed
with `unavailable`, including after local process restart. This prevents a lease
already observed expired from becoming active again on a backward sample.

This is an authority-clock contract, not a measured real-time lease guarantee.
The high-water mark cannot detect backward motion between observations, prevent
forward jumps, or make a stalled clock progress. A forward jump can expire a
lease early in elapsed real time; a backward jump can reduce availability until
clock recovery. Hosted Workers clocks advance after I/O, and local behavior
is different; see Cloudflare's [performance/time documentation](https://developers.cloudflare.com/workers/runtime-apis/performance/).
Hosted object replacement, failover, and clock behavior have NOT been tested.
No client may infer current ownership solely from its wall clock or an old
response. Expired clients can continue computing; external writes still require
a cooperating fenced resource. Snapshot restoration and authority migration are
unsupported.

## Idempotency and status

Successful mutation receipts are immutable. Request IDs share one namespace
per resource across TryAcquire, Renew, and Release. Reusing a committed ID with a
different method or payload returns `already_exists`; another subject cannot
read or reuse its receipt. Status authenticates the original subject and returns
that receipt. Contention and other rejected attempts do not reserve an ID.

A receipt proves a historical mutation. Acquire/Renew replies also contain a
separate observation: Active (with the latest renewed lease), Expired, Released,
or Superseded. This observation is made during the transaction and may already
be stale on delivery. Renew receipts retain the original acquisition ID in
`GrantReceipt.request_id`; the Renew request ID remains the lookup/dedupe key.
Replaying Renew never extends expiry again. Status itself performs no renewal.
`NotFound` means no committed receipt was observed; it cannot cancel an in-flight
operation or prove that operation will never commit.

Timeouts, connection loss, and unavailable acknowledgement must be reconciled
with the SAME prepared request ID and payload or GetRequestStatus. There is no
blanket exactly-once or timeout-implies-rollback promise. The Go SDK exposes
explicit lifecycle calls and stable prepared requests; it has no automatic retry,
background renewal, Lost signal, or SDK contention loop in P1.

## Retention and bounded operation

P1 retains receipts for the lifetime of the resource's stored state, with no GC
or eviction. The bound is 1,024 successful mutations per resource. Acquire and
Renew stop at 1,023 receipts, reserving one final slot for Release. Replays and
status remain available at the limit (subject to clock validity). This is a
conservative local milestone policy, not sufficient for a sustained production
service. Counter exhaustion likewise fails closed.

Do not delete/recreate resource state to free capacity: doing so resets fencing
history and permits old request IDs to execute again. A later retention design
must define safe retry expiration/tombstones or an epoch scheme before GC is
introduced. Capacity can permanently stop new work on a resource; this explicit
availability limitation is preferable to silent dedupe eviction.

## Protocol and fixture separation

Buf generates Go messages/Connect clients and Prost Rust messages. The existing
narrow binary unary Connect Fetch adapter now exposes TryAcquire, Renew, Release,
and GetRequestStatus. No third-party JavaScript coordination algorithm is used.

P1 is enabled only with `P1_ENABLED=true`; default Wrangler configuration enables
no service fixture. P1 rejects mixed P0/P0b modes. Its DO identity is the canonical
JSON tuple `(authenticated tenant, namespace, "mutex-v1", resource key)`. P0's
disposable experiment uses a different primitive identity. This is isolation of
unpublished test data, not a safe production migration protocol. No automatic
migration of prior grants is claimed.

The schema additions pass Buf's P0b FILE compatibility baseline, but semantic
compatibility is not promised: P1 requires a nonzero TTL whereas the legacy P0b
fixture did not implement leases. The protocol and Go module path remain
unpublished. Authentication is still the ephemeral local credential fixture;
a production credential provider is not implemented.

## Evidence

See [P1 results](../p1-results.md) for native model exploration, controlled runtime
schedules, every-write rollback, expiry edges, acknowledgement loss, deadlines,
restart, generated-client/SDK integration, and remaining limitations.

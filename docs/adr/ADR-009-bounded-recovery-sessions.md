# ADR-009: Bounded recovery sessions and automatic reclamation

Status: accepted for local P1.1, 2026-10-26. Supersedes ADR-008's permanent receipt
policy for the P1.1 service. P0/P0b/P1 fixtures remain available for regression.
No hosted deployment or P2 blocking service is included.

## Contract

A resource's authoritative transaction owns its lease state, fencing/revision
counters, clock high-water mark, session allocation counter, and recovery sessions.
No second database or cross-DO session service is introduced. Session decisions
are pure Rust in `crates/core/src/session.rs`; Cloudflare supplies storage, time,
random capabilities, durability acknowledgement, and alarm scheduling.

The client opens a resource-scoped session. The server allocates a strictly
increasing decimal u64 ID, persisted in the same resource. IDs never wrap or
reset. The authoritative attempt identity is `(resource, session_id, sequence)`.
`request_id` remains an immutable payload label, not a standalone dedupe key.
Changing payload/method/request_id for a retained attempt is a conflict. Reusing
a label with a DIFFERENT sequence is a new operation. SDK retries must preserve
the complete prepared request, including the attempt envelope.

The server accepts a bounded span of sequence numbers after the acknowledged
boundary. Concurrent, out-of-order requests are permitted within that span.
Each executed terminal decision (including contention and ownership rejection)
is cached. Thus a contended attempt stays contended on replay; a deliberate new
attempt needs a new sequence. Invalid envelopes, expired sessions, absent status,
window exhaustion, clock regression, and payload conflicts are not new operations.
Storage errors abort the transaction and consume no attempt.

Successful lease receipts retain ADR-008's immutable historical result plus a
fresh current-ownership observation on replay. The existing lease transition is
reused through a transaction view that supplies its cached receipt and replaces
its legacy permanent ledger/count. Owner, expiry, revision, fence, and clock
reads/writes always reach the real transaction. No coordination logic is copied
into the SDK or Cloudflare adapter.

## Acknowledgements and recovery

`AcknowledgeSession(through)` means the application relinquishes the results of
all sequence numbers up to that boundary. It does not release, shorten, or renew
an ownership lease. The server requires all newly acknowledged sequence numbers
to have a cached terminal result: gaps cannot be skipped. It advances the durable
boundary and removes those entries atomically. A delayed duplicate at or below
the boundary returns `failed_precondition` with `result_acknowledged`, never
executes again, and does not expose a successor's capability. Repeating an older
ACK is harmless. Mutation envelopes may piggyback an ACK boundary.

The SDK allocates sequences under a mutex. It never automatically acknowledges
results on network receipt. Applications call Session.Acknowledge after consuming
results, or after durably recording them if cross-process recovery is required.
Client.AcknowledgeAttempt supports a persisted request after client restart.
Session.Heartbeat sends the existing acknowledgement boundary and refreshes idle
recovery lifetime; it does not acknowledge additional results or renew the lock.
There is no automatic heartbeat goroutine or automatic replacement session.

Prepared requests must not be silently discarded: an unsent/unresolved sequence
can block contiguous acknowledgement and eventually fill the window. Finish or
reconcile that attempt, or deliberately abandon the session; no cancellation of
pending operations is implemented before P2. Opening a new session after an
unknown mutation does not make retrying that mutation with a new identity safe.

OpenSession itself grants no lock and has no idempotency key. A lost response can
leave an orphan session, bounded by the session quota and cleaned by expiry. A
retry can allocate another session; the SDK must not claim it recovered the first.
There is no durable promise of session allocation success after response loss.

## Bounds and lifecycle

Initial fixed policy (not performance-tuned):

| Item | Bound |
| --- | --- |
| Live sessions per resource | 16, with the final slot reserved for the current authenticated owner's recovery |
| Sequence window per session | 32 beyond the acknowledged boundary; final position reserved for Release |
| Idle session lifetime | 5 minutes after successful activity/heartbeat |
| Oldest unresolved receipt lifetime | 10 minutes; heartbeats cannot pin unresolved history forever |
| Serialized session payload | At most 65,536 bytes |

The effective expiry is the earlier of the idle deadline and the oldest retained
entry's creation time plus ten minutes. The ten-minute value is an upper limit,
NOT a guaranteed minimum recovery window: inactivity expires the session sooner.
Equality at the expiry deadline is expired. Requests check expiry transactionally
before executing or extending the session. A late heartbeat cannot revive it.
Persisted clock regression rejects requests/cleanup without resurrecting state.
The clock assumptions and unsupported hosted behavior from ADR-008 still apply.

All session quota and request-window decisions are server-enforced, including
for raw generated clients. Clients can temporarily exhaust capacity; they cannot
force unsafe eviction. Reservations are finite, not unlimited renewal capacity.
An owner must acknowledge earlier results to keep renewing indefinitely. Rejected
or lost owner operations still require explicit reconciliation and lease-loss
handling. Session expiration does not release an ownership lease: that lease
keeps its original expiry and capability. A known owner may deliberately use a
new session to continue operations on that still-active lease.

There is no per-resource lifetime operation limit in P1.1. Session IDs and fencing
counters remain checked finite u64 values; exhaustion fails closed rather than
wrapping.

## Storage choice and cleanup

Use 16 fixed, reusable session slots in the existing SQL `records` table. Each
slot stores a bounded JSON object containing metadata and up to 32 cached
results. ACK removes entries from the JSON; expiry replaces the slot with `null`.
Opening another session reuses the slot with a new ID. There are no accumulating
receipt rows or per-expired-session tombstones. Missing session IDs are always
rejected; only OpenSession can allocate a fresh server ID.

This is a deliberate simplification versus an indexed receipt table: fixed slots
work through the existing generic get/put transaction boundary, bound cleanup
work, and avoid introducing range scans/deletion into the portable interface.
The cost is bounded scans of session slots and rewriting a session JSON record
on activity. This may cost more reads/writes on Cloudflare than a normalized
layout; hot-key throughput and billing are unmeasured. Change layout if evidence
justifies it, without changing the recovery contract.

For a fresh P1.1 resource, logical storage is at most 16 bounded session slots
plus five owner/metadata keys (clock, session counter, owner, fence, revision).
Session payload totals are at most 1 MiB, plus those small records. SQLite pages,
indexes, WAL, and platform history have additional physical overhead. This is
not a one-MiB physical/billing guarantee.

Cleanup runs on requests and scheduled Durable Object alarms. Before admitting
a session transaction, the adapter durably arms a 60-second fallback alarm. After
commit/sync, it re-reads the committed session deadlines and sets the next alarm,
or removes the alarm when no sessions remain. This covers process loss between
commit and final scheduling. Alarm callbacks first arm their own fallback, then
run bounded transactional cleanup, sync, and reschedule. Repeating cleanup is
safe. No await occurs between reading the current schedule and issuing its alarm
storage operation. The persisted clock floor prevents stale samples from
postponing scheduling after later authority-time observations.

Errors keep the fallback or platform alarm retries available. A platform storage
outage can still prevent arming/recovery; failed requests cannot claim success,
future requests retry cleanup, and production needs monitoring. Alarms are not a
proof of availability through arbitrary outages. Inactive resources normally
need no manual cleanup and no separately operated cron server. Other backends
must provide an equivalent durable scheduler adapter.

## Compatibility and limits

P1.1 is selected by `P11_ENABLED=true`, with no flag enabled in default Wrangler
configuration. `P1_ENABLED` remains the old capped fixture for regression, and
P1.1 refuses mixed legacy flags and requests missing an attempt. The SDK's new
Session preparation methods target P1.1; legacy standalone request constructors
remain for old tests. The same `mutex-v1` resource identity and lease/fence keys
are used: this change does not reset fencing. Existing legacy receipt rows are
not automatically deleted; migration of already-used P1 fixtures is outside this
local milestone and would need a defined end to their old retention promise.

The schema remains unpublished. GetAttemptStatus / Session.Status require the
original attempt envelope. Acknowledged/expired results are explicitly no longer
recoverable, which intentionally replaces indefinite historical lookup. Status
NotFound is still a snapshot and does not cancel in-flight work.

Unlimited distinct resources still retain per-resource fencing/identity metadata.
Resource retirement, generation-safe name reuse, namespace quotas, backup restore,
and authority migration remain separate work. Do not delete resource state to
recover capacity. No external side-effect exactly-once guarantee is introduced.

## Prior art and evidence

The protocol follows the session/sequence/acknowledgement principles described
in [RIFL §4](https://web.stanford.edu/~ouster/cgi-bin/papers/rifl.pdf) and implemented
in [LogCabin](https://github.com/logcabin/logcabin/blob/master/Server/StateMachine.cc).
It adapts them to a single-resource transactional authority; it does not adopt a
global lease coordinator. See [P1.1 results](../p11-results.md) for actual tests and
[Cloudflare alarm semantics](https://developers.cloudflare.com/durable-objects/api/alarms/)
for the platform's finite automatic retry behavior.

# Correctness & Guarantees — Local Evidence and Production Targets

**Status: draft/specification, NOT proof of an implementation.** Nothing in this repository yet establishes production guarantees. An AI agent must never turn the items below into marketing claims just because a test fixture passed.

## Local implementation evidence

The local P0 spike now has passing atomicity, rollback, contention, replay,
precision, and persistent-restart checks. See [spike-results.md](spike-results.md)
for exact scope and results. These do **not** establish the full lease invariants
or hosted production guarantees below.

P0b additionally verifies a generated Go Connect client against the local
Rust/Wasm gateway and DO, including authenticated tenant/namespace isolation,
bounded requests, exact tokens, and recoverable deadline/response-loss cases.
See [p0b-results.md](p0b-results.md). In the legacy P0b fixture, a `GrantReceipt` has no lease or ownership
capability; P1 extends the unpublished schema as described below.

Deterministic hardening now checks bounded model traces, controlled transaction
admission/response schedules, logical deadline races, and real-runtime histories.
See [hardening-results.md](hardening-results.md) for the finite bounds, failure
matrix, and untested platform behavior. The production targets below remain
unverified.

**P1 lease semantics now pass locally.** See [p1-results.md](p1-results.md) and
[ADR-008](adr/ADR-008-p1-lease-lifecycle.md) for the implemented contract. TTL is
100–60000ms; equality at expiry is expired. Renew never shortens expiry. Owner
capability plus authenticated subject guards Renew/Release. Immutable operation
receipts are separate from current-state observations; replay never renews again
or releases a successor. Clock regression below the persisted high-water mark
fails closed. Authority time is not a bounded elapsed-real-time guarantee.

| Invariant | Current evidence / unsupported scope |
| --- | --- |
| I1–I4 | Native independent lease model and controlled expiry/concurrency tests; only within the declared authority-clock and continuous-storage model. |
| I5 | Actual local SQLite rollback at every lifecycle write; storage.sync before acknowledgement; local process restart. Hosted replication/durability unverified. |
| I6 | P2 durable FIFO registration, sticky bounded notification, controlled release/read gaps, actual expiry alarms, and restart/resume pass locally. Hosted liveness remains unverified. |
| I7 | Same-ID retry and status after response loss, controlled precommit/post-sync shutdown, deadline and acknowledgement errors. NotFound cannot rule out later commit. |
| I8 | Hosted partition/failover and authority replacement unverified. No cross-authority safety claim. |
| I9 | Authenticated tenant/namespace routing and local fixture isolation tests. Production authentication policy remains open. |
| I10 | Bounds, checked counters, receipt capacity, and unsupported wire modes fail explicitly in local tests; operational limits beyond tested cases remain unverified. |

**P1.1 recovery sessions pass locally.** [P1.1 results](p11-results.md) and
[ADR-009](adr/ADR-009-bounded-recovery-sessions.md) supersede the 1,024 lifetime
receipt cap for the new mode. Sixteen reusable session slots, 32 sequence positions
per session, explicit contiguous ACKs, five-minute idle expiry, and a ten-minute
oldest-result limit bound retained history. ACK and actual DO alarm cleanup are
transactional. Acknowledged/expired attempts fail explicitly and cannot re-execute.
The attempt identity includes resource/session/sequence, not request_id alone.
Recovery ends at ACK or expiry; neither changes lease ownership or resets fences.
The legacy P1 fixture remains capped and its existing receipts are not migrated.
Do not reset resource storage to recover capacity or fencing history.

**P2 server-owned waiting passes locally.** [P2 results](p2-results.md) and
[ADR-010](adr/ADR-010-server-owned-bounded-acquire.md) record FIFO registration,
atomic cancellation/grant, no-bypass TryAcquire, deadline boundaries, actual
alarms, and process recovery. Pending intents survive HTTP disconnection;
cancellation can return an already committed grant and never implicitly releases
it. Sixteen pending intents and sixteen HTTP subscriptions are permitted per
resource/instance. Waits are at most sixty seconds, with eight-second resumable
unary holds. ACK cannot discard pending work. These are finite local tests, not
proof of hosted availability, bounded real-time expiry, or starvation freedom.

## Language of guarantees

- **Linearizable per-resource grant:** each committed ownership decision for the same resource has one authoritative order within that backend's declared transaction scope.
- **Fencing generation:** strictly increasing for *distinct* successful grants on one resource; stable on idempotent replay; old never reused after release/expiry or legitimate migration.
- **Lease:** time-bounded permission granted by service authority; does *not* guarantee stale client ceases computation after expiry.
- **External write safety:** only possible with cooperating protected resource checking an appropriate fencing/transaction protocol; issuing tokens alone is insufficient.
- **At-least-once notification:** not assumed. Watches are best-effort wakeup hints reconciled with authoritative revision; important distinction between notification delivery and no-lost-wakeup waiting semantics.

## Target mutex model (P2 supplies locally tested blocking and nonblocking operations)

| Request | Preconditions | Durable change | Response |
|---|---|---|---|
| New `TryAcquire` success | No valid owner, no higher-priority eligible waiter if fairness enforced | Atomically write owner, expiry, strictly increased fence, dedupe record, revision | Confirmed lease/token |
| Contended `TryAcquire` | Valid owner or not eligible | No new owner/fence | Contended + observation |
| `Acquire` | Same grant rules, server owns wait state | Durable waiter/eligibility metadata as required | Lease once grant committed, else deadline/cancel status |
| Duplicate acquisition ID | Existing request with equal parameters | No extra grant, no additional fence increment | Previously committed outcome or defined terminal status |
| `Renew` | Matching active owner credentials and lease still valid | Atomically extend lease, optionally revision | Current lease info |
| `Release` | Matching active owner | Atomically clear owner, change revision and wake eligible waiters | Idempotent defined release result |
| Expire | Lease deadline reached using specified authoritative clock model | Treat as no longer active; may be materialized lazily or by alarm | New eligible grants allowed |
| Different owner `Renew`/`Release` | Token/owner mismatch or superseded grant | No effect on successor | `NOT_OWNER`/`LEASE_LOST` |

**Outstanding design obligation:** hosted time behavior under drift, pauses, and region replacement; reliable fencing across snapshots/migration; hosted cleanup availability and storage/cost behavior; hosted wait/notification liveness and performance; tenant fairness beyond the bounded FIFO policy.

## Required invariant suite

- `I1`: Never commit two different *unexpired grants* for the same resource whose validity overlaps under the server's declared time model. Caveat: execution of stale workers may still overlap.
- `I2`: For the same resource, `fence(new_grant) > fence(any_prior_distinct_grant)` within a continuous or correctly migrated authority history.
- `I3`: An idempotent replay never creates a second grant/token for one logical attempt (resource/session/sequence in P1.1).
- `I4`: An old owner cannot renew/release a later owner's lease.
- `I5`: Every acknowledged successful grant corresponds to a durable atomic commit of owner+fence+dedupe+revision; no partial success.
- `I6`: Waiting clients must not remain blocked forever purely because a state transition occurred between inspection and registration; reconnect always reconciles authoritative state.
- `I7`: Timeouts/network failures yield explicit unknown/recoverable status when commit might have happened; no blind new-ID retry.
- `I8`: Partition/uncertain authority must not become a second active decision authority for one resource.
- `I9`: Tenants cannot access or collide with another tenant's resources by choosing identical key strings.
- `I10`: All documented limits and capability restrictions are observable and fail explicitly.

## Risks and non-guarantees

The [recovery policy and runbooks](recovery-policy.md) define permitted recovery
actions and the fail-closed response to missing or uncertain authority history.

- **No generic exactly-once execution.** Fencing and idempotency of coordinator requests cannot make arbitrary HTTP/payment/file operations atomic.
- **No instant stale-client revocation.** Lease expiry cannot forcibly interrupt a paused worker. Downstream fencing is separate.
- **No cross-DO atomic transactions.** Every multi-key invariant must fit within one DO object for the first backend.
- **No universal fast global latency.** One authority per key can be distant from some callers.
- **No unbounded durable event log from watches.** A watch is not Kafka; recheck revisions on notification/reconnect.
- **No unlimited recovery history.** P1.1 rejects acknowledged/expired attempts; window/session exhaustion requires reconciliation or expiry. Distinct resource names still retain fencing metadata. Counters never wrap.
- **No guaranteed safe backup restoration/handoff** until an authority-epoch/fencing-migration protocol is implemented and tested.

## Evidence required before labeling guaranteed

For each invariant list: model/property test, local runtime integration test, failure-injection test, **live-platform** validation where Cloudflare lifecycle matters, source revision/CI link, limits/assumptions, and unresolved risks. `docs/spike-results.md` begins that evidence trail.

See authoritative platform semantics: https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/ ; https://redis.io/docs/latest/develop/clients/patterns/distributed-locks/ .

## P3 local hardening evidence

[P3 results](p3-results.md) adds a two-waiter reference model, credential expiry/rotation
and admission-boundary checks, renewal/Lost failure ordering and real committed-response
loss recovery, SDK response validation, sanitized metrics and local latency observations.
Lost is not proof of ownership or immediate revocation; static key expiry/removal does
not cancel already admitted durable intent. Hosted/release guarantees remain unverified.

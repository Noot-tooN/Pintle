# P1 — Nonblocking lease service results

> Tooling update: command names below now use the root `justfile`. The milestone
> runs originally used equivalent shell scripts; their captured transcripts are
> unchanged. See [Just migration results](just-migration-results.md) for a full
> suite run through Just.

**PASS locally on macOS arm64, 2026-10-26.** P1 adds TryAcquire, Renew, Release,
and GetRequestStatus through a portable Rust state machine, compiled Rust/Wasm
Worker/DO SQLite adapter, generated Protobuf/Connect clients, and a small Go SDK.
No deployment, push, additional backend, or P2 blocking acquisition was performed.
This is finite local evidence, not proof of all possible schedules or hosted HA.

## Implementation and acceptance

| P1 gate | Implemented evidence |
| --- | --- |
| Infrastructure-independent core | `crates/core/src/lease.rs` takes a generic transaction, time sample, and fresh capability; native tests need no cloud/database. |
| Atomic nonblocking lifecycle | TryAcquire, Renew, Release update all state and successful receipts in one DO SQLite transaction; actual SQL and Rust errors roll back every lifecycle write. |
| Ownership and fencing | 256-bit grant capability plus authenticated subject; stale owners cannot mutate successors; fences increment only on distinct acquisition; exact decimal u64 encoding and checked overflow. |
| TTL/expiry | Explicit 100–60000ms TTL, exact expiry boundary, non-shortening renewal, lazy expiry, persisted clock high-water mark. |
| Idempotency and uncertain outcomes | Same-ID immutable replay plus current lease observation; status recovery; controlled precommit/post-sync process death, deadline expiry, and lost acknowledgements. |
| Go SDK and real clients | `sdk/go/pintle` wraps generated Connect clients; tests run against ordinary local Wrangler with SQLite persistence, concurrent retries/contention, response drop, and restart. |
| Tested vs unsupported guarantees | Updated [GUARANTEES.md](GUARANTEES.md) and [ADR-008](adr/ADR-008-p1-lease-lifecycle.md). |

P1 follows the nonblocking milestone in [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md).
The project specification's older feasibility-spike numbering includes a separate
blocking-wait experiment; that work belongs to executable milestone P2 and was
not implemented here.

## Reproduce and executed checks

```sh
./bin/just bootstrap  # Only needed for initial pinned tool installation
./bin/just test
```

Tools use the existing Hermit environment and checked-in Cargo/npm/Go lockfiles.
No new executable dependency was needed. The portable core now directly depends
on pinned `subtle` 2.6.1 for capability comparison (already present in the existing
adapter dependency graph); Cargo.lock records that edge. No global installation.
Buf remains the schema tool, with generated Go Protobuf/Connect code and Prost
Rust messages. The runtime is local Wrangler/Miniflare/workerd, not a JavaScript
simulation of lease logic. Narrow wasm-bindgen bindings adapt transactionSync
and Web Crypto; coordination remains Rust.

The full command runs Buf lint/format/FILE compatibility and regeneration checks,
Rust fmt, 15 native tests, native/Wasm Clippy with warnings denied, release Wasm
builds, generated Go/SDK/runtime tests with Go's race detector, instrumented
runtime tests, and restoration of a normal Wasm artifact. The original P0/P0b
regressions remain included. [p1-test-output.txt](p1-test-output.txt) is the actual
successful transcript, with only the absolute checkout path normalized. The
final ordinary integration run took 12.742 seconds, and the instrumented run
12.680 seconds; these are suite durations, not latency benchmarks. Both exited
successfully, and the final default Wasm rebuild also completed.

One development run initially failed three new acknowledgement tests because
the harness simultaneously injected an acknowledgement error and deadline
expiry. The gateway correctly prioritized its expired deadline. The harness now
injects acknowledgement failure before the deadline, separately from the exact
deadline cases; the final full run passed all nine recovery cases.

Remote Linux CI and hosted Cloudflare staging: **NOT RUN** (nothing pushed or
deployed). The existing CI workflow includes the full suite. Tests create
short-lived local credentials, remove the temporary credential file on cleanup,
and retain ignored logs and SQLite state under `test-results/p1-*`. Instrumented
runs also record `lease-histories.json`; no capabilities are included in histories.

## Deterministic coverage

| Layer | Executed coverage |
| --- | --- |
| Native lease model | 271,452 trace prefixes, lengths 1–5, twelve events: acquisitions, renewals, releases, status, time advances/regression, serialize/reload; compare outcomes and persisted state against an independent reference model. |
| Native lease boundaries | Exact expiry ±1ms, immutable replay/current renewal distinction, wrong principals/parameters, stale owners, TTL/time bounds, counters above 2^53 and exhaustion, full receipt capacity/reserved Release, corrupted records, per-write rollback. Seven lease tests, in addition to eight existing P0 core/model tests. |
| Controlled lifecycle concurrency | Six transaction orders × six response orders × two logical times (one millisecond before expiry and exactly at expiry) = 72 histories of overlapping Renew(A), Release(A), TryAcquire(B), checked against an independent small reference model. |
| Individual expiry edges | Acquire/Renew/Release at expiry−1, expiry, expiry+1 = nine scenarios. |
| Duplicate mutations | Concurrent same-ID Renew and Release, with reversed commit/response order: one fresh result and one identical replay. |
| Actual SQLite rollback | SQL constraint failure and Rust error after each of six Acquire/five Renew/five Release writes = 32 scenarios; same-ID retry must be fresh and counters exact. |
| Process shutdown | SIGKILL before transaction entry and after real storage.sync for all three mutation methods = six scenarios; restart against the same SQLite state distinguishes fresh execution from replay without extending twice. |
| Deadlines/acknowledgements | For each mutation: deadline exactly at precommit admission, deadline after durable commit, and injected acknowledgement error after actual storage.sync = nine scenarios. Same-ID retries verify original expiry/revision versus a fresh operation. |
| Clock regression | Observe expiry via status, restart, reject backward renewal time, acquire successor at the original expiry with the next fence. |
| Ordinary Go SDK runtime | Lifecycle/status/ownership, 24 overlapping same-ID requests (one fresh result), 24 distinct contenders (one winner), consumed-and-dropped successful HTTP response, status, restart, retry and cleanup. |

The instrumented P1 suite has **131 leaf scenarios**, including 72 ordered
histories; [four recorded examples](p1-history-examples.json) are checked in.
Existing P0 hardening additionally checks 187,240 native prefixes,
108 controlled schedules, timer races, and 148 independently checked histories.

Logical time and checkpoint release determine the P1 ordering and expiry
results. No sleeps decide who wins or whether a lease expires. Real-time
watchdogs only fail stalled tests. The ordinary 24-client smoke tests leave the
winner nondeterministic but assert the invariant independent of winner identity;
the controlled suite supplies exhaustive ordering within its stated three-call
bound. A fixed finite model is not an exhaustive proof over arbitrary clients,
trace length, runtime scheduling, or hardware failures.

## Contract and usage

Prepare each SDK request once using NewAcquire, NewRenew, or NewRelease. Keep
that exact request for retries; persist it before sending if recovery must survive
an application restart. GetRequestStatus uses its resource and operation request
ID. Do not create a fresh ID merely because the transport timed out. The SDK
provides no background renewal or automatic contention/retry algorithm.

A successful operation's `Receipt` is immutable. `Current` and `State` describe
the authority's observation during this RPC; even an Active response can be stale
by arrival. Acquisition replay after renewal returns the original receipt plus
the latest lease in Current. After expiry/release/supersession it returns the old
receipt with an inactive state and no Current. Release replay returns the old
revision without touching a successor. Status NotFound is a snapshot, not
cancellation of a pending RPC. Client time is not an ownership oracle.

All coordinator data, including fencing counters, resides in the same SQLite
store. A future PostgreSQL fenced-write example would protect external business
data; PostgreSQL is not required to store coordinator fencing tokens.

## Significant limitations and next milestone

Historical P1 limits below are retained as evidence. [P1.1](p11-results.md) now
replaces the lifetime cap in its new service mode with bounded recovery sessions,
explicit ACKs, and alarm cleanup. Legacy P1 remains unchanged for regression.


- **1,024 successful mutation receipts per resource lifetime**, without eviction.
  Acquire/Renew stop at 1,023, reserving a final Release slot. Replays/status stay
  available. This intentionally fails closed; safe long-running retention/GC
  must be designed before production. Deleting state to regain capacity would
  reset fencing/dedupe history and is unsafe.
- The clock contract is authority-relative. The persisted high-water mark rejects
  observed regression but does not provide bounded drift or elapsed-time safety.
  Hosted Workers time semantics and failover remain unverified; see ADR-008.
- Local SIGKILL and persistent SQLite recovery do not establish hosted durability,
  regional failover, replication behavior, backup restoration, or safe migration.
- Authentication is still an ephemeral local fixture. The service flags are
  absent from default Wrangler config. P1 must run separately from legacy P0/P0b
  fixtures. The unpublished schema adds fields/methods but changes TTL semantics;
  passing Buf structural compatibility is not a published compatibility promise.
- No server-owned blocking Acquire, waiter fairness, cancellation protocol,
  alarms/wakeup contract, auto-renew/Lost SDK lifecycle, external fenced-write
  enforcement, performance/cost benchmark, or additional backend exists yet.

Recommended next: **P2 server-owned blocking Acquire**, sharing the same grant
transition. First design durable wait registration/revision reconciliation,
expiry wakeups, cancellation crossing commit, and restart recovery; keep the
same deterministic scheduling approach. Resolve the bounded receipt policy
before sustained workloads or production release. Hosted validation remains an
explicit later gate requiring authorization.

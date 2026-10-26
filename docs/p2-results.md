# P2 — Server-owned blocking Acquire

> Tooling update: command names below now use the root `justfile`. The milestone
> runs originally used equivalent shell scripts; their captured transcripts are
> unchanged. See [Just migration results](just-migration-results.md) for a full
> suite run through Just.

**PASS locally on macOS arm64, 2026-10-26.** Local-only Rust/Wasm + Wrangler,
Miniflare/workerd + SQLite + generated Go Connect client/SDK. No deployment,
hosted credential provisioning, dependency installation, commit, or push.

The final `./bin/just test` exited successfully (0). See the complete
[p2-test-output.txt](p2-test-output.txt), with checkout paths normalized. It records
30 native core tests, three SDK tests under Go's race detector, normal local
integration in 23.906s, and instrumented integration in 37.826s. The normal Wasm
artifact was restored successfully at the end. CI configuration includes P2;
remote CI and hosted staging were not run.

## Implementation

`Acquire` now registers a durable FIFO intent in its existing recovery session.
The server grants it on release or lease expiry, including without a connected
client. `CancelPending` atomically cancels an intent or returns the grant that
won the race. `GetRequestStatus` exposes Pending, Cancelled, Expired, and retained
results. A disconnect or HTTP timeout does not imply cancellation.

All queue/lease decisions live in portable Rust: `session::waiting::apply` and
`drive` reuse `lease::execute`. The Cloudflare adapter supplies transactions,
notifications, cryptographic capabilities, authority time, durability, and alarms.
No second store, JavaScript coordination algorithm, client TryAcquire/watch loop,
or new dependency was introduced. Existing Hermit-pinned tools were used.

[ADR-010](adr/ADR-010-server-owned-bounded-acquire.md) defines exact semantics,
limits, transport choices, recovery, and compatibility. The unpublished Protobuf
schema adds Acquire and CancelPending; Buf generates Go/Connect and Prost types.

## Reproduction and evidence

```sh
./bin/just test
```

This runs Buf lint/format/breaking checks and regeneration checks; native core
models/tests; Rust formatting and Clippy; actual release Wasm; Go tests with the
race detector against ordinary local Wrangler; the instrumented deterministic
runtime suite; and restoration of the normal Wasm artifact. No new toolchain
installation is required if the existing Hermit environment is bootstrapped.

| P2 requirement | Executed evidence |
| --- | --- |
| Portable authoritative algorithm | Native core builds without Cloudflare. Six wait-specific tests cover lifecycle, boundaries, queue precision/overflow, rollback, renewal/wakeup, and an independent reference model. |
| Independent model | All 9,330 prefixes through depth five over six events: register/retry, cancel, release, advance time, maintenance, serialize/reload. Independent expected owner/deadline/fence/terminal-state model. This finite model uses one waiter; FIFO/concurrent waiters are tested separately. |
| No lost wakeup | Actual workerd release before/after the initial waiter notification/read gap. Sticky subscription registration precedes durable state inspection. |
| Cancellation/grant race | Both commit orders × both response orders. Cancellation before registration prevents a delayed Acquire; cancellation after grant returns the grant without releasing it. Payload changes conflict. |
| FIFO/no bypass | Two pending requests grant in registration order with fences 2 then 3. A concurrent TryAcquire observes contention after the first queued grant. |
| Deadline/renewal | Runtime expiry−1/equality/+1 checks. Native renewal reschedules the next wakeup and cannot make a wait grant after its deadline. Pending ACK rejected; terminal ACK reclaims safely. |
| Transaction rollback | SQL UNIQUE failures and Rust errors after every write: pending registration 7, cancellation 4, Release plus queued grant 13, each in both fault modes: 48 injected faults. Six successful final-position controls delimit the write counts. |
| Alarm correctness | Actual workerd alarm grants an expired owner's successor, including after runtime restart. SQL failure during alarm grant rolls back ownership/fence/receipt; subsequent alarm recovers. |
| Unknown commit/restart | Kill before registration admission, after registration storage.sync, or after Release/grant storage.sync. Restart recovers NotFound/Pending/one grant as appropriate; same-attempt retry keeps fence 2. |
| Duplicate/disconnected requests | Two concurrent HTTP calls for one attempt recover the same grant. Cancelling one HTTP context does not cancel durable intent or create a second fence. |
| Two Go processes | Separate owner/waiter OS processes use the SDK. A checkpoint confirms the waiter is registered before the owner releases. The waiter receives fence 2 and retries the same receipt. |
| Bounds/reuse | Sixteen pending acquisitions and sixteen live HTTP subscriptions, overflow rejection, cancellation at capacity, and subscription/queue reuse. Unrelated-resource activity does not trigger waiter rechecks. |
| Normal runtime | Ordinary Go SDK waits for a successor grant after lease expiry, replays, cancels before admission, reconciles status, and observes cancel-after-grant. |
| SDK bounded resumption | An explicit timer returns Pending; a second gateway checkpoint proves SDK Acquire resumes the same durable intent. Release while that resumption is held grants exactly fence 2. |
| Regressions | All P0/P0b/P1/P1.1 models, transport tests, SQL atomicity/fault tests, precision checks, and runtime schedules remain in the full suite. |

Logical clocks and explicit checkpoint/commit/response gates determine expected
race/expiry outcomes. No sleep selects a winning operation. Wall-clock watchdogs
only fail stalled tests. Ordinary runtime alarm smoke tests supplement the
controlled checks. Test-only alarm acceleration uses real alarm callbacks, not
an HTTP endpoint that invokes cleanup/grant. Normal builds omit these controls.

Checkpoint traces, logs, and SQLite state remain under ignored `test-results/p1-*`
directories. The directory prefix is historical and includes P2 fixtures.

## Storage and compute observations

The capacity workload uses one session with one owner receipt and sixteen pending
entries. Read-only inspection of real local SQLite measures seven rows, 5,350
logical key/value bytes, and 36,864 allocated bytes in the full run. These are
measurements of that workload, not a maximum-size guarantee. P2 adds only a
persistent queue counter beyond P1.1's fixed session slots and metadata.

The capacity test records elapsed local registration time for sixteen waiters,
including test-controller I/O (215.522ms in the final full run). The deterministic checkpoint trace verifies zero
waiter rechecks during unrelated-object work; state-changing notifications can
broadcast to at most sixteen subscribers. There is no periodic contention poll
loop. Each live HTTP subscription is bounded to eight seconds and requires at
most one transport timer; resumption rereads the same durable intent.

Host CPU/RSS profiling was **NOT RUN successfully**: this environment rejects
`ps` execution (`operation not permitted`). An initial full-suite run failed only
on that optional measurement; it was removed from correctness assertions. No
per-DO memory or hosted cost inference is made from the storage/latency results.

Hosted cost, hibernation behavior, hot-key throughput, WAN latency, CPU limits,
and fair-use policy need P3 measurements. Bounded pending HTTP requests may keep
an object active. No hosted cost or availability claim follows from local tests.

## SDK usage and recovery

```go
session, err := client.OpenSession(ctx, resource)
request, err := session.NewWaitAcquire("worker", 30*time.Second, 60*time.Second)
// Handle errors and persist the full prepared request if crash recovery matters.
result, err := client.Acquire(ctx, request)
// Inspect err, result.State, and result.Granted.Current/State before using ownership.
```

Acquire resumes only server-returned Pending responses using the same attempt.
`ResumeAcquire` exposes a single bounded HTTP exchange. On transport/context error,
use a fresh bounded context to query `GetAttemptStatus` or `CancelPending` with the
original prepared request. CancelPending may return a historical grant; reconcile
its current ownership observation and explicitly Release if appropriate. It never
silently releases a winning grant. ACK remains an explicit application decision
and cannot discard a pending intent.

## Limits and next milestone

- 16 pending intents per resource and 16 subscribed HTTP handlers per instance;
  duplicate connections consume handler slots. Raw clients are also bounded.
- Wait duration 1–60000ms; equality is expired. HTTP holds are at most eight seconds.
  Queue deadline does not reset on resume. Session expiry may end recovery earlier.
- FIFO applies to eligible registered waiters; owners may keep renewing. No global
  fairness across tenants or guaranteed grant before a deadline is promised.
- Alarms can be late. A crash before final scheduling can fall back to the existing
  60-second alarm and cause a short wait to expire. Correctness rechecks deadlines;
  arbitrary outages do not have an availability guarantee.
- Broadcast notification/slot scanning are deliberately bounded. This first local
  implementation has not been optimized or benchmarked for production hot keys.
- No auto-renew/Lost SDK helper, production authentication, streaming, staging
  failover validation, safe snapshot restore, or authority migration is added.
- P2 is opt-in via `P2_ENABLED=true`, mutually exclusive with earlier fixture modes.
  No default service is enabled and no deployed data migration is claimed.

Recommended next: **P3 v0.1 hardening and release preparation**, starting with the
remaining SDK lease-loss contract, production auth/operational design, transport
conformance/security review, and performance tests. Hosted staging is a separate
gate that still requires explicit authorization. Do not treat P2 local acceptance
as approval to deploy or release publicly.

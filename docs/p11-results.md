# P1.1 — Bounded recovery sessions and automatic cleanup

> Tooling update: command names below now use the root `justfile`. The milestone
> runs originally used equivalent shell scripts; their captured transcripts are
> unchanged. See [Just migration results](just-migration-results.md) for a full
> suite run through Just.

**PASS locally on macOS arm64, 2026-10-26.** Scope: portable session/receipt lifecycle,
Protobuf/Connect, explicit Go SDK acknowledgement, and local DO SQLite alarms.
No deployment, push, second database, new executable dependency, or P2 feature.

## What changed

P1.1 replaces the 1,024-success lifetime stop with reusable recovery capacity.
The server allocates resource-local sessions and tracks numbered operations.
Acknowledgements reclaim completed receipts while retaining a durable boundary
that rejects delayed duplicates. Expired sessions are reclaimed automatically,
and old session IDs can never be used to execute operations. Fencing, current
lease ownership, and the session-allocation counter survive cleanup.

The complete contract, limits, rationale, and migration caveats are in
[ADR-009](adr/ADR-009-bounded-recovery-sessions.md). Core logic is in
`crates/core/src/session.rs`; the existing lease transition remains authoritative.
No JavaScript coordination algorithm or new database dependency was introduced.

## Local evidence

Run the full pinned Hermit suite:

```sh
./bin/just bootstrap  # Only if the existing toolchain is not installed
./bin/just test
```

The full suite includes P0/P0b/P1 regressions, Buf lint/format/compatibility and
regeneration checks, Rust fmt/Clippy, native core tests, actual release Wasm,
generated Go/SDK tests with the race detector, normal Wrangler integration,
instrumented runtime checks, and restoration of the normal Wasm artifact.

| Check | Evidence |
| --- | --- |
| Long-lived resource | Native test: 4,096 acquisitions with successive fences and at most six logical rows. Ordinary Wrangler/SDK: 1,100 renewals with explicit ACKs, then Release and a successor grant. |
| Storage reuse | Read-only inspection of real local SQLite via Hermit Node's bundled sqlite API. At renewal 32 and 1,100: six rows, zero retained receipts, one session; allocated database size 20,480 bytes at both checkpoints in the final full run. This is one measured workload, not a universal physical-size guarantee. |
| Independent session model | 37,448 trace prefixes through depth five over eight events: two operations, two ACK boundaries, expiry-time advance, regression, cleanup, serialization/restart. Predict outcomes, fencing, session state, ACK boundary, and receipt count independently. |
| Boundary and capacity tests | ACK is not Release; gaps/conflicting payloads; stale and expired attempts; idle equality; heartbeat cannot retain unresolved history forever; 40 rounds of session-slot reuse; reserved Release/owner session capacity; exact session IDs above 2^53; corrupt records/overflow fail closed. |
| Native rollback | Fail each write in acquisition with piggyback ACK, explicit ACK, and expired-session cleanup; original records remain unchanged. |
| Ordinary client concurrency/restart | 16 same-attempt callers produce one fresh grant; conflicting payload/other principal denied; restart preserves unacknowledged receipts and acknowledged rejection boundary. |
| Controlled ACK/retry race | Both transaction orders × both response orders = four histories. ACK-first rejects the late retry; retry-first returns original receipt. Neither releases the owner. |
| Exact session expiry | Request at expiry−1, expiry, and expiry+1 with injected authority time. |
| SQLite rollback | Real SQL failure and Rust error after every Acquire/Renew/Release write through the session protocol: 32 cases. |
| Unknown commit | Kill local runtime before transaction admission or after actual storage.sync; same-attempt retry after restart distinguishes fresh execution from replay. |
| Actual alarm callback | workerd invokes DurableObject.alarm. Test controls its clock/admission, observes expired-session reclamation directly in SQLite, and verifies retained fencing and stale-attempt rejection. Additional fault/restart cases exercise cleanup rollback and persistent scheduling. |

The test-only build accelerates alarm scheduling to 20ms and controls logical
time using the existing loopback controller. There is no HTTP cleanup endpoint.
No sleep decides expiry or concurrency outcomes; checkpoint gates do. Watchdogs
only fail stalled tests. Normal builds compile out controller access and alarm
acceleration. No live Cloudflare tests or remote CI were run.

Full `./bin/just test` completed successfully with exit code 0. The transcript
is [p11-test-output.txt](p11-test-output.txt) (checkout path normalized). It records
24 native core tests, three SDK tests under `-race`, normal local integration in
22.822s, and instrumented integration in 18.882s. P1.1 contributes 44 controlled
runtime leaf scenarios; all prior suites remain included. The normal Wasm build
was restored successfully at the end. Protocol comment-only regeneration was
subsequently checked with `./bin/just check-proto`.

Session checkpoint traces, Wrangler logs, and SQLite state are retained under
ignored `test-results/p1-*` directories. The SDK tests also exercise status,
heartbeat, persisted-attempt ACK, and concurrent sequence allocation with a
reserved Release position. No new dependencies were needed; all executable tools
were run through the existing pinned Hermit environment.

## SDK usage

```go
session, err := client.OpenSession(ctx, resource)
// Handle err. A lost OpenSession response can leave an expiring orphan session.
request, err := session.NewAcquire("worker", 30*time.Second)
// Save the complete request if application restart recovery is required.
result, err := client.TryAcquire(ctx, request)
// On uncertainty, retry this exact request or query:
status, err := session.Status(ctx, request.RequestID, request.Attempt)
// After safely consuming/persisting every result through this sequence:
err = session.Acknowledge(ctx, request.Attempt.Sequence)
```

The excerpt illustrates lifecycle; application code must handle every error and
inspect Contended/State/Current. A historical receipt does not prove ownership.
Prepare Renew and Release through session.NewRenew/NewRelease and invoke the
ordinary client methods. Session.Heartbeat keeps an idle recovery session alive
without acknowledging new results or renewing ownership. There is no background
heartbeat/retry loop. Client.GetAttemptStatus and Client.AcknowledgeAttempt work
with saved attempt metadata after a client restart.

ACK relinquishes historical result recovery; it is not merely a network receipt
notification. Session expiration likewise makes old outcomes unavailable. The
SDK never silently opens a replacement session and repeats an uncertain operation.
Prepared but unsent requests leave sequence gaps: do not silently discard them.

## Bounds and operational limits

- Sixteen reusable session slots per resource, including one reserved for the
  current authenticated owner. Thirty-two sequence positions per session,
  including a final Release position. Server-enforced per-session payload cap
  64 KiB; receipt payloads therefore have a fixed aggregate logical bound.
- Five-minute inactivity expiry; oldest unacknowledged result is limited to ten
  minutes even with heartbeats. These are initial policies, not performance-tuned
  defaults or a promise of ten minutes of recovery after every result.
- Requests and alarms reclaim expired slots. The adapter arms a durable fallback
  before session mutations and before alarm cleanup, then schedules the next
  expiry. An empty authority cancels its cleanup alarm. No routine manual cleanup
  or separate cron server is required under normal platform operation.
- Storage failures/outages can delay reclamation. Hard capacity still fails closed;
  production monitoring and hosted alarm/clock/failover validation remain open.
- Bounded JSON slots deliberately favor a small portable transaction surface.
  They require bounded scans and rewrite a session record per activity; actual
  hot-key throughput, read/write billing, and worst-case physical storage have not
  been benchmarked. Local SQLite page reuse does not prove hosted billing behavior.
- Resource count is separate: unlimited new names retain fencing metadata.
  Safe retirement/generations and namespace quotas remain future work.
- `P11_ENABLED=true` is the new service mode. Legacy `P1_ENABLED` retains the
  capped fixture for regression. No default mode is enabled, and flags cannot be
  mixed. Existing P1 receipt rows are not migrated/deleted or fencing reset.

Follow-up: [P2 now passes locally](p2-results.md). The original next-step guidance
was P2 server-owned blocking Acquire, with wait-registration,
cancellation/grant, expiry-wakeup and restart tests. Its pending operations must
respect this bounded session protocol; session cleanup is not a substitute for
cancelling a pending acquisition.

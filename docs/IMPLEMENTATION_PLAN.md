# Implementation Plan — ordered, acceptance-driven

**Important:** `PROJECT_SPEC.md` is canonical. This plan is an implementation sequence, not a date-based promise. Only start the *next* milestone after evidence for the previous gate is recorded.

## P0 — Prove Rust + DO SQLite transaction semantics (first PR)

**Scope:** minimum workspace, gateway/DO wiring, SQLite migration, exercised small state transition, deterministic concurrency test, exact fencing counter integer handling, rollback and idempotency tests. Do *not* begin a complete wait queue or full server protocol.

**Acceptance checks:**

- Rust core crate compiles and tests on native host without Cloudflare dependencies; Rust DO adapter compiles to Wasm and runs under local Wrangler.
- One atomic transition writes at least two logical records, e.g. lease+fence counter+idempotency/revision, and intentionally injected SQL failure leaves none partially updated.
- Concurrent acquisition attempts for the same resource cannot both commit success; conflicting attempt is observed, not overwritten.
- Stable repeated logical request ID produces the same grant/token (or a formally documented terminal status) despite simulated lost response.
- Fencing generation remains exact above 2^53 and after local restart.
- Document the exact `workers-rs` API path for SQLite transactions (native binding, maintained wrapper or minimal JS interop) and explain trade-offs.
- Include reproducible commands and test outputs in `docs/spike-results.md`. If a guarantee cannot be established, mark failed/open and stop progression.

## P0b — Protocol feasibility (second narrowly scoped PR)

- Define minimal `pintle.v1` Protobuf schema and Buf config, generate Rust types and Go client types.
- Test Connect unary interoperability under Cloudflare Worker Fetch and DO binding; **do not assume native connect-rust server networking works under Wasm**. If incompatible, evaluate small Connect-compliant Fetch adapter or HTTPS/ProtoJSON alternative.
- End-to-end `TryAcquire` RPC only to the extent P0 state transitions support it.
- Bound names and payloads; basic authenticated namespace isolation (real auth policy design remains open). CI compatibility check.

## P1 — Mutex state machine and nonblocking service

Local acceptance passed 2026-10-26; see [p1-results.md](p1-results.md) for tests,
bounded receipt retention, clock assumptions, and unsupported production scope.

- Pure Rust authoritative transition model; no storage imports.
- `TryAcquire`, `Renew`, `Release` implemented through atomic DO state transitions; fencing monotonic, correct owner validation, documented time/TTL approach.
- Idempotent request replay, unknown-commit handling and request-status ability or equivalent recovery path.
- Go SDK for those methods; isolated local integration tests with real clients, shutdowns, high contention and retries.
- Publish initial `GUARANTEES.md` with tested vs unsupported behaviors.

## P1.1 — Bounded recovery sessions (completed locally)

Local acceptance passed 2026-10-26. See [p11-results.md](p11-results.md) and
[ADR-009](adr/ADR-009-bounded-recovery-sessions.md).

- Portable Rust session/sequence/ACK transitions reuse the lease core atomically.
- Replace lifetime receipt exhaustion with bounded live sessions and request windows.
- Reject acknowledged/expired attempts without re-executing them; preserve fences.
- Explicit Go SDK ACK and heartbeat, generated Protobuf/Connect against Wrangler.
- Automatic transactional cleanup via actual DO alarms, including rollback/restart.
- Independent bounded model, controlled races/expiry/faults, and measured SQLite reuse.
- No automatic migration of legacy receipts; no hosted production validation.

## P2 — Server-owned blocking Acquire, not SDK spin loop

Local acceptance passed 2026-10-26; see [p2-results.md](p2-results.md) and
[ADR-010](adr/ADR-010-server-owned-bounded-acquire.md). Durable FIFO intents,
bounded unary resumption, atomic cancellation, actual alarms, two-process SDK
handoff, rollback/restart, and capacity tests are implemented. Hosted cost and
lifecycle validation remain P3 gates; host CPU/RSS profiling was sandbox-blocked.

- Design wait protocol for Cloudflare lifecycle: compare revision/register interest without lost wakeups, object restarts, expiry alarms, disconnects, timeouts, cancellations, response lost after grant. Decide bounded wait/resume vs streaming based on verified runtime behavior.
- Implement `Acquire` as an authoritative service operation using same grant core; no SDK watch-then-retry contention algorithm.
- Demonstrate two Go processes: B waits while A owns; A releases; B acquires once with greater fencing token.
- Tests: no lost wakeup between check and registration, waiter cancellation crossing grant, ambiguous connection loss, restart and lease expiration.
- Measure cost/compute characteristics of many idle waiters; do not hold an unbounded number of active handlers without proof.

## P3 — Harden v0.1 and prepare public OSS

Local hardening passed 2026-10-26; see [p3-results.md](p3-results.md) and
[ADR-011](adr/ADR-011-p3-local-hardening.md). Renewal/Lost SDK behavior, bounded
static auth, sanitized metrics, expanded deterministic/conformance tests, local
latency, deployment preparation and first-pass security review are complete.
**Release gate remains OPEN:** hosted lifecycle/WAN/cost/operations validation,
independent security review and final release approval. No deployment or push.

- Conformance/fault-injection suite, live Cloudflare staging checks with approval, p50/p95/p99 local and WAN measurements, hot-key behavior, operational limits, deployment documentation, basic metrics, security review, license approval.
- Open-source release must explicitly state limitations and unsupported resource-fencing semantics.

## P4 — Additional helpers (later only)

- Semaphore; singleflight; cooldown; rate-limits/quotas; PostgreSQL fenced-transaction integration; additional language SDKs, in that order only if supported by user need and shared primitives.
- Validate *true* portability using a native Rust + FDB/etcd backend; do not build a large generic backend framework for hypothetical capabilities.

## Decisions needing targeted ADR or experiment

- Exact storage atomic interface and sync callback mechanism.
- DO per-resource routing / object fanout and durable request-ID retention.
- Lease-expiry clock guarantees and maximum TTL.
- Persistent waiter records versus reconstructable ephemeral subscriptions.
- Cancellation after committed grant; status lookup and clean-up protocol.
- Connect on Wasm vs fallback; wire error model and authentication.
- Snapshot/restore, authoritative handoff and fencing epoch safety (no support promised in v0.1).

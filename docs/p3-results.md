# P3 — Local hardening and release preparation

> Tooling update: command names below now use the root `justfile`. The milestone
> runs originally used equivalent shell scripts; their captured transcripts are
> unchanged. See [Just migration results](just-migration-results.md) for a full
> suite run through Just.

**LOCAL PASS, 2026-10-26, macOS arm64. Full P3 release gate remains OPEN.**
No deployment, push, commit, hosted credentials, or new third-party dependency
installation. Existing Hermit toolchains and locked dependencies were used.

## Changes

- Go lease maintenance with a permanent Lost signal, explicit unknown-renewal
  recovery, bounded renewal/ACK time, and deterministic Stop/response ordering.
- Strict SDK lease/response correlation, canonical numeric validation, response
  size limits, HTTPS except loopback, redirect rejection, and credential redaction.
- Portable static credential policy with expiry, rotation, tenant/principal and
  namespace isolation; separate Cloudflare secret/time adaptation.
- Opt-in sanitized JSON RPC/alarm metrics, operational preparation documentation,
  first-pass security review and separate online dependency advisory checks.
- Independent two-waiter model and a fail-closed check for regressed queue counters.

[ADR-011](adr/ADR-011-p3-local-hardening.md) records decisions and limitations.
Coordination algorithms still live entirely in the platform-independent Rust core.

## Executed validation

```sh
./bin/just test
./bin/just audit
```

[Full test output](p3-test-output.txt) records the final full run, with checkout
paths normalized. It includes Buf lint/breaking/generated-code checks, formatting,
Clippy (normal and test-hooks Wasm), Go vet (normal and testhooks), native tests,
actual release Wasm builds, generated Go Connect client/SDK integration under the
race detector, deterministic instrumented runtime tests, and normal Wasm restoration.
Native tests: 32 core and 2 auth. SDK: 11 top-level tests under `-race` (1.220s).
Normal runtime integration passed in 34.896s; instrumented integration in 38.638s.
CI configuration was extended; remote CI was NOT RUN.

| Area | Evidence |
| --- | --- |
| Independent FIFO model | 37,448 prefixes through depth five over eight events: register A/B, cancel A/B, release, advance time, drive, serialize/reload. Independent expected owner, ticket order, fence and terminal states. Prior 9,330-prefix one-waiter model retained. |
| Corrupt queue counter | Retained ticket above allocation counter fails without partial grant; transaction rollback asserted. |
| Renewal/loss | Fake monotonic time at budget minus 1 ns, equality and plus 1 ns; inactive/wrong-owner response, ACK uncertainty, cancellation, both Stop/response orders, concurrent Stops and deep-copied recovery intent. |
| Real renewal recovery | Generated SDK against normal Wrangler renews and ACKs; response discarded after actual server commit produces Lost and exact recoverable renewal. Status recovers the committed result; Stop does not release. |
| Credential lifecycle | Same-principal overlapping rotation, removed old key after runtime restart, recovery through replacement key, cross-principal denial, cross-tenant isolation and namespace denial. |
| Auth failure boundaries | Invalid/missing configuration and missing expiry fail closed. Controlled gateway clock at 999/1000/1999/2000/2001 for validity [1000,2000). Authorization just before expiry remains admitted after dispatch crosses expiry. |
| Transport | Eight RPCs times six malformed transport variants = 48 rejection cases, alongside all earlier protocol tests. SDK tests reject redirects, oversized replies, malformed lease/session observations and unsafe endpoints. |
| Metrics | Actual Wrangler debug log JSON inspected for exact four-field schema and absence of credential, resource, session and lease values. |
| Regressions | All P0/P0b/P1/P1.1/P2 precision, idempotency, write-by-write rollback, session cleanup/reuse, cancellation/grant ordering, restart, alarm, capacity and two-process tests remain passing. |
| Dependency advisories | npm audit: 91 dependencies, zero reported vulnerabilities. OSV: 115 public Rust/Go package versions, zero findings. Reports recorded 2026-10-26; registry coverage is not proof of safety. |

Correctness schedules use explicit gates, injected clocks and model enumeration;
no sleep decides a winning operation. Watchdogs fail stalled tests. Ordinary alarm
and latency smoke tests use real runtime time and supplement the controlled tests.
These are finite models and selected fault boundaries, not coverage of every
possible interleaving or proof of arbitrary platform failures.

Initial metrics validation failed because it read Wrangler's info log instead of
the debug log containing console events; the reader was corrected and the full
suite passed. Review also found incomplete SDK observation checks and undetected
queue-counter regression; both were fixed and covered before the final run.

## Local latency observations

Nearest-rank quantiles, 12 synchronized batches, normal local Wrangler and Go race
detector enabled. Hot-key calls are TryAcquire attempts against an occupied key;
independent-resource calls are successful TryAcquire grants. Release and ACK are
outside latency samples. No performance thresholds are correctness assertions.

| Workload | Samples | p50 ms | p95 ms | p99 ms |
| --- | ---: | ---: | ---: | ---: |
| Hot key, one client | 12 | 6.189 | 8.776 | 8.776 |
| Hot key, eight clients | 96 | 39.897 | 45.899 | 46.017 |
| Independent keys, one client | 12 | 6.519 | 11.886 | 11.886 |
| Independent keys, eight clients | 96 | 40.215 | 47.839 | 47.971 |

These small samples characterize this local harness only. They are not production
SLOs, a saturation/throughput benchmark, or evidence of WAN latency, fairness,
per-DO CPU/RSS, billing, or platform failover. P2's host profiling was sandbox-blocked.

## SDK maintenance contract

Call `client.Maintain(ctx, knownLease, ttl)` only after validating a successful
ownership observation. It opens a dedicated session and immediately starts Renew.
Observe `maintenance.Lost()` and stop protected work on closure; inspect `Err()`.
`Latest()` is the last validated observation, not a new claim of live ownership.
`Recovery()` returns the exact unacknowledged renewal, which may have committed.
Reconcile with request status or a same-attempt retry using a fresh bounded context.
An ACK may itself have committed; acknowledged/expired recovery is terminal, not
permission to execute the old renewal again. Preserve recovery data yourself if
process-crash recovery matters. `Stop()` waits for renewal shutdown and never
releases; Release remains explicit. The dedicated session eventually expires.

The helper ACKs only results it owns. Application acquisitions/releases still need
explicit ACK after safe consumption. No automatic reacquire or ambiguous retry is
hidden in the helper. Custom transports must honor context cancellation. A paused
process can miss Lost and lease expiry; downstream fencing remains necessary.

## Operational limits and open gates

Existing limits remain: 16 session slots (one reserved for owner release), 32
positions per session (final position reserved for Release), 16 pending intents,
16 HTTP subscriptions, TTL 100–60000 ms, wait 1–60000 ms, at most eight seconds per
held HTTP response, five-minute idle session expiry and ten-minute oldest-receipt
expiry. One maintenance helper consumes one session slot. Metadata/fences remain
per resource indefinitely; total distinct-resource storage is not globally bounded.

New static auth allows 1–16 credentials, secret JSON at most 16 KiB, explicit
1–16 namespace allowlist entries, and mandatory expiry. Rotation preserves tenant
and subject. Revocation denies new admissions; existing durable intent can still
grant. See [deployment preparation](deployment.md) and [security review](security-review.md).

NOT RUN: hosted staging lifecycle/failover/clock checks, WAN measurements, hosted
CPU/RSS/cost/limits, secret propagation, telemetry export and independent security
review. These require separate preparation/authorization and cannot be inferred
from local workerd. Snapshot restore/migration remains unsupported. Existing
Apache-2.0 licensing is preserved; final OSS release approval is still required.

Recommended next: complete the remaining **P3 staging and release gates**, when
explicitly authorized, before P4 helpers or any production claim.

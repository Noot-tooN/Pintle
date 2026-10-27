# Adapter refactoring after the P3 local baseline

**Local validation passed, 2026-10-27, macOS arm64.** Baseline commit:
`f8310f1` (P0 through P3 local hardening, Hermit, Just and local test permissions).

## Changes and review

The Cloudflare lease adapter now represents requests with an `Operation` enum.
Lease execution, waiting, session opening and acknowledgement have distinct
variants. Session management no longer fabricates a Status command; wait duration
and acquisition/cancellation kind are named fields. Optional attempt parsing and
authority-side mode validation retain the legacy P1 fixture's behavior.

Response conversion now builds typed Protobuf outcomes and encodes only the final
RPC response. Acquire and CancelPending no longer decode bytes just encoded by
the server, and status no longer encodes an unused inner response. No throughput
or latency improvement is claimed without a comparative benchmark.

The working diff was reviewed against the baseline for RPC dispatch, response
variants, replay/observation fields and asynchronous ordering. Subscription before
admission, fallback alarm persistence, transactional clock sampling, notification,
durability acknowledgement, wakeup reads and response deadlines retain their
ordering. Core algorithms, SDK code, schemas, generated types, persisted records,
dependency versions and CI workflow are unchanged. No architectural deviation or
new dependency was introduced; existing Hermit tools were used.

## Validation

`./bin/just test` exited **0** under the active local permission profile:

- Buf lint, formatting, compatibility and generated-code drift checks.
- Rust/Just formatting, Clippy including normal and test-hook Wasm, and Go vet.
- 32 native core tests and 2 authentication tests, including reference models.
- Generated Go packages and SDK race tests (SDK: 1.360s).
- Normal local Wrangler/SQLite integration: 37.390s.
- Instrumented deterministic integration: 41.063s.
- Successful restoration of the normal release Wasm build.

A new controlled-clock integration case verifies Renew and Release status
response variants, immutable receipt/current lease fields, revision and replay
flags through the generated Go client. Existing tests cover acquisition,
contention, wait/cancel results, session management, faults, alarms and restarts.
The complete local transcript is `test-results/refactor-full-suite.log` (ignored).

The first run was interrupted at Wrangler startup: its banner tried to check npm
after the update cache expired, and the sandbox blocked the external destination.
The integration launcher now sets `WRANGLER_HIDE_BANNER=true`; the complete rerun
above passed without additional test permissions. See
[local test permissions](local-test-permissions.md). No sandbox allowlist changed.

This was a focused local refactor review, not an independent security audit or
hosted validation. Online advisory checks were not rerun. P3's hosted lifecycle,
WAN, cost, operations and release gates remain open. No deployment was performed.

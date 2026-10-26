# ADR-007 — Deterministic validation before P1

**Status: ACCEPTED for P0/P0b test infrastructure (2026-10-25).** No lease
semantics or production architecture are added. Results:
[hardening-results.md](../hardening-results.md).

## Decision

Use three complementary layers without adding dependencies:

1. A native Rust reference model explores every trace prefix to depth five over
   eight fixture events and five initial counter states, comparing outcomes and
   the complete persisted record set with the actual core.
2. A compile-time `test-hooks` feature adds checkpoints outside the synchronous
   transaction in the actual Rust/Wasm adapter. A loopback Go test controller
   acknowledges arrival and explicitly releases each request. It supplies logical
   time and, when armed, a separate timeout signal for the same gateway response
   versus timer selection used by the ordinary build.
3. A bounded, independent Go history checker searches for a legal sequential
   ordering consistent with call/return ordering, receipts, and ambiguous
   outcomes. Known invalid histories exercise the checker itself.

The controller is not an authority or alternate transaction implementation. DO
SQLite and the portable Rust grant algorithm still perform all real grants.
Checkpoints may suspend before the transaction, after it, or after durability
acknowledgement, but never between reads and writes inside `transactionSync`.
Read-error injection and existing SQL/Rust write faults remain synchronous.

## Isolation and time

Normal builds omit the controller client and test header parsing/forwarding.
The instrumented build also requires an explicitly configured loopback controller
and a test call identifier. No schema changes or public fault fields are added.
The full test script verifies the normal build ignores a configured poison
controller, then builds/runs the instrumented suite and restores normal Wasm on
exit. Do not deploy the instrumented build.

Logical time controls deadline checks; an explicit controller signal controls
timer completion. Wall-clock watchdogs fail stuck tests but do not select the
expected winner. The normal adapter continues using Worker Date and Delay.
The existing stalled-upload smoke test exercises that real timer path. This is
not a lease clock design or a guarantee about wall-clock drift.

## Evidence limits

The model's restart is serialization/reload of its test store; real kill/restart
tests separately use local workerd and SQLite. Likewise, native rollback depends
on the test adapter discarding aborted candidates; runtime tests exercise real
SQLite rollback. Neither substitutes for the other.

An injected failure following real `Storage::sync()` tests the caller's uncertain
acknowledgement path. It does not reproduce a platform disk or replication
failure. An outbound post-commit checkpoint can itself cross Cloudflare's output
gate; no claim about crashing in the platform's unflushed commit window follows.

The finite model and schedule bounds are explicit. Existing barrier-released
stress tests remain supplemental. Neither those runs nor the bounded checker
prove all possible executions, hosted failover, or future lease invariants.

## Defect found

A malformed persisted dedupe receipt could previously replay a zero or
noncanonical counter. The core now rejects invalid receipt counters, invalid
owner lengths, and receipts whose embedded request ID differs from their lookup
key. These errors preserve storage unchanged. This is a fail-closed correction
to existing P0 replay behavior, not a new operation or corruption-repair system.

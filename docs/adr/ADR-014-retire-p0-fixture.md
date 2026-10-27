# ADR-014: Retire the P0 runtime fixture and consolidate its tests

Accepted, 2026-10-27.

P0 established SQLite multi-record atomicity and lossless counters. Its non-expiring
grant algorithm, JSON `/spike` controls and P0b authority are now redundant with
the lease/session implementation. Pintle is undeployed: this is obsolete feasibility
plumbing, not support for older customers or a persistence migration requirement.

Remove those implementations, public test identity labels, seed/vacate/snapshot
routes and the `dev-spike` recipe. Preserve the generic Rust transaction boundary,
canonical counter parser and minimal `transactionSync` Wasm binding. P1/P1.1
modes still exercise the shared lease/session implementation; removing them is
outside this consolidation. The mutex routing identity and stored format stay unchanged.

Preserve unique tests on the current code. Counter extremes use a private Go
harness invoking Hermit's existing Node `node:sqlite`: stop Wrangler, seed only
fence/revision in exactly one unused authority, restart and verify through the
real generated protocol/SDK. Reads compare complete records after settled or parked
mutations. These are test helpers, not an administration tool or recovery procedure.
Use real Release/expiry for successor grants; do not retain artificial vacate.

The compile-time test scheduler remains for deterministic logical clocks, fault
injection and order control. Normal builds contain no controller client. No new
dependency or endpoint replaces the removed fixture. The old P0 model/history
checker is removed with its algorithm; current native independent models and
explicit runtime scheduling assertions remain. Keep the P0b Buf descriptor as a
schema compatibility baseline, not as executable server support.

[Coverage map and execution evidence](../test-consolidation-results.md) document
what was transferred, what was obsolete, and the limits of local evidence.

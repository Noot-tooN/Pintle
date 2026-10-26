# ADR-005 — P0 synchronous SQLite interop and exact counters

**Status: ACCEPTED for the P0 spike only (2026-10-25).** The general storage
contract in ADR-004 remains PROPOSED. Evidence: [spike-results.md](../spike-results.md).

## Decision

Use workers-rs 0.8.7's `State::storage()`, `Storage::sql()`, and
`Storage::as_raw()`. Add one structural, exception-catching wasm-bindgen method
binding for `DurableObjectStorage.transactionSync(callback)`. The synchronous
Rust callback performs all reads, grant decisions, and four writes. A Rust
`Err` becomes a JavaScript exception through wasm-bindgen and is caught outside
the platform transaction; it is never returned as a successful callback value.
After success, await `Storage::sync()` before acknowledging the result.

The portable core owns the grant algorithm over a provisional generic
`Transaction { get, put }` view. The Cloudflare adapter owns SQL, atomic scope,
callback lifetime, persistence acknowledgement, and HTTP routing. No lock-specific
algorithm is reimplemented in the storage adapter. No read/await/write sequence
or standalone CAS calls substitute for the transaction.

Fence and revision arithmetic is checked Rust `u64`; canonical decimal strings
cross every serialization, JS, and SQLite boundary. SQLite uses a STRICT table
with TEXT values. Never bind fence/revision as `SqlStorageValue::Integer`: its
conversion passes through `f64`. Exhaustion returns `exhausted` without mutations;
counters never wrap. Fence and revision are independent (fixture `vacate`
increments only revision).

## Evidence and alternatives

The native wrapper exposes an asynchronous `Storage::transaction()` but no
synchronous transaction binding in either worker or worker-sys 0.8.7. Platform
documentation also allows asynchronous transactions for SQLite; this spike
selects the smaller synchronous path so an async boundary cannot accidentally
enter a grant transition. This does not claim the async API is unsafe.

Compiled Wasm under local workerd passed SQL UNIQUE failures and Rust errors
following each of four writes, multiprocess contention, stable replay after
response loss, exact u64 counter tests, and persistent process restarts.

## Scope and tradeoffs

- Read predicates and mutations execute together under the same object-local
  transaction. This is a minimal concrete implementation of the conditional
  transition hypothesis, not the proposed async snapshot/batch/watch interface.
- No custom JavaScript domain implementation or new runtime dependency is needed.
  The extra binding must be removed if an upstream equivalent becomes available.
- Expected failures use `Result`; arbitrary Rust traps, out-of-memory, and runtime
  termination inside a callback have not been fault-tested.
- Strings cost more storage than integer columns and cannot be lexically sorted
  to obtain numeric order. Arithmetic and comparison stay in Rust.
- The fixture has no clock or lease. Test-only `vacate` creates availability;
  public Release/Renew/Acquire semantics remain future work.
- No accepted architectural decision is superseded. No second backend or stable
  generic transaction API is claimed.

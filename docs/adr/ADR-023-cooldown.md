# ADR-023 — Keyed cooldown for search index refreshes

Accepted for local Worker and Go SDK development, 2026-11-27.

## Admission contract

Several workers see that a tenant's search index is stale. They use the same
tenant, namespace and index key. The first admission at authority time 1000 ms
with an interval of 500 ms reserves the half-open window [1000, 1500).
Other callers receive 1500 as the earliest time to try again. A worker asking
for 50 ms or 5000 ms gets the same answer. At 1500 a fresh attempt can win,
and its own interval determines the next deadline. Contention and replay never
extend or shorten a window. Callers compete again at expiry with no reserved turn.

Admission consumes the window even if the winner crashes before starting work.
There is no completion or early release. A slow or paused refresh can overlap a
later one. The application should recheck index freshness and make publication
safe to repeat, for example by building from a source revision and atomically
publishing only if the indexed revision is older. Use a durable dirty marker or
another source of retries if the refresh must eventually happen. Pintle cannot
tell whether the winner touched the index.

## Attempts and lost replies

1. `Prepare(key)` reads the authority's current window ID, or the upcoming
   ID if the key is eligible. Reading an ID reserves nothing.
2. `TryAdmit(key, window_id, request_id, interval_ms)` returns
   `Admitted(admitted_at_ms, retry_at_ms)` or `CoolingDown(retry_at_ms)`.
   The interval is a whole number of milliseconds from 1 through 86,400,000
   (24 hours). The authority serializes calls for each key and supplies the clock. Concurrent prepares may see the
   same ID. The first admission for that ID chooses the interval.
3. On an ambiguous reply, repeat the exact call under the same authenticated
   subject. While its window is the most recently admitted one, the winner
   recovers the original admission, including both timestamps. A different
   attempt targeting that window gets its original deadline, even after expiry.
   A recovered admission is historical evidence and grants no additional start.
   Changing the winning attempt's interval conflicts. A losing attempt's
   interval has no effect and is never stored.
4. Once a successor commits, attempts for older windows return
   `Superseded(retry_at_ms)` with the latest window's deadline. Their original
   outcome is then unknown to the caller. A delayed packet cannot acquire a
   later window. To seek another admission, recheck the application state,
   prepare again and choose a new request ID.

Each key retains a clock high-water mark and its latest winner, including the
subject and request ID. The first window ID is zero. Each subsequent ID is the
previous window's deadline, which increases strictly. Treat IDs as opaque in
clients. Every observer of a deadline must still ask the authority for admission.
Clock regression and unrepresentable deadlines fail closed. Times use the core's
existing exact millisecond range.

## Worker and Go SDK

The Go call is `client.TryAdmit(ctx, resource, interval)`. Its first RPC reads
the window ID. The SDK keeps preparation internal and uses one request ID for
all transmissions of the admission. Retries use the original interval. A
superseded attempt returns its own terminal status to the application.
Only a new logical call can prepare a new window. One winner per key is
sufficient for this use case because the application can compare the desired
and published index revisions after any ambiguous result.

The local Worker enables the service with `P2_ENABLED=true` and
`PINTLE_COOLDOWN_ENABLED=true`. Routing binds authenticated tenant, namespace,
primitive and key. The Durable Object commits the clock and winner in one
SQLite transaction and waits for storage acknowledgement before replying.

Run the [core tests](../../crates/core/tests/cooldown.rs) with
`./bin/cargo test --locked -p pintle-core --test cooldown`.

## Local verification

On 2026-11-27 the [Go integration tests](../../tests/integration/cooldown_test.go)
ran against the compiled Worker in local Wrangler. Twelve concurrent calls with
different intervals produced one winner and one shared deadline. Three tests
discarded a successful HTTP response, killed Wrangler, and replayed the same
admission against a new process using the same SQLite storage. The SDK prepared
once in each case. It recovered the winner's original timestamps after expiry,
recovered a denial through its recovery handle, and returned `Superseded` when
a successor had committed. Tenant and subject isolation and interval validation
also passed.

The [controlled-clock test](../../tests/integration/cooldown_storage_test.go)
injected a SQL failure during admission at exact expiry. The stored records
matched their pre-failure values. The first winner remained recoverable and a
fresh attempt then succeeded at that same authority timestamp.

```sh
./bin/just test-runtime '^TestCooldown'
./bin/just test-hardening '^TestCooldownStorage$'
```

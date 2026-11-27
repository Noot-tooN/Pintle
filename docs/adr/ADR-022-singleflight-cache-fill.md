# ADR-022 — Singleflight for concurrent cache misses

Accepted for the native development slice, 2026-11-27. The bounds below are
prototype settings. The wire contract and deployment limits are open.

## Cache-fill contract

Ten callers miss the same immutable catalog key. The authority admits the
first as the producer and attaches the other nine to that generation. Only the
producer reads the origin. It rechecks the application cache first, since a
previous producer may have filled the cache before disappearing. After storing
the value, it reports completion to Pintle. Every attached, uncancelled caller
can read the same outcome. A later caller checks the cache as usual. A fresh
miss after completion can start a new generation immediately, capacity permitting.

Use a key that identifies the tenant, namespace, content version and all inputs
that can change the answer. The application owns cache freshness and writes.
Use immutable versioned keys or a conditional write if a delayed producer could
overwrite newer data. Cache storage and flight completion are separate commits.

| Event | Outcome for this generation |
| --- | --- |
| Producer disappears before reporting completion | The generation stays pending until its 30-second authority deadline. At the deadline all remaining waiters get `LeaderExpired`. A caller may explicitly retry after checking the cache. A late producer cannot complete a newer generation. |
| Producer disappears after completion, or loses the reply | The first terminal outcome can be read by its participants for 60 seconds, including while a newer fill runs. Retrying completion cannot replace it. |
| A waiter cancels | If cancellation is ordered first, that ticket remains `Cancelled`. If completion is ordered first, cancellation returns the recorded outcome. Other waiters and the producer continue, even when the last waiter leaves. |
| Result is too large | Up to and including 4,096 bytes can be shared inline, including an empty value. A larger value ends the flight with `ResultTooLarge` for the producer and remaining waiters. No partial bytes are stored. A producer can report oversize with a small control message. |
| Producer reports an ordinary fill error | The remaining waiters receive `FillFailed`. A new fill requires a fresh call. |

There is no automatic promotion or retry of the callback in this slice. A
producer that pauses beyond its deadline can still be computing when another
generation starts. A consumer needing large values can put them in its own
cache and use a small versioned reference as the agreed result format.

## Bounds and recovery

Each resource admits one running generation, up to 16 waiter tickets per
generation and up to eight running or retained generations combined. A
cancelled ticket keeps its slot so its outcome can be recovered. An admission
that would exceed either bound returns `Capacity`. This can throttle repeated
misses on a single key to eight completed generations in a recovery window.
Recovery expires at exactly 60 seconds after completion or the producer's
deadline. Expired tickets return `UnknownTicket`. Regressing authority time is
rejected, and generation identifiers must never wrap.

## Local implementation

The [Rust core](../../crates/core/src/singleflight.rs) models one serialized
resource. Run the [cache-fill example](../../crates/core/examples/singleflight_cache_fill.rs)
and the [boundary tests](../../crates/core/tests/singleflight.rs) locally.

```sh
./bin/cargo run --locked -p pintle-core --example singleflight_cache_fill
./bin/cargo test --locked -p pintle-core --test singleflight
```

The local Worker exposes the [experimental unary Connect service](../../proto/pintle/v1/singleflight.proto)
when both `P2_ENABLED=true` and `PINTLE_SINGLEFLIGHT_ENABLED=true` are set.
It uses the existing authenticated gateway and a distinct SQLite Durable Object
per tenant, namespace, primitive and resource. One synchronous transaction stores
the authority clock, generation counter, memberships, private tickets and result.
The adapter waits for storage acknowledgement before sending success.

1. Call `GetFlightGeneration` after a cache miss. Prepare and retain the
   resource, generation and request ID before calling `JoinFlight`. Replay that
   exact admission after an ambiguous reply. Each authenticated subject gets its
   own receipt. If the generation closed before admission, recheck the cache
   before preparing a fresh attempt.
2. Preserve the returned ticket as a credential. The producer flag records the
   original assignment. Use `ObserveFlight` to recover the current state. A lost
   reply or a pending observation cannot establish whether an external callback
   already ran. A waiter may disconnect and later observe the same ticket.
3. Only the producer can call `CompleteFlight`. A retry with the same outcome
   recovers the recorded result. A different outcome conflicts. A waiter can
   call `CancelFlight` and replay it safely. Expired receipts cannot admit a
   caller to a newer generation.
4. Use the explicit oversize failure when the producer detects more than 4,096
   bytes. Inline values above that limit in an accepted envelope also settle
   the flight as oversized. An envelope above the 8,192-byte transport limit
   is rejected before dispatch, so the producer must send the small marker.

Recovery windows are enforced on access. Idle authorities can retain the last
bounded snapshot on disk until the next operation. The local service supports
explicit observation. A managed Go callback, server-held waiter notification,
and hosted deployment are separate work.

```sh
./bin/just test-runtime '^TestSingleflightRecovery$'
./bin/just test-hardening '^TestSingleflightStorage$'
```

The [normal Worker test](../../tests/integration/singleflight_test.go) drops
successful admission, completion and cancellation replies at the HTTP client.
After each dropped reply it stops Wrangler and opens the same SQLite state in
a new process. It compares the reissued ticket or result with the discarded
response. It also exercises concurrent retries, subject and resource isolation,
changed completions, empty values and both size limits.

The [controlled-clock test](../../tests/integration/singleflight_storage_test.go)
injects SQL and callback failures inside the transaction and inspects the SQLite
records around rollback. It exercises completion at the producer deadline and
receipt recovery one millisecond before, and exactly at, expiry.

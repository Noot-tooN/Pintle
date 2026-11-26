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

## Implementation and next integration gate

The [Rust core](../../crates/core/src/singleflight.rs) models one serialized
resource. Run the [cache-fill example](../../crates/core/examples/singleflight_cache_fill.rs)
and the [boundary tests](../../crates/core/tests/singleflight.rs) locally.

```sh
./bin/cargo run --locked -p pintle-core --example singleflight_cache_fill
./bin/cargo test --locked -p pintle-core --test singleflight
```

The next slice is durable admission and completion in a local Worker/DO.
Atomically persist the generation counter, authority clock, membership, result
and session receipt. Scope the authority by tenant, namespace, primitive and
resource. Bind each ticket to the authenticated subject and authorize producer
completion separately. A core ticket is an index into one resource. Only the
verified adapter may use it. A lost admission reply must recover the same
ticket, including after cancellation. Reusing an operation ID with a different
payload must produce a conflict.

Exercise two independent clients, a lost completion reply, object restart,
transport size rejection and disconnect/reconnect under local Wrangler before
exposing the Go helper. A disconnected waiter may resume its ticket. Explicit
cancellation must reconcile a possible completion race. For oversized results
the SDK should send the control message before serializing the value so an HTTP
size rejection cannot strand the other callers. The native tests use an
in-memory authority and injected time.

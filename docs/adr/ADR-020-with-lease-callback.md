# ADR-020: Managed Go lease callbacks with cooperative cancellation

Accepted for development, 2026-10-28. Extends ADR-011/019's low-level SDK without
changing Rust coordination, wire messages, authentication, storage or deployment.

## Decision

Add `Client.WithLease(ctx, Resource, LeaseOptions, callback) error`. Use the existing
Resource type (namespace and key), rather than the earlier illustrative bare string.
Options default to TTL 30s, wait 60s, cleanup 5s, holder label `with-lease`.
TTL/wait retain protocol bounds; wait also bounds local session allocation and
acquisition. Cleanup is positive and at most 60s. Parent cancellation applies
throughout acquisition and work. Callers bound total work with the parent context.

Acquire one durable server-owned waiting intent. Never use a TryAcquire loop,
reacquire automatically, or retry a callback. Require an Active observation and
an initial successful Renew plus ACK before invoking work. Existing maintenance
performs renewal/ACK within TTL/3 and waits TTL/3 before the next renewal. Its
internal readiness signal is not an ownership oracle. Preserve actual renewal
errors if Stop races with their reporting.

Use one private recovery session for acquisition, maintenance and Release. The
maintenance ACK consumes acquisition and renewal outcomes, keeping the request
window and oldest-result lifetime bounded during long callbacks. No second
session allocation is required for renewal. A bounded per-Client cache retains
at most 16 fully settled idle sessions, exclusively borrowed by an invocation.
An idle session is heartbeat-checked before reuse; only explicit expiration
permits replacement before allocating a new intent. Other heartbeat errors fail
closed. Eviction/uncertainty leaves server sessions to expire normally. This is
not a way around server admission limits; concurrent callers, new clients and
high resource churn can still exhaust them. Client copies share the cache pointer.

A child context carries parent cancellation or a loss/uncertainty cause. The
callback runs synchronously and must join its own child work. On loss we cancel
work but wait for it to return: we cannot kill a goroutine, and must not voluntarily
release underneath a running callback. Actual lease expiry may still admit a
successor. A callback ignoring cancellation can keep WithLease blocked indefinitely.
The callback receives a lease snapshot; capability and token are stable across
renewal, but the expiry field is not updated and does not prove current ownership.

After callback return or panic unwinding, cancel work, stop/join maintenance and
its watcher, then attempt cleanup with one bounded context detached from parent
cancellation. For an ambiguous Acquire, CancelPending uses its exact original
request; if grant won, Release that capability. Never use an old receipt to start
work. Release uses one prepared identity. ACK only a fully known contiguous
prefix; an unresolved renewal or failed Release prevents ACK/reuse. A renewal
interrupted during normal callback completion can therefore produce uncertainty
even when Release succeeds. Do not silently discard its recovery evidence.

Join callback, cancellation and cleanup errors. `WithLeaseError` retains exact
Wait/Renew/Release requests and last observed lease as applicable; `errors.Is/As`
remain usable and formatted errors do not dump recovery capabilities. An ACK
failure may leave a request whose outcome is already consumed/acknowledged.
Panics propagate after attempted cleanup; cleanup errors cannot be returned during
panic. No detached unbounded retry workers, new dependency or automatic durable
journal is introduced. Crash-safe pre-send persistence still requires low-level
session/request APIs. Custom transports must honor cancellation, as before.

## Optional fencing

Always issue the existing exact decimal token. Applications may ignore it when
redundant/stale work is harmless or pass it to a cooperating destination. The
helper does not operate another database or intercept business writes. The
runnable inventory example uses an in-memory application store to demonstrate
atomic token comparison and value update, including rejected stale writes. It
is not a PostgreSQL adapter, durable storage example or exactly-once guarantee.

## Development versus publication

The maintainer explicitly deferred external security review and formal publication.
Neither blocks continued P4 development. Retain recorded security work and its
limitations; do not describe it as an independent external assessment. Future
publication/production decisions remain separate and need explicit authorization.
WithLease is the current SDK usability increment, not a claim that additional P4
coordination primitives or downstream database integrations are implemented.

See [results and limits](../with-lease-results.md) for executed verification.

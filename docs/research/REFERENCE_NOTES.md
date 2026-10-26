# Technical Reference Notes (research through 2026-10-24)

**These links document external platforms, not our unbuilt implementation. Recheck current versions before using code.**

## Storage/runtime

- Cloudflare SQLite DO storage supports `transactionSync()` and states that `sql.exec()` cannot issue `BEGIN`/`SAVEPOINT` directly. It warns about JS number precision for large int64 SQL values. https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/
- Cloudflare `workers-rs` supplies Rust/Wasm Worker/DO classes and SQL storage integration; a clean high-level `transactionSync` Rust binding is **not yet confirmed**. https://github.com/cloudflare/workers-rs
- Cloudflare local Wrangler/Miniflare uses a `workerd` process with simulated DO state. It's appropriate for integration tests but does not reproduce all cloud failure semantics: https://developers.cloudflare.com/workers/testing/miniflare/ and https://developers.cloudflare.com/workers/testing/
- DO does not natively provide FoundationDB-style server watches. Server-side no-lost-wakeup behavior will be an application protocol plus notifications/alarm semantics.

## RPC

- `connect-rust` offers Connect, gRPC and gRPC-Web in a Tower-based Rust implementation, but its standard Hyper networking server feature is native-oriented, not a drop-in Workers/Wasm server. Test actual Worker integration. https://github.com/connectrpc/connect-rust
- The critical performance factors for this project will be storage commits, contention and WAN RTT; choose wire protocol primarily for interoperability, testing and SDK experience, not small unconstrained microbenchmark differences.

## Design references

- etcd Go concurrency package supports mutex, election, leases, revisions and conditional transactions; it uses revision-aware watches for waiting. https://github.com/etcd-io/etcd/tree/main/client/v3/concurrency
- FoundationDB has strict serializability, FDB native watch futures (one-shot change notification, not durable event replay), and atomic operations. https://apple.github.io/foundationdb/developer-guide.html
- Redis standard primary/replica failover is asynchronous, so its baseline locking guarantees differ from consensus-backed transactional systems; don't overgeneralize that every Redis usage is invalid. https://redis.io/docs/latest/develop/clients/patterns/distributed-locks/
- `ubgo/lock` is a Go common `Locker` over backend-specific algorithms, not a generic storage transaction layer. One researched Redis implementation does separate lock acquisition from fencing increment. https://github.com/ubgo/lock
- FenceKit is a Python/Redis backed background-job library with real fencing + Django integration, but its own docs acknowledge Redis failover rollback/lease limitations; not a hosted multi-language service. https://github.com/ahmed5145/fencekit

## Competitive context

Mature libraries and products already cover fragments of the space: etcd concurrency, Redisson, Dapr, Upstash RateLimit, Inngest/Trigger.dev flow controls, Netflix concurrency-limits and BullMQ. The project is not claiming those categories are unserved; it targets independent multi-language infrastructure and unusually rigorous usability/correctness.

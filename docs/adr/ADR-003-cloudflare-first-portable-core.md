# ADR-003 — Cloudflare Durable Objects first, provider-neutral Rust core

**Status:** ACCEPTED as first implementation order (2026-10-24). Transaction and protocol bindings require spikes.

## Decision

We will build the first coordination authority with a Rust/Wasm Cloudflare Durable Object and attached SQLite, reached through a Cloudflare Worker entry point. This minimizes infrastructure operation and enables a future optional hosted endpoint. The state transition/domain logic remains in a pure provider-neutral Rust core crate. Cloudflare-specific persistence, networking, object lifecycle, watch/timers and initialization stay in the adapter.

A fully native self-hostable Rust server with FDB or etcd is an intentional future alternative, *not* a release-0.1 prerequisite. Shared core code, protocol and conformance tests prepare for that possibility but do not make portability automatic.

## Immediate risk-gates

1. `workers-rs` SQLite multi-statement atomicity and rollback from Rust/Wasm (platform's `transactionSync` exists, wrapper requires verification).
2. Fence integer precision via JS/Wasm, including >2^53.
3. Server-side waiting without lost wakeups or unbounded active DO compute.
4. Stable unary public protocol mapping for Cloudflare Fetch; native Connect Rust server can't be assumed to run directly in Workers.

These MUST be demonstrated before claiming a working mutex service.

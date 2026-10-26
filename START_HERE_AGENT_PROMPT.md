# Paste this prompt into your coding agent

You are implementing the first milestone of **Pintle**, the open-source distributed coordination toolkit described in this repository. **Before changing code, read `AGENTS.md`, `PROJECT_SPEC.md`, `docs/GUARANTEES.md`, `docs/TEST_PLAN.md`, and `docs/IMPLEMENTATION_PLAN.md` completely.** All accepted design decisions take precedence over earlier brainstorming.

Goal for THIS session/PR: **P0 Cloudflare Rust Durable Objects SQLite atomicity feasibility spike**, plus a very small portable Rust domain-core placeholder only insofar as needed for an exercised transition. Do not build SaaS, billing, many SDKs, semaphores, dashboard or FDB/etcd adapters. The project/repository name is **Pintle**; use Pintle naming consistently in new code.

1. Inspect existing files/toolchains, report key environmental constraints, and choose current compatible pinned Rust/Node/Wrangler/workers-rs versions from authoritative docs.
2. Scaffold a minimal Rust workspace: platform-independent `core` and a Rust Cloudflare Worker/Durable Object adapter with SQLite-backed DO migration configured correctly. Run it locally through Wrangler/Miniflare/workerd.
3. Implement one exercised **atomic conditional transition** that changes owner, fencing sequence, resource revision, and request-dedupe record together. Prove rollback when a later statement intentionally fails. Avoid unguarded async read-modify-write across handlers.
4. Prove concurrent callers cannot both commit a conflicting grant and that retrying a logical `request_id` recovers the same committed outcome. Run tests for durable persistence across local process restart.
5. Prove exact fencing counter roundtrip near and beyond JavaScript `Number.MAX_SAFE_INTEGER`; handle unsafe numeric conversions explicitly.
6. Check whether `workers-rs` exposes the transaction callback needed; if not, verify the narrowest safe interop path or suitable dependency and document trade-offs. If a defensible atomic transaction cannot be implemented, stop and report the blocker rather than silently weakening guarantees.
7. Optionally spike unary Go/HTTP/Protobuf gateway feasibility after atomicity works, including whether Connect-compatible server semantics are feasible in the Cloudflare runtime. Do not spend the whole milestone implementing a general custom transport.
8. Produce `docs/spike-results.md`: versions, exact commands, results, observed failure behavior, gaps between local simulator and production, and recommended next PR. Update ADRs only if experiments justify a changed decision.

Quality bar: successful actual local build/test commands, readable APIs, no hardcoded secrets, no unsupported correctness claims. Report uncertainty and incomplete work clearly. Do not claim broad production guarantee based solely on a simulator.

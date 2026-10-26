# Agent instructions — Pintle

This repository is **Pintle**, an open-source distributed coordination platform. The name and repository are chosen; use Pintle naming in new packages and documentation unless this would break an already published protocol.

## Source of truth (read in order)

1. [`PROJECT_SPEC.md`](PROJECT_SPEC.md) — vision, accepted architectural choices, strict guarantee boundaries, product roadmap, and current Cloudflare-first strategy.
2. [`docs/IMPLEMENTATION_PLAN.md`](docs/IMPLEMENTATION_PLAN.md) — executable work plan and gates. **Start with P0 feasibility spike**, not full helper library.
3. [`docs/GUARANTEES.md`](docs/GUARANTEES.md) — target invariants and explicitly *unverified* guarantees.
4. [`docs/TEST_PLAN.md`](docs/TEST_PLAN.md) — evidence requirements.
5. [`docs/PROTOCOL_DRAFT.md`](docs/PROTOCOL_DRAFT.md) — candidate wire contract, not frozen.
6. [`docs/REPO_LAYOUT.md`](docs/REPO_LAYOUT.md) — suggested module structure.
7. [`docs/adr/`](docs/adr/) — decision history. New decisions need ADRs; superseded earlier design must not creep back in.
8. [`docs/NAMING.md`](docs/NAMING.md) — historical naming exploration; **Pintle is the selected name**.

## Non-negotiable architecture

- Rust-only authoritative coordination algorithms, independent of cloud APIs in the `core` crate; Go SDK first.
- SDK -> versioned protocol -> authoritative server. **Both server `Acquire` and `TryAcquire`** share one atomic grant transition. SDKs must not reimplement watch/contention loops.
- **First deployment uses Cloudflare Workers + Durable Objects + SQLite** for low operational burden; the Rust domain core and public protocol remain infrastructure agnostic. The Worker is the gateway; the DO is authoritative, not a raw database remotely accessed by AWS.
- Backend-specific transaction/notification/clock adaptation must be explicit. Future etcd/FDB/native Rust server possible, **not implemented yet**.
- Prefer a generic conditional transactional storage boundary to backend-specific `acquire_lock` helpers; exact trait remains **provisional** until tests.
- Protobuf + Buf is our leading schema approach; Connect-compatible HTTP is a **candidate**, not verified to run as a server inside Workers/Wasm.
- No unsafe downgrades: no separate non-atomic owner/fence writes, missing request dedupe, weak Redis-like failover semantics represented as equivalent, blanket exactly-once claims, or unguarded external side-effect guarantees.

## What to build FIRST

1. Verify Rust `workers-rs` + DO SQLite can atomically commit **multiple records** with rollback on failure, including fencing generation and request dedupe, from a real compiled Wasm object.
2. Prove exact integer representation across JS/Wasm/SQL above `2^53` or choose a lossless encoding.
3. Test concurrency/restart/idempotent same-request retries using local Wrangler/workerd; record commands/results.
4. Verify viable public unary Protobuf/Connect gateway mechanism under Workers; use a documented fallback only if necessary.
5. Write `docs/spike-results.md` with *evidence*, not confident speculation. Stop/raise blocker if required atomicity isn't supported through a maintainable integration.

## Development conduct

- Inspect the repository and installed tools before changing files. Do not invent dependency APIs or assert tests passed without running them.
- Keep PRs narrow. Prefer correctness and failure tests over adding features.
- For uncertain choices, document hypothesis and smallest confirming test. Do not silently finalize `OPEN`/`PROPOSED` items.
- No live Cloudflare deployment, purchases, credential storage, or creation of an actual GitHub repo without explicit permission.
- Use local Wrangler/Miniflare for initial tests; hosted staging tests come later and require credentials/approval.
- Target result: compilable, reproducible minimal P0 vertical slice; **do not claim the whole coordination toolkit is implemented**.

## Current implementation prompt

See [`START_HERE_AGENT_PROMPT.md`](START_HERE_AGENT_PROMPT.md). Paste it into Codex or another coding agent from the root of this repository.

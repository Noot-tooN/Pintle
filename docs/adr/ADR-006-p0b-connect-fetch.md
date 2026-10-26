# ADR-006 — P0b binary Connect over Worker Fetch

**Status: ACCEPTED for the local P0b experiment (2026-10-25).** The schema,
authentication policy, and transport feature set remain provisional. Evidence:
[p0b-results.md](../p0b-results.md).

## Decision

Generate Go Protobuf messages and a Connect client from the minimal `pintle.v1`
schema using Hermit-pinned Buf and local plugins. Generate Rust Prost messages
from Buf's descriptor set using a locked native workspace tool. Generated types
live outside the domain core; the core retains no Cloudflare or protocol imports.

Implement a small Rust adapter from Worker Fetch to the unary binary Connect
wire format: POST, `application/proto`, protocol version 1, identity encoding,
standard JSON errors, bounded bodies, and a bounded request deadline. The Worker
authenticates and routes to the DO; the DO invokes the same synchronous atomic
Rust grant transition established in P0. There is no JavaScript coordination
algorithm or native server process on the authoritative path.

This is a deliberate use of the plan's small Fetch-adapter option, not evidence
that all of connect-rust is incompatible with Wasm. The inspected connect-rust
0.9.0 documentation says its core compiles to wasm32; its native networking
client/server features are not a drop-in Worker Fetch listener. We did not build
a connect-rust server or its alternate transport integration. A narrow Prost
adapter directly tests the required runtime boundary with fewer abstractions.
Reassess library reuse before expanding protocol coverage.

## Boundaries

- `TryAcquire` returns a `GrantReceipt`, not a lease. No invented TTL, secret
  ownership capability, Renew, Release, or blocking Acquire is introduced.
- Fence and revision remain canonical decimal strings through Protobuf, matching
  P0's lossless SQL/Wasm representation. Arithmetic remains checked Rust u64.
- Random local bearer tokens map to tenant, stable subject, and allowed
  namespaces. Authority identity comes from authenticated tenant plus resource;
  dedupe binds the stable subject and holder reference. Token rotation can
  preserve replay identity. This is a basic test policy, not final auth design.
- Deadlines bound the gateway's body read and response wait. The DO checks before
  the atomic transition, but timeout/disconnect cannot undo a completed commit.
  A caller must retry the same ID and payload to reconcile an uncertain result.
- JSON messages, compression, streaming, gRPC, and gRPC-Web are unsupported.
  Explicit errors are tested. Full Connect conformance is not claimed.
- Buf FILE breaking checks use a committed provisional descriptor snapshot and,
  in pull-request CI, the fetched base schema when one exists. Regeneration also
  checks for drift. This is a guardrail, not a public compatibility commitment.

## Consequences

Actual generated-client calls now establish local protocol feasibility while
preserving the infrastructure-independent domain boundary. The custom adapter
owns protocol maintenance and needs broader conformance/security testing before
release. No accepted architecture decision is superseded. P1 must define lease
time, ownership, replay retention, and recovery semantics before growing the API.

Primary references: [Connect protocol](https://connectrpc.com/docs/protocol/),
[connect-rust](https://github.com/connectrpc/connect-rust), and
[Buf generation](https://buf.build/docs/generate/).

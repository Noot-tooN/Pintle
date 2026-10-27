# Suggested repository shape — illustrative, not mandatory

```text
pintle/
  AGENTS.md
  README.md
  PROJECT_SPEC.md
  START_HERE_AGENT_PROMPT.md
  docs/
    IMPLEMENTATION_PLAN.md
    GUARANTEES.md
    TEST_PLAN.md
    PROTOCOL_DRAFT.md
    NAMING.md
    adr/
      ADR-001-server-authoritative.md
      ADR-002-server-owned-acquire.md
      ADR-003-cloudflare-first-portable-core.md
      ADR-004-transaction-boundary-candidate.md
    research/REFERENCE_NOTES.md
  crates/
    core/              # Native Rust platform-independent deterministic transitions
    store/             # provisional generic atomic transaction contract
    cloudflare/        # Rust/Wasm Worker entry point, Durable Object + SQLite adapter
    service/           # typed services, only when useful as separate crate
  proto/pintle/v1/
  sdk/go/
  tests/integration/
  wrangler.jsonc
  buf.yaml
  Cargo.toml
```

These crates/config files are **NOT supplied by the documentation package**. Coding agents will create them during the milestones, and may combine service/store crates when over-separation creates complexity. Do not scaffold unused TypeScript, Kotlin, Redis, etcd or FDB implementations merely to fill an architecture diagram.

The important separation is Rust coordination state transitions independent of platform, authoritative service operations, and Cloudflare-specific transaction/timer/routing adapters. Authenticated public gateway protocol must remain independent of Durable Object class names.

## Implemented through P2

The current workspace has `core` (generic Transaction and lease
transitions), `protocol` (generated Prost messages), and `cloudflare` (Fetch,
authentication, DO/SQLite, clock/randomness adapters). Generation tooling is in
`tools/proto-gen`; generated Go messages/Connect clients are in `gen/go`, and the
explicit lifecycle SDK is in `sdk/go/pintle`. A separate store/service crate has
not been introduced because it adds no required boundary at this stage. The
portable core has no Cloudflare dependency. See [P1 results](p1-results.md).

P1.1 adds `core/src/session.rs`, explicit Go recovery sessions, management RPCs,
and a Cloudflare alarm adapter. Fixed reusable slots use the same generic get/put
transaction boundary. See [ADR-009](adr/ADR-009-bounded-recovery-sessions.md).

P2 adds `core/src/session/waiting.rs` for queue policy and shared grant driving,
`cloudflare/src/notify.rs` for bounded ephemeral notification hints, and Go
`wait.go` for prepared intents, bounded transport resumption, and cancellation.
Authoritative queue state remains in the existing session records.

## P3 additions

`crates/auth` contains bounded static credential validation without Cloudflare imports.
`cloudflare/src/metrics.rs` emits sanitized adapter events. `sdk/go/pintle/maintain.go`
provides optional renewal/loss behavior; `validation.go` holds SDK input/response checks.
Core coordination stays in Rust. P3 integration and deterministic tests are in
`tests/integration/p3*_test.go`; operational/review evidence is in `docs/p3-results.md`.

## Development commands

The root `justfile` is authoritative for bootstrap, generation, checking, builds,
tests, audits and explicit staging operations. Just is pinned through Hermit. CI and Wrangler
invoke the same recipes. There are no project shell/npm wrappers; the standalone
OSV query program lives in `tools/security-audit.mjs`. Test assertions and runtime
lifecycle control remain in Rust/Go. See [ADR-012](adr/ADR-012-just-command-interface.md).

`wrangler.staging.jsonc` isolates the hosted staging Worker and SQLite namespace.
`tests/hosted` holds bounded public-API smoke, lifecycle and resilience/capacity scenarios; the normal local integration
suite exercises them before any opt-in remote run. See [staging.md](staging.md).

The root Go module `github.com/Noot-tooN/Pintle` packages `sdk/go/pintle` and the
generated Go protocol together. `examples/mutex` is the runnable public SDK command;
`tests/integration/sdk_examples_test.go` exercises it against local Wrangler.
`tools/proto-compat` preserves strict Buf comparisons across the one historical
pre-publication Go import-path correction. See [ADR-019](adr/ADR-019-go-sdk-packaging-and-examples.md).

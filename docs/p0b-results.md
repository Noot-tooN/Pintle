# P0b — Protobuf / Connect feasibility results

> Historical milestone evidence: the P0/P0b runtime fixture and its commands
> have since been retired. Useful coverage now runs against the current mutex;
> see [test consolidation](test-consolidation-results.md). Captured results below
> describe the implementation at that milestone, not current runtime controls.


> Tooling update: command names below now use the root `justfile`. The milestone
> runs originally used equivalent shell scripts; their captured transcripts are
> unchanged. See [Just migration results](just-migration-results.md) for a full
> suite run through Just.

Current follow-up: [P1 results](p1-results.md) adds the nonblocking lease lifecycle
and Go SDK. This document preserves the earlier milestone evidence.

**Subsequent hardening:** see [hardening-results.md](hardening-results.md).
The original timing-based deadline cases below have been replaced by controlled
checkpoints and logical time, with a separate real-timer smoke test retained.
Additional boundary, fault, model, and history checks now run in the full suite.
The report below preserves the original P0b evidence and counts.

**Result: P0b acceptance checks PASS locally**, 2026-10-25, macOS arm64.
A real generated Go Connect client calls the release Rust/Wasm Worker through
local Wrangler, then its Durable Object and SQLite. Wrangler uses its locked
Miniflare/workerd dependencies. There is no mock DO, JavaScript grant algorithm,
or native Go authority in this path. No deployment, push, or P1 implementation
was performed.

## Reproduce and inspect

```sh
./bin/just bootstrap
./bin/just test
```

The successful final run includes:

1. Buf lint/format, fresh Rust/Go generation into a temporary directory, diffs
   against checked-in generated code, and FILE breaking checks against the
   provisional descriptor baseline.
2. Rust formatting, four native core tests, native Clippy for core/protocol/codegen
   and Wasm Clippy for the adapter, all with warnings denied.
3. A real release Wasm build with `worker-build --release --locked`.
4. `go test -count=1 -timeout=3m -v ./gen/go/... ./tests/integration`.
   Nine P0b groups and seven original P0 groups pass. The final combined Go
   integration package took 9.530 seconds; this is not a performance benchmark.
   `TestClientProcess` skips in the parent and runs in P0's 36 child processes.

Bootstrap and full test output are in [p0b-test-output.txt](p0b-test-output.txt),
with only the absolute checkout path normalized. Bootstrap's npm audit reported
zero vulnerabilities. Raw runtime logs and SQLite files remain under ignored
`test-results/p0b-*` and `test-results/p0-*`. Test credentials are generated
randomly into a mode-0600 `.dev.vars` beside the temporary Wrangler config and
removed on normal test cleanup. Abrupt termination of the test process can leave
that ignored file behind; its tokens are local, disposable test material.

To run only the generated-client suite after building:

```sh
./bin/just build
./bin/go test -count=1 -timeout=3m -v ./tests/integration -run '^TestP0b$'
```

## Dependencies and generation

Existing Hermit 0.54.1, Rust 1.99.1, Go 1.27.3, Node 24.22.1, workers-rs and
worker-build 0.8.7, Wrangler 4.152.0, Miniflare 5.20261019.2-beta, and workerd
1.20261019.1 are unchanged from [P0](spike-results.md).

| New component | Exact version / management |
| --- | --- |
| Buf | 1.75.0, Hermit package |
| protoc-gen-go | 1.36.12, Hermit package |
| protoc-gen-connect-go | 1.21.0, Hermit package |
| Go Connect runtime | connectrpc.com/connect 1.21.1, go.mod/go.sum via Hermit Go |
| Go Protobuf runtime | google.golang.org/protobuf 1.36.12, go.mod/go.sum |
| Rust messages/codegen | prost, prost-types, prost-build 0.14.4, Cargo.lock via Hermit Cargo |
| Rust adapter helpers | futures-util 0.3.34 and subtle 2.6.1, Cargo.lock |

`just generate` uses local pinned plugins, not a hosted generator. Buf
builds a descriptor set; `tools/proto-gen` feeds it to Prost without installing
protoc. Buf's cache is project-local under `.hermit`. No new global tools were
installed. Generated Go messages/client live under `gen/go/pintle/v1`; generated
Rust messages live in `crates/protocol`. The core still has no protocol or cloud
dependency.

## Acceptance evidence

| Check | Executed evidence | Result |
| --- | --- | --- |
| Minimal versioned schema | `proto/pintle/v1/lock.proto`, Buf STANDARD lint, generated Rust types and Go messages/client compile. | PASS |
| Actual unary interoperability | Generated client POSTs binary Protobuf to `/pintle.v1.LockService/TryAcquire`; Rust Fetch gateway forwards to the compiled DO and returns a typed receipt/contention result. Unknown additive request fields are accepted. | PASS |
| Stable replay and conflicts | Same ID/payload returns the identical receipt with `replayed=true`; changed holder or stable subject returns `already_exists`. | PASS |
| Auth and isolation | Missing server credentials fail closed; invalid token is unauthenticated; disallowed namespace is denied; two tenants with identical keys each get fence 1; same principal with a rotated token replays. Forged internal identity/deadline headers have no effect. | PASS |
| Fixture separation | P0b requests run with `LOCAL_SPIKE` absent; public `/spike` and internal DO paths cannot invoke fixture operations. | PASS |
| Precision | Generated client sees exact fence 9007199254740993 and 18446744073709551615, revision 9007199254740993, stable replay, and explicit overflow error. | PASS |
| Concurrent clients | Barrier-released 16 distinct requests yield one fresh grant and 15 contended; 16 same-ID requests yield one fresh grant and 15 identical-token replays. | PASS |
| Response loss and persistence | A Go HTTP fault proxy consumes a committed protobuf response then closes the connection. Kill/restart local Wrangler with the same SQLite directory; generated client recovers that exact receipt and the high-token receipt. | PASS |
| Standard errors and request bounds | Typed client validation/JSON/gzip errors; 13 raw wire cases assert HTTP status, JSON error code, and no-store. Known-length and chunked oversized bodies reject; subsequent valid request remains fresh. | PASS |
| Deadline before commit | Environment-only delay before transaction exceeds a 20ms deadline; 504, then same-ID retry is fresh. Generated-client context timeout and already-cancelled context return expected codes. | PASS |
| Deadline after commit | Delay after transaction/sync exceeds a 40ms deadline; 504, then same-ID retry is a replay. Timeout is not proof of rollback. | PASS |
| Stalled upload | Raw TCP chunked request never finishes its body; the gateway returns 504 for a 30ms Connect deadline. | PASS |
| P0 regression | Original rollback at all four writes, multiprocess contention, historical replay, precision/overflow, and persistent restart suite passes unchanged in meaning. | PASS |
| Compatibility guard | Fresh generation matches; baseline check passes. Deleting `GrantReceipt.fencing_token` in a temporary schema produces Buf's field-deleted violation and expected exit 100. | PASS |

The breaking-change negative experiment ran:

```sh
# Temporary copy of buf.yaml + proto/, with fencing_token field removed:
./bin/buf breaking /tmp/pintle-p0b-breaking-check --against proto-baseline/p0b.binpb
```

It reported: `Previously present field "4" with name "fencing_token" on message
"GrantReceipt" was deleted.` The repository schema/baseline were not changed by
this experiment. CI runs the same generation/baseline checks and additionally
compares to the fetched PR base when it contains a schema. Remote Linux CI and
the PR-base comparison are **NOT RUN** in this unpushed workspace.

## Implemented contract and boundaries

The adapter supports binary unary Connect POST, `application/proto`, required
`Connect-Protocol-Version: 1`, and identity encoding. Errors use Connect JSON
envelopes and standard HTTP mappings: invalid argument 400, unauthenticated 401,
permission denied 403, request-ID conflict/already exists 409, resource exhausted
429, unavailable 503, deadline exceeded 504. Unsupported codecs return 415,
unsupported compression/unknown methods 501, and GET 405. Contention is a normal
typed outcome. Both success and error responses are `Cache-Control: no-store`.

Request bodies are capped at 8192 bytes while accumulating the stream. Namespace
is 1–64 ASCII identifier bytes; key is 1–128 UTF-8 bytes without control characters;
request ID is 1–128 and holder reference 1–64 ASCII identifier bytes. Identifier
characters are alphanumeric plus `._:-`. No Unicode normalization is performed.

The local secret `PINTLE_TEST_CREDENTIALS` maps bearer tokens to tenant, stable
subject, and namespace allowlist. There are no default public credentials.
`P0B_ENABLED=true` is separately required and absent from checked-in Wrangler
configuration. The Worker derives the authority from tenant/namespace/key and
constructs fresh internal metadata; caller-supplied DO IDs or subjects are not
trusted. Core owner identity encodes stable subject plus holder reference, so
token rotation need not invalidate retry identity. No real auth provider, token
issuance/revocation service, rate limiting, or final tenant policy is implemented.

`Connect-Timeout-Ms` accepts at most ten decimal digits and clamps to ten seconds;
an omitted timeout also receives that cap. Zero expires immediately. The gateway
races the body read/DO request against a timer; the authority checks before its
synchronous transaction and after response preparation. These deadlines use
Worker wall-clock Date/timers, **not a verified lease clock**. Dropping a pending
fetch or losing a connection is not a rollback operation. An uncertain outcome
must be reconciled with the same request ID and payload; callers must not infer
absence of a grant from timeout. The tests exercise selected schedules, not all
cancellation races or platform suspension behavior.

## Decisions, fixes, and remaining work

- [ADR-006](adr/ADR-006-p0b-connect-fetch.md) records the small Rust Fetch adapter
  choice. It uses Prost plus workers-rs; there is no third-party Connect server
  library on the Worker. The third-party Go runtime and generated client verify
  interoperability. We did not establish that connect-rust's Wasm-compatible
  core is unusable; its native server is not a drop-in Fetch listener.
- The initial stalled-upload test blocked its own Go request writer. Replacing
  that with raw TCP exposed a separate harness cleanup bug: a subtest stopped
  shared Wrangler before the next test. Shared runtimes now belong to the parent
  test. The final isolated P0b and combined P0/P0b runs both pass.
- This is an unpublished schema, despite the experimental `pintle.v1` package.
  `GrantReceipt` has no TTL or ownership capability. Historical replay is not
  current ownership. Only committed grants reserve IDs; contention does not.
  Dedupe remains unbounded, as in P0.
- JSON message encoding, compression, streaming, gRPC/gRPC-Web, browser CORS,
  full official Connect conformance, and load testing are **NOT IMPLEMENTED or
  NOT RUN**. Explicitly rejected modes are not claimed as supported.
- Hosted deployment, TLS/auth provider integration, cloud failover, object
  replacement, snapshot restore, and sync-failure injection remain **NOT RUN**.
  Local restart preserves one local SQLite volume. No new production durability
  or external-effect guarantee follows from this result.

**Recommended next milestone: P1**, with the portable mutex state machine,
explicit clock/TTL model, ownership-checked Renew/Release, bounded replay and
unknown-outcome recovery, then a Go SDK and real-runtime lifecycle tests.
Blocking Acquire remains P2. No P1 work was started here.

Protocol references inspected:
[Connect wire protocol](https://connectrpc.com/docs/protocol/),
[connect-rust source/documentation](https://github.com/connectrpc/connect-rust),
[Buf code generation](https://buf.build/docs/generate/).

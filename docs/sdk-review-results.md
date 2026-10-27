# P3 API/Go SDK usability and packaging review

**PASS for this scoped review and local validation, 2026-10-27.** Confirmed findings
were fixed and runnable examples added. P3 OSS release remains open for independent
security review, vulnerability-report triage, final cross-document/release-candidate
checks and explicit maintainer approval. No deployment, hosted request, publication,
commit or push was performed. No new dependency or toolchain upgrade was required.

## Scope and review method

Reviewed the current working tree at Git baseline
`74cdb6694456256b258377b92e8aaf56b5428ff0`, preserving existing uncommitted staging,
lifecycle, capacity and release-scope work. This is a working-tree API review,
not a review limited to a committed PR range and not an independent security audit.
The scope was SDK constructors/results, sessions/ACK, Acquire/cancellation,
maintenance/recovery, transport defaults, package imports/code generation and
the installation/example path for a new Go application.

Requirements came from the project specification, guarantees and
[OSS checklist](release-checklist.md). Review followed the code-review skill's
correctness and comprehension checks. No CodeGraph index exists for this checkout;
bounded source/test reads were used without initializing an index.

## Findings fixed

| Finding | Consequence and correction | Evidence |
| --- | --- | --- |
| Go module still named `pintle.local/spike` | The SDK lacked a usable public repository import identity. Changed the root module, schema `go_package`, generated imports and consumers to `github.com/Noot-tooN/Pintle`. No public tag is claimed. | Independent external-module build; generated drift and Buf checks against both P0b and the Git baseline |
| Malformed Renew/Release capability accepted during preparation | Session constructors allocated a sequence for a request the server rejects before recording an outcome. That can leave a gap in the contiguous ACK window. Validate resource/canonical 32-byte hex capability before allocating a sequence; do not attempt to validate live ownership locally. | New regression failed before the fix on invalid renewal capability; after the fix both constructors reject empty/short/noncanonical/nonhex capabilities and the next valid acquisition still has sequence 1 |
| Current SDK entry point and recovery obligations were hard to discover | Package-level constructors omit session attempts and cannot serve as the current P2 happy path. Added package/constructor docs, a single SDK guide and real command examples covering Session methods, Current versus Receipt, cancellation, Lost, explicit Release and ACK. | `go doc`, three example modes executed as race-enabled processes against normal Wrangler |

The sequence bug's repro is in `test-results/sdk-before-fix.log`; no server/core
algorithm changed. The release-reservation regression now uses a valid sample
lease so its failure/success still tests window capacity, rather than malformed
capability rejection.

## Design choices and review coverage

| Requirement | Implementation/evidence | Limit |
| --- | --- | --- |
| Usable Go consumption | One root module contains SDK and generated clients; independent temporary consumer builds the actual example offline from cached dependencies | Local replacement only; published proxy fetch and earlier Go versions unverified |
| Stable retry identity | Session constructors, prepared Attempt, status/replay and ACK; existing fault suite plus lost-renewal example | Caller must keep requests immutable and persist them before send for crash recovery |
| Safe cancellation semantics | Example fixes both orderings explicitly: cancel before admission, and cancel after committed grant; existing controlled races cover concurrency | Cancellation is not Release, and transport cancellation does not retract durable intent |
| Lease-loss handling | Maintain/Lost/Recovery example discards a successful renewal response before ACK, reconciles same attempt and explicitly releases | ACK-response loss may already have reclaimed history; no ownership oracle or automatic effect recovery |
| Isolation and bounds | Existing auth/transport/session/waiter regressions run in the full suite | Tenant-wide SaaS controls remain deferred; no isolation or safety limit removed |
| Protocol compatibility | Buf FILE checks plus a narrow normalization of the exact historical Go import option | Go import path intentionally changes pre-publication; no full official Connect conformance claim |

Retained the existing Client/Session split: a new convenience wrapper would hide
sequence allocation, unknown outcomes and ACK ownership rather than remove their
obligations. Kept low-level constructors for focused earlier protocol modes and
documented the current entry point instead of deleting compatibility test paths.
No automatic retries, request journal, Session allocator restoration, generic
callback framework or new dependency was introduced.

The example command directly expresses the lifecycle in three modes. It adds
unique executable documentation and process-level entry-point coverage, rather
than copying the complete concurrency matrix. Fixed response/commit barriers
control the demonstrated outcomes; deadlines are watchdogs. The examples perform
no external business writes and are not a downstream fencing integration.

The descriptor normalizer is a narrowly scoped migration helper, not a relaxed
Buf policy: the exact historical file/path option is normalized in temporary
baseline copies; all other fields/options and future import changes remain checked.
The original P0b descriptor remains unchanged. See
[ADR-019](adr/ADR-019-go-sdk-packaging-and-examples.md).

## Commands and observed results

All maintained build/test entry points use existing Hermit pins and Just.

| Command | Result |
| --- | --- |
| `just test-sdk '^TestInvalidCapabilityDoesNotConsumeSessionPosition$'` before fix | Expected failure, invalid capability accepted; repro retained |
| `just generate`, initial `just check-proto` after module rename | Generation succeeded; Buf correctly rejected the intentional Go package metadata change |
| `just check-sdk-package` | PASS: temporary independent module, `GOWORK=off`, `GOPROXY=off`, `GOSUMDB=off`, cached dependencies; `go build -mod=readonly` and example help |
| `PINTLE_PROTO_BASE_REF=74cdb6694456256b258377b92e8aaf56b5428ff0 just check-proto` | PASS after the exact historical metadata normalization, against both baseline forms |
| `just test-sdk` | PASS after fix, race-enabled SDK/guard/compatibility-helper tests |
| `just test` | PASS, exit 0: schema/codegen, format, Clippy, Go vet, independent consumer build, 25 native core + 2 auth tests, SDK race tests **1.203s**, compatibility-helper tests **1.148s**, hosted guard tests **1.207s**, normal runtime integration **87.372s**, instrumented deterministic integration **81.423s** |
| `TestSDKExamples` within the normal full suite | PASS, **5.73s** including startup/build; lifecycle **1.61s**, cancel **1.12s**, recover **1.10s** |

CI already invokes `just test`, so the new packaging check and runnable examples
are included without a separate workflow or wrapper. `just example-local` selects
the same verified integration test; `just example MODE` runs a mode against an
explicitly configured endpoint. Default tests skip all remote staging opt-ins.

Normal release Wasm was restored and its SHA-256 remains
`86be573f0ef931917bf5d8824f50c6a1e5f6c1b9277a40264e69b3508acc8e1e`.
The P0b descriptor SHA-256 remains
`de716ec5c382c78521d0905afd574a6d63c1d962750fe4f86be9f229853cde82`.
Final Go/Just formatting, local documentation file links and `git diff --check`
passed. The later package documentation addition changes no executable behavior.

Ignored evidence: `test-results/sdk-before-fix.log`, `sdk-generate.log`,
`sdk-proto-check.log`, `sdk-package.log`, `sdk-review-unit.log`,
`sdk-review-full-suite.log` and `sdk-godoc.txt`.

## Coverage gaps and next work

The optional exact-scope Opengrep invocation against the SDK did not scan:
the installed CLI rejected `--metrics` (exit 2; `sdk-opengrep.log`). This is
**no scan evidence**, not a clean security result. Staticcheck was unavailable;
no analyzer/dependency was installed just for this review. New usage relies on
standard Go and existing pinned Protobuf APIs; targeted pinned-source inspection
found no deprecation notice for the newly used Protobuf operations, but no general
deprecated-symbol analyzer result is claimed. Advisory databases were not refreshed
for this dependency-unchanged pass.

The root module requires Go 1.27.3. No public release/tag, remote module download,
SDK-only distribution archive or crash-recovery journal was validated. Examples
use in-memory requests; the guide explains what production persistence must add.
Normal server/storage behavior, isolation and limits remain unchanged. Earlier
hosted telemetry incompleteness and unmeasured cost/memory/global behavior remain
open evidence limits, not silently cleared by this local review.

Next arrange independent security review and vulnerability-report triage, then
complete final release-contract review, candidate validation and explicit OSS
publication approval. Tenant-wide quotas and SaaS launch work stay deferred.

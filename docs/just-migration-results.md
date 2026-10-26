# Just command migration — 2026-10-26

The root `justfile` replaces project shell/npm command wrappers. Just 1.59.0 is
installed and pinned by Hermit (`bin/.just-1.59.0.pkg`, `bin/just`). No globally
installed Just is needed. No deployment, push or coordination algorithm change.

## Changes

All seven project shell scripts were removed. Build, bootstrap, generation,
compatibility, checks, native tests, SDK tests, runtime tests, controlled hardening
and advisory checks now live in one `justfile`. The three npm command wrappers
were removed. The substantive OSV program moved unchanged from `scripts/` to
`tools/security-audit.mjs`. Hermit's generated launcher/activation scripts remain.

CI uses `bootstrap-core`, `check-core`, `test-core`, `bootstrap` and `test` recipes;
Wrangler's build hook uses `./bin/just build`. README, protocol-baseline guidance,
operational instructions and historical report reproduction commands now point
to Just. Original milestone transcripts are unchanged and identified as historical.
See [ADR-012](adr/ADR-012-just-command-interface.md).

## Validation

`./bin/just test` exited **0**. The [full transcript](just-test-output.txt) records
32 native core and 2 auth tests, 11 SDK top-level tests with Go's race detector,
normal local integration (34.821s), instrumented integration (39.829s), Buf
lint/format/breaking/generated-code checks, Rust formatting/Clippy, Just formatting,
Go vet and actual release Wasm builds. Normal Wasm was restored at completion.

`./bin/just audit` completed with zero npm vulnerabilities and zero OSV findings
across 115 public Rust/Go package versions. Advisory results are time-specific.

Additional checks passed:

- `./bin/just --version`: 1.59.0; `--list` and `--fmt --check` succeed.
- `./bin/just bootstrap`: existing pinned toolchains verified, npm dependencies
  reinstalled from the lockfile, pinned worker-build retained and Go modules ready.
- `./bin/just test-hardening '['`: intentionally failed with Go's invalid-regexp
  error and returned nonzero; the EXIT trap successfully rebuilt normal Wasm.
- Without another build, `TestDefaultBuildIgnoresTestController` ran directly
  against the restored artifact and passed under the race detector (3.502s).
- `generate` with a temporary output directory containing spaces produced Rust/Go
  files identical to the checked-in generated sources; temporary output was removed.
- From `tests/integration`, `../../bin/just test-sdk
  '^TestTTLValidationAndStablePreparedRequests$'` found the root justfile, ran in the
  correct directory and selected exactly the requested SDK test (1.195s).
- Documentation links and whitespace checked; no project scripts directory or npm
  script wrappers remain. The default config has no deployment action enabled.

The cleanup failure was deliberate verification of the command runner, not a
failing coordination test. These supplemental checks and the advisory audit also
completed after migration; no remote CI execution is claimed.

## Boundaries

No remote CI, hosted staging, deployment or push. Shared normal/instrumented build
artifacts require serial workflows within one checkout; separate simultaneous
invocations are not locked. Cleanup cannot run after SIGKILL or machine failure.
Just does not change sandbox permissions or guarantee offline bootstrap/builds.
P3's remaining hosted and release gates remain open.

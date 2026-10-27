# Staging preparation evidence

2026-10-27, following baseline commit `74cdb66`. No deployment, remote secret
write, hosted smoke run, commit or push performed during this preparation.

The later authorized deployment and real hosted run are recorded separately in
[staging results](staging-results.md); the observations below describe preparation.

## Read-only Cloudflare verification

- Pinned Wrangler 4.152.0 `whoami --json`: logged in through OAuth; exactly one
  accessible account. Workers write/script permissions were present. Tokens were
  not printed or copied into repository configuration.
- `just staging-account` with the selected account ID: passed membership check.
  Account-specific identifiers remain outside tracked files.
- `wrangler deployments list --config wrangler.staging.jsonc` for that account:
  Cloudflare returned **10007, Worker does not exist**. This is expected evidence
  that `pintle-staging` is unoccupied, not a successful deployment test.
- API reads required access outside the local-only sandbox. Local builds/tests
  use the existing scoped permissions. Workers billing tier, quota availability
  and workers.dev subdomain were not independently verified.

## Local checks

- `just staging-dry-run`: passed with the normal Rust/Wasm build; **551.54 KiB
  total / 176.18 KiB gzip**. Wrangler reported the intended SQLite class binding,
  P2 mode, static auth and metrics, then exited without uploading.
- Initial shared smoke execution against local Wrangler passed auth, concurrent
  acquisitions/duplicates, pending cancellation/handoff, response loss and expiry.
  Its stale-owner assertion incorrectly expected failed_precondition; the existing
  contract returns permission_denied. The expectation was corrected without any
  production code change.
- Final `./bin/just test`: **passed**, exit 0. Buf/codegen compatibility,
  Rust/Just formatting, Clippy and Go vet passed; **25 core + 2 auth tests** passed.
  SDK race tests: **1.391s**; hosted-runner guard tests: **1.388s** (remote entry
  explicitly skipped); normal Wrangler integration: **44.596s**; instrumented
  integration: **85.738s**. Normal Wasm was restored.
- The shared hosted smoke scenarios passed locally with **46 transport attempts**
  in **9.480s** (10.17s including runtime setup). This is local runtime evidence,
  not a hosted latency measurement.
- Final packaging dry run passed again after the full suite. Missing hosted
  settings and an invalid account ID were rejected before remote execution.
  `git diff --check` and local documentation-link checks passed.

Ignored transcripts: `test-results/staging-preparation-full-suite.log`,
`staging-preparation-dry-run.log`, `staging-smoke-local.log` (initial expectation
failure), and `staging-inventory.log`. There is no hosted test transcript because
no hosted test was run. Online advisory checks were not refreshed in this task.

The remote runner is explicitly opt-in, bounded to 96 transport attempts and five
unique authorized resources. Negative endpoint/token and budget guards run locally.
No new dependency or coordination-core change was introduced. See
[staging setup and limitations](staging.md) for commands, credential handling,
persistent test metadata and the remaining hosted evidence gates.

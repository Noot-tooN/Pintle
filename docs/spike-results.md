# P0 — Rust / Durable Objects / SQLite feasibility results

> Historical milestone evidence: the P0/P0b runtime fixture and its commands
> have since been retired. Useful coverage now runs against the current mutex;
> see [test consolidation](test-consolidation-results.md). Captured results below
> describe the implementation at that milestone, not current runtime controls.


> Tooling update: command names below now use the root `justfile`. The milestone
> runs originally used equivalent shell scripts; their captured transcripts are
> unchanged. See [Just migration results](just-migration-results.md) for a full
> suite run through Just.

**Hardening follow-up:** [hardening-results.md](hardening-results.md) records
187,240 native model trace prefixes, 108 controlled concurrency schedules,
logical deadline/timer races, expanded faults, and 148 checked runtime histories.
The full P0/P0b regression suite passed again. The original milestone reports
below are historical. P1 now passes locally: see [p1-results.md](p1-results.md)
for the lease core, SDK, deterministic expiry/Renew/Release tests, and limits.

**P1.1 follow-up (2026-10-26):** [p11-results.md](p11-results.md) records bounded
recovery sessions replacing the lifetime receipt cap, explicit ACKs, actual DO
alarm reclamation, rollback/restart, and measured SQLite reuse. Core logic remains
independent of Cloudflare; all earlier milestone suites passed again.

**P2 follow-up (2026-10-26):** [p2-results.md](p2-results.md) records server-owned
FIFO Acquire, atomic cancellation, actual alarm wakeups, no-lost-wakeup tests,
rollback/restart, bounded waits, and a two-process SDK handoff. Earlier milestone
reports remain historical; production guarantees are still unverified.

**Result: P0 acceptance checks PASS locally.** Tested 2026-10-25 on macOS arm64.
This is evidence for the compiled adapter and the listed schedules, not a proof
of production distributed coordination. No Cloudflare deployment, credentials,
repository push, or full mutex implementation was performed.

**P0b follow-up (2026-10-25):** unary binary Protobuf/Connect now passes through
the compiled Worker and DO with a generated Go client. See
[p0b-results.md](p0b-results.md) and [p0b-test-output.txt](p0b-test-output.txt).
The full P0 regression suite also passed again. The original report below records
P0's scope at completion; its P0b “NOT RUN” and next-step notes are historical.
The current `just test` additionally runs code-generation/compatibility
checks and the P0b scenarios.

## Reproduce

From the repository root on macOS or Linux, with Git, Bash, a native linker,
and initial download access:

```sh
./bin/just bootstrap
./bin/just test
./bin/npm audit --audit-level=moderate
```

Bootstrap uses the checked-in Hermit launcher, installs exact Node/Go/Rustup
packages, then runs:

```sh
rustup toolchain install 1.99.1 --no-self-update --profile minimal --component rustfmt --component clippy --target wasm32-unknown-unknown
npm ci --ignore-scripts
cargo install worker-build --version 0.8.7 --locked
```

Those commands run inside the Hermit environment. `--no-self-update` keeps Rustup
managed by Hermit. `worker-build` has no upstream Hermit package, so its exact
Cargo release is installed into Hermit's project-local `CARGO_HOME`; there is no
global Cargo install. The build tool downloads its own pinned helper binaries
into the OS `worker-build` cache (on macOS, `~/Library/Caches/worker-build`).
Dependency resolution is captured in `Cargo.lock` and `package-lock.json`.

`just test` actually executes:

```sh
cargo fmt --all -- --check
cargo test --locked -p pintle-core
cargo clippy --locked -p pintle-core --all-targets -- -D warnings
cargo clippy --locked -p pintle-cloudflare --target wasm32-unknown-unknown -- -D warnings
# Inside crates/cloudflare:
worker-build --release --locked
# Back at repository root:
go test -count=1 -v ./tests/integration
```

The Go harness derives its configuration from `wrangler.jsonc`, retaining the
SQLite DO binding and migration, using an absolute path to the built Wasm shim,
and removing only the already-executed build hook. Each runtime uses:

```text
bin/node node_modules/wrangler/bin/wrangler.js dev
  --config <run>/wrangler.json --local --ip 127.0.0.1
  --port <free-port> --inspector-port 0 --persist-to <run>/state
  --log-level info [--var LOCAL_SPIKE:true]
```

The first activation deliberately omits the variable and must reject requests.
The harness stores logs, client responses, and persistent SQLite files under
ignored `test-results/p0-*`; it kills only process groups it starts. It uses
fresh state per run and `-count=1`, so neither previous data nor Go test caching
can supply a passing result. Wrangler telemetry is disabled.

## Tested versions

| Component | Version / source of pin |
| --- | --- |
| Hermit | Active stable executable updated from 0.52.3 to **0.54.1**, latest GitHub release at setup |
| Rustup | **1.29.2**, Hermit package |
| Rust / Cargo | **1.99.1**, `rust-toolchain.toml`; `wasm32-unknown-unknown`, rustfmt, Clippy |
| Node / bundled npm | **24.22.1 / 11.19.1**, Hermit package |
| Go | **1.27.3**, Hermit package; standard-library HTTP test clients, no SDK |
| worker / worker-sys / worker-macros | **0.8.7**, Cargo lockfile |
| worker-build | **0.8.7**, exact Cargo install with upstream lockfile |
| wasm-bindgen / js-sys | **0.2.129 / 0.3.106**, Cargo pins |
| wasm-opt / build esbuild | **132 / 0.28.2**, worker-build's pinned helpers |
| Wrangler | **4.152.0**, exact npm dependency |
| Miniflare | **5.20261019.2-beta**, Wrangler's locked dependency (upstream beta label retained) |
| workerd | **1.20261019.1**, npm lockfile |
| Undici | **7.30.1**, explicit compatible-major override |
| Compatibility date | **2026-10-24**, Wrangler config |

The original repository had only `.gitignore` and `LICENSE`. The canonical
planning documents were copied from the supplied folder without changing their
accepted decisions or the existing license. No CodeGraph server was available;
no index was initialized.

## Acceptance evidence

| Check | Executed evidence | Result |
| --- | --- | --- |
| Platform-independent domain | Native `cargo test -p pintle-core`; four tests cover contention/replay/conflicts, boundaries/overflow, historical replay, malformed state/requests. Core dependencies are serde/serde_json, with no Cloudflare or Wasm imports. | PASS |
| Actual Rust/Wasm DO | Release Wasm built by worker-build and executed by local Wrangler/workerd using `new_sqlite_classes`. | PASS |
| Multi-record atomic commit | SQL `records` contains separate owner, fence, revision, request-dedupe rows. Tests compare all four persisted records to the acknowledged grant. | PASS |
| SQL failure rollback | Real duplicate-primary-key INSERT after each of writes 1, 2, 3, and 4; assert the UNIQUE error, exact pre/post snapshots, and fresh successful retry of the same ID. Also fail the first grant in a pristine authority: no rows remain. | PASS |
| Rust callback error rollback | Return Rust `Err` after each of writes 1–4; assert the injected error and unchanged snapshot. | PASS |
| Concurrent distinct requests | Three availability rounds, each with 12 separate Go client processes released by a parent barrier: one fresh winner, 11 contended outcomes, tokens 1, 2, 3. | PASS |
| Concurrent duplicate requests | 24 concurrent same-ID requests: one fresh grant, 23 replays, identical token; different owner payload is explicitly rejected without mutations. | PASS |
| Lost response after commit | Local HTTP proxy consumes a successful upstream response, then closes the client connection without returning it. Same-ID retry recovers the exact original grant; no new token. | PASS |
| Precision | Sequential grants from fence 9007199254740990 through 9007199254740995, with revisions independently exceeding 2^53; exact decimal strings asserted. JSON numeric seed input is rejected. | PASS |
| Overflow | Last legal grant reaches 18446744073709551615 (`u64::MAX`); subsequent grant is exhausted without changes. Independently exhausted revision also prevents a grant. | PASS |
| Persistence/repeated activation | SIGKILL the Wrangler process group (including local workerd), restart twice with the same persistence directory, compare snapshots and same-ID replay. Next grant is 9007199254740996. Empty rolled-back authority and max-u64 history also survive. | PASS |
| Historical dedupe | Replaying an old request after a successor exists returns the original record without changing current ownership or counters. | PASS |
| Routing isolation | Same key in two local test identities gets separate authorities; ambiguous delimiter combinations in namespace/key do not collide; unknown identity rejected. | PASS for local routing only |
| Disabled fixture | Missing `LOCAL_SPIKE` rejects both health and grant requests with 403. | PASS |
| Formatting/lint | rustfmt and native/Wasm Clippy with warnings denied. | PASS |
| Dependency audit | `npm audit --audit-level=moderate`: zero vulnerabilities in final locked graph. | PASS at test time |

See [p0-test-output.txt](p0-test-output.txt) for the actual test transcript (only
the absolute checkout path is normalized). Seven integration scenario groups
passed; the standalone `TestClientProcess` reports SKIP in the parent process
because it is a helper. It is actually run in all 36 child processes.

## Transaction path and what the experiment establishes

`worker::State::storage()` supplies `worker::Storage`.
`Storage::sql().exec()` performs synchronous SQL. Published workers-rs 0.8.7
exposes `Storage::as_raw()` and async `Storage::transaction()`, but neither its
high-level wrapper nor worker-sys exposes `transactionSync`.

The adapter adds one structural wasm-bindgen binding to the platform method.
It passes a scoped `Closure::once_assert_unwind_safe` whose return type is
`Result<JsValue, JsValue>`. All conditional reads, the core grant decision, and
four writes execute in that synchronous callback. `Err` becomes a thrown JS
exception, and the platform rolls back. The outer binding catches it for the
fixture's HTTP 500 response. Returning an error object as a successful value
would not roll back; the SQL and Rust-error tests explicitly verify the chosen
path. The closure is dropped after the synchronous call on success or failure.

The operation checks dedupe before ownership. A matching committed request
replays without writes; a conflicting payload rejects. A new request can write
only when owner is absent. No event-loop yield occurs between checking the
predicate and writing. Serialization/SQLite atomicity is confined to one DO;
there is no independent read/await/write sequence. The acknowledged operation
awaits `Storage::sync()` after transaction success. A sync failure returns 503
with instructions to retry the **same** ID, since it may already have committed.
That sync-failure branch itself was not injected/tested.

The domain owns `try_grant` over a generic synchronous `Transaction { get, put }`
view; the adapter supplies SQL and the transaction boundary. This is an exercised
conditional transition, not a frozen generalized snapshot/batch/watch API. Its
atomicity comes from the adapter, not from the Rust function alone.

The grant linearization candidate is the object-local atomic storage commit.
Successful local histories match this model. Testing cannot prove all possible
schedules, hosted durability/failover, or arbitrary failures during commit.

## Exact integer choice

Fence and revision use checked `u64` arithmetic only in Rust. SQL bindings,
SQLite TEXT values, stored grant JSON, JS strings, and HTTP JSON fields all carry
canonical decimal strings. STRICT TEXT columns avoid numeric-affinity conversion.
No `as f64`, SQL arithmetic, `SqlStorageValue::Integer`, or JS `Number` participates
in counter generation. Parsing rejects signs, leading zeros, fractions, invalid
values, and values beyond u64. At exhaustion there are no writes and no wrap.
The HTTP tests use Go strings/uint64, not float-based JSON extraction.

## Deliberate P0 limits and remaining work

- **Not a mutex or lease.** Owner is a fixture label, not an ownership capability.
  There is no clock, TTL, expiration, renewal, authorized release, waiting,
  cancellation, fairness, notification, or downstream fencing integration.
- Test-only `vacate` removes owner and advances revision, preserving fence and
  dedupe. It is how tests create availability intervals; it proves no production
  Release semantics. `seed` works only on pristine objects. No authority reset,
  migration, or restore protocol exists.
- Dedupe is retained for the lifetime of local test storage, with no GC or bound.
  Only committed grants reserve IDs. Contended requests do not create durable
  request records and can retry later. Historical replay is a receipt for an old
  grant, **not evidence of present ownership**. P1 must design status/retention.
- `/spike` uses disposable JSON test plumbing, not a permanent protocol. HTTP
  500 injected errors and outcome strings are not Connect error semantics.
  Protobuf/Buf/Connect interoperability is **NOT RUN (P0b)**. The small Go HTTP
  clients satisfy the practical roundtrip test without introducing an SDK.
- `local-a`/`local-b` are explicitly insecure identity fixtures. Payload size is
  checked after buffering; streaming limits and real authentication belong to
  P0b. The default config disables fixture operations and workers.dev routes,
  but deliberately enabling `LOCAL_SPIKE` on a deployed Worker would be unsafe.
- Restart tests kill processes **after acknowledged operations**, not at a
  randomized instruction inside a commit. Response loss is a controlled local
  proxy fault. Rust panic/trap/OOM, filesystem loss/corruption, true unknown
  durability outcome, and cloud object replacement were **NOT TESTED**.
- **Live staging/production: NOT RUN**, by scope. No WAN latency, platform
  failover, partition, replication, hibernation, alarms, or restored fencing
  safety claims. Local process restart preserves the same local SQLite volume.
- No cross-object transactions and no tested second backend. Portable domain
  code does not mean the Cloudflare adapter runs on another cloud.
- Linux CI jobs `core` and `do-local` are configured but **NOT RUN remotely**;
  changes have not been pushed. This session's runtime evidence is macOS arm64.

## Setup findings and corrections

- Updated the existing Hermit stable cache executable to 0.54.1 using its
  official installer; retained the existing global launcher. The Homebrew
  package receipt is separate from that cached executable.
- Hermit activation follows `$SHELL`, which was Fish even inside Bash/Zsh tool
  sessions. Project Bash scripts explicitly set `SHELL=/bin/bash` before
  activation. Rustup, Cargo, Go build cache, and Wrangler config now live under
  `.hermit`; initially the sandbox rejected host-cache writes. These were setup
  failures, not atomicity failures.
- Rustup's install command initially attempted self-update in Cargo home.
  `--no-self-update` fixes it under Hermit's Rustup installation.
- Wrangler's locked default Undici 7.x version initially yielded three audit
  findings (two moderate, one high including transitive reports). An explicit
  `undici: 7.30.1` override removed them; the real integration suite passed on
  that graph. Revisit/remove the override when upstream carries the fix.
- npm installation uses `--ignore-scripts`; supported-platform optional native
  binaries for workerd/esbuild work without lifecycle scripts in this test.
- Existing architecture was retained. The concrete synchronous transaction view
  and integer choice are recorded in [ADR-005](adr/ADR-005-p0-transaction-interop.md);
  ADR-004's general storage interface remains provisional.

## Recommendation

Proceed next to **P0b**, the separately scoped unary Protobuf/Connect Worker
Fetch → DO interoperability spike with a generated Go client, standard error
mapping, deadlines, and authenticated/bounded resource identity. Preserve this
atomicity suite as regression coverage. Only then proceed to P1's real
nonblocking mutex lifecycle. Do not infer a production lock guarantee from P0.

## Primary sources inspected

- [Cloudflare SQLite storage API](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/):
  transactionSync rollback/synchronous callback, sync, precision limits, and
  prohibition on SQL BEGIN/SAVEPOINT.
- [workers-rs 0.8.7 durable.rs](https://github.com/cloudflare/workers-rs/blob/v0.8.7/worker/src/durable.rs)
  and [worker-sys storage binding](https://github.com/cloudflare/workers-rs/blob/v0.8.7/worker-sys/src/types/durable_object/storage.rs):
  actual published methods, verified against downloaded crate source/build.
- [workers-rs 0.8.7 SQL bindings](https://github.com/cloudflare/workers-rs/blob/v0.8.7/worker/src/sql.rs):
  integer-to-f64 conversion and string alternative.
- [Cloudflare Rust runtime docs](https://developers.cloudflare.com/workers/languages/rust/)
  and [worker-build 0.8.7](https://github.com/cloudflare/workers-rs/tree/v0.8.7/worker-build).
- [Hermit setup](https://cashapp.github.io/hermit/usage/get-started/)
  and [Hermit v0.54.1 release](https://github.com/cashapp/hermit/releases/tag/v0.54.1).

## P3 follow-up

[P3 local hardening](p3-results.md) extends deterministic tests, SDK lease maintenance,
credential policy, transport validation, metrics and local performance evidence.
Hosted staging and public release gates remain open; no deployment was performed.

# Pintle

An open-source distributed coordination platform with a portable Rust core.

**Current status: P3 local hardening passes; hosted release gates remain open.** The portable Rust core implements
server-owned FIFO Acquire, TryAcquire, Renew, Release, cancellation, and request-status
recovery. A generated Go Connect client and [Go SDK](sdk/go/pintle/client.go) call the compiled Rust/Wasm
Worker and Durable Object backed by SQLite. The schema is unpublished.

Wait registration, cancellation/grant races, lease expiry, renewal/release races,
replay, rollback, and restart are exercised with controlled logical time and transaction/response ordering. See
[P3 results](docs/p3-results.md) for current evidence and limits. Resource-scoped
recovery sessions replace the 1,024 lifetime receipt cap with bounded reusable
capacity. Explicit acknowledgements reclaim results; Durable Object alarms reclaim
expired sessions while retaining fencing history. Hosted clock/failover validation
remains open. This is not production-ready.

## Run locally

macOS or Linux, with Git, a native linker/build toolchain, Bash, and network access
for initial dependency downloads:

```sh
./bin/just bootstrap
./bin/just test
```

The checked-in `bin/hermit` launcher bootstraps Hermit. Hermit pins Just 1.59.0 and manages Node, Go,
Rustup, Buf, and the Go Protobuf/Connect generators; `rust-toolchain.toml` pins
Rust and its Wasm target. Bootstrap installs
pinned `worker-build` into Hermit's project-local Cargo home and Wrangler from
`package-lock.json`. No globally installed Just, Rust, Node, Go, or Wrangler is needed.

The root [`justfile`](justfile) is the single command interface, also used by CI
and Wrangler's build hook. See [migration evidence](docs/just-migration-results.md).
Run `./bin/just` or `./bin/just --list` to discover tasks.
After `source ./bin/activate-hermit`, use `just` directly from any subdirectory.

| Command | Purpose |
| --- | --- |
| `./bin/just bootstrap` | Install pinned tools and dependencies |
| `./bin/just check` | Formatting, lint, codegen compatibility and Go vet |
| `./bin/just test-core` | Native Rust core/auth tests |
| `./bin/just test-sdk` | Go SDK unit tests with the race detector |
| `./bin/just test-runtime` | Build normal Wasm and test real local Wrangler |
| `./bin/just test-hardening` | Instrumented deterministic tests; restore normal Wasm |
| `./bin/just test` | Complete local suite, sequentially |
| `./bin/just generate` | Regenerate Rust/Go Protobuf code |
| `./bin/just audit` | Online npm/OSV advisory checks |

Focused test recipes accept an optional filter, e.g.
`./bin/just test-sdk TestMaintenance` or `./bin/just test-core independent_two_waiter`.
`./bin/just generate '/tmp/pintle generated'` supports an alternate output directory.
Run only one Wasm build/runtime/hardening workflow per checkout at a time: they
share build artifacts. Just sequences the complete suite; it does not isolate
separately launched invocations. Existing cache/localhost sandbox permissions
still apply. For Codex on this Mac, the project defines a `pintle-local` profile
for the two tool caches and loopback tests; see [setup and activation](docs/local-test-permissions.md).
External downloads/audits remain separately authorized.

`just test` checks Buf lint/breaking changes and generated-code drift, then runs
native core tests, formatting, Clippy, a real release Wasm build, and Go HTTP and
generated Connect client and SDK tests against local Wrangler/Miniflare/workerd. It starts and kills
its own runtime processes; logs and persistent SQLite state are retained under
ignored `test-results/protocol-*`, `test-results/p1-*`, and
`test-results/p3-*` directories. No Cloudflare
account is needed. Protocol tests create random local credentials and delete their
temporary `.dev.vars` file during cleanup.

The full suite also runs `just test-hardening`, which compiles an explicit
`test-hooks` Wasm feature and uses a local controller to choose execution order
and logical time. Go tests run with `-race`. The recipe restores normal Wasm on
exit. Hardening traces are retained under ignored `test-results/p1-*` directories.

Regenerate the checked-in Rust types, Go messages, and Go client with
`./bin/just generate`; validate them with `./bin/just check-proto`.
The schema is in [`proto/pintle/v1/lock.proto`](proto/pintle/v1/lock.proto), and the
generated Go client is under [`gen/go/pintle/v1`](gen/go/pintle/v1).

The obsolete P0 JSON fixture and P0b grant implementation have been removed.
Their useful checks now exercise the lease/session implementation. There are no
HTTP seed, vacate or snapshot controls. Private tests inspect SQLite at controlled
boundaries and seed counters only while the local runtime is stopped. See the
[coverage map and results](docs/test-consolidation-results.md).

The P2 integration suite uses `P2_ENABLED=true` and ephemeral authenticated
test identities and explicit `PINTLE_AUTH_MODE=test`. Omitted auth mode defaults to
static credentials. P1/P1.1/P2 modes cannot be mixed; P1/P1.1 remain focused lease/session regression modes.
See [P2 SDK usage](docs/p2-results.md#sdk-usage-and-recovery). Acquire keeps durable
server intent across bounded HTTP responses and disconnects; CancelPending must
reconcile a grant that may have won the race. Open a session, prepare each request
once, and preserve its full payload/session/sequence for retries. Explicitly ACK
only after safely consuming the results; ACK is separate from Release. Distinguish
historical Receipt from Current/State; an old grant is not current ownership.

Run `./bin/just audit` separately for online npm/OSV advisory checks.
See [deployment preparation](docs/deployment.md), [security review](docs/security-review.md),
and the [SDK maintenance contract](docs/p3-results.md#sdk-maintenance-contract).
For incident handling, see the [recovery policy and runbooks](docs/recovery-policy.md).

## Architecture and evidence

- [`docs/p3-results.md`](docs/p3-results.md): deterministic hardening, renewal/Lost,
  static credential policy, metrics, local latency and remaining release gates.
- [`docs/adr/ADR-011-p3-local-hardening.md`](docs/adr/ADR-011-p3-local-hardening.md):
  policy boundaries and implementation decisions.

- [`docs/p2-results.md`](docs/p2-results.md): durable FIFO waiting, cancellation,
  alarms, two-process SDK handoff, fault tests, and operational limits.
- [`docs/adr/ADR-010-server-owned-bounded-acquire.md`](docs/adr/ADR-010-server-owned-bounded-acquire.md):
  atomic queue semantics, bounded unary resumption, and notification design.

- [`docs/p11-results.md`](docs/p11-results.md): bounded recovery sessions, automatic
  cleanup, storage measurements, and full-suite evidence.
- [`docs/adr/ADR-009-bounded-recovery-sessions.md`](docs/adr/ADR-009-bounded-recovery-sessions.md):
  acknowledgement, expiry, capacity, storage layout, and compatibility decisions.

- [`docs/p1-results.md`](docs/p1-results.md): lease contract, Go SDK, deterministic
  coverage, actual full-suite output, and remaining limitations.
- [`docs/adr/ADR-008-p1-lease-lifecycle.md`](docs/adr/ADR-008-p1-lease-lifecycle.md):
  ownership capabilities, clock model, recovery, and bounded receipt retention.

- [`docs/spike-results.md`](docs/spike-results.md): acceptance matrix, actual
  commands/results, implementation choices, and unverified semantics.
- [`docs/p0b-results.md`](docs/p0b-results.md): generated-client interoperability,
  authentication, deadlines, wire limits, and supported Connect subset.
- [`docs/hardening-results.md`](docs/hardening-results.md): deterministic schedules,
  reference model, history checks, fault matrix, and the replay-validation fix.
- [`PROJECT_SPEC.md`](PROJECT_SPEC.md): canonical design and guarantee boundaries.
- [`docs/IMPLEMENTATION_PLAN.md`](docs/IMPLEMENTATION_PLAN.md): ordered milestones.
- [`docs/GUARANTEES.md`](docs/GUARANTEES.md): local evidence versus unverified production targets.
- [`docs/adr/ADR-005-p0-transaction-interop.md`](docs/adr/ADR-005-p0-transaction-interop.md):
  scoped transaction and integer decisions.
- [`AGENTS.md`](AGENTS.md) and [`START_HERE_AGENT_PROMPT.md`](START_HERE_AGENT_PROMPT.md):
  implementation instructions.

The next step is to complete **P3 staging and release gates** under separate
explicit authorization, before P4 helpers. Local hardening includes lease-loss
behavior, static credential rotation/expiry, sanitized metrics, and expanded tests;
hosted clock/failover, WAN/cost/operations and independent review remain open.
Existing licensing is preserved in [`LICENSE`](LICENSE).

# P0/P0b deterministic hardening results

> Tooling update: command names below now use the root `justfile`. The milestone
> runs originally used equivalent shell scripts; their captured transcripts are
> unchanged. See [Just migration results](just-migration-results.md) for a full
> suite run through Just.

Current follow-up: [P1 results](p1-results.md) adds the nonblocking lease lifecycle
and Go SDK. This document preserves the earlier milestone evidence.

**PASS locally on macOS arm64, 2026-10-25.** The full suite built and executed
normal and instrumented Rust/Wasm under local Wrangler/Miniflare/workerd, with
Go's race detector enabled. No deployment, push, or P1 implementation occurred.
No new dependencies were needed; all tools used the existing Hermit environment
and pinned Cargo/npm/Go dependency graphs.

## Reproduce

```sh
./bin/just bootstrap  # Existing pinned toolchain setup, if needed
./bin/just test
```

Executed in this hardening session: `./bin/just test` (exit 0), including Buf
lint/format/breaking checks and regeneration comparison, Rust formatting, native
core tests, native/Wasm Clippy with warnings denied, release Wasm builds, the
normal P0/P0b integration suite, and the instrumented hardening suite. Bootstrap
was already completed in P0b; no dependency installation was needed here.

The full transcript is [hardening-test-output.txt](hardening-test-output.txt).
Only the absolute checkout path is normalized. The final normal Go integration
run took 11.001 seconds and the instrumented run 5.247 seconds; these are test
durations, not latency benchmarks. Remote Linux CI has **NOT RUN** because nothing
was pushed. Its existing `just test` job includes the new checks.

For the instrumented tests alone:

```sh
./bin/just test-hardening
```

This script builds with `--features test-hooks`, runs Go with `-race -tags=testhooks`,
and restores the default Wasm build on exit, including when tests fail. The full
suite first tests the ordinary artifact without hooks. Raw runtime logs, SQLite
state, checkpoint events, and operation histories are retained under ignored
`test-results/hardening-*`. Local test credential files are removed on normal
cleanup. Histories and checkpoint events contain no bearer tokens.

## Coverage and evidence

| Layer / scenario | Executed coverage | Result |
| --- | --- | --- |
| Native reference model | All 187,240 trace prefixes of lengths 1–5 over eight events and five counter starting states; compare outcomes and every persisted record after each event. | PASS |
| Core boundaries and faults | Fail each of four reads and four writes, retry same ID; request/owner length boundaries; malformed fence/revision representations; malformed dedupe receipt rejection. | PASS |
| Controlled concurrency | Three simultaneously admitted handlers; every one of six transaction-admission orders × six observed response orders × distinct IDs / duplicate IDs / conflicting holders = 108 schedules. | PASS |
| Logical deadline boundaries | Immediately before, exactly at, and immediately after the deadline at gateway dispatch, authority commit admission, authority response, and gateway response = 12 cases. | PASS |
| Timer versus work race | Explicitly release timer or work first, while paused before commit or after sync = four cases. Same-ID retry distinguishes absent versus committed grant. | PASS |
| Failure boundaries | Inject unavailable before dispatch, before commit, after commit, after sync, before response, and at gateway response = six cases. | PASS |
| Real adapter read faults | Fail dedupe, owner, fence, and revision reads; state remains empty; same ID then grants freshly. | PASS |
| Real SQLite rollback | SQL UNIQUE errors and Rust callback errors after each of four writes, through the generated Connect client = eight cases. | PASS |
| ID reuse after abort | Abort after the last write, allow another holder to use that ID, then reject the original holder's retry as conflicting. | PASS |
| Historical replay | Grant, fixture vacate, successor grant, original replay: successor state remains unchanged and history is legal. | PASS |
| Cancellation / restart | Cancel or kill runtime at acknowledged precommit and post-sync checkpoints = four cases; fresh versus replay verified. | PASS |
| History verification | All 148 runtime histories checked against an independent sequential model, respecting completed-before-invoked order. Unknown results allow either commit or no effect. | PASS |
| Checker negative controls | Nine fixtures include valid overlapping/ambiguous histories and reject double fresh grants, invented tokens/revisions, replay without an original, principal mismatch, and impossible real-time ordering. | PASS |
| Default-build isolation | Configure a poison controller and send its test header to normal Wasm: ordinary grant succeeds and controller receives zero calls. | PASS |
| Public input boundaries | Seventeen field-boundary cases plus valid Protobuf bodies of 8191/8192/8193 bytes with known length and chunked encoding; reject over-limit without reserving the ID. | PASS |
| Regression and tooling | Eight native tests total; ten P0b groups; all seven original P0 groups, including multiprocess stress, actual response loss, precision, and restart; Go `-race`; both Wasm builds and Clippy variants. | PASS |

The native event alphabet is: grant A, conflicting holder with A's ID, grant B,
fixture vacate, serialize/reload, aborted candidate, lost response plus retry,
and unknown acknowledgement plus retry. Initial `(fence, revision)` pairs are
`(0,0)`, `(2^53-1,2^53)`, `(u64::MAX-1,u64::MAX-1)`, `(u64::MAX,0)`, and
`(0,u64::MAX)`. The model uses numeric counters and an abstract receipt map; it
does not call the implementation to decide expected outcomes. Failure messages
include the deterministic event trace. This is bounded exploration, not random
fuzzing or a claim of exhaustive unbounded verification.

## What is deterministic now

The three-request tests wait until **all three handlers have reached the actual
DO's precommit checkpoint**. The harness releases one transaction at a time and
waits for its post-sync acknowledgement before releasing the next. Separately,
it releases each response and observes completion before releasing the next
response. Both orders are chosen explicitly; neither depends on sleeps or which
goroutine happens to win a race.

The adapter's transaction stays synchronous throughout. The harness cannot
interleave another handler halfway through its conditional reads/writes. Real
SQLite rollback tests cover faults within that atomic scope.

Logical time starts at 1000ms for each scheduled request. The server's capped
deadline is 11000ms; boundary cases supply 10999, 11000, or 11001 at a chosen
checkpoint. A separately armed timeout checkpoint controls completion of the
timer future used by the gateway's work-versus-timer selection. The old
150ms-delay/200ms-sleep tests and adapter delay knobs were removed.

The instrumented suite contains no sleep-based ordering. Watchdog timeouts only
fail stuck tests. Existing process/goroutine barrier stress tests and the
explicitly named `real_timer_stalled_upload_smoke` remain as supplemental runtime
checks; they are not presented as deterministic schedule exploration.

## History checker and trace interpretation

Each operation records unique logical invocation and completion event numbers,
request ID, holder, outcome, exact numeric fence/revision, and replay flag. The
checker searches serial orders compatible with real-time precedence. For an
unavailable/deadline/cancelled/unknown response, it considers both no effect and
the model transition. It never treats every error as rollback. Known successful
responses must match the model exactly.

The checker is bounded to at most twelve operations for one resource, starting
empty. Actual scheduled histories contain two to four operations. The native
model separately covers nonzero counter starts and exhaustion. A small set of
recorded examples is in [hardening-history-examples.json](hardening-history-examples.json);
the complete events and histories are in each run's `histories.json`.

Cancellation by itself does not establish that no grant occurred. In the
precommit cancellation case the harness also advances logical time to the
deadline before releasing the transaction; post-sync cancellation must preserve
the committed receipt. Timer-race tests follow the same conservative recovery
rule. Runtime restart tests kill the Wrangler process group at an acknowledged
checkpoint and reuse its local SQLite directory.

## Defect found and fixed

The new `malformed_dedupe_never_returns_a_successful_receipt` test initially
failed: a parseable persisted grant with a noncanonical or zero counter could
be returned as a successful replay. The core now validates persisted receipt
counters, embedded request ID, and owner length before returning a receipt.
Malformed receipts produce an error without mutation. The test failed before
the correction and passed afterward, together with the exhaustive model and
full real-runtime suite. This does not claim arbitrary storage corruption can
be detected or repaired.

The testing architecture and this scoped correction are recorded in
[ADR-007](adr/ADR-007-deterministic-hardening.md).

## Remaining limits before production

- Test-controller awaits can affect runtime input/output gates. They force useful
  boundary schedules but cannot explore workerd/SQLite internals. The normal
  uninstrumented regression suite remains necessary.
- A failure injected after real `Storage::sync()` exercises the caller's
  acknowledgement-error handling. It is **not** a real disk/replication failure.
  Post-commit controller I/O may itself flush an output gate. Crashing midway
  through an unacknowledged platform commit remains **NOT TESTED**.
- Native restart/rollback simulation is not evidence for SQLite durability;
  actual local runtime tests provide separate, limited evidence. Hosted HA,
  partitions, platform object replacement, and backup restoration are **NOT RUN**.
- Logical request deadlines do not establish a safe lease clock under drift,
  clock regression, suspension, or region changes. No TTL, Renew, Release,
  waiting, request-retention policy, or external fencing integration was added.
- Full Connect conformance and arbitrary malformed-wire fuzzing remain future
  work. Boundary tests are enumerated, not every possible input byte string.

**Next: P1**, retaining these gates while adding an explicit lease/clock and
ownership model. Extend the model and deterministic scenarios alongside each
new transition. No production “bulletproof” guarantee is inferred from passing
this finite local suite.

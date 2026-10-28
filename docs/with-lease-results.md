# WithLease development results

Date: 2026-10-28. Development-only SDK change; no deployment, formal publication,
new dependency, wire/schema change or Rust coordination change in this increment.
The accompanying upstream-coverage additions are documented separately.

## Implemented contract

`Client.WithLease` acquires through server-owned Acquire, waits for initial
Renew/ACK, runs a callback once, renews automatically, cancels its work context on
parent cancellation or ownership uncertainty, and attempts bounded Release/ACK.
Fencing tokens remain exact strings and optional for the application. See
[ADR-020](adr/ADR-020-with-lease-callback.md) and the [SDK guide](../sdk/go/pintle/README.md).

Cancellation/grant races reconcile the same prepared request. Error values retain
uncertain request identities. Cleanup never ACKs past unresolved renewal/release
outcomes. Callback errors and cleanup failures are both preserved. Callback panics
propagate after cleanup. A 16-entry idle session cache prevents sequential callbacks
from allocating a new server session each time; leases are never cached/reused.

## Deterministic coverage

- SDK protocol doubles plus channel barriers and injected renewal ticks/elapsed
  time: initial Renew/ACK gate, multiple renewals beyond the 32-position window,
  token precision above 2^53, success/error/panic/parent cancellation, loss while
  work runs, callback completion during in-flight Renew, exact initial renewal
  budget boundary, lost ACK/Release, inactive grant, cancelled/expired acquisition,
  committed-grant cancellation reconciliation and unresolved cancellation.
- No voluntary Release while a cancelled callback is still cleaning up; no
  automatic callback retry/reacquisition. End-of-call work context cancellation,
  private error formatting, bounded/exclusive session reuse, idle session expiry
  and failed heartbeat behavior are asserted.
- Normal compiled Rust/Wasm through public generated Connect transport: 40
  sequential callbacks on one resource, successful committed Acquire/Renew/Release
  reply loss and exact-request recovery, parent cancellation and subsequent grant.
- Instrumented Wrangler: two concurrent WithLease invocations share one Client
  but use separate sessions; durable wait registration orders owner release and
  waiter cancellation, with fixed authority time. Handoff preserves increasing
  fences; cancelled waiting invokes no callback and consumes no fence.
- Runnable examples: immutable-document checksum caching without destination
  fencing; inventory update with destination enforcement in an in-memory store.
  Destination unit checks exercise our stale-write rejection, per-product scope,
  exact integer comparison, equal-token policy and unchanged state on rejection.

Expected race outcomes use channels, committed-response barriers or logical time,
not sleeps. Real deadlines are watchdogs/budget enforcement, not race selectors.

## Validation

Focused Hermit/Just commands passed:

- `./bin/just test-sdk 'TestWithLease|TestMaintenance'` (race detector).
- `./bin/just test-runtime 'TestSDKWithLease|TestSDKExamples'`: 11.269s; both new
  examples ran as separate executable processes against normal local Wrangler.
- `./bin/just test-hardening '^TestP2WithLease'`: 2.918s; normal Wasm restored.

`./bin/just test` **passed (exit 0)** after the final code/test changes:

- Rust formatting/Clippy, Go vet, Just formatting, Buf generation/compatibility,
  staging configuration checks and external Go consumer packaging passed.
- Native Rust: **26 core + 2 auth tests** passed.
- Go SDK, example, protocol-tool and hosted-runner guard tests passed with the
  race detector; SDK 1.320s, examples 1.269s.
- Normal local Wrangler integration: **93.382s**.
- Instrumented local Wrangler integration: **91.509s**, including both new
  deterministic callback handoff/cancellation schedules (1.75s).
- Normal Wasm restored; SHA-256 `86be573f0ef931917bf5d8824f50c6a1e5f6c1b9277a40264e69b3508acc8e1e`.
- Final diff whitespace and updated documentation file-link checks passed.

No new dependencies were needed; all build/test tools came through the existing
Hermit/Just setup. No fresh online advisory scan or hosted test is claimed.
Ignored transcripts:
`test-results/with-lease-sdk.log`, `with-lease-runtime.log`,
`with-lease-hardening.log`, and `with-lease-full-suite.log`.

## Limits and next work

Cancellation is cooperative, not a distributed revocation barrier. A paused
callback or previously sent request may outlive its lease. Destination fencing
is still necessary where stale effects would violate correctness. Equal-token
writes are permitted in the example; that does not deduplicate business effects.

The helper waits for callback termination; it cannot impose a hard runtime limit
on uncooperative code. It does not persist requests before sending, retry business
work, or resolve uncertain external side effects. Recovery must happen before
session expiry, and ACK may already have discarded a result. Cleanup during a
panic cannot return its own error. Expiry is the fallback after failed cleanup.
High churn/concurrency or creating new Clients can still exhaust bounded server
sessions; cache eviction does not immediately delete a server session.

No hosted run was needed for this SDK-only increment. Existing hosted guarantees
and limitations are unchanged. No PostgreSQL service/adapter is introduced.
External security assessment and formal release are deferred per maintainer;
continue development with recorded evidence instead of claiming production readiness.


## Pre-commit review

Reviewed the complete working diff against
`d820d418d277648ac03949dae4f0a8d58c8afec1`, including the upstream test additions,
WithLease implementation, examples and documentation. This was a separate pass by
the same agent, not an independent external security review.

No production defect was established. Strengthened the cleanup-budget regression:
it now compares CancelPending and Release deadlines after parent cancellation,
rather than merely asserting that Release has some deadline. It would now catch
accidentally restarting the cleanup budget between RPCs. Updated the SDK installation
text to distinguish pushing development commits from publishing a versioned release.

The session cache is retained because fresh sessions on each short callback would
exhaust the existing per-resource capacity. Initial renewal/ACK and unresolved
request retention preserve the existing maintenance and recovery contracts. Unit
schedules exercise callback lifetime/error paths; runtime tests separately verify
real commit loss, queue order and reuse. The inventory example checks our token
enforcement rather than database internals. No broader abstraction or backend was
needed for this pass.

The scoped working-tree Opengrep 1.30.1 scan passed: 42 bundled Go rules, nine
production/example/tool files, zero findings and zero scan errors. Test/generated
files are outside that scan's scope. Logs and scanner state remained project-local.
Staticcheck was not installed; new context/error APIs were inspected in the pinned
Go toolchain and have no deprecation notices. This is not a full deprecated-symbol
analysis or a new online dependency audit.

The focused cleanup regression passed with the race detector. An isolated Go
source overlay deliberately gave Release a fresh, longer timeout; the same test
failed at the intended deadline-comparison assertion. The repository source was
never mutated for that probe.

Final `./bin/just test` passed (exit 0): all static/packaging checks, 26 core + 2 auth
tests, SDK/example/guard race tests, normal local Wrangler (94.008s) and instrumented
Wrangler (86.872s). Normal Wasm was restored with the same hash recorded above.
Final whitespace and documentation-link checks passed. Review evidence is under
ignored `test-results/with-lease-review-*` and `with-lease-budget-mutation.log`.
No deployment is part of this work.

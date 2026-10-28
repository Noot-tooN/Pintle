# Bounded upstream test comparison

2026-10-28. Pintle baseline: `d820d418d277648ac03949dae4f0a8d58c8afec1`.
Compared selected upstream test assertions with Pintle's current mutex, session
and maintenance coverage. Earlier research informed the design but did not supply
a recorded test-by-test map. This pass adds two missing combinations, without
changing production behavior, protocol, dependencies or release scope.

## Scope and pinned references

Read actual test bodies, not just README claims or test names. This is a bounded
comparison of the following files/scenarios, not parity with every project suite.
Upstream tests were read, not executed. Downloaded source stays in ignored
`test-results/upstream-test-review/`; no upstream code is vendored into Pintle.

| Project / revision | Selected tests | Pintle disposition |
| --- | --- | --- |
| [etcd mutex tests](https://github.com/etcd-io/etcd/blob/f164b70fdf458a17aba05dfefc685d7de4c9ca37/tests/integration/clientv3/concurrency/mutex_test.go) | `TestMutexLockSessionExpired`, `TestMutexUnlock` | Add expired queue-head coverage with a live successor. Existing lease tests cover stale Release and replay without releasing a successor. Pintle has recovery sessions, not etcd session revocation, so reproduce the applicable expiry outcome rather than copy the API. |
| [Redisson renewal tests](https://github.com/redisson/redisson/blob/b52a6502eb7225112334edfcb2808f204d2a0987/redisson/src/test/java/org/redisson/RedissonLockExpirationRenewalTest.java) | `testLockAfterTimeout`; inspected read/write and reentrant variants for applicability | Add fresh maintenance after a previous generation fails and expires, plus a delayed old reply. Read/write and reentrant locks are not Pintle mutex features. Redis restart/sleeps are replaced with committed-response loss, explicit time and response barriers. |
| [LogCabin state-machine tests](https://github.com/logcabin/logcabin/blob/ee6c55ae9744b82b451becd9707d26c7c1b6bbfb/Server/StateMachineTest.cc) | `apply_tree`, `expireResponses`, `expireSessions` | Already covered: no execution after receipt reclamation, monotonic ACK, exact session expiry. `acknowledgement_is_not_release_and_late_duplicates_never_execute`, `gaps_conflicts_limits_and_expiry_are_explicit` and `exhaustive_session_histories` check these outcomes, including ACK 2 followed by ACK 1 and retries after serialization. Snapshot implementation tests are outside our supported recovery model. |
| [ubgo/lock etcd tests](https://github.com/ubgo/lock/blob/48e1f081b6b3666b1fea6b263873bb771eca2692/etcdlock/etcdlock_test.go) | contention, acquire/release/reacquire, release idempotency, monotonic tokens, repeated cycles and cancelled context | Covered by lease/session models, `TestProtocol`, P1 lifecycle tests and the cancellation example. Match semantic outcomes: Pintle retries retain request/session identity; a new Release request is not automatically equivalent to an old one. Callback-wrapper cases do not require adding a wrapper to Pintle. |
| [FenceKit expiry tests](https://github.com/ahmed5145/fencekit/blob/6ebb6f2c7cc7055e7b81ef34eddcee788c3fd866/tests/property/test_fencing_expiry.py) and [Django storage tests](https://github.com/ahmed5145/fencekit/blob/6ebb6f2c7cc7055e7b81ef34eddcee788c3fd866/tests/unit/test_django_storage.py) | concurrent winner, abandon/expiry/reacquire, stale Renew/Release; atomically reject stale downstream writes | Coordination outcomes already covered, including exact counters above 2^53 and every-write rollback. Actual downstream fenced writes remain a missing P4 integration, not a feature or guarantee added by this pass. FenceKit's Django test uses SQLite; it is not evidence of a PostgreSQL test run. |

The semaphore/observability extras inspected in ubgo/lock do not create new P3
requirements: semaphores are later scope; Pintle's bounded telemetry and isolation
already have runtime checks. Dapr, FoundationDB's engine simulation, other Redisson
lock types and whole-project chaos suites were not comprehensively reviewed.
No cross-project percentage or claim that Pintle has universally stronger coverage
is justified by this selection.

## Added assertions

### Expired session at the FIFO head

`session_expiry_skips_only_the_ineligible_queue_head` in
`crates/core/tests/waiting.rs` exercises six cases: one millisecond before, exactly
at and one millisecond after the old session's recovery limit, triggered by either
owner Release or alarm processing. The initial receipt remains unacknowledged;
heartbeats keep the session idle timer alive but cannot extend that receipt's
ten-minute retention limit. The head and tail wait deadlines are still in the
future. The tail belongs to a fresh session, so expiring everything cannot make
the test pass.

Pintle recovery-session expiry does not itself release an owned lease. The test
ends the separate live owner's lease through Release or its own TTL, preserving
that distinction from etcd's lease-backed sessions.

Before session expiry, the head receives the grant and the tail remains pending.
At/after expiry, the head is rejected, the tail receives the next fence without
an intermediate grant to the expired head, and a late head retry cannot rejoin.
`TestP2ExpiredQueueHead` repeats the three Release boundaries through generated
Go RPCs, compiled Wasm and SQLite, using the private logical-clock controller.

This adds a combination the older all-expired test did not distinguish: session
expiry must remove only the ineligible head while preserving a live successor.
Native alarm cases test the domain transition; existing runtime alarm tests remain
the evidence for actual adapter scheduling and persistence.

### Maintenance failure and a fresh generation

`TestMaintenanceGenerationsAreIsolated` uses injected operations, logical elapsed
time and channels. A successor maintenance instance consumes its own renewal,
then an old successful reply arrives exactly at the old helper's budget boundary.
The old helper must report Lost, preserve its recovery identity, reject that reply
before updating Latest or ACKing, and never stop or contaminate the successor's
next renewal. It models two lease observations; it does not itself prove a server
handoff.

`TestP2MaintenanceSuccessorRecovery` supplies that server evidence. It drops a
fully read committed renewal reply, observes Lost, advances authority time to
exact expiry, acquires a successor with fence 2, and starts real SDK maintenance
for that successor. Replaying the old renewal then returns its immutable fence-1
receipt with a superseded observation and no Current lease. A new stale renewal
is denied. Old Stop/recovery cannot close the fresh helper's Lost channel or
alter the current lease; stopping fresh maintenance still requires explicit Release.

No sleep selects an outcome. Logical time determines expiry; request completion,
ACK, Lost and injected ticks establish ordering. Real timers are watchdogs or the
SDK's normal renewal scheduler, not the source of expected race outcomes.

## Validation

All commands use the existing Hermit/Just setup; no dependencies were installed.
New runtime tests start with `TestP2`, so the existing `test-hardening` selection
and CI's `just test` include them without another wrapper or test runner.

| Command / check | Result |
| --- | --- |
| `./bin/just test-core session_expiry_skips_only_the_ineligible_queue_head` | PASS; six logical cases, 0.02 s |
| `./bin/just test-sdk '^TestMaintenanceGenerationsAreIsolated$'` | PASS with race detector; SDK package 1.305 s |
| `./bin/just test-hardening 'TestP2ExpiredQueueHead\|TestP2MaintenanceSuccessorRecovery'` | PASS; integration 3.544 s, normal Wasm restored |
| Change session cleanup from `>= expiry` to `> expiry` temporarily | Expected FAIL in the new native queue test |
| Change the pre-ACK renewal budget guard from `>= budget` to `> budget` temporarily | Expected FAIL in the new SDK test: old reply accepted/ACKed |

Both mutation probes failed for their intended assertions. Original production
files were restored byte-for-byte before the full-suite run. These are sensitivity
checks, not exhaustive mutation testing. They do not claim that earlier tests
could not also detect either mutation.

`./bin/just test` completed with exit status **0**:

| Full-suite stage | Result |
| --- | --- |
| Formatting, Clippy, Go vet, codegen/Buf and external consumer packaging | PASS |
| Native Rust | PASS: 26 core tests and 2 auth tests, including the new six-case test |
| Go SDK / compatibility helper / hosted-runner guards, race-enabled | PASS: 2.063 s / 2.016 s / 2.065 s |
| Normal local Wrangler integration | PASS: 86.416 s |
| Instrumented integration | PASS: 78.200 s; new queue-head test 1.07 s, maintenance test 0.81 s |

Normal Wasm was restored, SHA-256
`86be573f0ef931917bf5d8824f50c6a1e5f6c1b9277a40264e69b3508acc8e1e`,
unchanged from the prior production artifact. Final whitespace, Just formatting
and local documentation-link checks passed. Remote staging tests were skipped;
hosted scenarios in the normal suite used local Wrangler. No fresh advisory or
Opengrep scan was run for this tests/documentation-only change.

Evidence logs are ignored:
`test-results/upstream-{core-focused,sdk-focused,runtime-focused,coverage-full-suite}.log`
and `test-results/upstream-mutation-{session-boundary,late-renewal}.log`.

## Remaining boundaries

No product defect was established by the added tests. No production code,
Cloudflare configuration, stored format or dependency change is required.
Downstream transactional fencing remains P4; no PostgreSQL dependency or SaaS
quota implementation was added. Physical failover/partition and snapshot rollback
remain outside the verified guarantee. This comparison does not replace
independent security review or final release-candidate validation.

No hosted traffic, deployment, commit or push was performed.

# Test and Conformance Plan

## Test environments

1. **Pure-core Rust tests:** no internet, no database; deterministic injected logical time and operations; model-driven/property-based schedules.
2. **Local Cloudflare tests:** Rust/Wasm under `wrangler dev`/Miniflare/workerd with SQLite-enabled DO; HTTP Go concurrency clients; simulated lost replies and worker restart. These validate local implementation, not hosted HA.
3. **Real integration tests:** Go SDK + Worker/DO + (later) real PostgreSQL container for fenced writes.
4. **Live Cloudflare staging:** hosted object lifecycle and scaling tests with user approval/credentials; test hibernation, alarms, actual network latency, errors and deployment replacement behavior.
5. **Cross-backend conformance:** later, run the same externally observable operation matrix against FDB/etcd/native server; never pass a backend solely because it compiles.

## First PR (P0) concrete matrix

| Scenario | Expected evidence |
|---|---|
| Same key, concurrent new requests | At most one valid winner per availability interval |
| Atomic multi-row write | Owner/fence/dedupe/revision all change, or none do |
| Forced SQL error mid-transition | No partial persisted state |
| Client response dropped after commit | Stable request ID recovers original outcome |
| Duplicate same ID with different payload | Explicit rejected conflict, no new grant |
| Increment next fence beyond 2^53 | Exact integer bytes/value preserved across boundary |
| Kill/restart local simulator process | Persisted state survives using configured local persistence; restart caveat documented |
| Repeated local object activation | No unexpected duplicated initialization or reset counters |
| Cross-tenant same resource key | Different authority / isolation |

## v0.1 additional matrix

### Implemented hardening gate before P1

`just test` runs the native reference-model suite, the normal runtime
regressions, and `just test-hardening`. The latter compiles test-only
checkpoints outside the atomic transaction, explores all admission and response
orders for three overlapping requests, controls deadline time/timer signals,
injects read/write/acknowledgement faults, and asserts the expected committed
outcomes. Independent native models cover lease/session/wait transitions. The default-build test verifies hooks are absent. Go's
race detector covers both integration runs.

Evidence, exact bounds, reproduction commands, and remaining gaps are recorded
in [hardening-results.md](hardening-results.md). Preserve these checks when
extending the state machine. P1 now supplies corresponding lease coverage below;
the obsolete P0 algorithm and its history checker have since been removed.
See [the consolidation coverage map](test-consolidation-results.md).

### Implemented P1 gate

[P1 results](p1-results.md) records 271,452 independent native lease-model trace
prefixes, 72 controlled Renew/Release/acquisition histories around expiry, nine
exact expiry edges, duplicate Renew/Release, 32 every-write rollback scenarios,
six process shutdown boundaries, nine deadline/acknowledgement recovery cases,
and persistent-clock regression. Real Go SDK tests add 24-client retries and
contention, response-loss/status recovery, and local restart. The full Hermit
suite includes the current implementations with Go's race detector.

Logical time and explicit checkpoint release determine edge/concurrency outcomes;
watchdogs only detect stalled tests. Finite schedules are not a proof of all
possible histories. See the recorded bounds and transcript before expanding
claims.

### Additional v0.1 coverage obligations

P2 now covers the bounded local wait/cancellation scenarios below. Expand these
for hosted lifecycle, broader models, and operational release gates; see the P2
evidence section at the end.

- Cancellation after wait registration, cancellation exactly before/after committed grant; abandoned grant cleanup.
- Subscription wakeup between state read/registration; reconnect after missed notification, compaction or object restart.
- Contended waiters and fairness; thundering-herd sensitivity.
- Storage commit unknown, duplicate request after timeout, idempotency record GC and name/key reuse.
- Time-source drift/clock regression and very long pauses; test assumptions rather than universal guarantee.
- Hot-key throughput, object limits, many idle waiters, p50/p95/p99, cost, heap and lifetime.

## Formal and chaos track

Design a compact state-machine specification (Rust model, eventually TLA+) with nondeterministic network loss, crash/restart and timeout events. Use invariant assertions and generated traces. Inject object access failures and response drops. Treat testing of live Cloudflare failover as partial observability, not proof of arbitrary partition behavior. Preserve sample traces and test commands in repo.

## Required CI labels

- `core` pure logic tests (every PR)
- `do-local` runtime/SQLite tests (every PR)
- `e2e-go` Go clients (every PR after SDK exists)
- `staging` explicitly credentialed live tests (pre-release, not anonymous PRs)

Any test not actually executed must be marked `NOT RUN` with the reason.

### Implemented P1.1 recovery/cleanup gate

[P1.1 results](p11-results.md) records nine native session tests, 37,448 independent
model prefixes, and 44 controlled runtime leaf scenarios. Coverage includes ACK
versus retry in every transaction/response order, exact expiry boundaries, SQL and
Rust failures after each lifecycle write, unknown commits/restart, and actual
workerd alarm cleanup with fault/restart recovery. SDK allocation is race-tested;
1,100 local renew/ACK cycles demonstrate storage reuse beyond the old cap. The
full suite preserves P0/P0b invariants on the current implementations. Hosted validation remains a separate gate.

### Implemented P2 wait/cancellation gate

[P2 results](p2-results.md) records six native wait tests and 9,330 independent
model prefixes, all cancellation/release transaction and reply orders, no-lost-wakeup
gaps, exact deadlines, FIFO/no-bypass, 48 real SQL/Rust per-write faults, actual
alarm grant/rollback/restart, three unknown-commit boundaries, duplicate/disconnected
waiters, sixteen-waiter capacity/reuse, and a two-OS-process SDK handoff. All prior
milestones remain regression coverage. Timing/watchdog and model bounds are explicit.

### Implemented P3 local hardening gate

Release scope is defined by the [OSS/deployment/SaaS checklist](release-checklist.md).
Keep isolation, authorization and existing resource-bound regressions in the OSS
suite. Tenant-wide customer quotas are deferred; no such implementation or tests
are required by P3. Hosted measurements only support their recorded claims, and
operator alert/incident rehearsals are separate from deterministic conformance.

[P3 results](p3-results.md) records 37,448 independent two-waiter model prefixes,
renewal budget and Stop/response boundaries, actual unknown-renewal recovery, static
auth/rotation/isolation and exact credential times, 48 transport rejection cases,
SDK malformed-response/redirect/size tests and sanitized metrics inspection.
`just test` includes all prior regression gates and Go vet/race checks; separate
`just audit` checks changing public advisory data. The hosted smoke subset now
passes; the broader release gates remain open (see below).

### Recovery-policy security follow-up

`TestSecurityConfigurationBoundaries` runs in normal local integration. It checks
that absent auth mode cannot select fixture secrets, valid static defaults work,
and removed fixture routes stay absent even with obsolete enabling variables.
These assertions use fixed configuration and responses. Private SQLite snapshots
check complete record rollback; offline counter setup covers values around 2^53
and uint64 exhaustion. `TestMutexSchedules` covers all 108 three-request schedules,
12 logical deadline boundaries, four timer races and six failure boundaries.
`TestProtocol` exercises generated Connect transport against the lease implementation. See [security review](security-review.md)
and the [recovery policy](recovery-policy.md); hosted rehearsals remain separate.

### First hosted smoke evidence

`tests/hosted` contains public-API smoke scenarios reused by normal local
`TestHostedSuiteLocally`. `just test-staging` explicitly enables remote execution
with a validated staging origin/token and a 96-request cap. Default CI and
`just test` run only guard tests and the local copy. On 2026-10-27 the real
Cloudflare Free staging deployment passed all six scenarios with 46 transport
attempts in 17.732s: authentication, lifecycle/replay/ACK, concurrent grants and
duplicates, pending cancellation/handoff, committed-response loss and expiry
handoff. These are observed public-API invariants, not deterministic hosted
clock/fault schedules. See [hosted results](staging-results.md),
[staging scope and limits](staging.md) and [local preparation evidence](staging-preparation-results.md).

### Hosted lifecycle and rotation follow-up

`TestHostedLifecycleLocally` reuses public-API scenarios with real local process
replacement and credential overlap/removal. It verifies stable per-instance
diagnostic markers and changed markers after replacement. Parser/body-buffer
tests check strict log field filtering and bounded response preservation.
`just test-staging-lifecycle` explicitly enables the remote version (one resource,
96 transport attempts, 12-minute watchdog). It is excluded from default CI.

Hosted replacement/receipt/FIFO persistence and credential rotation passed twice.
The combined live-tail test did not pass because required authority events were
missing. The separate `just test-staging-alarms` subsequently passed using an
explicitly provided observability token and persisted logs: alarm-only handoff,
idle reconstruction after 300,023 ms without client RPCs, expired-session rejection
and session/fencing continuity. It used ten client calls and 75 bounded management
queries, without deploying or rotating keys. Decoder/budget checks are local and
deterministic. See [the full evidence](staging-lifecycle-results.md). Logical-clock
and failure schedules remain controlled locally; hosted timing is observational.

### Bounded hosted resilience and capacity evidence

`RunCapacity` is shared by `TestHostedCapacityLocally` in normal Wrangler integration
and the explicitly opted-in `TestStagingCapacity`. It covers cancellation after
observed Pending admission, same-attempt reconciliation, normal maintenance,
lost/delayed committed renewal responses, Stop/Lost ownership semantics, small
concurrency batches, 16-waiter exhaustion/reuse and the reserved session slot.
ACK/Pending/Lost barriers control sequencing; fault injection follows a fully read
successful response. Concurrent winners and wall-clock latency are not assertions.
The exact logical-clock races remain in native/instrumented tests.

`TestCapacityGuards` deterministically checks the request budget, server-error stop,
cancelled transport-slot wait, nearest-rank quantiles and response-fault barriers.
Default CI runs these guards and local scenarios; hosted runs remain opt-in.
The 2026-10-27 staging pass used 389 RPCs, six resources and peak concurrency 16.
See [results and measurement limits](staging-capacity-results.md): small samples
from one network location do not establish saturation throughput, global latency,
resource billing, memory use or arbitrary provider failure recovery.

### SDK usability, consumer packaging and runnable examples

`TestInvalidCapabilityDoesNotConsumeSessionPosition` verifies malformed ownership
capabilities fail before preparation consumes an ACK-window position. It reproduced
the prior failure; session reservation/concurrency coverage remains unchanged.
`TestOnlyHistoricalGoPackageIsNormalized` constrains the exact pre-publication
Go import metadata exception; Buf still compares all wire fields/methods/options.

`just check-sdk-package`, included by `just test`, builds the real example from an
independent module with a local replacement, `GOWORK=off` and network fetching off.
`TestSDKExamples` builds the command with `-race` and executes all three modes as
processes against normal local Wrangler. It covers both ordered cancellation
outcomes, explicit Release/ACK and Maintain/Lost with a real committed reply dropped.
It supplements existing concurrent/fault tests rather than duplicating their full
matrix. No hosted traffic is added. See [review results](sdk-review-results.md).

### Separate security-review regressions

The existing Client formatting test now covers pointer/value, slice and struct
contexts with `%v`, `%+v` and `%#v`, using a synthetic credential. It reproduces
the pre-fix disclosure without printing the credential itself.
`TestSecurityP2PrincipalBoundaries` runs against normal Wasm: another authenticated
principal with the full owner capability/request cannot replay/read/ACK, renew,
release or cancel the owner's intent. A forged subject header on the owner's
actual acquisition envelope is rejected, the direct internal route is absent,
and the real owner can still reconcile, Release and ACK. Ordering is explicit;
deadlines are watchdogs. See [security evidence](security-pass-results.md).

### Bounded upstream test comparison

The [upstream coverage map](upstream-test-review.md) pins selected etcd, Redisson,
LogCabin, ubgo/lock and FenceKit tests and distinguishes existing coverage from
new assertions and deferred features. New tests cover an expired FIFO-head session
with a live successor and fresh maintenance after old-generation renewal failure,
including late replies and stale recovery. Native/SDK tests use logical time and
channels; `TestP2ExpiredQueueHead` and `TestP2MaintenanceSuccessorRecovery` use the
existing compiled-Wasm controller and run in the default hardening suite.

### Managed Go callbacks

[WithLease results](with-lease-results.md) records injected renewal ticks/time,
callback cancellation/completion schedules, uncertain cleanup identities, bounded
session reuse and exact tokens. Normal local Wrangler verifies committed-response
loss and both new executable examples; instrumented Wrangler orders concurrent
callback handoff/cancellation at durable wait registration. These run in the
existing Hermit/Just full suite with the Go race detector.

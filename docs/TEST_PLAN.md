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
injects read/write/acknowledgement faults, and checks recorded histories against
an independent model. The default-build test verifies hooks are absent. Go's
race detector covers both integration runs.

Evidence, exact bounds, reproduction commands, and remaining gaps are recorded
in [hardening-results.md](hardening-results.md). Preserve these checks when
extending the state machine. P1 now supplies corresponding lease coverage below;
the original grant tests alone do not establish lease correctness.

### Implemented P1 gate

[P1 results](p1-results.md) records 271,452 independent native lease-model trace
prefixes, 72 controlled Renew/Release/acquisition histories around expiry, nine
exact expiry edges, duplicate Renew/Release, 32 every-write rollback scenarios,
six process shutdown boundaries, nine deadline/acknowledgement recovery cases,
and persistent-clock regression. Real Go SDK tests add 24-client retries and
contention, response-loss/status recovery, and local restart. The full Hermit
suite includes both P0 and P1 tests with Go's race detector.

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
full suite retains all P0/P0b/P1 checks. Hosted validation remains a separate gate.

### Implemented P2 wait/cancellation gate

[P2 results](p2-results.md) records six native wait tests and 9,330 independent
model prefixes, all cancellation/release transaction and reply orders, no-lost-wakeup
gaps, exact deadlines, FIFO/no-bypass, 48 real SQL/Rust per-write faults, actual
alarm grant/rollback/restart, three unknown-commit boundaries, duplicate/disconnected
waiters, sixteen-waiter capacity/reuse, and a two-OS-process SDK handoff. All prior
milestones remain regression coverage. Timing/watchdog and model bounds are explicit.

### Implemented P3 local hardening gate

[P3 results](p3-results.md) records 37,448 independent two-waiter model prefixes,
renewal budget and Stop/response boundaries, actual unknown-renewal recovery, static
auth/rotation/isolation and exact credential times, 48 transport rejection cases,
SDK malformed-response/redirect/size tests and sanitized metrics inspection.
`just test` includes all prior regression gates and Go vet/race checks; separate
`security-audit.sh` checks changing public advisory data. Hosted gates remain NOT RUN.

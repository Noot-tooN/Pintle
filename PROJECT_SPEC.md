# Pintle — Canonical Project Specification

**Version:** Planning baseline v1 — 2026-10-24  
**Baseline stage:** pre-implementation on 2026-10-24. Current implementation status
is [P3 hardening and bounded hosted validation](docs/IMPLEMENTATION_PLAN.md#p3--harden-v01-and-prepare-public-oss),
with OSS release gates open as of 2026-10-27, separately from production deployment
and future SaaS requirements. See the [release checklist](docs/release-checklist.md).
The hypotheses below preserve
the planning baseline; consult the linked ADRs/results before treating them as
still untested or as verified guarantees.
**Project name:** **Pintle** (`pintle` repository).  
**Intended audience:** engineer or coding agent implementing the first production-oriented, open-source vertical slice.

> **Read this first.** This specification supersedes earlier discussions and documents *where they disagree*. Treat **ACCEPTED** as a decision, **PROPOSED** as an implementation hypothesis, **OPEN** as requiring experiments, and **LATER** as not part of the current milestone. Do not silently transform proposals into guaranteed behavior. Follow [`AGENTS.md`](AGENTS.md) and [`docs/IMPLEMENTATION_PLAN.md`](docs/IMPLEMENTATION_PLAN.md).

## A. Product vision, user and differentiation

**ACCEPTED** Build a small, open-source coordination platform for distributed applications — a kind of distributed-systems standard library. Application developers use idiomatic language SDKs for distributed mutexes, semaphores, rate limiting, singleflight and eventually other high-level helpers. They should not need to recreate the failure-prone mechanics of locking, contention, fencing, lease management, retries, watches and crash recovery.

- We are **not** building a new database, consensus protocol, full workflow engine, message broker, Kubernetes controller or generalized event-sourcing platform.
- Correctness, explicit failure contracts, excellent ergonomics, observable behavior and maintainability take precedence over feature count, fast launch or monetization.
- The **core and deployable server implementations are open source**; optional hosted infrastructure is a convenience product, not a prerequisite for project success. A portfolio-quality engineering project and healthy community utility are valid outcomes.
- **PROPOSED** Apache-2.0 licensing; do not add a definitive license notice until the maintainer approves it.
- The experience goal is `client := pintle.New(endpoint, credential)` then `Acquire(ctx, key)` or `TryAcquire(ctx, key)` — with transparent lease/loss semantics rather than an illusion that every external operation is automatically safe.
- Competitors/reference points: etcd concurrency, Redisson, Dapr, FoundationDB, Upstash, Inngest/Trigger.dev, FenceKit, ubgo/lock. Existing products have overlapping primitives. We differentiate with one shared authoritative engine, multilingual SDKs, higher-level helpers, optional managed deployment, correctness conformance and resource-safe integrations.

## B. Accepted architectural decisions

### B0. OSS and hosting responsibility boundary — ACCEPTED, 2026-10-27

P3 prepares the open-source core, deployable server and Go SDK for release.
Tenant-wide request/concurrency quotas, resource-count allowances, customer plans,
billing and abuse policy are deferred to a future hosting/SaaS control plane.
They are not prerequisites for OSS release and must not enter the portable core.

Keep authenticated tenant/namespace/resource isolation and principal authorization:
these protect self-hosted applications too. Keep all existing request, session,
receipt, waiter and subscription bounds; this decision changes no runtime behavior.
The core owns coordination correctness, while server/gateway adapters provide
authentication, routing, validation and telemetry. Operators configure deployment
access, workload protection, alerts and incident response. The OSS project supplies
secure defaults, useful signals, recovery guidance and documented operating limits.
The [release checklist](docs/release-checklist.md) separates those responsibilities;
[ADR-018](docs/adr/ADR-018-oss-release-and-saas-boundaries.md) records the decision.

### B1. Server-authoritative model — ACCEPTED

```text
Go SDK (first) / additional SDK languages later
     |  versioned language-neutral public protocol
     |  request IDs, deadlines, auth, lease-loss handling
     v
Cloudflare Worker public entry point (first deployment)
     |  validates/authorizes/routes to correct resource authority
     v
Cloudflare Durable Object for the resource
     |  Rust/Wasm adapter around shared portable Rust core
     |  *authoritative*: grants, fencing, idempotency,
     |  renewal/release, wait queues, wakeups, expiry
     v
DO-attached SQLite transactional storage

Future alternate deployment (NOT in first milestone):
SDKs -> same public API -> standalone native Rust server -> etcd/FDB adapter
```

**IMPORTANT CORRECTION TO OLD DOCS:** We are **Cloudflare DO first for practical deployment and optional hosted SaaS**, *while keeping an infrastructure-agnostic core.* This supersedes an interim proposal to implement etcd first. Using Cloudflare DO does mean the *first deployable adapter* requires Cloudflare; it does **not** mean SDKs or the Rust state-transition logic should import Cloudflare types. Do not claim DO can be run as a production replacement directly on AWS. External calls reach DO through a Cloudflare Worker binding.

- **Rust preferred** for server/domain. Go for first application SDK. Other SDKs (Kotlin/JVM, TS, possibly Rust) follow demand.
- No mandatory sidecar and no business/coordination logic copied across each language SDK.
- **Service layer** has strongly typed operations such as `LockService::acquire`, `try_acquire`, `renew`, `release`. It owns the meaning of coordination requests.
- **Storage layer** is backend-agnostic and exposes *transactional/conditional state transitions and change observation*, not `acquire_lock()` implemented separately for each backend. See Section F.
- Keep all core state-machine decisions portable (ordinary `cargo test` without Cloudflare). Actual atomicity is enforced by the storage adapter; a pure function is not a transaction.
- FDB/etcd are future candidates; do not build two backends in v0.1 purely to satisfy an abstraction. Maintain conformance tests and run a second backend later to validate portability for real.
- No cross-object transactional guarantee on Cloudflare DO. Route *each resource requiring atomic coordination* into one authority. Any multi-resource operation spanning objects is unsupported until independently designed/proven; do not emulate atomicity with two RPCs.
- A DO is a logical single-resource authority, not a single global server or database cluster; multiple DO instances scale independent keys, while hot keys remain potential bottlenecks.

### B2. Both blocking and nonblocking acquisition are server operations — ACCEPTED

- `TryAcquire` returns promptly: granted committed lease or contention/rejection.
- `Acquire` is a *server-owned* wait/resume operation. The server manages contention, waiter eligibility/queueing, change watches, internal retries, deadlines, expiration and fairness policy. Both call the **same atomic grant transition**.
- Do **not** implement `Acquire` as a client loop around `TryAcquire` and `Watch` in every language SDK. SDK retry of a failed network transport is allowed **using the same request ID**; it is not the ownership algorithm.
- Server remains authoritative for semaphore admissions, quota decisions, fencing and revisions.
- SDK responsibilities: idiomatic API, request ID generation/reuse, connecting/reconnecting, transport retry, context cancellation, renewal scheduling and lease-loss notification, application callback execution, and later preallocated local permit caching.
- **OPEN:** whether Cloudflare can hold bounded pending unary `Acquire` requests sufficiently safely, or requires a resumable ticket/stream architecture. Keep `Acquire` in the *server service contract* regardless of transport. A retry/resume must preserve the same pending logical request, not restart acquisition as a new contender.

### B3. Protocol direction — PROPOSED, not yet verified on Cloudflare

- Protobuf schemas and Buf are the **default contract tooling** and enable generated Go and future language types.
- **Connect-compatible unary HTTP RPC** is the leading public API candidate, allowing binary Protobuf and optional JSON, with gRPC compatibility for a future native Rust server.
- Native `connect-rust` has a working Tower/Hyper server stack, but its *native networking server* is **not a drop-in Cloudflare Workers/Wasm server**. **Feasibility test required**: either implement a small strict Connect unary handler over Workers Fetch (conformance test its framing/status/error semantics), select a compatible integration, or start with documented HTTPS + standard Protobuf JSON while retaining schema stability. Do not pretend the transport is decided until the integration works.
- DO internal calls may use platform bindings; that is **not** the public protocol.
- Backend watch and wait implementation **does not require public WebSockets**. Bounded unary `Acquire` first if feasible; streaming or resumable wait tokens are candidates. Avoid a bespoke WebSocket protocol until real needs justify it.
- Security basics even in dev: namespace from authenticated identity, stable resource IDs, no arbitrary internal DO IDs, bounded key/TTL/payload sizes. Local developer auth may be explicitly insecure but must never default to insecure in public deployment.

## C. Core correctness contract (mandatory design targets, not completed claims)

### C1. Exclusive grants vs safe effects

- For one resource authority, every successful lock grant has one unambiguous linearization point: the **durable atomic transition** that records ownership, fencing generation, request dedupe state and revision.
- Distinct successful grants for a resource receive **strictly increasing monotonic fencing generations**; never reset on release, expiry, recreation, restore or authority handoff. Use an explicit documented representation and overflow policy. Passing through JavaScript/JSON must not truncate 64-bit numbers: use exact encodings (e.g., string/protobuf decimal mapping) and test beyond 2^53.
- Only the authorized current lease holder may renew or release. Expired/lost lease holders must not renew/release successors' leases. Use unguessable ownership credentials, not just a public resource key.
- Lease expiry, renewal, pause/restart and time-source behavior must be specified. DO alarms are scheduling hints, not the sole correctness mechanism; acquisition must recheck validity.
- A granted lease **cannot force a stale worker to stop external side effects**. Strict fencing guarantees for PostgreSQL or other resources require downstream cooperation; no broad exactly-once promises.

### C2. Idempotency, unknown outcomes and cancellation

- Every logical acquisition must carry a stable `request_id` reused across transport retries, and a stable tenant/namespace/key scope. A response lost *after commit* must not result in a new grant/token for the same logical request.
- Define request dedupe retention, expired-key semantics, same request ID with different parameters, lookup/reconciliation, and garbage collection. Do not reuse request IDs to identify distinct grants.
- Treat **ambiguous commit** as a first-class result. Timeouts/network failures are not proof of transaction abort; reconcile by request ID before any new grant attempt.
- `Acquire` cancellation and deadline races may overlap a committed grant. A disconnect does not automatically undo the grant. Define `CancelPending` or `GetRequestStatus`, with safe release/reconciliation/expiry behavior; **OPEN** final protocol.
- Do not promise infinite exactly-once result retention without a designed durability/retention policy.

### C3. Waiting and change observation

- No polling during healthy, supported watch operation. Use revision-aware no-lost-wakeup semantics: read state and register interest atomically *with respect to state transitions*, or immediately notice a later revision.
- Wakeups are hints: retry the authoritative grant transition after notification; do not infer ownership from a notification.
- State revisions are independent of fencing tokens. Revision changes on relevant shared-state transitions; reconnect must re-read/reconcile if watch continuity is lost.
- FDB has one-shot watch futures, etcd has revision-based watches, Cloudflare DO requires an implementation (alarms, in-object waiter tracking, WebSocket / bounded pending response etc). Don't claim Cloudflare has a built-in FDB `watch()` equivalent.
- Fairness is a policy choice. FIFO when documented, but durability/ordering/cancel/expire behavior needs a tested protocol. Use targeted notifications when safe to avoid thundering herds.

### C4. Failure and operational model

- Strict operations must **fail closed** on lost authority/uncertain consistency; never silently fall back to weaker storage or local mutexes.
- Authoritative per-key placement creates WAN round-trip latency for remote callers; do not promise fast strict global coordination everywhere. For rate limiting, local preallocated permits or relaxed regional modes can be separately offered later with explicit trade-offs.
- Do not permit unsafe snapshot restoration or authority migration to reuse lower fencing tokens. These are later implementation topics but must be tracked in `GUARANTEES.md`.
- Cloudflare platform availability and DO instance replacement/failover behavior require live tests. Local Miniflare/workerd cannot prove all failure claims.

## D. Public service interfaces (domain level — proposed conceptual Rust)

```rust
pub trait LockService: Send + Sync {
    async fn acquire(&self, request: AcquireLock) -> Result<Lease, LockError>;
    async fn try_acquire(&self, request: TryAcquireLock) -> Result<TryAcquireResult, LockError>;
    async fn renew(&self, request: RenewLease) -> Result<Lease, LockError>;
    async fn release(&self, request: ReleaseLease) -> Result<ReleaseOutcome, LockError>;
    // Pending cancellation / GetRequestStatus likely needed; design in spike.
}

pub trait SemaphoreService: Send + Sync {
    async fn acquire(&self, request: AcquirePermit) -> Result<Permit, SemaphoreError>;
    async fn try_acquire(&self, request: TryAcquirePermit) -> Result<TryPermitResult, SemaphoreError>;
    async fn renew(&self, request: RenewPermit) -> Result<Permit, SemaphoreError>;
    async fn release(&self, request: ReleasePermit) -> Result<ReleaseOutcome, SemaphoreError>;
}

pub trait RateLimitService: Send + Sync {
    async fn check_and_consume(&self, request: RateDecision) -> Result<Decision, RateLimitError>;
}
```

Signatures are design sketches, not finalized public types. Service methods do **not** imply implementation inside every storage adapter. Do not implement semaphore/rate limit in first PR.

Conceptual Go SDK:

```go
c := coordination.New(coordination.Config{Endpoint: endpoint, APIKey: apiKey})
lease, err := c.Locks.Acquire(ctx, "invoice:123") // server waits/adjudicates
if err != nil { return err }
defer lease.Release(context.Background()) // real API should have bounded cleanup context
// Access lease.FencingToken(), lease.Lost(), or equivalent explicit safety indicators.

outcome, err := c.Locks.TryAcquire(ctx, "invoice:456")
```

## E. First backend/runtime design: Cloudflare Durable Objects

**ACCEPTED as first implementation**: Worker gateway + Rust `workers-rs` DO adapter + embedded SQLite; pure Rust core remains platform-neutral. No AWS gateway in hot path unless a concrete need emerges. AWS-hosted native Rust server and FDB/etcd are *alternate deployments later*, not a transparent swap of Cloudflare's runtime.

Per-resource authority suggestion for v0.1:

- Deterministic mapping of `(tenant, namespace, type, key)` to one DO name/id. Canonical encoding and tenant scoping; never cross-tenant collisions.
- One DO contains durable records for the resource's owner/lease, monotonic fence, revision, request-id dedupe and optional waiter metadata. More than one SQLite row may participate in a transition; commit atomically.
- SQL transactions: platform's `transactionSync()` is documented; `sql.exec` does **not** accept `BEGIN`/`SAVEPOINT`. Investigate current workers-rs binding or maintained third-party integration; prefer minimal and inspectable interop to unproven wrappers. Do not implement write-then-write with an `await` and claim it is atomic.
- Use exact integer handling over Rust/Wasm/JS, committed-result durability, schema initialization/migrations, and idempotent retries. Test rollback on intentional fault and SQL constraint violation.
- Bounded waiting in DOs needs a *separate lifecycle spike*: avoid keeping indefinitely active DO compute, and never imply a disconnected pending HTTP request can safely own/resolve waiting. Investigate hibernatable WebSocket internal mechanism, alarm scheduling and request-status resumption, while preserving server-owned `Acquire` semantics.
- Free-tier DO and Wrangler/Miniflare local testing are useful, but use current Cloudflare limits rather than hard-code historic pricing into protocol.

### Feasibility gate before implementing full mutex

**Spike P0: atomic Rust/DO storage transitions (mandatory):**

1. Compile a minimal Rust Worker and SQLite-backed Durable Object to wasm32; run with Wrangler/Miniflare.
2. Execute atomic update of owner, fencing token, revision and idempotency record. Prove all-or-nothing rollback using a deliberately failing statement midway.
3. Run concurrent acquisitions from multiple clients; prove no duplicate success grants and no token regressions.
4. Verify exact token roundtrips past JS safe-integer range; overflow policy tested.
5. Restart local Worker/object and verify persistent state and idempotent same-request replay. Document local simulator caveat.
6. Record whether `transactionSync` is exposed in `workers-rs`; document chosen integration (native, safe JS binding, other) and precise caveats. **If unworkable, stop and revise the adapter; do not quietly weaken guarantees.**

**Spike P0b: HTTP protocol compatibility:** Protobuf/Connect unary request round trip Go SDK → Worker → DO; exact errors/status, deadlines, request IDs. If unsuitable under Workers/Wasm, document an alternate standards-based transport and reason; do not invent a parallel bespoke permanent protocol by default.

**Spike P1: server-owned blocked wait:** A and B contend; one waits on server, receives/grants after release, handles expiry and disconnect without missed wakeups or a second grant from retry. Confirm idle/runtime behavior and bounded resources.

## F. Store-layer abstraction (candidate, explicitly NOT frozen)

From ubgo/lock and etcd research: do **not** model stores with helper-specific `acquire_lock()` etc; that duplicates algorithms and hides backend safety differences. Initial `read + single-record CAS + watch` is simple but risks large blobs and limits future fairness/quotas. Favor a **conditional multi-record commit** while keeping a clear atomic scope/capability contract. The precise Rust async trait syntax, value representation and transaction callback model are still open.

```rust
// Directional pseudocode, NOT final trait or stable public API.
trait CoordinationStore {
    async fn read_snapshot(&self, keys: &[Key]) -> Result<Snapshot, StoreError>;
    async fn commit(&self, conditional: ConditionalBatch) -> Result<CommitOutcome, StoreError>;
    async fn wait_for_change(&self, key: &Key, after: Revision)
        -> Result<ChangeOutcome, StoreError>;
}
// ConditionalBatch = version/absent-key checks + all-or-nothing mutations.
// CommitOutcome must distinguish committed, conflict, AND unknown outcome.
```

Strict requirements before accepting this abstraction:

- `read_snapshot` must be within declared serializable/linearizable scope; all keys touched in one atomic transition must share a backend-supported transaction scope. For DO this means **within one object**.
- `commit` validates read predicates/versions and persists *all* related records atomically, not separated fence and owner updates.
- `wait_for_change` must not have lost wakeups; a separate `ChangeNotifier` component is acceptable if its lifecycle cannot be expressed naturally in storage trait.
- Beware range-phantom checks and missing-record checks; don't claim general transactional composability with a list of naive CAS operations.
- Native backend clocks/leases are not all equivalent. Time/lease authority is a separate capability/decision.
- Backend capability detection and conformance tests must reject missing guarantees; no silent fallback to Redis-like failover semantics.
- Avoid premature elaborate trait hierarchies; implement one real atomic lock transition, then reevaluate interface before promising future helpers.

## G. Helper catalog / roadmap

**First implementation focus: one exceptionally trustworthy fenced mutex.** Then a semaphore; later high-level helpers. Not all algorithms automatically make external side effects exactly once.

| Priority | Helper | Core value / caution |
|---|---|---|
| v0.1 | Fenced mutex/lease, `Acquire` + `TryAcquire` | Atomic grant/token, recovery, waiting, renew/loss, strict failure behavior |
| v0.2 | Distributed semaphore (possibly weighted) | Atomic global concurrency; permit expiry, waiter fairness |
| v0.2 | Distributed singleflight | Deduplicate simultaneous work across processes; result handoff policy; not exactly-once external side effects |
| v0.2 | Cooldown/once-every | Rate-bound keyed actions, idempotency nuance |
| v0.3 | Rate limits / token, leaky, sliding bucket | Different semantics; strict vs regional/preallocated relaxed modes |
| v0.3+ | Multi-dimensional budgets / admission | Atomic coupled quotas require same authority or proven cross-scope transaction |
| Later | Distributed debounce | Requires registered executor/worker; cannot invoke vanished process closure |
| Later | Keyed coalescing work queue | One in-progress key with repeated dirty notifications; durable execution coordination |
| Later | Idempotency registry | External side effects require downstream transaction/participation |
| Later | Leader election, RW locks, barriers, CAS state/condition variables | Distinct fairness/expiry/clock semantics |
| Experimental | Retry-storm coordinator, adaptive concurrency, regional budget allocation | Prove control-loop and capacity guarantees |

**PostgreSQL fenced transaction integration:** useful later as SDK+resource adapter; conditional writes compare fencing tokens atomically, but simple `max_seen` checks are not instantaneous lease revocation. Newer tokens must be activated and guarded transactions serialized appropriately. Never claim arbitrary external APIs are protected.

## H. Testing requirements and proof culture

Three levels, plus staging:

1. **Pure Rust state machine tests**: deterministic steps, clock injection, property/state-machine invariant checks, idempotency, fuzz/failure sequences; TLA+ is a useful later formalism, not proof by assertion.
2. **Local Cloudflare integration**: real Rust Wasm under Wrangler/Miniflare `workerd`, SQLite, object mapping, rollback, concurrency, token precision, wake/reconnect and object restart. Cloudflare docs warn local simulation is not a hardened sandbox and not identical to every hosted failure mode.
3. **SDK E2E**: Go clients across multiple processes; local stack; cancellation, retry, wait/no-lost-wakeup, lease-loss signaling; add live PostgreSQL fenced updates when that integration is built.
4. **Live Cloudflare staging**: separately provisioned low-cost staging; deployment/restart behavior, hibernation, alarms, WAN p50/p95/p99, failure/ambiguous responses, platform-specific edge cases; do not claim tested failover unless reproducible test evidence exists.

Run lint + tests in CI; tests should report both guaranteed and untested semantics. See [`docs/TEST_PLAN.md`](docs/TEST_PLAN.md).

## I. Non-goals for initial PR and definitions of success

**Do not** build SaaS billing, polished dashboard, 8 SDKs, FDB or etcd adapter, hosted multi-region replication, a full workflow scheduler, retry-budget or distributed singleflight before foundational mutex correctness.

Success for **first PR**: running Rust/DO atomicity spike with documentation + tests and a small `Go/HTTP` roundtrip if practical. This is **not** a production-ready lock service.

Success for **first vertical slice**: Open-source Cloudflare DO server gateway, portable Rust transition crate, one Go SDK and basic `TryAcquire`/`Renew`/`Release` with verified atomic fencing, request dedupe and documented limitations.

Success for **first usable release**: bounded server-owned `Acquire`, no-lost-wakeup mechanism, cancellation and ambiguous-outcome recovery, verified lease-loss behavior, conformance/failure tests, warnings about stale side effects, deploy docs and observability.

## J. Decision inventory, open issues, and change discipline

### Accepted / superseding earlier drafts

- Open-source coordination toolkit with optional hosted endpoint; business success is optional.
- Infrastructure-agnostic **Rust domain core**, server-authoritative logic, Go SDK first.
- Cloudflare DO + SQLite as **first runtime/backend** (supersedes interim etcd-first/native-server-first implementation ordering).
- Service exposes server-owned `Acquire` AND `TryAcquire` with shared grant algorithm.
- Store APIs are *generic transactional* and domain-agnostic; **conditional multi-record commit is preferred hypothesis** vs single-blob CAS, not yet proven.
- Protobuf + Buf leading contract; Connect-compatible HTTP leading unary transport candidate, subject to Cloudflare spike.
- Avoid client-side database access, mandatory sidecars and language-by-language coordinator implementations.
- Guarantee transparency and tested capability differences; no universal low-latency, exactly-once, or unstated global transaction claims.

### Open decisions (resolve by targeted evidence, not speculation)

1. Cloudflare Rust SQLite transaction API binding and exact type/precision interoperability (P0).
2. Connect unary support in Wasm Workers gateway vs standards-compatible simpler fallback (P0b).
3. Blocking wait protocol with DO lifecycle, cancellation race, fairness and bounded resource usage (P1).
4. Exact store trait type signature and atomic scope; transactional compare+commit in DO (P0/P1).
5. Clock/TTL design, fencing-token counter overflow, idempotency retention/recovery/status RPC (P1).
6. Production disaster recovery and safe authority migration (later hardening, no premature guarantee).
7. Stronger PostgreSQL fencing integration and threat model (later).
8. Product/repository name is **Pintle**. Licensing was recommended as Apache-2.0; respect the existing repository `LICENSE` and ask the maintainer before changing it. Exact package names and publishing strategy can be decided during implementation.

## K. Research and links

Primary technical references (review versions at implementation time):

- Cloudflare Rust Workers: https://developers.cloudflare.com/workers/languages/rust/
- `workers-rs` Rust Durable Object/SQLite examples: https://github.com/cloudflare/workers-rs
- DO SQLite transaction semantics, precision: https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/
- DO architectural rules: https://developers.cloudflare.com/durable-objects/best-practices/rules-of-durable-objects/
- Local tests/Wrangler/Miniflare: https://developers.cloudflare.com/workers/testing/ and https://developers.cloudflare.com/workers/local-development/
- Connect Rust and Wasm limitations: https://github.com/connectrpc/connect-rust
- etcd concurrency implementations: https://github.com/etcd-io/etcd/tree/main/client/v3/concurrency
- FDB watches/consistency: https://apple.github.io/foundationdb/developer-guide.html
- Redis failover and lock safety: https://redis.io/docs/latest/develop/clients/patterns/distributed-locks/
- Reference designs: https://github.com/ahmed5145/fencekit and https://github.com/ubgo/lock

## L. Instructions to the implementing AI agent

**Start at `docs/IMPLEMENTATION_PLAN.md` milestone 0. Do not build everything at once.** First inspect the current repo; if it is empty, scaffold only the minimal architecture needed to test atomicity in Rust/Wasm/DO. Verify dependencies and APIs from official sources and pin working versions. Run tests. Produce working files and evidence, including any constraints discovered.

Expected first deliverables:

1. Minimal Rust workspace with a portable `core` crate and Cloudflare runtime crate (do not invent opaque abstractions without an exercised path).
2. `wrangler.jsonc` with SQLite-backed DO migrations, a minimal Worker gateway, documented local-run command.
3. SQLite atomicity + rollback + concurrent grant + fencing precision spike; test code including intentional transaction failure.
4. Short `docs/spike-results.md` showing tested versions, exact commands, what passed, what remains unknown, and recommended next implementation step.
5. Explicit note if library APIs make correctness difficult. Do not silently replace a transaction with multiple independent writes.

Do not claim completion of locks, hosted SaaS or verified global correctness based solely on a local demo. Commit no credentials; use local `.dev.vars` or equivalent ignored config; separate dev/staging resources.

# Bounded hosted resilience and capacity results

**PASS for the bounded functional scenarios, 2026-10-27. P3 production release
gate remains OPEN.** The existing Cloudflare Free staging deployment handled
389 client RPCs across six resources in 62 seconds. No production or staging
deployment, credential update, paid-plan upgrade, commit or push was performed
for this pass. No dependency, core algorithm, protocol or persisted format changed.

## Scope, target and quota check

The authenticated dashboard was refreshed before execution and showed
**Free / $0 / Current plan**, **2,403 / 100,000 Worker requests today** and
200 observability events. These are account-wide observations, not reserved
capacity. Current [Workers limits](https://developers.cloudflare.com/workers/platform/limits/)
and [DO pricing/Free allowances](https://developers.cloudflare.com/durable-objects/platform/pricing/)
were rechecked: 100,000 requests/day for each service and 13,000 GB-s/day for DO
duration. The SQL/storage allowances are recorded in the [first hosted results](staging-results.md).
Free-plan limit violations fail rather than enabling paid service. No billing
setting was changed. No quota-exhaustion error occurred in this run.

The client enforced a 600-attempt limit, six-resource limit, 16-request concurrency
limit and six-minute scenario deadline. Responses are bounded and redirects
rejected. Unexpected failures stop the scenario; a 5xx stops new admission.
DO requests, subsequent alarms, SQL operations and log events are additional
consumption. Exact post-run SQL, storage and billed-duration deltas were not
available; small client counts must not be presented as a cost model.

- Worker: `pintle-staging`, existing SQLite authority namespace
  `11349c2a8e6548c488575dd872a48f1b`.
- Endpoint: <https://pintle-staging.aleksa-novcic31.workers.dev>.
- Unchanged deployed version: `91ac8c08-b8ee-4724-a17d-da2ad3a1eb0d`.
- Normal `index_bg.wasm` SHA-256 after full local validation:
  `86be573f0ef931917bf5d8824f50c6a1e5f6c1b9277a40264e69b3508acc8e1e`.
- Staging configuration SHA-256:
  `dd4d77e5dc6a189549b68d3f41fddadc4c3809025adaea864caa1d9c71970363`.
- Source baseline: `74cdb6694456256b258377b92e8aaf56b5428ff0`, with earlier
  uncommitted staging/lifecycle work preserved and this validation follow-up added.
- Existing private staging-only key retained; expires **2026-10-28T16:56:09.820Z**.

## Implemented scenarios and results

The generated Go Connect client and SDK exercise real Rust/Wasm/SQLite DOs.
Shared `tests/hosted/capacity.go` runs under both local Wrangler and hosted staging.
New guard tests cover the finite request budget, server-error admission stop,
cancelled transport-slot wait, percentile calculation and deterministic response
fault barriers. Just owns remote orchestration and scoped telemetry queries;
existing Hermit pins supply all tools. See [ADR-017](adr/ADR-017-bounded-hosted-resilience-capacity.md).

| Scenario | Observed result |
| --- | --- |
| Disconnect after durable admission | Observe Pending, cancel the held HTTP request, confirm intent is still Pending, Release owner, resume the same attempt, verify higher fence and identical replay receipt, then Release successor |
| Normal lease maintenance | Observe renewal ACK; Stop closes Lost but does not release ownership; explicit Release succeeds |
| Lost committed renewal response | Discard a fully received successful reply; Lost retains recovery envelope; status and same-attempt replay agree; renewal preserves fence; ACK and explicit Release succeed |
| Delayed committed renewal response | Hold the successful reply until the request context expires; same Lost/reconciliation/ownership assertions pass |
| Small-load grant/contention batches | 32 sequential grants; 12 batches each of 1, 4 and 8 simultaneous contenders against an owner; every contender is busy; ACK recycles receipt capacity |
| Durable waiter limit | 16 concurrent waits return bounded Pending; 17th intent is rejected; cancel one, retry the same rejected attempt and observe Pending; cancel all and Release owner |
| Session limit and release reservation | 15 ordinary sessions fill available capacity without an owner; another is rejected; after acquiring, the reserved 16th session opens; 17th rejects; reserved session releases owner |

Hosted interval: **19:26:02.277–19:27:04.267 UTC**. Go test **62.00s**, package
**63.168s**, exit 0. **389 transport attempts/sends**, peak **16** in flight.
HTTP responses: **384 × 200**, **1 × 404** during bounded pre-admission status
observation, **3 × 429** at the asserted capacity limits. The remaining send is
the deliberately cancelled HTTP request (**one network cancellation**). No 5xx
response was observed. All 388 received responses identified gateway colo **BEG**;
this is not evidence of DO placement or geographically distributed clients.

Successful cleanup released all known owners and cancelled all pending intents.
Session records expire through normal alarms; six resource histories retain
fencing metadata. Nothing deletes/recreates authority history to reclaim usage.
This pass did not re-observe each cleanup alarm; [prior alarm evidence](staging-lifecycle-results.md)
covers that mechanism. Failure-path cleanup is bounded by expiry and does not
promise successful cleanup after an unavailable service.

## WAN latency samples

Milliseconds, end-to-end client observations, nearest-rank percentiles:

| Operation | n | p50 | p95 | p99/max |
| --- | ---: | ---: | ---: | ---: |
| Uncontended TryAcquire | 32 | 199.810 | 210.235 | 224.956 |
| Contended TryAcquire, 1 concurrent client | 12 | 132.797 | 444.175 | 444.175 |
| Contended TryAcquire, 4 concurrent clients | 48 | 137.171 | 261.147 | 261.331 |
| Contended TryAcquire, 8 concurrent clients | 96 | 136.047 | 159.455 | 165.093 |
| 16 idle Acquire handlers returning Pending | 16 | 8,180.718 | 8,181.372 | 8,181.372 |

The last row includes the intended approximately eight-second server wait.
Batch levels run sequentially, so cache/network/time effects confound comparisons.
With these small samples, p99 equals the sample maximum; it is not a reliable
population tail estimate. This establishes a modest operating baseline, not
saturation throughput, multi-region latency, a latency SLO or a tenant fairness
claim. The waiter scenario tests 17th durable intent admission after prior HTTP
responses finish; it does not separately overflow 16 simultaneous subscriptions.

## Runtime observations

Persisted invocation metrics were queried with the existing scoped observability
token. Only aggregate CPU/wall times, counts and allowlisted outcomes were retained;
raw request metadata and credentials were discarded. Invocation CPU/wall time is
not billed duration, memory or SQL usage.

The initial collector retained only its last query page and counted repeated
entries before deduplication. It was corrected to merge/deduplicate by event ID
across polls and rerun against the same window, sending no additional Worker RPC.
Both bounded query passes stopped at 12 queries (24 management queries total).
The corrected result still contains only **300 distinct gateway invocations** for
389 client sends, plus **293 authority invocations**. Log completeness is therefore
**unresolved**; neither query success nor a configured sampling setting proves
full ingestion. No assertion was weakened in the functional suite.

| Observed subset | n | CPU p50 | CPU p95 | CPU max | Wall p50 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Gateway TryAcquire | 129 | 1 ms | 3 ms | 4 ms | 118 ms |
| Authority TryAcquire | 102 | 4 ms | 10 ms | 15 ms | 95 ms |
| Gateway Acquire | 11 | 1 ms | 2 ms | 2 ms | 8,151 ms |
| Authority Acquire | 21 | 4 ms | 9 ms | 75 ms | 8,082 ms |

Acquire mixes idle waits, reconciliation and cancellation; this is not a clean
per-idle-waiter CPU profile. Observed gateway CPU totals 441 ms, authority CPU
1,419 ms; these incomplete invocation sums are not full workload utilization.
Maximum observed gateway CPU was 4 ms. Each side recorded one `canceled` outcome,
consistent with the intentional disconnect; other observed outcomes were `ok`.
No runtime memory data was obtained. Do not extrapolate these samples into
billed GB-seconds, SQL costs or a saturation ceiling.

## Validation executed

All commands ran through Hermit and Just; no new tools were installed.

| Command | Result |
| --- | --- |
| `./bin/just test-sdk 'TestCapacityGuards'` | PASS, deterministic guard/fault tests with race detector |
| `./bin/just test-runtime '^TestHostedCapacityLocally$'` | PASS, shared scenarios 20.85s; package 22.149s |
| `./bin/just test-staging-capacity` | PASS, 389 RPCs; 62.00s scenario / 63.168s package |
| `./bin/just staging-capacity-metrics` | Query execution succeeded; incomplete telemetry, 300/389 distinct gateway events after deduplication; not a resource-accounting pass |
| `./bin/just test` | PASS: schema/codegen/compatibility, formatting, Clippy, Go vet; 25 native core and 2 auth tests; SDK race tests 1.188s, hosted guard tests 1.191s, normal integration 88.574s, instrumented integration 91.347s |

The full suite includes the shared capacity scenario (20.59s on that run) and
restores the normal release Wasm. Hosted tests remain explicit opt-ins and are
skipped by default CI. Exact logical-clock/transaction/response schedules remain
deterministic locally; hosted concurrency uses observed Pending/ACK/Lost barriers
and never assumes which request arrives first. WAN timing is observational.
No advisory refresh was necessary for this dependency-unchanged pass; the earlier
[dated advisory result](staging-results.md) remains a separate evidence item.
The later telemetry-only collector correction was exercised against the same
hosted log window; it changes neither tested Wasm nor the Go scenarios. Final
formatting, diff whitespace, documentation links and credential-leak checks passed.

Ignored evidence: `test-results/capacity-guards.log`, `capacity-local.log`,
`capacity-full-suite.log`, `staging-capacity.log`, `staging-capacity.json`,
`staging-capacity-metrics.log`, `staging-capacity-metrics.json` and the initial
partial telemetry snapshot `staging-capacity-metrics-initial.json`.

## Remaining P3 work and recommendation

No core defect was exposed by these scenarios. The test harness and documentation
changed; this is not an independent audit or proof of every failure schedule.
Dropping replies after successful receipt tests ambiguous client outcomes, not
provider partitions or physical replica loss. Large fencing values, every-write
rollback and exact expiry/grant races remain local instrumented evidence, with
no public staging fault/seed endpoint.

Scope clarification after this run: [ADR-018](adr/ADR-018-oss-release-and-saas-boundaries.md)
supersedes the initial recommendation to build tenant-wide quotas next. Those
quotas/resource allowances belong to a future hosting control plane, not P3 OSS.
Per-resource limits and authenticated isolation remain required; operators must
account for unbounded distinct-resource creation and configure appropriate access.

Next finish API/SDK usability, examples and packaging review, independent security
review and vulnerability triage, documentation and final release-candidate checks.
The [release checklist](release-checklist.md) separates OSS approval from operator
alerts/incident readiness and future SaaS requirements. Representative load/cost
measurements remain useful before broader operating claims or service commitments,
under a separately authorized budget; incomplete telemetry is still an open limit.
Global credential propagation, physical failover/clock assumptions, safe snapshot
rollback/authority migration and downstream fencing boundaries remain explicit
limitations. P4 helpers follow the P3 OSS gate, not a future SaaS launch.

Current README, implementation plan, guarantee matrix, test plan, deployment,
recovery and security summaries now distinguish completed hosted scenarios from
remaining gates. Dated milestone results retain their historical observations.

# First hosted staging results

2026-10-27. Explicitly authorized staging deployment, application credential setup
and bounded hosted tests. **No production deployment, paid-plan upgrade, commit or
push.** No dependency or coordination algorithm changed during this exercise.
Hermit and the root Just recipes ran the build, advisory checks, deployment,
secret upload and tests. Local preparation is recorded separately in
[staging-preparation-results.md](staging-preparation-results.md).
Subsequent redeployment/rotation evidence, the current credential state and the
successful persisted-log alarm/idle follow-up are recorded in
[staging-lifecycle-results.md](staging-lifecycle-results.md). The version and token
creation history below describe the first exercise, not the currently active version.
The later [bounded resilience/capacity pass](staging-capacity-results.md) supersedes
the next-step recommendations below for its measured scenarios.

## Account, Free plan and quotas

Wrangler OAuth membership verified the selected account. The read-only Workers
API confirmed the existing `aleksa-novcic31.workers.dev` subdomain. The subscriptions
API returned 403 because this OAuth login lacks billing access; the Workers
`default_usage_model=standard` field was **not** treated as billing-plan proof.
Instead, the authenticated dashboard's Workers plans page explicitly showed
**Free / $0 / Current plan**. No upgrade control or billing mutation was used.

Before deployment, the dashboard showed **1,750 / 100,000 Worker requests today**
(98,250 remaining at observation time), no Durable Objects, and zero DO requests,
duration, SQL reads/writes and storage. These are account-wide snapshots, not
reserved capacity; other applications and delayed reporting can change them.

The dashboard and current official documentation agreed on these Free allowances:

| Resource | Free allowance |
| --- | --- |
| Worker requests | 100,000/day |
| Worker CPU per HTTP invocation | 10 ms |
| Durable Object requests, including alarms | 100,000/day |
| Durable Object duration | 13,000 GB-s/day |
| SQLite rows read | 5,000,000/day |
| SQLite rows written | 100,000/day |
| SQLite stored data | 5 GB total |

Sources checked on 2026-10-27: [Workers limits](https://developers.cloudflare.com/workers/platform/limits/)
and [Durable Objects pricing](https://developers.cloudflare.com/durable-objects/platform/pricing/).
SQLite-backed DOs are supported on Free. On Free, operations exceeding the DO
daily limits fail until reset at 00:00 UTC; storage-limit violations also fail.
Deleting rows and scheduling alarms consume writes. The client request cap does
not bound total SQL work or later alarms. No paid plan was activated to run this test.

After the run, a refreshed dashboard listed the new SQLite namespace, but requests,
duration, SQL rows and storage still displayed zero. Those counters had not yet
reflected the successful RPCs and are **not evidence of zero consumption**. Exact
post-run quota deltas remain unmeasured; this run establishes Free-plan operation,
not a usage/cost model.

## Exact target and artifact

- Worker: `pintle-staging`, using `wrangler.staging.jsonc` and normal release Wasm.
- Endpoint: <https://pintle-staging.aleksa-novcic31.workers.dev>.
- Namespace: `pintle-staging_PintleAuthority`, ID
  `11349c2a8e6548c488575dd872a48f1b`; the API confirms `use_sqlite=true`.
- Source baseline: `74cdb6694456256b258377b92e8aaf56b5428ff0` plus the uncommitted
  staging configuration, Just recipes, hosted tests and documentation described
  in [ADR-015](adr/ADR-015-isolated-staging-validation.md).
- Wrangler: pinned 4.152.0; compatibility date: 2026-10-24.
- Initial code version: `fb885ef5-cc2f-43c1-98e9-eced15ef22cc`.
- Secret upload created the tested active version:
  `1b799a63-039e-40c3-ac14-42aabc0347ca`, at 100% in deployment
  `675afe5c-be02-44d7-9fb4-05eb59b4dbc2` (16:56:18 UTC).
- Upload: 551.54 KiB / 176.18 KiB gzip; Cloudflare reported 5 ms startup.
  Startup time is not per-request CPU usage.
- Built `index_bg.wasm` SHA-256:
  `45886e11acd6c1c0ab0a0fe35ac364d6f03e5981c8271fde05f4d635aeea9c38`.
- Staging config SHA-256:
  `ff31dcde225891be93b5d8f78eb8b4d593377658cb26e6095f992ac31d30531d`.

Read-back verified the own-namespace binding, P2 mode, static auth, metrics and a
`secret_text` credential binding. There is no production route or test-hooks
build. The namespace was created fresh; no existing authority history was reset.

## Credential handling

Before secret provisioning, an unauthenticated OpenSession returned HTTP 503:
the missing static configuration failed closed. Then `just staging-secret`
uploaded a random 256-bit application token through stdin. It permits only tenant
`pintle-staging`, subject `hosted-smoke`, namespace `staging`, and expires at
**2026-10-28T16:56:09.820Z**. Invalid credentials returned unauthenticated once
configuration was present, as asserted by the hosted suite.

The local credential is in Git-ignored `.hermit/staging/credentials.json`, with
directory mode 0700 and file mode 0600, verified after creation. No token value
appeared in command arguments, test output, documentation or chat. The remote
secret and local file remain for repeat testing until expiry; authentication
checks the timestamp on admission. Expiry does not delete those copies or cancel
already admitted work. See [rerun instructions](staging.md#local-staging-credential).

## Commands and observed tests

The preparation's complete `just test` and `staging-dry-run` already passed on
this implementation. This exercise then ran:

1. `just audit`: npm reported **zero vulnerabilities**; OSV checked **115 Rust/Go
   dependency versions, zero findings**, at 16:54:31 UTC. The initial sandboxed
   npm request returned 403; the approved network retry passed. This is an
   advisory snapshot, not an independent security audit.
2. `just staging-deploy`: membership/configuration checks, normal Rust/Wasm build,
   upload and staging trigger deployment all passed.
3. `just staging-secret`: passed; credential values supplied only through stdin.
4. `just test-staging`: generated Connect client and Go SDK over real HTTPS,
   with `go test -race -count=1 -timeout=3m -v ./tests/hosted -run '^TestStaging$'`.
5. Final Just formatting, staging configuration assertions, documentation file
   links and `git diff --check`: passed. A value-based credential scan found no
   token in Git-visible source or top-level evidence logs; the private file is
   ignored. The full local suite was not repeated for these documentation-only
   follow-up changes.

| Hosted scenario | Result | Duration |
| --- | --- | --- |
| Authentication rejection | PASS | 0.10s |
| Acquire/contention, payload conflict, replay, Renew/Release, status, ACK, stale owner | PASS | 3.40s |
| Four concurrent acquisitions and duplicate winner retries | PASS | 1.49s |
| Pending cancellation, cancellation retry and Release-to-waiter handoff | PASS | 10.02s |
| Real committed reply discarded by client transport, status/retry recovery | PASS | 1.01s |
| Server-owned Acquire handoff after lease expiry | PASS | 1.72s |

**One hosted suite run, all six scenarios passed, 46 transport attempts,
17.731815666s scenario elapsed; Go package completed in 19.400s, exit 0.**
The pre-secret probe is one additional Worker request outside the suite. Five
unique authorized resource names were used. No load test or fault-control route
was invoked. Concurrent tests assert one winner and stable receipts, not which
network request arrives first; handoff tests observe Pending before Release.
Exact logical-clock and fault schedules remain deterministic local coverage.

Ignored evidence files: `test-results/staging-deploy.log`, `staging-secret.log`,
`staging-hosted.log`, `staging-inventory-after.json`, `npm-audit.json` and
`security-audit.json`. The inventory records metadata with secret bindings
redacted. One-off operator scripts in `test-results/` are ignored; maintained
project commands remain in Just and test assertions remain in Go/Rust.

## Limits and next evidence

This proves the smoke scenarios on real SQLite DOs under the Free plan. It does
not prove arbitrary failover, cold eviction/restart, deployment replacement,
alarm-only progress/cleanup, rotation propagation, exact large fencing tokens on
the host, latency percentiles, saturation or a production cost envelope. No
observability exporter or hosted storage inspection/fault endpoint was added.
Final leases were released by successful tests. Sessions expire normally, but
five resource fencing histories remain; deleting them is not safe test cleanup.

Next: a bounded staging lifecycle rehearsal for idle/alarm behavior, credential
rotation and deployment replacement while preserving authority history, followed
by representative capacity/latency measurements. Keep separate authorization and
budgets for those remote exercises. Independent security review and the remaining
[P3 release gates](deployment.md#remaining-stagingrelease-evidence) are still open.

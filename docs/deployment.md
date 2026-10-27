# Deployment and release gates

The [release checklist](release-checklist.md) distinguishes P3 OSS publication,
operator readiness for a particular deployment and a future SaaS launch.
The operational guidance below applies to deployments; it does not require
building a SaaS control plane before releasing the toolkit.

P3 has local hardening, hosted smoke/lifecycle and bounded resilience/capacity
evidence on Cloudflare Free; see [capacity results](staging-capacity-results.md),
[hosted smoke results](staging-results.md) and the [isolated staging setup](staging.md).
The local `wrangler.jsonc` disables public workers.dev and preview URLs and enables
no service mode by default. The separate staging configuration enables static
authentication, P2 mode and a staging workers.dev URL. Do not treat it as a
production deployment template with completed security/operations review.

## Artifact and configuration

Run `./bin/just bootstrap`, `./bin/just test` and separately
`./bin/just audit` using Hermit. The test suite restores a normal release
Wasm artifact. A hosted artifact must omit `test-hooks`; never enable P1_ENABLED or P11_ENABLED alongside the current P2 service mode.
Select `P2_ENABLED=true`, `PINTLE_AUTH_MODE=static`, and explicitly opt into
`PINTLE_METRICS=true` if sanitized metrics are wanted. The local configuration
enables no service flags; the staging configuration explicitly selects these values.

Provide the `PINTLE_CREDENTIALS` secret as a JSON array. Each entry has `token`,
`tenant`, `subject`, `namespaces`, optional `not_before_ms`, and required
`expires_at_ms`. Times are canonical decimal Unix milliseconds, less than 2^53;
validity is start-inclusive and expiry-exclusive. Use a cryptographically random
ASCII alphanumeric token of 32–256 characters. Tenant identifiers allow ASCII
letters/digits plus `._:-`, at most 64 characters; subject at most 32; namespaces
use the same identifier alphabet and maximum 64. Each key permits 1–16 explicit
namespaces, no wildcard. Total secret is at most 16 KiB and contains 1–16 keys.
Do not put secrets in Wrangler vars, source control or command-line examples.
Use the platform's [secret management](https://developers.cloudflare.com/workers/configuration/secrets/).
Static mode never falls back to `PINTLE_TEST_CREDENTIALS`. Omitted auth mode now
defaults to static; local protocol fixtures explicitly select `PINTLE_AUTH_MODE=test`.
The obsolete `/spike` and P0b internal handlers have been removed. Old
`LOCAL_SPIKE`/`P0B_ENABLED` variables cannot enable them. Counter setup and
record inspection exist only in the local test process.

Rotate by adding a new key with the same tenant/subject, distributing it to clients,
verifying recovery through it, then removing/expiring the old key. Namespace changes
can intentionally remove access to existing recovery records. Old admissions and
queued intents are not cancelled by key removal; an emergency revocation policy
must account for those intents and existing downstream access.

## Authority history and operations

Follow the [recovery policy and incident runbooks](recovery-policy.md) for unknown
outcomes, storage/clock faults, alarms, credential compromise and software rollback.

Keep Durable Object binding/class, resource routing and migrations stable. Never
reset/delete fencing counters or restore an older snapshot while external systems
may recognize newer tokens. No live legacy receipt migration or safe authority
transfer is implemented. Select a fresh, isolated staging namespace initially;
production migration/backup recovery requires its own design and evidence.

Metrics emit JSON with `event=pintle.rpc`, fixed `operation`, HTTP `status` and
`duration_ms`. Alarm events use operation `alarm`. Aggregate durations and error
rates outside the core; logs currently provide no histogram exporter. Investigate
401/403 changes, 429 capacity pressure, 503 storage/configuration errors and delayed
alarm progress. Operators define alert thresholds from their workload baselines,
configure log retention/access and assign incident response. OSS supplies the
signals and guidance; shipping a managed alerting service is not a P3 gate.
Never add raw bodies or resource names.

With metrics enabled, `pintle.authority` events also carry a random, in-memory
instance marker, operation (`fetch`/`alarm`) and status. Markers change on object
construction and contain no resource identity or ownership capability. Use them
for lifecycle diagnostics, not high-cardinality aggregate metric labels. See
[ADR-016](adr/ADR-016-hosted-lifecycle-observation.md).

For capacity, track 16 sessions, 32 positions per session, 16 waiters and 16 live
HTTP subscriptions per resource/instance as applicable. Maintenance consumes a
session. ACK and expiry reclaim bounded recovery state; distinct resources retain
fencing metadata. Tenant-wide quotas, resource-count allowances and customer cost
policy are deferred SaaS/control-plane work. They are not OSS release gates.
Keep the existing isolation and safety limits; operators must restrict exposure
or admission appropriately because total distinct-resource storage is unbounded.

## Remaining staging/release evidence

Generated Go clients over HTTPS, static authentication, committed-response loss,
basic renewal/release and expiry handoff passed the first hosted smoke run.
The [lifecycle follow-up](staging-lifecycle-results.md) observed authority
replacement with preserved ownership/receipts/queue and successful key rotation
from one client location. A separate persisted-log rehearsal passed alarm-only
handoff, idle authority reconstruction, expired-session rejection and monotonic
session/fencing history. The earlier live-tail failure remains unresolved, but
does not block that persisted-log evidence. Global credential propagation remains
unmeasured. The [bounded capacity follow-up](staging-capacity-results.md) passed
disconnect/reconciliation, committed renewal-response faults and session/waiter
exhaustion/reuse, with single-location WAN p50/p95/p99 samples. This is a small-load
baseline, not a throughput ceiling or latency SLO. Exact large fencing tokens and
controlled expiry/renewal/grant fault schedules remain local instrumented evidence;
no hosted seed/fault API is exposed. Representative hot-key saturation, memory,
billed duration, SQL/storage costs, global behavior and alert/incident rehearsals
remain open. Never infer physical failover guarantees from runtime reconstruction.

The [API/SDK and packaging review](sdk-review-results.md) passed locally with
runnable examples; public module fetching remains unverified until publication.
For OSS release, finish independent security review, private vulnerability
reporting with maintainer triage, documentation and final
candidate validation, and obtain Apache-2.0 OSS release approval. Publish the
[guarantee boundaries](GUARANTEES.md), especially unsupported snapshot migration,
external side effects and paused-worker revocation. Incomplete telemetry and
resource-cost measurements remain documented limits on claims.

For a production deployment, its operator separately validates workload/cost
headroom, credentials, alerts and incident/rollback procedures. A future public
SaaS also needs tenant-wide quotas, resource allowances and abuse policy. These
are separate gates in the [checklist](release-checklist.md), not additional core
algorithms required for P3. P4 follows the OSS gate. Only isolated staging has
been deployed; no production deployment or paid-plan upgrade has been performed.

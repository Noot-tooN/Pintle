# Deployment preparation — no deployment performed

P3 has local evidence only. This document prepares a later explicitly authorized
staging exercise. The checked-in Wrangler configuration disables public workers.dev
and preview URLs and enables no service mode by default. Do not treat it as a
production deployment template with completed security/operations review.

## Artifact and configuration

Run `./bin/just bootstrap`, `./bin/just test` and separately
`./bin/just audit` using Hermit. The test suite restores a normal release
Wasm artifact. A hosted artifact must omit `test-hooks`; never enable LOCAL_SPIKE,
P0B_ENABLED, P1_ENABLED or P11_ENABLED alongside the current P2 service mode.
Select `P2_ENABLED=true`, `PINTLE_AUTH_MODE=static`, and explicitly opt into
`PINTLE_METRICS=true` if sanitized metrics are wanted. No flags are enabled here.

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
Static mode never falls back to `PINTLE_TEST_CREDENTIALS`.

Rotate by adding a new key with the same tenant/subject, distributing it to clients,
verifying recovery through it, then removing/expiring the old key. Namespace changes
can intentionally remove access to existing recovery records. Old admissions and
queued intents are not cancelled by key removal; an emergency revocation policy
must account for those intents and existing downstream access.

## Authority history and operations

Keep Durable Object binding/class, resource routing and migrations stable. Never
reset/delete fencing counters or restore an older snapshot while external systems
may recognize newer tokens. No live legacy receipt migration or safe authority
transfer is implemented. Select a fresh, isolated staging namespace initially;
production migration/backup recovery requires its own design and evidence.

Metrics emit JSON with `event=pintle.rpc`, fixed `operation`, HTTP `status` and
`duration_ms`. Alarm events use operation `alarm`. Aggregate durations and error
rates outside the core; logs currently provide no histogram exporter. Investigate
401/403 changes, 429 capacity pressure, 503 storage/configuration errors and delayed
alarm progress. Define alert thresholds from staging baselines, not local smoke
latencies. Configure log retention/access; never add raw bodies or resource names.

For capacity, track 16 sessions, 32 positions per session, 16 waiters and 16 live
HTTP subscriptions per resource/instance as applicable. Maintenance consumes a
session. ACK and expiry reclaim bounded recovery state; distinct resources retain
fencing metadata. Tenant-wide quotas and cost controls are still release work.

## Required staging/release evidence (NOT RUN)

After separate authorization, verify generated Go clients over HTTPS; credentials
and rotation propagation; committed-response loss; object restart and idle/alarm
lifecycle; bounded waits under disconnect; expiry/renewal/grant schedules; exact
large fencing tokens; and observed failure behavior without claiming arbitrary
failover proof. Measure WAN p50/p95/p99, hot-key saturation, idle-waiter CPU/memory,
request/storage cost and alarm delay with representative loads. Never silently
infer host guarantees from local workerd.

Complete independent security review, private vulnerability reporting, incident
ownership, telemetry/quotas, and final Apache-2.0 OSS release approval. Publish the
[guarantee boundaries](GUARANTEES.md), especially unsupported snapshot migration,
external side effects and paused-worker revocation. P4 is not a substitute for
these remaining P3 gates. No deploy or push commands were executed during P3.

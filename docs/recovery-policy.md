# Recovery policy and incident runbooks

Status: local v0.1 policy, 2026-10-27. Hosted procedures require staging rehearsal
before production use. This policy preserves the authority-history requirements
in [GUARANTEES.md](GUARANTEES.md); it does not add a restore or migration feature.

## Supported recovery boundary

One resource's durable authority owns its lease, fence, revision, clock floor,
session counter and queue counter, plus retained sessions and wait intents.
Recovery must preserve that complete history and the tenant/namespace/resource
routing that selects it. A backup is sensitive evidence, not automatically a safe
replacement authority. Retain credentials and lease capabilities only in approved
secret/evidence storage, never ordinary logs or issue reports.

| Situation | Policy |
| --- | --- |
| Client timeout, lost reply or process restart | Reconcile the same prepared request against the same authority, while its session/result remains retained. |
| Worker restart with unchanged durable history | Resume against existing storage; local process-restart evidence exists. Hosted lifecycle remains unverified. |
| Expired/acknowledged recovery result | Historical outcome is unavailable. Do not infer failure or repeat the logical operation with a fresh identity. |
| Missing, corrupt or uncertain authority history | Fail closed and quarantine affected work. No automatic empty-state replacement. |
| Older snapshot, namespace replacement, routing change or authority migration | Unsupported. Do not reopen admissions against restored/reset counters. |
| Software rollback | Only a revision proven compatible with current persisted state, protocol and configuration; otherwise roll forward or keep admissions stopped. |

A larger fencing token protects external effects only where the protected resource
atomically enforces the fencing protocol. Waiting for the maximum TTL alone does
not make reset/restore safe: paused clients and downstream token history can
outlive a lease. A fresh resource name does not reset the protected business
resource's history. No RPO, RTO, instantaneous revocation or arbitrary failover
promise is established by local tests.

Quarantine is an operational requirement, not automatic detection of every loss.
The service cannot distinguish a completely deleted authority from a new resource,
or reliably detect a self-consistent older snapshot. Preventing those operations
through storage access controls and incident procedure is mandatory.

## Incident ownership and evidence

Before staging, assign an incident lead and an operator with access to deployment,
secret and storage controls. Before production, assign on-call coverage and a
private vulnerability-reporting channel. These roles/contact details are not yet
configured by this repository.

For every incident, record time, affected scope, deployed source/artifact revision,
configuration revision, observed RPC codes, known commit/response boundaries and
application effects. Keep identifiers/capabilities out of routine telemetry. If
recovery requires prepared requests, store them securely with their complete
resource, method, payload, session, sequence and ACK envelope. Preserve originals;
never alter the envelope of an ambiguous attempt to make a retry succeed.

Stopping HTTP admissions does not cancel admitted intents or stop existing
holders. Containment may also require pausing application jobs and disabling
writes at the protected resource. Identify who can perform each action before
using the service for consequential work.

## Runbook: timeout, disconnect or lost reply

1. Stop dependent external work unless ownership and its downstream enforcement
   are known. A timeout, 503 or Lost signal is not evidence that a mutation aborted.
2. Use a fresh bounded RPC context and the original prepared attempt for
   GetAttemptStatus or a same-attempt retry. Use the same tenant, principal,
   namespace and resource. Replacing a bearer key is allowed only with preserved
   identity and authorization.
3. Inspect the result, not just HTTP success:
   - Acquired/Renewed: the receipt is historical; inspect the current observation.
     Superseded, expired or released results do not authorize work.
   - Released: recover the original release result; it does not describe a later
     holder's ownership.
   - Pending: resume or CancelPending using the original complete Acquire request.
     Cancellation may return an already committed grant; it is not Release.
   - NotFound: a point-in-time observation, not proof that an in-flight request
     cannot later commit. Continue reconciliation; do not create a replacement.
   - Acknowledged/SessionExpired: stop automated historical recovery. Reconcile
     application effects separately; do not claim the operation never happened.
4. ACK only contiguous terminal results the application has safely consumed or
   durably recorded. ACK relinquishes recovery; it does not release a lease.
5. A known current owner may use a new session to renew/release its existing lease
   when the old session expired. That is not permission to retry an unknown grant
   as a new acquisition. Treat a lost OpenSession reply as a potentially orphaned
   allocation, not an idempotently recoverable session creation.

Session idle expiry is five minutes, capped by ten minutes from the oldest
retained result. These are upper bounds, not a guaranteed recovery interval;
ACK or idle expiry can end recovery sooner. Heartbeats cannot retain old results
forever. See [ADR-009](adr/ADR-009-bounded-recovery-sessions.md).

## Runbook: storage errors, corruption or clock regression

1. Pause affected admissions/application work and preserve diagnostic evidence.
   Do not retry mutations under new IDs to bypass errors.
2. Distinguish transient storage unavailability from lost/corrupt history. A
   process restart is acceptable only with the same intact durable authority.
   Use same-attempt reconciliation after service becomes available.
3. For clock regression, keep the persisted high-water mark. Wait for the trusted
   authority clock to recover or escalate a platform time fault. Never lower the
   stored clock, shorten a lease manually or change counters to unblock traffic.
4. For missing/corrupt history, keep the resource quarantined. Recovery requires
   a separately designed and reviewed epoch/fencing migration involving protected
   resources and outstanding clients. No current admin command establishes this.
5. Resume only after history continuity is established, known ambiguous attempts
   are reconciled and read/write behavior is validated in the affected scope.
   If continuity cannot be established, remain unavailable.

## Runbook: delayed alarms or capacity exhaustion

1. Inspect error rates and alarm progress using sanitized telemetry. Alarms are
   scheduling hints; current requests still check expiry transactionally.
2. Reconcile existing attempts before ACKing terminal results. Let automatic
   expiry reclaim abandoned sessions; do not delete records or reset metadata.
3. Pending work cannot be ACKed away. Cancel it explicitly with its original
   request, then inspect whether cancellation or grant won.
4. Reduce admission pressure when queues/session windows are full. Rate-limit
   tenant traffic externally until tenant-wide quota/cost controls are designed.
5. Persistent scheduler/storage failure requires incident escalation. The durable
   fallback alarm and platform retries are not an unlimited availability promise.

## Runbook: credential loss or compromise

1. For routine rotation, add a high-entropy key with the same tenant/subject and
   required namespace access. Verify existing-session recovery through it before
   removing the old key. Do not put secrets in source, command arguments or logs.
2. For compromise, revoke the key and contain downstream writes/application jobs.
   Revocation denies future admission; it cannot retract already admitted requests,
   queued grants, existing capabilities or external effects. Do not assume that
   shutting the gateway stops alarm-driven queue progress.
3. Reconcile known attempts and cancel/release only where their identities and
   outcomes are known. There is no bulk immediate principal-revocation primitive.
4. A lost key must not cause reassignment of the old principal to a different
   customer: retained sessions are authorized by stable principal identity.
5. Resume after access restoration and the effect of outstanding intents are
   understood. Secret propagation/access controls still require staging evidence.

## Runbook: failed upgrade or proposed rollback

1. Pause new work where necessary; preserve the binding/class, namespace, routing,
   state and configuration. Do not run an earlier P0/P1/P1.1 development mode over P2 data.
2. Identify whether new code wrote data older code cannot interpret, or whether
   configuration/auth rules differ. Build and test against representative copied
   data in an isolated local fixture; never point test tooling at live storage.
3. If compatibility is not established, prefer a forward fix. A Git revert alone
   is not a storage rollback plan. Never restore an older snapshot with old fences.
4. Require approval and staging rehearsal for the eventual hosted action. Resume
   after validating same-attempt recovery, lease ownership and queue behavior.

## Local drills and remaining gates

Use Hermit: `./bin/just test`. Its native models and controlled-clock runtime
schedules exercise expiry, clock regression, concurrent cancellation/grant,
per-write rollback, lost replies, restart, ACK and session reclamation. See
[TEST_PLAN.md](TEST_PLAN.md) for the finite bounds and test ownership. Production
SDK Lost is a conservative stop signal, not proof of release or stopped effects.

These tests validate mechanisms used by the runbooks; they do not prove a human
incident response, safe snapshot restore or hosted recovery. Before production,
rehearse hosted restart/alarm and rotation behavior, establish alerting/incident
ownership, and review the containment procedure for each protected resource.
Unsupported restoration/migration stays prohibited until a separate design and
its tests are accepted. No deployment, data reset or migration was performed here.

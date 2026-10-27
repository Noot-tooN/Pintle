# Hosted lifecycle and credential rotation evidence

2026-10-27. This follow-up is limited to `pintle-staging` on Cloudflare Free.
No production deployment, paid upgrade, commit or push is authorized/performed.
The Rust core and public protocol are unchanged; no dependency was added.
Hermit and Just remain the command interface. See
[ADR-016](adr/ADR-016-hosted-lifecycle-observation.md) for the diagnostic design.

**Result: hosted lifecycle gates in this rehearsal passed.** Redeployment
persistence and credential rotation passed twice. A subsequent scoped-token,
persisted-log run passed alarm-only handoff, idle authority reconstruction,
expired-session rejection and session/fencing continuity. See the
[completed follow-up](#persisted-log-alarm-and-idle-follow-up).
The old combined live-tail command never passed; its failures below remain
recorded, and its logging root cause is unresolved. The successful follow-up
uses a separate observer and requires no new deployment or credential rotation.

## Plan, quotas and isolation

The authenticated dashboard again showed **Free / $0 / Current plan**. Worker
usage before the rehearsal was **2,058 / 100,000 requests today**. DO counters
still displayed zero despite earlier successful RPCs, so they are not reliable
post-run consumption evidence. The published allowances remain those recorded
in [the first hosted results](staging-results.md#account-free-plan-and-quotas).

The maintained rehearsal is capped at 96 client transport attempts and one new
resource, with a 12-minute watchdog, two normal staging code deployments and two
secret updates. Management API calls and alarms are additional platform usage.
There is no load generator, paid-plan API, namespace recreation or data deletion.
The read-only `staging-origin` check ties the endpoint to the selected account
before a deployment/secret mutation. Existing authority history is preserved.

## Local verification

- Final `just test`: passed, exit 0; 25 core and 2 auth tests, formatting,
  Clippy, Buf/codegen checks, Go vet/race and all prior deterministic faults.
- Final transcript: `test-results/lifecycle-full-suite-final-observer.log`. SDK
  race package: 1.303s; hosted guard/parser/buffer package: 1.302s; normal local
  integration: 66.654s; instrumented integration: 91.294s. Normal Wasm restored.
- Shared `TestHostedLifecycleLocally` passed in that run. It restarts actual
  local Wrangler over the same SQLite state, overlaps keys, removes the old key,
  recovers admitted requests and checks monotonic fences. It also verifies one
  stable diagnostic marker per runtime and different markers after each restart.
- An earlier supplemental metric check read the console file, where Wrangler
  did not put those events. Reading the existing debug log fixed the test;
  the focused rerun passed in 19.94s. No coordination fix was needed.
- The log parser deterministically rejects extra fields and non-metric data.
  Remote tests remain skipped by default; CI never deploys or rotates credentials.

## Hosted observer investigation

The first two rehearsal attempts stopped at the first OpenSession while waiting
for authority telemetry. They did not reach credential rotation. The client
observer's timeout does not establish that OpenSession was rolled back: each
could leave an orphan recovery session, which expires normally.

Three small diagnostic OpenSession probes, all returning HTTP 200, reused one
`tail-probe` resource. Filtered logging produced no authority events; unfiltered
logging initially showed only gateway events. Explicit live-only observability
with persistence/invocation logging disabled also did not deliver authority
metrics to the runner. Cloudflare's documented default `observability.enabled=true`
then delivered both gateway and authority events. The probes do not isolate
which logging setting caused the difference. Diagnostic entropy is initialized
lazily during the first event so it never depends on constructor I/O context;
that change alone did not resolve the missing hosted logs.

Sources: [Durable Object logging setup](https://developers.cloudflare.com/durable-objects/observability/metrics-and-analytics/#view-logs),
[Workers Logs](https://developers.cloudflare.com/workers/observability/logs/workers-logs/),
[platform lifecycle](https://developers.cloudflare.com/durable-objects/concepts/durable-object-lifecycle/).
The default logging configuration stays within Free's 200,000 events/day and
three-day retention. This is not a paid logging subscription. Application events
contain no keys, request IDs, resource names, capabilities or credentials. The
runner disables Wrangler disk logs, discards raw tail metadata/headers in memory,
and consumes only the four allowlisted authority fields. Cloudflare also retains
its standard invocation metadata under the default observability configuration.

The probes and aborted attempts used additional resource names; their normal
session alarms were allowed to finish before each subsequent isolated attempt.
The trace intentionally has no resource label, so exclusive staging traffic and
quiescence after prior probes are prerequisites for attribution. Tailing starts
before admissions because attaching it can itself cause a platform replacement.

## Hosted rehearsal result (earlier live-tail attempts)

The next rehearsal (`staging-lifecycle-verified.log`) verified authority-instance
replacement across normal redeployment, preserved owner/receipt/queued intent,
and completed both credential updates. Both keys worked during overlap; the old
key was then rejected on acquisition and status, while the new key recovered
receipts, renewed/released ownership and completed already-admitted work. The
new credential retained the original expiry, `2026-10-28T16:56:09.820Z`.

That run stopped at the first alarm-owner acquisition with a client deadline
error (101.74s). Alarm/idle completion is not established by that attempt. The
observer waited for post-invocation logs before reading the response body, which
could delay invocation completion. The harness now consumes and closes the
bounded unary response first, preserves it for Connect decoding, and labels
telemetry timeouts separately. Deterministic tests verify closure, preservation
and the 16 KiB bound.

The buffered rerun (`staging-lifecycle-buffered.log`) again passed replacement
and rotation, then identified the boundary precisely: alarm-owner acquisition
returned HTTP 200, but its authority event was absent from the live tail. It
failed after 102.85s and 26 client attempts. Buffering did not fix the missing
telemetry, so response ordering is not an established root cause. Read-back
confirmed observability and both log sampling rates remained enabled/1, with
the same SQLite namespace. The successful acquisition may have committed and
expires normally; observer failure must not be represented as server rollback.

The runner now closes the tail that spans code/secret updates and attaches a fresh
tail before admitting the independent alarm scenario. Tail attachment may itself
replace an instance, so the idle marker comparison begins after attachment and
after releasing the alarm successor. However, the next attempt
(`staging-lifecycle-fresh-tail.log`) failed even earlier: its initial OpenSession
returned HTTP 200 but no authority event was observed within 30 seconds. It
stopped after 54.35s / one client attempt, before rotation or the fresh-observer
alarm phase. That phase therefore remains unverified; tail longevity alone does
not explain the failure. The unused candidate key was removed only after
confirming that this attempt never invoked `staging-secret`.

A bounded read-only query against the documented
[Workers Observability REST API](https://developers.cloudflare.com/api/resources/workers/subresources/observability/subresources/telemetry/methods/query/)
returned **HTTP 403, code 10000** with the existing Wrangler OAuth credential.
No raw telemetry or credentials were persisted by the probe. The documented
query permission is named `Workers Observability Write`, even for this temporary
read query. No new API credential or permission grant was created.

| Check | Observed result |
| --- | --- |
| Normal redeploy reconstructs authority | Passed twice: per-instance marker changed |
| Owner, immutable receipt and admitted FIFO waiter survive | Passed twice, including replay and increasing fences after handoff |
| Old/new key overlap, same identity and namespace restriction | Passed twice |
| Old-key removal | Passed twice: acquisition and status rejected the old key |
| Recover/renew/release with replacement key | Passed twice; renewal replay preserved fence and receipt |
| Previously admitted waiter after key revocation | Passed twice; revocation blocks new admissions but does not cancel admitted work |
| Alarm-only expiry/grant with no client RPC | Blocked: missing authority telemetry after successful acquisition |
| Idle reconstruction and session cleanup | Not reached |
| Expired session rejection and session/fence continuity after hosted idle | Not reached; existing deterministic local coverage still passes |

## State left behind and limits

Final read-back (`staging-lifecycle-inventory.json`) verified:

- Worker `pintle-staging` at
  `https://pintle-staging.aleksa-novcic31.workers.dev`.
- Active code version `91ac8c08-b8ee-4724-a17d-da2ad3a1eb0d` at 100%, deployment
  `cf6e34f9-e12a-4529-b35e-5a1d22f20dc9` (2026-10-27 18:47:49 UTC).
- Original SQLite namespace `11349c2a8e6548c488575dd872a48f1b`, static auth,
  P2 mode, metrics and `secret_text` credential binding retained.
- Normal Wasm SHA-256
  `86be573f0ef931917bf5d8824f50c6a1e5f6c1b9277a40264e69b3508acc8e1e`;
  staging config SHA-256
  `dd4d77e5dc6a189549b68d3f41fddadc4c3809025adaea864caa1d9c71970363`.
- The last completed rotation had created version
  `6b3629b8-42a5-4924-90aa-1f5a74f984a3`; the subsequent code deployment retained
  its replacement-only credential binding.

The private credential file contains one replacement key, mode 0600 inside a
0700 directory, with the original expiry **2026-10-28T16:56:09.820Z**. Keys removed
by the completed rotations are not restored. Candidate files are absent. Existing
authority history is retained; successful but unobserved sessions/leases expire
through normal lifecycle rules. At most six new resource names were used across
the five attempts and the three diagnostic RPCs on one shared probe resource.
Those resources retain fencing metadata by design; no authority was deleted to
clean up a test. The known attempts plus the third attempt's 96-request cap bound
this task's client traffic by 128 requests. Alarms and management calls are extra.

The account remained on Free; there was no quota-exhaustion response or paid
upgrade. Exact SQL/CPU/storage deltas are not established because the dashboard
counters did not reliably reflect requests. Default Cloudflare logs retain for
three days on Free; no custom retention or paid logging service was provisioned.

The hosted checks are observations of platform timing, not deterministic fault
schedules. They do not prove physical host failover, global credential revocation
latency, multi-region behavior, quota/cost behavior under load, or production
readiness. The core's deterministic tests remain the source of schedule/fault
coverage. A changed in-memory marker proves authority reconstruction, not a
particular physical failure mode.

**Next step at that point:** obtain explicitly authorized access to persisted staging telemetry
or resolve missing DO events with Cloudflare, then rerun the isolated alarm/idle
phase. Do not remove the observation assertions or infer alarm-only progress from
a status RPC that can itself advance state. The fresh-observer path is implemented
and locally checked but has not completed remotely. No further automatic retries,
production deployment, commit or push were performed.

Reproduction uses the pinned Hermit tools: `./bin/just test` locally, and the
explicitly authorized `./bin/just test-staging-lifecycle` with
`CLOUDFLARE_ACCOUNT_ID=5479fed88dbcde9491c75cce6a2e9a19` and the above endpoint in
`PINTLE_STAGING_URL`. The command reads the private credential file; never put a
token in command arguments. Final Just formatting, `git diff --check`, local
Markdown-link checks and a value-based credential scan passed. Failed-attempt
transcripts remain in ignored `test-results/`; this document records their limits.

## Persisted-log alarm and idle follow-up

On 2026-10-27 the user supplied a temporary observability API token in ignored
`.hermit/staging/observability-token` (mode 0600). A dry query scoped to this
account's `pintle-staging` logs returned HTTP 200 and historical authority/alarm
events. The earlier OAuth 403 is resolved for this workflow by using the separate
API token. It is not exported to Wrangler or used for deployment/secret access.
Two bounded diagnostic queries inspected event shapes without printing raw
metadata, credentials or API error bodies.

`./bin/just test-staging-alarms` **passed**, exit 0, with the existing account and
endpoint variables. Transcript: `test-results/staging-alarms-persisted.log`.
Test duration **397.68s**, Go package **398.856s**. Actual usage: **10 client RPC
attempts, 75 management log queries, one new resource**. The management queries
do not invoke the Worker; subsequent alarms and SQL work are additional usage.
There was no deployment, application-key rotation, authority reset, paid-plan
upgrade, commit or push in this follow-up. The active code version, namespace,
artifact hash and original application credential expiry above are unchanged.

Observed sequence:

1. Open a session, acquire an owner and confirm a FIFO waiter is Pending.
2. Stop all client RPCs. Query persisted logs until exactly three authority fetch
   events and the successful expiry alarm are visible, in the expected order.
3. Inspect the granted successor only after observing the alarm. Its fence is
   greater than the owner's, and its immutable TTL-derived grant time matches
   the expiry alarm (within one second of the provider log timestamp). Replay
   preserves the receipt; release succeeds.
4. Observe the sixth/final pre-idle fetch at **19:08:06.809 UTC**. Send no Worker
   RPC while awaiting the normal session cleanup alarm.
5. Observe a successful alarm with a **different authority instance marker**
   after **300,023 ms** (19:13:06.832 UTC), with no intervening fetch events.
6. The expired session's prepared request returns FailedPrecondition. A new
   session and grant advance both session ID and fencing token; final release
   succeeds.

This closes the alarm/idle observation blocker for the bounded scenario. It
establishes observed authority reconstruction and persistent history, not proof
of physical host failure or global failover semantics. The live-tail root cause,
multi-region revocation, WAN/load/cost measurements and remaining production
release gates are still open. The final fresh session expires normally later;
the resource retains fencing metadata by design.

The runner uses dry, staging-filtered queries over a fixed time window and
re-reads it for delayed/out-of-order ingestion. It validates event shape, service,
counts and ordering, deduplicates identical IDs, and fails on conflicting IDs,
truncated windows or unexpected status. Bounds are 90 management queries, 200
events / 2 MiB per response, 96 client attempts and an eight-minute scenario
deadline. Exclusive staging traffic is required because application events omit
resource identifiers. Free-plan allowances remain those verified above; no
quota-exhaustion response occurred. Exact SQL/CPU/storage deltas remain unmeasured.

Deterministic observer tests passed through Hermit/Just, including out-of-order
and duplicate entries, late arrivals, wrong services, malformed events, private
metadata suppression, conflicting IDs, truncation and the query budget. No new
dependency or core/protocol change was needed.

The first full local run found a separate inspection-helper failure in
`TestP2Rollback/release-grant-error`: Node's read-only SQLite connection returned
`database is locked` while reading the schema. The helper now sets a bounded
five-second SQLite busy timeout before inspection; every snapshot read and
rollback assertion is still required. Domain scheduling and logical clocks are
unchanged. The final full-suite rerun is recorded below.

Final `./bin/just test`: **passed, exit 0**. Transcript:
`test-results/persisted-alarms-full-suite-final.log`. All 25 core and two auth
tests, schema/codegen checks, formatting, Clippy, Go vet/race and normal/fault
runtime tests passed. SDK package: 1.205s; hosted guards: 1.209s; normal integration:
63.145s; instrumented integration: 85.443s. The normal Wasm artifact was restored.
Final Just formatting, staging configuration checks, Markdown file links and
`git diff --check` passed. A value-based scan for both the application credential
and observability token found neither in Git-visible files or top-level evidence
files. Both private files remain mode 0600. The observability token remains local
for authorized follow-up and can be revoked after testing; it was not uploaded as
a Worker secret.

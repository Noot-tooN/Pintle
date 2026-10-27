# Isolated staging and hosted smoke tests

First deployed and tested on Cloudflare Free on 2026-10-27 under explicit user
authorization. See [hosted results](staging-results.md) for the exact version,
credential expiry and observed checks. Remote operations remain explicit opt-ins;
they are not part of CI. This is P3 smoke evidence, not production readiness or a public release. See
[ADR-015](adr/ADR-015-isolated-staging-validation.md) for the scope decision.

## Target and account

`wrangler.staging.jsonc` defines the separate `pintle-staging` Worker, its own
`PintleAuthority` SQLite namespace, a workers.dev URL, static authentication and
sanitized metrics. No custom-domain route, preview URL, test scheduler, seed API
or production binding is configured. The normal build command is fixed in the
configuration. The local-only `wrangler.jsonc` remains unchanged.

The existing SQLite migration declaration is reused for a new namespace; it does
not migrate/copy local databases or share another Worker's authority history.
Keep this Worker name, class/binding and account stable after the first deployment.
Do not delete/recreate its storage to reset test counters. See
[the recovery policy](recovery-policy.md).

Account IDs are not secrets, but are operator-specific, so none is checked in.
Select the intended account for each remote command:

```sh
export CLOUDFLARE_ACCOUNT_ID='<selected-account-id>'
./bin/just staging-account
```

This command verifies membership through Wrangler's existing login and prints
only the selected account's name/ID. It does not establish the account's Workers
billing plan, workers.dev subdomain, available quota, or deploy successfully.
A different or nonexistent account is rejected before a deployment command.

## Commands and effects

All commands use the existing Hermit toolchain and pinned Wrangler; no dependency
was added. Command orchestration stays in the root `justfile`.

| Command | Effect |
| --- | --- |
| `./bin/just staging-check` | Local assertions for intended Worker, bindings, modes, normal build and routing |
| `./bin/just staging-account` | Read-only authenticated account membership query; explicit account ID required |
| `./bin/just staging-dry-run` | Normal Wasm build and Wrangler packaging/validation, **no upload**; output under ignored `test-results/staging-bundle/` |
| `./bin/just staging-deploy` | **Remote mutation**, only after authorization; checks configuration/account, builds and deploys only the staging target |
| `./bin/just staging-secret` | **Remote mutation**, only after authorization; hidden prompt for staging `PINTLE_CREDENTIALS` JSON |
| `./bin/just test-staging` | **Remote test mutations**, only when explicitly invoked with endpoint/token; creates test sessions/locks within the request budget |
| `./bin/just staging-origin` | Read-only check that the staging URL belongs to the selected account |
| `./bin/just test-staging-lifecycle` | **Remote mutations**: build/deploy normal staging, redeploy with admitted work, rotate the application key twice, observe alarms/instance replacement |
| `./bin/just test-staging-alarms` | **Remote test mutations** on the existing deployment: observe expiry/idle alarms through persisted logs; no deployment or secret update |
| `./bin/just test-staging-capacity` | **Remote test mutations** on the existing deployment: bounded disconnect/renewal faults, session/waiter limits and WAN samples; no deployment or secret update |
| `./bin/just staging-capacity-metrics` | Read-only persisted invocation query for the last capacity run; sanitized CPU/wall summaries, no Worker RPC |

Neither `just test` nor CI invokes deployment, secret writes or hosted smoke tests
by default. `TestStaging` skips without `PINTLE_RUN_HOSTED=1`; `test-staging` sets
that opt-in and rejects missing endpoint/token. Do not export the opt-in globally.
The ordinary suite checks endpoint/budget guards and runs the exact smoke scenarios
against local Wrangler with static authentication.

## First deployment, when authorized

1. Confirm the account, free/paid plan, available quotas and workers.dev subdomain
   in Cloudflare. Recheck that the staging Worker name is not occupied. Account
   membership alone is not billing verification.
2. Run `just test`, the separately networked `just audit`, and `just staging-dry-run`.
   Deploy only a normal artifact; the staging build never enables `test-hooks`.
3. Invoke `just staging-deploy`. The first deployment has no application secret:
   RPC authentication fails closed until the next step. `/health` is public and
   is not a credential or database-readiness check.
4. Prepare one expiring, high-entropy application credential authorized only for
   namespace `staging`, with a dedicated tenant and subject. Use the exact static
   credential JSON schema in [deployment.md](deployment.md). Set it using
   `just staging-secret`. This application token is separate from Wrangler's
   Cloudflare OAuth credential. Neither belongs in Git, shell command arguments,
   logs or chat. The first staging exercise keeps its credential in the private,
   Git-ignored local file described below.
5. Set `PINTLE_STAGING_URL` to the returned
   `https://pintle-staging.<account-subdomain>.workers.dev` origin. Load
   `PINTLE_STAGING_TOKEN` securely into the environment from your secret manager
   or a non-echoing local prompt; do not put its literal value in shell history.
   Run `just test-staging`, then unset the token. Custom domains and production
   Worker names are deliberately rejected by this first runner.
6. Record the source/deployment revision, account/endpoint, sanitized test output,
   failures and Cloudflare usage metrics. Revoke an unused credential, or retain it
   only for the short repeat-testing window bounded by its expiry. Revocation does
   not cancel already admitted intents.

## Hosted smoke scope and bounds

The runner uses the Go SDK and generated Connect client over the real public API:

- Invalid credential rejection; acquire/contention, conflicting-payload rejection,
  immutable replay, Renew/Release retries, stale-owner rejection, status and ACK.
- Four distinct concurrent requests, then four identical retries of the winner.
  Assertions require one winner and stable receipts, not a particular network order.
- Registered pending cancellation and a waiting successor granted after Release.
  A returned Pending/status observation establishes admission; no guessed sleep
  selects the concurrency winner. Status observation polls at most twenty times.
- A real successful upstream response consumed and discarded by the client test
  transport; status and same-attempt retry recover the committed result.
- Expiry handoff via server-owned Acquire, without client-clock ownership decisions.

The suite has a 90-second context, 12-second HTTP watchdogs, a maximum of 96
transport attempts, no redirect following and five unique authorized resource
names per invocation. Successful paths Release their final owner. A failed run
may leave leases/pending intents until their at-most-one-minute TTL/deadline, and
sessions until normal reclamation. **Fencing metadata for new resource names
remains**; the runner never deletes authority storage. Repeated runs consume
additional storage and request/row/duration quotas. This is a smoke suite, not a
load generator or guaranteed cost estimate; automatic alarm work is additional
platform usage beyond the client request budget.

Ordinary local tests remain deterministic for exact clock/scheduling boundaries.
These public-API smoke tests cannot control network arrival order or hosted clocks.
They assert invariants and use observed admission barriers. They are locally
validated using the same normal adapter, not a mocked server.

## Local staging credential

The first exercise created `.hermit/staging/credentials.json` (directory mode 0700,
file mode 0600, ignored by Git). It contains one random 256-bit token, scoped to
tenant `pintle-staging`, subject `hosted-smoke`, namespace `staging`, expiring at
**2026-10-28T16:56:09.820Z**. This local plaintext file is not a secret manager;
do not copy it into source, logs or chat. Cloudflare holds it as `PINTLE_CREDENTIALS`,
separate from the Wrangler OAuth login. Authentication rejects it at expiry;
expiry requires no scheduled job and does not erase the secret or local file.
The lifecycle rehearsal rotated this token and verified rejection of the removed
keys. The file now contains the replacement key with the same identity and expiry.

To rerun while valid, load the token in process memory and invoke the same Just
recipe (no token value in shell history):

```sh
export PINTLE_STAGING_URL='https://pintle-staging.aleksa-novcic31.workers.dev'
./bin/node --input-type=module <<'JS'
import {readFileSync} from 'node:fs';
import {spawnSync} from 'node:child_process';
const [credential] = JSON.parse(readFileSync('.hermit/staging/credentials.json', 'utf8'));
if (Date.now() >= Number(credential.expires_at_ms)) throw Error('Staging credential expired');
const result = spawnSync('./bin/just', ['test-staging'], {
  stdio: 'inherit', env: {...process.env, PINTLE_STAGING_TOKEN: credential.token}
});
process.exit(result.status ?? 1);
JS
```

After expiry, a future authorized exercise must replace it with a fresh scoped,
expiring credential and upload through `just staging-secret` via stdin or the
hidden prompt. Never extend the lifetime of a previously exposed token.

## Evidence still missing

The [first hosted smoke run passed](staging-results.md). The
[follow-up](staging-lifecycle-results.md) verified redeployment reconstruction,
preserved owner/receipts/queue and credential rotation from one client location.
The subsequent persisted-log test verified alarm-only handoff and idle
reconstruction, plus expired-session rejection and monotonic history. Global
credential propagation, representative saturation/cost and arbitrary failover
remain open. A [bounded capacity pass](staging-capacity-results.md) now supplies
single-location WAN samples and disconnect/maintenance/limit checks. The old
live-tail observer is still unreliable; the scoped API token
unblocked the separate persisted-log workflow.
Exact large counters and every-write rollback remain local instrumented evidence;
there is no hosted counter-seeding/fault endpoint. Separate approved rehearsals
and observability are needed for the remaining [P3 gates](deployment.md).
Independent security review and production release approval remain open.

## Lifecycle and rotation rehearsal

Run only with exclusive staging traffic, verified Free-plan headroom, the selected
`CLOUDFLARE_ACCOUNT_ID`, and `PINTLE_STAGING_URL` set to that account's exact
staging origin. The private credential file must contain one staging-only key with
at least 15 minutes remaining:

```sh
./bin/just test-staging-lifecycle
```

**Current limitation:** this combined live-tail command has not passed. Its
restart/rotation phases passed, but missing authority events stop the observer.
Use the independently passing `test-staging-alarms` recipe below for alarm/idle
validation. Do not weaken live-tail assertions or treat its failures as passes.

Allow at least six minutes after a previous hosted run/probe for its session alarms
to finish before starting this single-resource observation. The live trace omits
resource identity and must not be interpreted as resource-correlated under load.

This command is intentionally absent from default tests and CI. It uses one new
resource, a 96-client-request cap, two normal code deployments (initial telemetry
build and replacement), two secret updates (overlap then replacement-only), and a
temporary live tail. No storage reset, migration or paid API is used. It has a
12-minute watchdog; the normal idle-expiry observation takes five minutes. Alarm
work and Cloudflare management API calls are additional to the client-request cap.

Requests and queued intents keep their full original attempt envelopes across
deployment and rotation. A random per-construction diagnostic marker establishes
observed instance changes. The runner discards raw tail metadata/headers and
disables Wrangler disk logs; only allowlisted authority events are consumed.
Because events omit resource identity, other traffic invalidates attribution.
Tailing can itself replace instances, so it starts before creating test state.
The runner reattaches its tail after rotation, before admitting alarm-test work;
the idle replacement comparison begins after this attachment. A missing log is
an observation failure, not proof that an RPC did not commit.

Staging enables Cloudflare's default observability configuration. Free-plan logs
retain for three days (200,000 events/day allowance, checked 2026-10-27). A live-only
configuration did not deliver authority metrics in our probes. No paid logging
product is provisioned. The application metric flag controls emission separately.

The new token preserves the old key's tenant, subject, namespace and expiry.
After each confirmed upload, the runner atomically updates the private local
credential file to match. An unfinished `rotation-candidate.json` is a recovery
artifact: investigate the last confirmed upload before retrying. Do not blindly
restore a possibly revoked key. Successful removal of the old key also removes
the candidate file. Losing the process between remote success and local update
still requires explicit reconciliation; this is not a cross-system transaction.

Exact local clock/fault schedules remain deterministic. Hosted lifecycle tests
use observed admission, log events and bounded propagation probes; platform
timing, availability and geographic rollout are not under test control.

## Persisted-log alarm and idle rehearsal

The scoped API token belongs in `.hermit/staging/observability-token` (mode 0600),
separate from `.hermit/staging/credentials.json`. It needs the account-level
`Workers Observability Write` permission required by Cloudflare's query API.
The runner uses it only for dry queries filtered to `pintle-staging`; it never
passes this token to Wrangler. API credentials and raw log bodies are not printed.

With the same explicit account and endpoint variables, run:

```sh
./bin/just test-staging-alarms
```

This uses the already deployed normal artifact. It creates one resource, confirms
Pending admission, then sends no Worker RPC until a successful persisted expiry
alarm is observed. It verifies the successor receipt, replay and release. It then
makes no Worker RPC while waiting for the normal session-expiry alarm and checks
that the authority instance changed, expired requests fail, and session/fencing
history advances. Management log queries do not invoke the Worker.

Allow prior session alarms to finish and keep staging otherwise idle. The runner
rejects unexpected event counts, wrong service, truncation and invalid events;
it re-reads the run window to tolerate delayed and out-of-order ingestion. Bounds
are 96 client attempts, 90 management queries (five seconds between unsuccessful
polls), 200 events / 2 MiB per query, eight minutes for the scenario and ten minutes
for the Go process. Alarm and SQL work are additional platform consumption.
No new dependency, plan change, production deployment or credential rotation is
part of this recipe. Revoke the temporary observability token when no longer
needed. See [the evidence](staging-lifecycle-results.md) for actual run results.

## Bounded resilience and capacity rehearsal

Use the existing normal deployment, otherwise idle staging, the account/endpoint
variables above and one private staging-only credential with at least 15 minutes
remaining. Verify the account's Free plan and available quotas before running:

```sh
./bin/just test-runtime '^TestHostedCapacityLocally$'
./bin/just test-staging-capacity
./bin/just staging-capacity-metrics
```

The hosted scenario has a six-minute deadline, seven-minute process watchdog,
600 client-attempt cap, six unique resources and at most 16 in-flight HTTP requests.
Responses are limited to 16 KiB; redirects are rejected. A 5xx stops new request
admission. Unexpected errors fail the scenario; the three intentional capacity
rejections are asserted explicitly. No open-ended load generator or saturation
search runs. Durable Object calls, alarms, SQL and log events are additional
platform consumption, not covered by the client-RPC count.

The exact same scenarios run locally in the default suite. They observe Pending,
ACK and Lost barriers; response faults occur only after a successful upstream
response is fully read. No concurrent winner or WAN latency threshold is asserted.
Hosted platform clocks and scheduling remain observational. Successful cleanup
releases owners and cancels pending intents; bounded sessions expire by normal
alarms. Six resource histories retain fencing metadata, with no destructive reset.

Metrics use the existing private observability token and a fixed run time window.
Bounds are 12 dry management queries, five-second polling, 2,000 invocation events
and 8 MiB per response. Only aggregate counts, CPU/wall statistics and allowlisted
outcomes are saved. Raw headers, URLs and credentials are discarded. Run with
exclusive staging traffic: time-window counts alone cannot prove attribution.
Incomplete ingestion is reported as incomplete evidence, never inferred billing
or resource utilization. Results are private ignored JSON under `test-results/`.
See [ADR-017](adr/ADR-017-bounded-hosted-resilience-capacity.md) and
[the measured results](staging-capacity-results.md).

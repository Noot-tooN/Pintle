# ADR-016: Observe hosted authority replacement and credential rotation

Accepted for staging validation, 2026-10-27.

Successful redeployment alone does not prove a particular authority was recreated.
Add an opt-in `pintle.authority` adapter event after fetch/alarm handling, containing
only operation, HTTP status and a random instance marker initialized on that
instance's first event. Construction creates an empty in-memory OnceCell; entropy
is requested in an invocation context.
The marker is diagnostic, unrelated to resource identity, never persisted in
coordination storage, never
returned in the protocol, and never used for ownership or fencing. If diagnostic
randomness fails, omit the event instead of changing coordination behavior.
Existing RPC duration metrics keep their shape. No core or protocol change.

The staged lifecycle rehearsal uses one unique resource, immutable prepared
requests, observed Pending admission, a normal redeployment, then overlapping
same-principal credentials followed by old-key removal. It reconciles already
admitted work through the replacement credential. Propagation is observed with
bounded same-attempt probes, not guessed sleep barriers.

A live Wrangler tail observes instance changes and successful alarms. Staging
uses Cloudflare's documented `observability.enabled=true` configuration. A
live-only configuration with persistence/invocation logs disabled did not deliver
authority events in our probes; default observability did. These probes do not
isolate which setting caused that difference. Free-plan logs retain for three
days. The application metric flag controls emission separately. Its raw
request metadata/headers are discarded in memory; only strictly allowlisted
four-field authority events are consumed. No public inspection/reset/fault route
is introduced. The rehearsal requires exclusive staging traffic because events
deliberately contain no resource identifier. Start tailing before admissions;
enabling tail itself can cause platform instance replacement.

Close that observer after rotation and attach a fresh tail before admitting the
separate alarm scenario. A long-lived tail spanning the code/secret updates lost
authority events in rehearsals even though RPCs returned 200 and logging settings
remained enabled. Consume bounded unary response bodies before awaiting logs.
Do not turn a telemetry timeout into a claim that a mutation rolled back.

After a pending response, send no client RPC until a successful alarm is observed.
After releasing all owners, wait for the normal five-minute session-expiry alarm
without RPCs. Observe an instance change, reject expired attempts, then verify
session IDs and fences continue increasing. Hosted clocks/eviction are controlled
by Cloudflare: these are bounded observational rehearsals, not deterministic
failure schedules or proof of arbitrary failover. Deterministic schedules remain
in local Rust and instrumented runtime tests.

Use existing Hermit pins and Just commands; remote orchestration is opt-in and
excluded from CI/default tests. Application credentials remain expiring, private
and Git-ignored. Candidate keys survive a failed update for reconciliation; never
automatically restore a potentially revoked old key. No paid-plan upgrade,
authority reset, migration, production deployment or push is part of this work.

## Persisted-log observer follow-up

The existing OAuth login could not query persisted telemetry. A user-provided,
account-scoped `Workers Observability Write` API token now permits dry ad-hoc
queries. It is stored privately in ignored `.hermit/staging/observability-token`,
separate from application credentials and Wrangler authentication; never export it
as Wrangler's deployment token.

`just test-staging-alarms` observes the existing deployment without attaching a
live tail, deploying code or rotating keys. Its queries filter `pintle-staging`
and a fixed run time window. Polling only the management API cannot drive the
authority's state. Re-reading the window preserves delayed/out-of-order events;
event IDs deduplicate identical entries, and conflicting duplicates, truncation,
foreign services, malformed authority events and failures stop the test.
Only validated instance/operation/status, timestamp and event ID survive parsing;
raw provider metadata and API error bodies are discarded.

The run requires exclusive staging traffic and prior session alarms to have
finished. It verifies the expected fetch/alarm counts and order before issuing
subsequent RPCs. The idle alarm must occur after at least 290 seconds without a
fetch and have a different instance marker. This establishes observed authority
reconstruction; it does not identify physical host failure or guarantee eviction
timing. The original live-tail runner and its failed results remain historical
evidence, not a prerequisite for using the persisted observer.

Bounds: one new resource, 96 client transport attempts, 90 log queries, 200 events
per query, 2 MiB per query response and an eight-minute scenario deadline. No
dependency, core, protocol, deployment, credential or storage-reset change is
needed for this follow-up. Deterministic decoder/budget tests run in the local
suite; real platform timing remains observational.

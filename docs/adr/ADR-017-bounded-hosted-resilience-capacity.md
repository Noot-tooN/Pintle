# ADR-017: Bound hosted resilience and capacity validation

Accepted for staging validation, 2026-10-27.

Extend P3 evidence against the existing isolated Free-plan staging deployment.
Use shared Go public-API scenarios, exercised first under local Wrangler. Keep
authoritative algorithms, schema, storage and Cloudflare adaptation unchanged.
Hermit supplies the existing dependencies; Just owns command orchestration.

Bound the exercise to six resources, 600 transport attempts, 16 concurrent HTTP
requests and six minutes. Stop admission after a server error. Assert expected
session/waiter exhaustion explicitly; release owners and cancel waiters after
success. Session expiry handles bounded recovery state, while resource histories
retain fencing metadata. Do not delete authority history to clean up test usage.

Use observed Pending/ACK/Lost barriers and fully received successful responses
before dropping or delaying replies. Preserve immutable attempt envelopes for
reconciliation. These are client-side fault rehearsals, not simulated physical
Cloudflare failures. Hosted timing cannot be deterministic; exact time/race
schedules remain in local models and instrumented integration tests.

Measure small fixed batches of uncontended grants and busy responses at concurrency
1/4/8, plus 16 idle waiters. Never assert latency thresholds or claim these batches
find saturation. Record sample counts with nearest-rank percentiles. Request
latency and persisted invocation CPU/wall time are different from billed duration,
memory and SQL/storage usage; missing measurements remain explicit gaps.

Use the existing scoped observability credential for at most 12 dry queries of
the run window, filtered to the staging service and invocation events. Deduplicate
event IDs and discard raw headers/URLs; persist only aggregate metrics. Exclusive
staging traffic is required. Count shortfalls mean incomplete evidence and must
not be described as complete CPU/resource accounting.

No deployment, credential rotation, plan change, new dependency or production
operation is required. This decision closes only the scenarios recorded in
[the results](../staging-capacity-results.md); P3 release gates remain open.

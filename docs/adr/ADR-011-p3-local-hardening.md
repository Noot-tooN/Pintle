# ADR-011 — P3 local hardening and release gates

Status: accepted for the local implementation, 2026-10-26. Hosted release approval remains open.

## Context and decisions

P2 supplies the authoritative lease and bounded FIFO algorithms. P3 must improve
client failure behavior, authorization, observability, and evidence without moving
coordination into an SDK or a cloud-specific layer.

1. Keep coordination in `pintle-core`. A separate native-testable `pintle-auth`
   crate validates bounded static credentials; the Cloudflare adapter supplies
   secret JSON and admission time. This separates authorization from coordination
   and avoids a Cloudflare dependency in either policy crate. It reuses locked
   serde/serde_json/subtle dependencies; no new external package is introduced.
2. Add explicit `PINTLE_AUTH_MODE=static` with `PINTLE_CREDENTIALS`, required expiry,
   explicit namespace allowlists, stable tenant/subject identity, and overlapping
   rotation. Invalid configuration fails closed, without fixture fallback. Auth
   is checked at gateway admission. Expiry/removal denies new RPCs; it does not
   cancel already accepted durable intents or revoke leases. This is a deliberately
   limited operator-managed policy, not an identity provider or immediate revocation
   protocol. Full production auth operations still require staging validation.
3. Add opt-in Go `Maintain` using a dedicated recovery session. It renews only an
   existing capability, ACKs only its own renewal results, and permanently signals
   Lost on uncertainty, cancellation, or Stop. It never reacquires or releases.
   The exact unacknowledged renewal is retained for reconciliation. RPC plus ACK
   has a TTL/3 budget; a monotonic elapsed-time check rejects late success, and
   the next renewal begins after TTL/3. Lost is not an ownership oracle: process
   pauses and external effects still require resource-side fencing.
4. Validate SDK response identities, canonical counters, authority-time bounds,
   revisions and expiry observations; cap responses at 16 KiB. Default transport
   rejects redirects and requires HTTPS except loopback. Supplied transports must
   preserve those policies and honor cancellation. Debug formatting redacts client
   credentials and maintenance capabilities.
5. Emit opt-in fixed-label JSON RPC/alarm duration/status events at the adapter.
   No resource names, credentials, capabilities, request bodies or raw errors
   enter these metrics. Export/aggregation remains deployment work.
6. Fail closed if a retained queue ticket exceeds its allocation counter. This
   is local corruption detection, not a safe snapshot-restore protocol.

## Evidence and consequences

See [P3 results](../p3-results.md): compiled normal/instrumented Wasm, generated
Go client against local Wrangler, all prior faults and deterministic schedules,
an independent two-waiter model, authorization boundaries, lease maintenance
failure orders, transport rejection matrix, metrics inspection and local latency.
Hermit remains the dependency/toolchain entry point. No cloud deployment or push.

No new backend, database, streaming API, authority migration, general exactly-once
claim, public release, or hosted reliability guarantee is introduced. The static
key policy and log metrics are deliberately bounded first implementations. P3's
hosted/WAN, operations, cost and release-review gates remain open.

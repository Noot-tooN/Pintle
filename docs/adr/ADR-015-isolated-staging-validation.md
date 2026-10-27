# ADR-015: Isolated staging with opt-in public-API smoke tests

Accepted for preparation, 2026-10-27. Deployment remains separately authorized.

Use a separate `wrangler.staging.jsonc` targeting `pintle-staging` and its own
SQLite authority namespace. Keep the account ID operator-selected through the
environment, verified against authenticated membership before remote mutations.
Keep the local configuration unchanged. Require static application credentials
and a normal Wasm build; do not expose the local test scheduler or storage controls.

Keep build/account/deploy/secret/test orchestration in Just using existing Hermit
pins. Deployment and secret recipes are explicit remote operations, never CI or
full-suite dependencies. Packaging dry runs remain local.

Share bounded public-API Go smoke scenarios between local Wrangler integration
and an explicitly enabled hosted entry point. Restrict that entry point to the
staging workers.dev origin pattern and reject missing/invalid credentials before
sending requests. Bound client requests, runtime and new resource names. Use
observed admission/status to order handoff tests; network races assert aggregate
invariants, while exact scheduling and fault injection remain local.

Hosted smoke success will not establish alarm-only cleanup, cold eviction,
failover, cost, credential rotation or production readiness. Retained fencing
metadata from test resource names is expected and must not be reset as cleanup.
See [staging.md](../staging.md) for the operational contract and
[preparation evidence](../staging-preparation-results.md) for actual checks.

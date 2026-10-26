# P3 security review — local first pass

> Tooling update: command names below now use the root `justfile`. The milestone
> runs originally used equivalent shell scripts; their captured transcripts are
> unchanged. See [Just migration results](just-migration-results.md) for a full
> suite run through Just.

Reviewed 2026-10-26 as part of implementation. This is a self-review with executed
regression/advisory tests, not an independent security audit or production signoff.

## Boundaries and controls

Untrusted public requests reach a bounded Connect unary/binary gateway. The
adapter authenticates and binds tenant/subject/namespace before routing to one
resource authority. Coordination uses one atomic SQLite transition and generic
Rust core logic. A lease capability authenticates ownership; client-supplied holder
labels are not authorization. Cross-tenant resources remain separate, and another
principal cannot read a session's retained results.

Static credential JSON rejects malformed entries even if an earlier key matches.
Token comparison uses the existing constant-time primitive; validation and lengths
are not claimed to be timing-independent. Secrets must contain randomly generated
high-entropy ASCII alphanumeric tokens, 32–256 characters, never human passwords.
The code validates format, not entropy. Key validity is checked at gateway admission
using adapter time. Rotate with overlap and stable tenant/subject identity. Removing
keys prevents new requests; it does not revoke admitted intents or downstream work.

The default SDK requires HTTPS except loopback, rejects redirects and URL credentials,
limits replies and verifies response correlation. Custom HTTP transports must
preserve these controls and respect context cancellation. Metrics use fixed labels
and omit bodies/capabilities. Debug formatting redacts Client, Lease and Maintenance;
applications must still avoid serializing raw request/response objects to logs.

## Findings addressed

- SDK could accept incomplete/inconsistent lease observations: identity, canonical
  numbers, revision/expiry ordering, status and wait-response correlation checked.
- Client debug formatting could reveal bearer credentials: redacted.
- A regressed queue allocation counter could evade integrity checks: retained
  tickets above the counter now abort atomically.
- Fixture-only authentication lacked rotation/expiry policy: explicit static mode,
  separate required secret, validity interval and fail-closed validation added.

See [P3 evidence](p3-results.md) for deterministic and actual-runtime checks.
`./bin/just audit` queried npm audit and OSV: zero reported findings on
2026-10-26. It sends only public dependency names/versions to OSV; no source,
credentials or private workspace module. Network/query errors fail the check.
The audit is separate from deterministic correctness because advisory databases
change. Hermit tools, OS packages and arbitrary platform internals are outside
this package-advisory coverage.

## Remaining risks and release obligations

- No tenant-wide rate limits, resource-count quotas or abuse/cost controls. Bounded
  per-resource queues do not stop creation of unlimited resource names.
- Static secret distribution, rotation propagation, operator access and emergency
  response need hosted validation. No OIDC/JWT provisioning or immediate lease/
  pending-intent revocation protocol is implemented.
- Alarm delay, clock jumps, authority replacement and restore can affect availability
  or invalidate assumptions. No safe snapshot rollback/handoff protocol is claimed.
- Local transaction tests do not prove arbitrary Cloudflare partitions/failover.
- External systems must enforce fencing. Lost, Release, TTL and request idempotency
  cannot make arbitrary external operations exactly once or stop paused clients.
- Metrics exporter, retention/access controls and service alerts require deployment
  configuration. Test state contains local-only capabilities; keep test-results
  and temporary secrets ignored. The suite removes temporary `.dev.vars` files.
- Before public release, assign a private vulnerability-reporting channel and an
  incident owner; obtain independent review and explicit release approval. No
  unverified contact address is published here. Existing Apache-2.0 is unchanged.

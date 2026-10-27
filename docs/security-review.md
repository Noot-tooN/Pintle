# P3 security reviews — local evidence

> Tooling update: command names below now use the root `justfile`. The milestone
> runs originally used equivalent shell scripts; their captured transcripts are
> unchanged. See [Just migration results](just-migration-results.md) for a full
> suite run through Just.

Reviewed 2026-10-26 as part of implementation. This is a self-review with executed
regression/advisory tests, not an independent security audit or production signoff.

Latest follow-up: [separate P3 security review and remediation](security-pass-results.md)
records a frozen pre-fix review, the Client-value credential-redaction finding,
deterministic regression and expanded P2 principal-boundary checks. Earlier dated
results below remain historical; independent external review is still open.

## Separate local review, 2026-10-27

Reviewed the implementation at `6a95342a0b2504c061eff39ac94b86b3b77eccdf` after
writing the recovery runbooks, then verified the fixes below in the working tree.
This was a separate security-focused pass by the same AI agent/session. It is not
an independent human assessment and does not close that release gate.

Scope: public/DO routing, auth defaults and identity propagation, request size and
timeout limits, SQL parameter binding and transaction boundaries, lease/session
authorization and replay, queue/counter bounds, SDK transport/renewal handling,
secret exposure in errors/metrics, test-hook isolation and CI/deployment settings.
Requirements came from PROJECT_SPEC sections B3/C, GUARANTEES I3/I4/I5/I9/I10 and
the existing deployment restrictions, not solely the new test expectations.

### Confirmed findings and fixes

1. **Implicit test authentication (P2, configuration-dependent).** With P2 enabled,
   no auth mode and only a `PINTLE_TEST_CREDENTIALS` secret, the gateway accepted
   a fixture credential without expiry. This violates the intended explicit
   opt-in to development authentication. Omitted mode now selects static policy;
   absent/invalid static credentials fail closed. Generated local test configs
   explicitly select test mode. The positive default-static path is also tested.
2. **Obsolete P0 fixture exposed alongside service mode (P2, configuration-dependent).**
   With P2/static auth and `LOCAL_SPIKE=true`, `/spike` still accepted the public
   `local-a` identity and seeded a fixture resource. Initially, both handlers were
   guarded against mixed service/auth modes. The subsequent consolidation removed
   these handlers altogether (see ADR-014). This exposed separate P0 fixture
   storage and resource/cost surface; it did not establish access to P2 mutex
   records, which use different routing identities. The removed fixture can no
   longer be enabled through configuration.

Both reproductions failed against the pre-fix normal Wasm for the expected
reason: session opening unexpectedly succeeded, and fixture seed returned 200
instead of 403. The fixed runtime passed those cases. Expanded regression coverage
checks all four service modes, plus positive static-default authentication.
See [ADR-013](adr/ADR-013-safe-authentication-defaults.md).

The full-suite run exposed a harness dependency on mixed P0/P0b modes. Its rollback
and historical-replay assertions are retained: after scheduled calls finish, the
harness stops the RPC runtime, inspects the same persisted state in fixture-only
mode, then restarts RPC mode. No concurrent SQLite readers/writers in separate
runtimes or production exceptions were introduced. Existing deterministic gates
still select RPC ordering; restarts and watchdogs do not select winners.

### Coverage limits

Final verification: `./bin/just test` exited **0** on 2026-10-27 using the existing
Hermit toolchain and locked dependencies, without additional test permissions.
It passed schema/codegen/compatibility checks, Rust/Just formatting, Clippy, Go vet,
32 native core tests, 2 auth tests, SDK race tests (1.176s), normal Wrangler/SQLite
integration (43.832s) and instrumented deterministic integration (64.314s).
Normal release Wasm was restored. Local documentation links and `git diff --check`
also passed. No dependencies, core algorithms, persisted formats or wire schemas
changed. No commit, push or deployment was performed for this follow-up.

Local evidence (ignored): `test-results/security-before-fix.log` records the two
reproductions; `security-after-fix.log` records the initial focused pass;
`recovery-security-full-suite.log` records the final complete run. The six new
configuration cases run in normal integration; controlled model/fault schedules
remain in the instrumented run. The initial full run failed on the mixed-mode
harness dependency described above; the final run includes its correction.

This pass found no further confirmed defect in the reviewed boundaries. Existing
tests supply the core concurrency/fault evidence; this is not exhaustive model
checking or a whole-platform penetration test. The obsolete P0 fixture has since
been removed; the configuration guards and harness workaround described above
are historical steps, superseded by [test consolidation](test-consolidation-results.md).

The optional host Opengrep scan could not start because its log path was outside
the sandbox (`~/.opengrep/semgrep.log`), even with a temporary data-directory
override. It produced no scan evidence; no permissions were widened or tool
installed. No online advisory database refresh or hosted checks were performed.
The earlier dated advisory results below are not a fresh vulnerability verdict.
The later [Opengrep follow-up](security-pass-results.md#opengrep-follow-up-project-local-state-and-completed-scan)
resolved the log path with project-local state and completed the bounded Go scan.

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

The [recovery policy and runbooks](recovery-policy.md) now document incident
containment, same-attempt reconciliation and prohibited restoration/migration.
They are operational requirements. Bounded [hosted lifecycle](staging-lifecycle-results.md)
and [resilience](staging-capacity-results.md) rehearsals now supplement the dated
local review. Independent review remains an OSS gate; production incident readiness
is a separate operator responsibility. See the [release checklist](release-checklist.md).

### OSS security obligations and guarantee limits

- Preserve authenticated tenant/namespace/resource isolation, principal/ownership
  checks, fail-closed auth and all existing bounded state/request controls. Deferring
  SaaS policy does not weaken these protections.
- Before OSS release, obtain independent review and resolve findings, assign a
  private vulnerability-reporting channel and a maintainer responsible for triage,
  and obtain explicit release approval. No unverified contact address is published
  here. Existing Apache-2.0 is unchanged.
- Alarm delay, clock jumps, authority replacement and restore can affect availability
  or invalidate assumptions. No safe snapshot rollback/handoff protocol is claimed.
- Local transaction tests do not prove arbitrary Cloudflare partitions/failover.
- External systems must enforce fencing. Lost, Release, TTL and request idempotency
  cannot make arbitrary external operations exactly once or stop paused clients.

### Deployment responsibilities and deferred SaaS controls

- No tenant-wide rate limits, resource-count quotas or abuse/cost controls. Bounded
  per-resource queues do not stop creation of unlimited resource names. These
  controls are deferred to future hosting/SaaS work, not blockers for OSS release;
  operators must restrict access/admission appropriately for their deployment.
- Static rotation passed from one hosted client location; global propagation,
  operator access and emergency response remain unverified. No OIDC/JWT provisioning or immediate lease/
  pending-intent revocation protocol is implemented.
- Scoped persisted staging logs are configured and queried; production retention,
  access policy, metrics export, service alerts and incident ownership belong to
  the deployment operator. The OSS project documents signals and runbooks.
  Test state contains
  local and staging capabilities; keep test-results
  and temporary secrets ignored. The suite removes temporary `.dev.vars` files.

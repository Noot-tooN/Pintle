# Release checklist: OSS, deployment and future SaaS

Status: 2026-10-27. **P3 OSS release remains open.** This checklist separates
shipping the open-source core, server and Go SDK from operating a production
deployment or launching a shared SaaS. Completing one does not approve the others.
See [ADR-018](adr/ADR-018-oss-release-and-saas-boundaries.md).

## P3 OSS release requirements

Checked items record existing evidence, not a new test run for this documentation
change. Final validation must target the actual release candidate.

- [x] Portable Rust coordination core with atomic ownership/fence/recovery
  transitions; no Cloudflare or commercial customer policy in the domain logic.
- [x] Deterministic native and instrumented runtime coverage for expiry, races,
  idempotency, rollback, precision and restart; generated-client interoperability.
  See [test plan](TEST_PLAN.md) and [latest full-suite evidence](security-pass-results.md).
- [x] Preserve authenticated tenant/namespace/resource isolation, owner/principal
  authorization, fail-closed credentials, request bounds and existing bounded
  sessions, receipt windows, waiters and subscriptions.
- [x] Bounded real Cloudflare staging evidence for smoke, lifecycle, rotation,
  alarms, disconnect/reconciliation and capacity limits. Preserve the documented
  gaps, including incomplete invocation logs and unmeasured resource costs.
- [x] Finish a release-focused API/Go SDK usability and packaging review: supported
  Connect subset, versioning/compatibility policy, installation and a runnable
  example showing cancellation, Lost, recovery, explicit Release and downstream
  fencing responsibilities. Fix confirmed defects and add targeted regressions.
  See [SDK review results](sdk-review-results.md); consumer builds use a local
  replacement, not a published module proxy version.
- [ ] Obtain independent security review, resolve findings, and configure a real
  private vulnerability-reporting channel with a maintainer responsible for triage.
  The [separate local review](security-pass-results.md) recorded findings before
  remediation and fixed the confirmed issue; the same-agent pass does not satisfy
  external independence or configure the reporting channel.
- [ ] Review the release documentation as one supported contract: reproducible
  build/deployment steps, safe defaults, resource limits, telemetry/failure signals,
  recovery/upgrade restrictions and tested versus unverified guarantees. Do not
  advertise general production readiness, an SLO or unsupported recovery behavior.
- [ ] Run `./bin/just test` and `./bin/just audit` on the release candidate; record
  source/artifact versions and results, review the final diff and confirm no
  credentials or test hooks enter the release artifact.
- [ ] Obtain maintainer approval for release scope, version, existing Apache-2.0
  licensing and publication. Publishing, pushing and deployment are separate actions.

Tenant-wide request quotas, resource-count allowances, customer provisioning,
billing and a managed monitoring/on-call service are **not OSS release gates**.
Missing cost/memory/global-latency measurements restrict claims and deployment
sizing; they do not require a SaaS implementation before the toolkit can ship.
A correctness or security defect discovered by those measurements still requires
assessment and resolution within the supported release scope.

## Responsibilities before each production deployment

These belong to the operator, whether self-hosting or running a managed service.
The repository supplies controls, signals, limits and runbooks; it cannot configure
another operator's service or certify its workload.

- [ ] Choose the access/trust model, provision scoped credentials and storage
  access, and rehearse rotation and containment for that deployment.
- [ ] Validate expected workload and platform quotas/costs. Restrict admission or
  network exposure externally where needed; protect recovery and Release capacity.
  Per-resource bounds do not prevent unlimited distinct resource names.
- [ ] Configure telemetry retention/access, useful alerts and incident ownership;
  rehearse the [runbooks](recovery-policy.md), including compatible software
  upgrades/rollback. Never reset fencing history or treat snapshots as safe restores.
- [ ] Establish downstream fencing where effects require it, accept the stated
  availability/recovery limits and obtain explicit deployment approval.

## Deferred requirements for a future public SaaS

These require a separate hosting/control-plane design and implementation scope;
they do not belong in the portable coordination core.

- [ ] Customer accounts, provisioning, plans, billing and credential lifecycle.
- [ ] Tenant-wide request/concurrency budgets, resource-count allowances,
  abuse/cost policy and fair admission across resource authorities. Design their
  consistency/failure semantics explicitly; do not assume cross-DO atomicity.
- [ ] Service-wide capacity/cost characterization, SLOs, dashboards, paging,
  on-call coverage, support and incident procedures for the offered service.

Future SaaS controls must preserve existing isolation and leave a safe path for
Release and same-attempt recovery under load. Deferring them does not make the
current server suitable for unrestricted public customer traffic.

## Next P3 work

1. Arrange independent security review and vulnerability-report triage. The
   API/SDK review above is not an independent security assessment.
2. Finish the OSS documentation/guarantee review across the supported release
   surface; preserve the documented packaging and hosted evidence limits.
3. Validate the release candidate and seek explicit OSS release approval. After
   authorized publication, verify the exact published version from an external
   consumer without a local replacement; no remote-fetch success is claimed yet.

Further hosted measurement is optional, separately budgeted evidence unless needed
to investigate a concrete issue or support a new claim. The incomplete telemetry
result remains open and documented. P4 coordination helpers follow the P3 OSS
gate, not completion of a future SaaS business or its control plane.

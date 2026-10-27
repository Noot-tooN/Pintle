# ADR-018: Separate OSS release from hosted-service requirements

Accepted, 2026-10-27, following explicit user scope clarification.

Pintle delivers an open-source coordination core, deployable server and SDK.
The bounded hosted capacity follow-up incorrectly made tenant-wide admission and
resource-count controls the next P3 OSS release requirement. Those controls address
operating a shared service; the optional SaaS must not become a prerequisite for
releasing the toolkit. This supersedes that recommendation and any combined
OSS/SaaS release wording in earlier P3 summaries.

The portable Rust core retains ownership, fencing, idempotency, expiry and bounded
coordination state. Authenticated scope and principal identity remain necessary
for isolation and recovery authorization; they do not imply customer accounts,
plans or billing. The server/gateway retains authentication, routing, validation,
existing safety limits and telemetry. No protocol, routing, storage or authorization
change follows from this decision.

Defer tenant-wide quotas, resource-count allowances and commercial/abuse policy
to a future hosting/control plane. Do not introduce cross-authority coordination
or a speculative quota abstraction in the core. Future generic rate-limit/quota
coordination primitives in P4 are a separate product feature, not a commitment to
implement SaaS customer policy now.

The OSS project must provide secure defaults, tested guarantees with explicit
limits, reproducible builds/deployment instructions, usable SDK/protocol docs,
telemetry signals, recovery guidance, independent security review and vulnerability
triage. Operators configure access, workload limits, dashboards, alerts and incident
response for their deployments. A future SaaS additionally needs customer controls
and service-level operating commitments.

Per-resource bounds do not cap storage across arbitrary new resource names.
Deferral preserves that documented limitation; operators must restrict access or
admission as appropriate. It does not weaken isolation, remove safety bounds,
authorize deletion of fencing metadata or establish production readiness.

Use the [release checklist](../release-checklist.md) for the three distinct gates.
P3 next focuses on API/SDK usability, packaging, security and release documentation.
No code change, dependency, deployment or push is part of this scope correction.

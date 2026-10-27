# P0 fixture retirement and test consolidation

2026-10-27. Scope: local tests and removal of obsolete feasibility code. No
Cloudflare deployment, push, new dependency or public schema change.

## Implementation

Removed the P0 non-expiring `try_grant` algorithm, JSON `/spike` controls, fixed
`local-a`/`local-b` identities, P0b gateway/authority, old grant model/history
checker and `just dev-spike`. Kept the portable transaction trait, canonical
counter parser, shared Connect parsing/auth and minimal synchronous SQLite/Wasm
transaction binding. Current lease/session/queue domain logic remains independent
of Cloudflare. The existing mutex authority identity and persisted format are unchanged.

The tests now share runtime lifecycle and controller helpers. Counter setup and
record inspection are private test-process helpers using the already pinned
Hermit Node's `node:sqlite`, with no third-party package. Setup refuses a running
Wrangler and requires exactly one unused mutex authority. Read snapshots use
SQLite read transactions after mutations settle or are held at checkpoints;
comparisons include every record, not only selected counters. Offline setup is
not a supported recovery operation. Successor grants use real Release or expiry.

The existing compile-time scheduler is still necessary for exact ordering, clock
boundaries and fault injection. Its controller client is absent from normal Wasm,
verified by the poison-controller regression. The removed fixture is not retained
behind another flag. P1/P1.1 test modes remain for focused coverage of the shared
lease/session algorithms. The Buf P0b descriptor remains a schema baseline, not
an executable old server.

## Coverage map

| Former P0/P0b coverage | Current coverage |
| --- | --- |
| Multi-record grant and rollback after each SQL/Rust write | `TestMutexStorage/complete_rollback`: actual P2 transactions, full records compared before/after, then changed-payload retry proves aborted intent was not reserved |
| Read failures | Same test injects failures on clock, session, queue counter, owner, fence and revision; full rollback and fresh retry |
| Precision above 2^53 and uint64 exhaustion | `TestMutexStorage/lossless_counters_and_exhaustion`: offline seed, real Wasm/SQLite/Protobuf/SDK round trip, persisted values, restart replay and real Release/successor |
| Three overlapping acquisitions | `TestMutexSchedules/all_three_request_orders`: 3 cases × 6 admission orders × 6 response orders = 108; distinct, identical duplicate and conflicting payload |
| Deadlines and timeout races | `TestMutexSchedules`: 12 exact logical deadline edges, four controlled timer races, six transport/durability failure boundaries; same-attempt retry distinguishes committed results |
| Response dropped, cancelled request, restart | `TestProtocol`, P1 lifecycle tests and P2 unknown-commit/disconnected-waiter tests |
| Historical replay after vacancy/successor | Real Release in precision tests plus current native lease/session tests; artificial `vacate` removed |
| Tenant/principal/namespace and tuple isolation | `TestProtocol` generated-client auth tests, ambiguous tuple pairs, special-character resource keys and P3 static-auth tests |
| Generated Connect wire format and limits | `TestProtocol` now targets P1 leases: codecs, malformed messages/headers, exact field/body boundaries, chunked upload, unknown fields, real stalled-upload timer smoke |
| Multiple OS clients | Existing `TestP2TwoProcesses` covers real SDK handoff and replay across processes; old uncontrolled 12-process races are replaced by explicit admission schedules, while 16/24-client transport smoke tests remain |
| Native P0 grant reference model/history checker | Removed with obsolete semantics; retained independent lease (271,452), session (37,448), waiting (9,330), and two-waiter (37,448) model-prefix checks |
| P0 seeding/envelope validation | Removed: these were validations of a deleted test API. Canonical counter checks remain native; actual protocol validation remains in generated-client integration |
| Mixed-mode fixture security guards | Stronger assertion: removed routes return 404 even with old enabling variables; real P2 remains available |

Existing P1.1 lifecycle and P2 registration/cancellation/handoff fault matrices now
also compare complete SQLite records after every injected write failure. Current
model and runtime suites continue to cover leases, ACK/expiry, FIFO, cancellation,
alarms, restart and credential boundaries.

## Verification

`./bin/just test` exited **0** on 2026-10-27 using Hermit and locked dependencies:

- Buf lint/format/schema compatibility and generated-code drift: passed.
- Rust/Just formatting, Clippy (normal and test-hooks Wasm), Go vet: passed.
- Native Rust: **25 core + 2 auth tests**, including all four current independent
  reference-model explorations. The lower count reflects removal of seven tests
  of the obsolete P0 algorithm, not removal of current lease/session models.
- Go SDK/generated packages with race detector: passed; SDK **1.333s**.
- Normal release Wasm + local Wrangler/SQLite integration: **30.138s**, passed.
- Instrumented deterministic integration with race detector: **77.909s**, passed.
  New initial P2 grant faults covered **eight writes** for both real SQL errors
  and Rust callback errors. Existing P2 register/cancel/release+grant matrices
  verified **7/4/13 writes**, respectively, with complete record comparisons.
- Normal release Wasm restored successfully; `git diff --check` and local
  documentation-link checks passed.

Ignored full transcript: `test-results/consolidation-full-suite.log`. The focused
migration run (`TestMutexStorage|TestMutexSchedules|TestP2Rollback`) also passed
in **38.529s**, before adding separate fence/revision exhaustion cases and full
P1.1 record comparisons; those additions passed in the final full run.

The first focused run timed out because new checkpoint IDs contained underscores,
which the existing scheduler correctly rejects. IDs were corrected to its allowed
format; no runtime validation was relaxed. The timeout left an orphaned Wrangler
process group, which was explicitly stopped, and its temporary credentials were
removed. Normal test commands needed no extra permissions; stopping that orphan
required one sandbox approval. This was harness setup failure, not atomicity
failure. No external advisory scan, deployment, push or commit was performed.

## Limits

Controlled cases are deterministic: logical time and checkpoint release choose
outcomes. Random local credentials/capabilities identify tests but do not choose
winners. Native bounded models are finite, not proofs over all histories. Existing
uncontrolled client/real timer smoke tests test aggregate invariants and runtime
plumbing; they are not scheduling evidence. Watchdogs and startup polling detect
stalls/readiness, not domain expiry.

Local snapshots depend on Wrangler's local SQLite layout and fail loudly if it
changes. They establish local rollback, not hosted replication/durability or safe
snapshot restoration. No hosted failover, clock/lifecycle, abuse/cost, or independent
security release gate is closed by this consolidation. See the recovery policy
and P3 deployment gates. The next milestone remains explicitly authorized staging
validation, before P4 helpers.

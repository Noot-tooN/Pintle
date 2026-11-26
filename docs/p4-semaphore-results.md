# P4 semaphore: implementation and local verification

Date: 2026-10-28. Development only; no deployment, push, paid service or new
dependency. Existing Hermit pins and Just recipes provide the entire toolchain.

## Implemented

Unweighted fixed-capacity semaphores, 1..64 permits, with server-owned FIFO
Acquire, TryAcquire, Renew, Release, cancellation, status, and bounded session
recovery. The first successful acquisition fixes capacity. Rust owns admission,
slot reuse, expiry, fencing and queue ordering. Cloudflare provides transactions,
clock/entropy, notifications and alarms. The Go SDK adds a semaphore-bound client
and WithPermit using the existing automatic-renewal/cancellation/cleanup lifecycle.
A separate executable example runs six jobs through two permits.

[ADR-021](adr/ADR-021-unweighted-semaphore.md) records capacity, storage, API and
slot-fencing decisions. The shared Protobuf lifecycle envelopes require a narrow,
file-scoped Buf uniqueness-rule exception; no other lint/compatibility checks are
relaxed. Mutex persistent records and authority routing remain compatible.

## Deterministic coverage

- Seven native semaphore tests, including 167,958 independently modelled transition
  prefixes (capacities 1, 2, 3; six action/time choices; depth six).
- Capacity 0/65/u32::MAX rejection and 1/2/64 boundaries; contention and immutable
  capacity; independent renewal/release, exact expiry −1/equality/+1; stale/wrong
  subject capabilities, regression of time, canonical counters and corrupt storage.
- Slot-scoped Active/Expired/Released/Superseded observations. Another slot's newer
  fence does not invalidate an active permit. Replay does not consume another slot.
- FIFO batch grants, no barging, cancellation, deadline expiry, ACK and reconstruction
  from persisted records; every-write rollback of a two-grant batch, including a
  duplicated entropy result that must abort the entire batch.
- Real local Wrangler/Wasm/SQLite: all six commit orders for three competing
  acquisitions at capacity two; duplicate commit/reply orders; conflicting retries;
  exact expiry; Renew; separate mutex/semaphore authorities; cancel/release orders;
  persisted FIFO batch handoff after process restart.
- Actual SQL constraint and Rust failures at each of 13 writes in a release plus
  queued handoff. Retry recovers one handoff and fence 3, without partial release.
- A real local Durable Object alarm survives restart; a mid-batch SQL fault leaves
  both prior permits and pending intents intact; the retry grants fences 3/4 with
  different capabilities. No request triggers that alarm transition.
- Offline boundary seeding, followed by real runtime operations and restart, checks
  exact fences/revisions around 2^53 and u64 exhaustion. This is private test setup,
  not a public fixture/control endpoint.
- SDK validation checks request/response primitive and capacity, slot consistency,
  unused sequence positions after invalid preparation, WithPermit cancellation and
  uncertain renewal recovery. Existing WithLease deterministic lifecycle tests
  exercise the shared machinery. Watchdogs bound hangs; no sleeps choose winners.

## Executed checks

Focused native semaphore tests passed (7 tests; model included). Focused SDK tests
passed with Go's race detector. `./bin/just test-hardening '^TestP4'` passed the
real runtime matrix (17.235s) and restored normal Wasm. Logs are retained locally
under ignored `test-results/p4-core.log`, `p4-sdk.log`, and `p4-hardening.log`.

Final `./bin/just test` **passed** (exit 0):

| Check | Result |
| --- | --- |
| Buf lint/format/generation, generated-code drift and historical breaking check | Passed |
| Rust format/Clippy (native, normal Wasm, test-hooks Wasm), Go vet, Just format | Passed |
| Local staging-configuration validation | Passed; no account call or deployment |
| Independent Go consumer module | Both mutex and semaphore executables built with cached dependencies |
| Native Rust | 33 core tests + 2 auth tests passed; existing models retained |
| Go SDK with race detector | Passed (1.384s) |
| Normal Wasm local Wrangler integration | Passed (94.675s) |
| Instrumented deterministic integration with race detector | Passed (101.744s) |
| Executable examples against normal Wasm | All five mutex modes plus semaphore passed |
| Normal build restoration | Passed after instrumented suite |
| `git diff --check` | Passed |

The semaphore executable admitted six jobs through two simultaneous permits,
including initial renewal and cleanup. The complete log is
`test-results/p4-full-suite.log` (ignored local artifact). Remote hosted test entry
points remained explicitly skipped; local rehearsals of hosted test logic use
local Wrangler. No new dependency was needed; existing Hermit tools ran all checks.
Source baseline: `903902431f6fc8cb7256d66ff32188c4801fbd03` plus this uncommitted
P4 diff. Restored normal `index_bg.wasm` SHA-256:
`ac46084180d4ec08d2a5ad1a0e9ba8e4e21708cdb0cce1ac3fe3ce9a4d593cde`.

CI already invokes `just test`; its hardening selector now includes TestP4 and its
package/example checks include the new executable.

## Limits and next work

Capacity is not weighted or dynamically resizable. Existing session, request
window and waiter limits still apply, so 64 permits does not promise 64 concurrent
WithPermit invocations on one resource. Each callback borrows a recovery session;
callers must handle explicit admission errors. Permit/slot history is bounded per
resource; receipts are ACKed or reclaimed on expiry, without manual receipt cleanup.

Fencing is optional at the caller, and per slot at a cooperating destination.
Cancellation is cooperative. A client that ignores cancellation may continue
external work after its permit expires. No second database is required or created.

P3's hosted mutex evidence remains evidence for mutexes, not a hosted semaphore
validation claim. This increment has not been deployed. Next: review this P4
increment, then an explicitly authorized, bounded staging semaphore rehearsal
before making hosted claims. Singleflight and the rest of P4 remain separate work.

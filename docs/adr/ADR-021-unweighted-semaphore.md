# ADR-021: Fixed-capacity, unweighted semaphore

Accepted for development, 2026-10-28. Extends the shared Rust lease transition,
recovery sessions and FIFO queue. Implements only the semaphore part of P4.

## Semantics

Each successful acquisition owns one expiring permit. Acquire and TryAcquire
carry expected capacity, an integer in 1..64. The first successful grant commits
capacity; every later acquisition must agree, including after all owners expire
or release. A mismatch is `failed_precondition`. Invalid bounds are
`invalid_argument`. Resizing, weighted acquisition and multi-resource atomic
acquisition are deferred. The bound limits synchronous work and storage per
mutation; it is not a tenant quota or a promise of 64 simultaneous SDK sessions.

There are fixed, one-based slots. Each slot retains its last fencing value and
optional current lease. A resource-wide exact u64 sequence issues a unique,
increasing fence on every grant. Renewal keeps its slot, capacity, capability and
fence. Release/expiry frees only that slot. Revision is resource-wide; another
slot's mutation does not invalidate an otherwise active permit. Authority time,
TTL boundaries, subject authorization and constant-time capability comparison
follow the existing lease contract. At expiry equality ownership is lost.

Historical Acquire/Renew replay reports Active, Expired, Released or Superseded
by examining the original slot, not the latest grant in another slot. Ownership,
slot fence, resource fence/revision and the recovery result commit together.
Storage errors abort the complete transition. Counter overflow fails closed.
A first grant that aborts does not configure capacity.

The queue grants as many available slots as it can in FIFO ticket order, inside
one transaction. TryAcquire cannot barge ahead of eligible waiters. Cancellation
before grant is terminal; cancellation after grant recovers that grant and does
not release it. Every grant obtains a distinct cryptographic capability from the
adapter. The portable core never accesses Cloudflare, entropy APIs or wall time.
The adapter schedules the earliest occupied-slot expiry while waiters exist,
plus existing waiter/session deadlines. Alarms use the same Rust queue driver.

## Persistence and isolation

Semaphore authorities route by `(tenant, namespace, semaphore-v1, key)`;
mutexes retain `(tenant, namespace, mutex-v1, key)`. Equal visible names across
primitives deliberately do not coordinate with each other. They still share the
existing authentication and namespace authorization policy.

The durable `semaphore` record marks the primitive even before the first grant;
an empty slot vector means unconfigured. It is initialized inside the first
transaction, so alarms reconstruct the correct behavior after restart. Never
reinterpret existing mutex owner/fence state as semaphore state. Existing mutex
owner and receipt JSON omit the new optional permit metadata and remain readable.
The shared lease engine owns both transitions; a small ownership-state module
adapts one owner versus bounded slots without duplicating recovery or queue logic.

The same limits remain: 16 recovery sessions per authority (the final slot is
reserved for an authenticated active owner's recovery), 32 positions per session,
16 pending intents/subscriptions, existing TTL/deadline/recovery horizons. Low-level
sessions can own multiple permits, but managed callbacks borrow separate sessions;
capacity 64 does not override session admission. Keep clients and their idle
session caches rather than constructing a new client per job. Acknowledgements
and session cleanup reclaim receipts automatically; capacity, slots and fencing
history stay bounded per resource and are never reset or deleted. Unbounded new
resource names still increase total storage.

## Protocol and Go API

Add `SemaphoreService` with the same eight unary RPCs and shared recovery/lease
envelopes. `TryAcquireRequest.semaphore_capacity` is required on that service and
must be zero on LockService. GrantReceipt adds `permit_slot` and
`semaphore_capacity`; both are zero for mutexes. Additive schema compatibility is
checked by Buf against the historical baseline and generated Rust/Go artifacts.
A file-scoped exception to Buf's RPC_REQUEST_RESPONSE_UNIQUE rule deliberately
allows the identical lifecycle envelopes; all other lint/breaking rules remain.
This trades independent message evolution for one recovery contract and generated
Go interfaces with identical method sets. Future semantic divergence should use
new messages/services rather than silently overloading existing fields.

`client.Semaphore(capacity)` returns a bound client. Its existing low-level session,
Acquire, TryAcquire, Renew, Release, status and ACK methods use SemaphoreService.
`WithPermit` uses the existing WithLease lifecycle: acquire once, initial Renew
and ACK, renew automatically, cancel work on loss/uncertainty, join work, bounded
Release/ACK cleanup. It never retries the callback or arbitrates admission in Go.
Prepared requests include capacity; recovery must use the same bound client.
Clients reject mismatched primitive/capacity and malformed permit observations.
The callback receives `Lease` with PermitSlot and Capacity, avoiding parallel
lease/permit representations and a second maintenance implementation.

## Optional fencing and limits

Destination fencing is scoped to `(resource, permit slot)`. Comparing all permits
against one resource-wide high-water mark would wrongly reject valid concurrent
holders. A cooperating destination must atomically compare/store the slot's fence
with the protected action, and the application must include the primitive/namespace
in its key. This provides stale-holder rejection after a newer grant has been
observed in that slot, not automatic revocation or global ordering of business
writes across slots. Pintle neither provisions nor connects to another database.
Applications doing disposable/cooperative work may ignore fencing.

Context cancellation cannot kill a paused or non-cooperative process. Semaphore
capacity bounds authoritative unexpired permits, not arbitrary external work
continuing after expiry. No hosted semaphore, physical failover, partition, clock
SLA, other backend or production guarantee is inferred from local tests.

See [P4 results](../p4-semaphore-results.md).

# ADR-010: Durable FIFO Acquire with bounded unary resumption

Status: accepted for local P2, 2026-10-26. Builds on ADR-008/009. No hosted
production guarantee, transport streaming, additional backend, or deployment.

## Decision

Implement `Acquire` as a durable intent in the existing resource-local recovery
session. The portable Rust service in `core/src/session/waiting.rs` owns queue
admission, FIFO selection, wait deadlines, cancellation, and grant transitions.
`TryAcquire` and queued `Acquire` both call the existing `lease::execute` grant
transition. Platform adapters do not implement eligibility or fairness policy.
The Worker still authenticates and routes; the DO supplies the atomic SQLite
transaction, clock, cryptographic capability, durable acknowledgement, alarms,
and bounded in-memory notification hints.

An acquisition stores its immutable command, wait duration, absolute authority
deadline, and monotonically allocated decimal u64 queue ticket in its session
entry. A separate checked `wait_counter` is never reset. Queue tickets order
successful registration transactions, not client invocation or network arrival.
Pending entries count against the existing request window and byte budget.

Limits: 16 durable pending acquisitions per resource, at most 16 simultaneously
subscribed Acquire HTTP handlers per DO instance (including duplicate attempts),
wait duration 1–60000 whole milliseconds, and at most eight seconds per unary
response hold. A transport deadline can shorten that hold. Existing lease TTL,
session quotas, receipt window, idle expiry, and oldest-result expiry still apply.
The final request-window position remains reserved for Release.

## Atomic transitions and fairness

The core `waiting::apply` performs maintenance and the requested action within
one transaction. It advances already queued work before ordinary new operations,
so a fresh TryAcquire cannot bypass an eligible queued acquisition. A Release and
the next queued grant commit atomically. Deadlines are evaluated before grants;
`now == wait_deadline` is expired. Renew extends the owner's lease as before and
changes the next required expiry wakeup. Renewing owners can postpone queued
work until its deadline; FIFO is not a starvation-freedom promise.

An alarm or an incoming request drives queued work. Expired sessions are first
reclaimed, expired wait deadlines become terminal Expired, and the oldest
remaining eligible acquisition calls the shared grant transition. No handler or
client connection is required for this transition. One drive can grant at most
one lease; subsequent work remains pending until release or expiry. Expected
errors such as fencing exhaustion become terminal cached decisions. Storage or
corruption errors abort the transaction, including any queue progress.

Maintenance may commit a queued grant while the triggering unrelated operation
returns a business rejection. No queued grant is inferred from a notification or
from a successful Release response; its own retained result is authoritative.

## Cancellation and unknown outcomes

`CancelPending` carries the SAME full Acquire request and attempt identity. It
uses the original sequence; it does not allocate a new SDK request number.

- If still pending, it records Cancelled before driving other queued work.
- If a grant already committed, it returns that historical grant and a current
  observation. It does not release ownership; the application reconciles and, if
  appropriate, explicitly releases the current lease.
- If the deadline already passed, it returns Expired.
- If Acquire has not registered, cancellation records a terminal Cancelled entry.
  A delayed Acquire with that identity can never join the queue afterward.
- A changed payload/duration is a conflict. Other principals cannot inspect or
  cancel the session. Acknowledged/expired attempts retain ADR-009 rejection rules.

Network disconnection and HTTP/context deadlines are not cancellation. Durable
intent can still grant after the client disappears. Retry/resume, status, or
CancelPending must preserve the full attempt. An unknown outcome must never be
replaced blindly with a newly numbered acquisition.

`GetRequestStatus` returns Pending, Cancelled, Expired, or a retained terminal
mutation/error. NotFound is still only a snapshot. Pending cannot be acknowledged:
contiguous ACK must wait for a terminal outcome. A session expiring can remove a
pending intent before its wait deadline, making the old attempt unrecoverable;
it cannot later grant. Completed results obey the existing retention horizon.

## Notification and alarm adapter

Before reading or mutating durable acquisition state, the DO allocates a bounded
subscription. Each subscription contains a sticky signal and a Rust task waker.
Successful transactional state changes signal subscribers before the next await.
The subscriber rechecks SQLite and awaits durability before returning a result.
Signals that occur before the future is polled remain observable. A later signal
also remains sticky while a handler is reading or awaiting storage. Thus a
release between registration, initial read, and waiting cannot be lost.

Notifications are hints and may be broadcast to at most 16 subscribers. They do
not survive restart, and no ownership state is stored only in memory. RAII removes
subscriptions on completion/cancellation. Subscriber exhaustion rejects admission
before a new intent is created; status, cancellation, renew, and release remain
available. Duplicate connections consume separate subscription slots but share
one durable intent and result.

After a mutation, the alarm deadline is the earliest session expiry, pending
wait deadline, or active owner's expiry while a queue exists. No periodic
contention polling is used. The existing pre-armed durable 60-second fallback
covers process loss before final alarm scheduling. Alarm callbacks re-arm the
fallback before maintenance, commit/sync, signal, and reschedule. A failed alarm
transaction cannot partially grant. Alarms may be late; a crash in the scheduling
window can defer work to the fallback and cause short waits to expire. This is
an availability limit, not permission to grant after a deadline. Cloudflare's
finite automatic alarm retries and arbitrary outages remain outside local proof.

## Transport and SDK

The unary response holds for up to eight seconds. If still pending, it returns
Pending without cancelling or re-registering the intent. `ResumeAcquire` makes
one such call; SDK `Acquire` resumes only returned Pending responses using the
same immutable envelope. It never calls TryAcquire or selects queue winners.
Transport/context errors are returned for explicit reconciliation rather than
silently creating a new operation. No automatic cancellation or release occurs.

This deliberately chooses bounded unary resumption over streaming/WebSockets.
It keeps the previously verified Protobuf/Connect Fetch adapter. Buf generates
both Go client and Rust wire types; the large Acquire response alternative is
boxed in Prost generation. No new dependency or custom JavaScript coordination
logic is needed. The schema and Go module path remain unpublished.

The logical wait deadline starts at first admission and is independent of each
HTTP deadline. Resuming does not extend it. Applications should persist the
prepared request before sending if recovery across application restarts matters.

## Compatibility, cost, and evidence limits

Use `P2_ENABLED=true`; it cannot be mixed with P1/P1.1/P0 fixtures. No mode is
enabled by default. The `mutex-v1` routing identity and all fencing/lease keys are
preserved. Session records add optional waiting metadata; old records remain
readable. Production upgrades/downgrades and old permanent P1 receipt migration
are not established by these local tests. Do not downgrade a resource with active
P2 intents to an older service mode.

A fresh P2 resource has at most 16 bounded session records plus six metadata/owner
keys. Completed waiter entries are reclaimed by ACK/expiry; the queue does not
create an accumulating row per acquisition. Slot scanning, JSON rewrites, bounded
broadcasts, and unary resumption have costs. This implementation is not a hosted
hibernation/cost claim: pending HTTP handlers can keep a DO active. We measure
local storage, registration latency, and notification work, not Cloudflare billing.
Host CPU/RSS profiling was blocked by this environment’s process-inspection policy.

See [P2 results](../p2-results.md) for actual builds, deterministic schedules,
rollback, restart, alarms, SDK/process tests, measurements, and remaining gates.
Primary platform references: [in-memory state](https://developers.cloudflare.com/durable-objects/reference/in-memory-state/),
[alarms](https://developers.cloudflare.com/durable-objects/api/alarms/),
and [limits](https://developers.cloudflare.com/durable-objects/platform/limits/).

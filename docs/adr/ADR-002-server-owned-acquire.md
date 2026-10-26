# ADR-002 — Both Acquire and TryAcquire live on server

**Status:** ACCEPTED (2026-10-24); exact waiter transport/recovery remains OPEN.

## Decision

`LockService` exposes server-side `acquire()` (blocking at semantic level), `try_acquire()` (nonblocking), `renew()`, and `release()`; semaphore will follow the same pattern. Both acquisition methods invoke one atomic grant transition. The server owns contention observation, waiter eligibility/ordering, retry-on-change, deadlines and expiry wakeup.

SDKs perform transport retries with stable request IDs, cancellation, local lease renewal/loss monitoring and callbacks. SDKs **must not** emulate `Acquire` as their own TryAcquire/watch loop.

## Specific caveats and required tests

- Bounded unary blocked request or resumable server-side wait ticket? Runtime feasibility to determine. Returning a retryable ticket/operation status to reconnect is acceptable **only if the server remains responsible for the logical waiting and re-acquisition**.
- Register interest without losing wakeups after inspecting revision.
- Distinguish network disconnect from cancellation; a grant can commit after the client disappears.
- Reusing the same request ID must never create an extra grant/fencing token.
- If a wait was granted but response lost, provide reconciliation/status or safe cleanup; never assume zero side effects.
- Test expiry, release, cancellation races, object restart, fairness and many idle waiters.

## Rationale

Minimizes duplicated correctness logic across SDK languages and enables server-targeted notification without application clients polling their own lock loops.

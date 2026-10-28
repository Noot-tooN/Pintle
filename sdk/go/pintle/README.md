# Go SDK: supported mutex workflow

Import `github.com/Noot-tooN/Pintle/sdk/go/pintle`. The module root is
`github.com/Noot-tooN/Pintle`; generated Protobuf messages and the Connect client
ship in that same module under `gen/go/pintle/v1`. No Rust, Wasm, Node, Buf or
Cloudflare tooling is needed by a Go application consuming the SDK. Those tools
are for building/testing the server or regenerating checked-in protocol sources.

## Installation and compatibility

The SDK is **unpublished and not yet stable**. The old `pintle.local/spike` import
path was a development placeholder. No versioned SDK release has been published.
For an external application using this checkout:

```sh
# In an existing Go module, using Go from an activated Pintle Hermit environment:
go mod edit -require=github.com/Noot-tooN/Pintle@v0.0.0
go mod edit -replace=github.com/Noot-tooN/Pintle=/absolute/path/to/Pintle
# Import the SDK in your code, then:
go mod tidy
```

`v0.0.0` above is a local replacement placeholder, not a published version.
After an approved release is actually published, use `go get` with that exact
module tag or commit and remove the local replacement. Public proxy fetching
is not verified by local replacement tests. Keep the case of the module path.

The module currently requires Go **1.27.3**, the Hermit-pinned/tested toolchain.
Support for earlier Go versions has not been established. Runtime dependencies
are the pinned Connect and Protobuf Go libraries; no new dependencies were added
for these examples. Use `./bin/just check-sdk-package` from the repository to build
the example as an independent consumer with `GOWORK=off` and network fetching off.

The Go API is v0/unfrozen and `pintle.v1` is an unpublished wire package name,
not a blanket stability promise. Before publication, pin a reviewed commit and
pair SDK/server versions. Release notes must identify Go API, wire and stored-state
compatibility separately. Buf checks preserve existing wire fields/methods;
they do not certify Go source compatibility or storage migration. See
[the packaging decision](../../../docs/adr/ADR-019-go-sdk-packaging-and-examples.md).

## Start here

Run `./bin/just example-local` from the repository: it builds normal Wasm, starts
local Wrangler with an ephemeral private credential, then builds and executes
all five [example modes](../../../examples/README.md). No Cloudflare account,
deployment or hosted traffic is involved.

The current service uses this sequence:

1. `New(endpoint, credential, nil)` creates a reusable client. HTTPS is required
   outside loopback. Default transport rejects redirects and caps replies at 16 KiB.
   Custom HTTP clients must preserve these policies and honor context cancellation.
2. `OpenSession(ctx, resource)` allocates a resource/principal-scoped recovery
   session. An uncertain OpenSession response may leave an orphan session; retrying
   may allocate another. Orphans expire normally, and capacity is finite.
3. Prepare once with the **Session methods**, then send with the Client. Preparation
   allocates a sequence even before sending. Do not discard a valid prepared request
   or edit its payload, request ID, session, sequence or ACK watermark for retries.
4. Inspect the result before doing protected work. Release explicitly when finished.
   ACK a contiguous prefix only after every outcome in that prefix has been safely
   consumed or persisted. ACK reclaims recovery evidence; it is not Release.

Package-level `NewAcquire`, `NewRenew`, `NewRelease` and `GetRequestStatus` omit a
session attempt for earlier focused protocol modes. They remain for those tests;
use Session constructors and `GetAttemptStatus`/`Session.Status` with the current
P2 service. Request fields are exported for serialization, not permission to mutate
an already prepared retry identity. Client/Session methods are concurrency-safe;
applications must coordinate ACKs across concurrently issued sequences.

## Managed callbacks: WithLease

For ordinary scoped work, let the SDK manage acquisition, renewal and cleanup:

```go
err := client.WithLease(ctx,
    pintle.Resource{Namespace: "shop", Key: "inventory:blue-shirt"},
    pintle.LeaseOptions{TTL: 30 * time.Second},
    func(workCtx context.Context, lease pintle.Lease) error {
        return inventory.SaveStockFenced(workCtx, productID, stock, lease.FencingToken)
    },
)
```

`inventory.SaveStockFenced` above is an application method, not a Pintle SDK method:
it must atomically validate the token and update data in the existing destination.
No second database is required by Pintle. For harmless duplicate work such as
caching a checksum of immutable content, ignore the lease argument (`_ pintle.Lease`)
and call the ordinary destination API. See runnable `with-lease` and `with-fencing`
[examples](../../../examples/README.md).

Defaults: TTL 30s, WaitTimeout 60s, CleanupTimeout 5s, HolderRef `with-lease`.
WaitTimeout bounds session allocation/acquisition locally and the server waiting
intent (1–60000 whole ms). TTL remains 100–60000 whole ms; cleanup must be positive
and at most 60s. Use a parent deadline to bound cooperative work overall.

The helper waits for initial Renew/ACK before invoking your callback at most once.
It renews automatically with the existing TTL/3 budgets and cancels `workCtx` on
parent cancellation or lease uncertainty; `context.Cause(workCtx)` includes
`ErrLeaseUncertain` for detected maintenance failure. It never retries callbacks
or reacquires on loss. The lease is a snapshot: its token is stable through renewal,
its expiry is informational and is not updated in your callback's value.
Let the helper own Renew/Release; do not manually change its lease lifecycle.

Pass `workCtx` to cooperative operations and join your child goroutines before
returning. WithLease waits for your callback to return, even after cancellation;
an uncooperative callback can block forever. On return it cancels work, stops and
joins maintenance, and attempts Release/ACK with one fresh bounded cleanup context.
Panics propagate after attempted cleanup. Cancellation cannot recall external
writes; destination fencing remains necessary when stale effects are unsafe.

One private session handles the whole lifecycle. Reuse the Client: it caches at
most 16 fully settled idle sessions, exclusively borrowed by invocations. Cached
sessions are refreshed before reuse and replaced only after confirmed expiry.
Uncertain/evicted sessions expire normally. High resource churn, simultaneous
callers or fresh Clients can still hit server limits. Tokens/leases are never reused.

### WithLease errors and recovery

Use `errors.Is` for callback/context errors, `ErrWaitExpired`, `ErrWaitCancelled`
and `ErrLeaseUncertain`. Callback and cleanup errors may be joined. A nil callback
result does not make uncertain renewal/cleanup succeed. With `errors.As`, inspect
`*pintle.WithLeaseError` for `Wait`, `Renew` and `Release` prepared requests and the
last observed `Lease`. These fields are recovery data, never proof of current
ownership. Keep them private and unchanged; do not blindly rerun business work.

- `Wait`: cancellation or final ACK was uncertain. Reconcile/CancelPending using
  the same intent; if grant won and remains active, explicitly release it.
- `Renew`: a renewal or its ACK was uncertain, including renewal interrupted by
  normal callback completion. Inspect/replay that exact attempt; its receipt may
  already be acknowledged. Do not treat missing history as proof of rollback.
- `Release`: release or its final ACK was uncertain, or its known result is kept
  because an earlier renewal remains unresolved. Replay/status using the exact
  request, and ACK only after consuming the entire contiguous prefix.

Even when cleanup reconciles a lost Acquire reply, the helper returns the original
error and does not invoke work. A failed Release does not ACK or recycle the session.
Normal formatting redacts recovery fields, but serialization exposes capabilities.
A callback panic propagates; cleanup errors cannot be returned during that panic.
The helper keeps recovery in memory only. Use explicit Session APIs with a private
pre-send journal when crash recovery of requests is required. It does not make
external business effects exactly-once.

## Operations and results

| API | Contract |
| --- | --- |
| `TryAcquire` | One attempt. `Contended` is a normal outcome. Only `State == Active` with a non-nil `Current` is an active server observation. |
| `Acquire` | Resumes the same server-owned intent only when it receives Pending; no client TryAcquire contention loop. Returns a grant, cancellation/expiry state, or error. |
| `ResumeAcquire` | One bounded unary exchange for a prepared waiting intent. Pending survives the response/disconnect. |
| `CancelPending` | Reuses the original WaitRequest. If grant won, returns that grant; it does not release it. Cancellation may precede admission. |
| `Renew` | Same ownership capability/fence; use the returned Current observation. Historical receipt and current expiry may differ on replay. |
| `Release` | Explicit ownership release. Retain the request for ambiguous-response reconciliation before ACK. |
| `GetAttemptStatus` | Observes retained history using the complete attempt. NotFound is not proof that an in-flight operation cannot commit. |
| `AcknowledgeAttempt` | Recovery after client restart, without reconstructing a live Session. Caller must have consumed every result through its sequence. |
| `Heartbeat` | Refreshes the recovery session via its current ACK watermark; does not renew a lease or retain old receipts forever. |

Receipt is an immutable historical result. An expired/released/superseded receipt
does not authorize new work. Even an Active observation can become stale before
your next instruction. Preserve fencing tokens as decimal strings, or parse them
with `strconv.ParseUint(..., 10, 64)`; never compare them lexicographically or use
floating point. Only downstream atomic validation can fence stale external writes.

## Errors, deadlines and recovery

Set bounded contexts on network calls. The SDK does not automatically retry
transport errors. `Acquire` only resumes successful Pending responses. On an
ambiguous result, retain the full prepared request, stop dependent work, and use a
fresh bounded context for status/retry/cancellation. Never create a new identity
merely because the old call timed out. Do not ACK unresolved sequences.

Use `connect.CodeOf(err)` for server/Connect errors and `errors.Is` for Go context
errors. Local preparation/validation errors are ordinary Go errors; their text
is not a stable machine-readable API. Relevant server codes:

| Code/outcome | Meaning and action |
| --- | --- |
| `Contended` / Pending | Expected protocol outcomes, not transport errors. |
| `Unauthenticated` / `PermissionDenied` | Check credentials/scope/owner; permission denied also covers stale ownership and another principal's receipts. |
| `AlreadyExists` | Conflicting use of an existing request identity; investigate changed method/payload. |
| `ResourceExhausted` | Session/window/waiter/counter bounds; respect limits and reconcile existing work. Do not spin or reset storage. |
| `FailedPrecondition` | Includes expired session, reclaimed receipt or inactive lease. It does not establish an unknown historical outcome. |
| `NotFound` | No retained outcome observed; an admitted/in-flight request can still commit. |
| Timeout, cancellation, unavailable or transport failure | May be ambiguous; reconcile the same prepared intent. Caller cancellation alone does not cancel durable waiting. |

Recovery sessions currently have 32 positions (the final one reserved for Release),
16 slots per resource with an owner-release reservation, five-minute idle expiry
and a bounded oldest-unacknowledged-result lifetime. The server bounds waiters and
subscriptions at 16. Lease TTL is 100–60,000 whole milliseconds; wait timeout is
1–60,000 whole milliseconds. Prepared requests may become unrecoverable after
ACK/expiry; there is no unlimited deduplication guarantee. See the
[session contract](../../../docs/p11-results.md) for exact retention semantics.

Persist complete prepared requests **before sending** if crash recovery matters.
They can contain secret lease capabilities; use private durable storage, not logs.
Persisted requests can be replayed with Client methods and `AcknowledgeAttempt`;
there is no API for restoring a Session's local sequence allocator. The examples
keep requests in memory and deliberately do not implement a crash-recovery journal.

## Automatic maintenance and Lost

`Maintain(ctx, lease, ttl)` opens its own session, renews immediately and then
schedules later renewal, ACKing only its own consumed renewal results. Its
renewal/ACK budget is TTL/3; a short TTL may be impractical for your network.
It does not acknowledge application acquisitions or start/stop application work.

Observe `Lost()` while doing protected work. It closes permanently on uncertainty,
inactive ownership, cancellation or Stop. Stop your work and inspect `Err()` and
`Recovery()`; a lost response may hide a committed renewal. Stop waits for the
maintenance goroutine but **does not Release**. A still-open Lost channel is not
an ownership oracle, and a paused process cannot be forcibly stopped by it.

Recovery may also describe an ACK whose response was uncertain: the renewal receipt
may already be reclaimed. Do not treat a missing status as a rolled-back renewal
or resume effects blindly. The recover example intentionally loses the renewal
response *before ACK*, so retained status/replay can be demonstrated deterministically.
Handle application recovery policy separately and enforce downstream fencing.

## Supported transport and limits

The server supports binary unary Connect POST, `application/proto`,
`Connect-Protocol-Version: 1` and identity encoding. It does not provide JSON RPC,
gRPC/gRPC-Web, public streaming, GET invocation or compressed request bodies.
The generated Connect client's defaults match this subset; arbitrary options may
not. Errors use Connect JSON envelopes; request bodies cap at 8 KiB. The gateway
caps each RPC deadline at ten seconds and each held Acquire response at eight;
Acquire may span multiple responses under the caller's overall context.
No full official Connect conformance or general hosted failover claim is made.

Namespace is 1–64 ASCII identifier bytes, key 1–128 UTF-8 bytes without control
characters, holder 1–64 ASCII identifier bytes. Identifier characters are
letters/digits plus `._:-`; no Unicode normalization occurs. Tenant identity comes
from authentication. Do not log credentials, request objects or lease capabilities;
debug redaction is not a general serialization guarantee.

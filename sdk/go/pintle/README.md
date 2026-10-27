# Go SDK: supported mutex workflow

Import `github.com/Noot-tooN/Pintle/sdk/go/pintle`. The module root is
`github.com/Noot-tooN/Pintle`; generated Protobuf messages and the Connect client
ship in that same module under `gen/go/pintle/v1`. No Rust, Wasm, Node, Buf or
Cloudflare tooling is needed by a Go application consuming the SDK. Those tools
are for building/testing the server or regenerating checked-in protocol sources.

## Installation and compatibility

The SDK is **unpublished and not yet stable**. The old `pintle.local/spike` import
path was a development placeholder; the current change has not been pushed or
tagged. For an external application using this checkout:

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
all three [example modes](../../../examples/README.md). No Cloudflare account,
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

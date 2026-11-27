# Pintle

Pintle is an open-source coordination service for distributed applications.
Workers written in different languages can coordinate on the same resource
through a language-neutral API. The server decides who holds a lock, which
permits are available, and which caller fills a cache entry. Go is the first
language with a Pintle SDK.

| Primitive | When to use it | Example |
| --- | --- | --- |
| [Lock](#lock) | One worker at a time needs a lease on a resource. | Update the stock count for one product. |
| [Semaphore](#semaphore) | A group of workers shares a fixed number of permits. | Let six render jobs share two permits. |
| [Singleflight](#singleflight) | Concurrent callers need the same result. | Fetch a catalog entry once for overlapping cache misses. |

**Release status.** Development candidate `rc-2026-11-27.3` includes the lock,
semaphore and experimental opt-in singleflight. All three have passed bounded
Cloudflare staging checks. Singleflight was exercised at its 16-waiter limit
and across Worker redeploys. [Singleflight staging results](docs/singleflight-staging-results.md)
The API is unpublished and may change.
[Release checklist](docs/release-checklist.md)

## Run locally

Start in a checkout on macOS or Linux. Setup requires Bash, Git, a native build
toolchain and network access for the initial downloads.

```sh
./bin/just bootstrap
./bin/just example-local
```

The first command installs the pinned tools. The second starts a local Worker,
runs the lock and semaphore examples with temporary credentials, then stops it.
The cache-fill integration test exercises singleflight locally.

```sh
./bin/just test-runtime '^TestManagedCacheFill$'
```

## Connecting an application

Clients call unary Connect RPCs over HTTP with binary Protobuf messages and a
bearer credential. To build a client in your language, start with the service
definitions and generate a client that supports this transport. Workers using
the same primitive, tenant, namespace and resource key reach the same
authority.
[Protobuf schemas](proto/pintle/v1/) · [Protocol and recovery semantics](docs/PROTOCOL_DRAFT.md)

The examples below use the Go SDK. The inventory store, renderer, cache and
origin are application code. [SDK setup](sdk/go/pintle/README.md#installation-and-compatibility)

## Lock

### Lock limits and responsibilities

Use the same lock key on every path that updates the resource, including retries
and background jobs. Stop issuing writes when the client signals that ownership
is uncertain. If you call the protocol directly, your client must confirm an
active grant, renew it during work, attempt release afterward and reuse the
original request identity and payload to recover a lost reply. [Lost-reply runbook](docs/recovery-policy.md#runbook-timeout-disconnect-or-lost-reply)

A paused worker can resume after a successor acquires the lock. Send the fencing
token with every protected write. The destination must keep the highest accepted
token for that tenant, namespace and lock key and check it atomically with the
update. [How to fence a write](#protecting-writes-after-a-lease-expires)

### Go example — update inventory under a lock

`WithLease` waits for an active lease before calling your function, renews it
while the function runs, and attempts release when it returns. Pass the callback's
`work` context to the inventory store so it can stop on cancellation. The store
checks the fencing token atomically with the stock update.
[Runnable lock example](examples/mutex/main.go)

```go
product := pintle.Resource{Namespace: "inventory", Key: "blue-shirt"}
return client.WithLease(ctx, product, pintle.LeaseOptions{}, func(work context.Context, lease pintle.Lease) error {
    return inventory.SaveIfCurrent(work, product, lease.FencingToken, stockOnHand)
})
```

## Semaphore

### Semaphore limits and responsibilities

All workers in a pool must use the same resource key and capacity. The first
successful grant sets the capacity for that key (1–64 permits). Each job holds
one permit. Expiry frees its slot even if the old job keeps running, so make
each job respond to cancellation. Clients using the protocol directly must
also manage permit acquisition, renewal and release. [Permit lifecycle and recovery](sdk/go/pintle/README.md#semaphore-permits)

For protected writes, track the highest accepted token **per permit slot**
(`permit_slot`) within the tenant, namespace and semaphore key. A newer token in one slot must leave
the other active slots usable. If multiple slots update the same business
record, that record still needs to handle concurrent writes. [How to fence a write](#protecting-writes-after-a-lease-expires)

### Go example — share two render permits

`WithPermit` waits for one permit, renews it during the callback and attempts
release when the callback returns. Reuse the semaphore client across jobs. The
renderer uses the callback context to stop if ownership becomes uncertain.
[Runnable semaphore example](examples/semaphore/main.go)

```go
sem, err := client.Semaphore(pintle.SemaphoreOptions{Capacity: 2})
if err != nil {
    return err
}
pool := pintle.Resource{Namespace: "images", Key: "render-pool"}
return sem.WithPermit(ctx, pool, pintle.LeaseOptions{}, func(work context.Context, _ pintle.Lease) error {
    return renderCacheCandidate(work, imageID)
})
```

## Singleflight

### Singleflight limits and responsibilities

Callers sharing a key can receive the same result. Include the content version
and every input that changes the answer in the key. Publish to the shared cache
before reporting completion. If the producer dies between those operations,
a waiter can recover the value by rechecking the cache after expiry.
[Cache-fill contract](docs/adr/ADR-022-singleflight-cache-fill.md)

A producer can resume after a successor has filled the cache. Use atomic
create-if-absent for an immutable entry or have the cache reject older
generations. Pintle shares up to 4,096 bytes inline. For larger values,
the callers can agree to share a small versioned reference instead.

### Go example — fill a catalog cache entry

`CacheFill` checks the cache before joining and again when selected as producer.
Here the application's `StoreIfCurrent` checks the generation in the same
operation that stores the value. [Go helper and recovery behavior](sdk/go/pintle/README.md#singleflight-cache-fills)

```go
key := pintle.Resource{Namespace: "catalog", Key: "v7:blue-shirt"}
value, err := client.CacheFill(ctx, key,
    func(work context.Context) ([]byte, bool, error) {
        return cache.Lookup(work, key)
    },
    func(work context.Context, generation uint64) ([]byte, error) {
        value, err := origin.Read(work, key)
        if err != nil {
            return nil, err
        }
        return value, cache.StoreIfCurrent(work, key, generation, value)
    },
)
```

## Protecting writes after a lease expires

For both locks and semaphore permits, **the destination can still accept a stale
write until it records a newer fencing token**. Checking ownership before
sending the write leaves time for ownership to change.

The wire field `fencing_token` is a decimal string. Compare tokens as exact
unsigned integers. Reject a lower token and record an accepted token atomically
with the data change. Retain the highest accepted token across restarts and
row deletion. Equal tokens allow several writes by one holder, so operations
that must take effect only once also need deduplication. External services that
cannot check the token can still accept stale work. [PostgreSQL example and observed handoffs](docs/TEST_PLAN.md#downstream-fencing)

## Architecture and evidence

A Cloudflare Worker authenticates each request and routes it to the resource's
Durable Object. A portable Rust core makes the coordination decisions, and the object
persists them in SQLite. The server owns the lock and semaphore wait queues.
[Design](PROJECT_SPEC.md) · [Protocol](docs/PROTOCOL_DRAFT.md) · [Guarantees and limits](docs/GUARANTEES.md)

Run `./bin/just test` for the full local suite and `./bin/just audit` for the
online dependency audit. The `chrono` advisory `RUSTSEC-2026-0543` remains
visible in the audit. The release checklist records its limited pilot acceptance.
Run one Wasm build or runtime test at a time per checkout because these commands
share build artifacts. [Test plan](docs/TEST_PLAN.md) · [All development commands](justfile)

- **Operating Pintle** — [Deployment](docs/deployment.md) · [Recovery and rollback](docs/recovery-policy.md) · [Security review](docs/security-review.md)
- **Contributing** — [Implementation plan](docs/IMPLEMENTATION_PLAN.md) · [Architecture decisions](docs/adr/) · [License](LICENSE)

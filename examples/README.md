# Runnable Go mutex examples

From a bootstrapped checkout, run all examples against automatically managed
**local** Wrangler:

```sh
./bin/just example-local
```

This uses Hermit, builds normal Rust/Wasm, creates a local static credential and
executes the real [mutex command](mutex/main.go) as separate processes. Temporary
credentials are removed on cleanup; ignored local SQLite test state remains.
The examples also run in `just test` and CI. No Cloudflare account, deployment,
hosted request or external side effect is involved.

| Mode | Demonstration |
| --- | --- |
| `lifecycle` | Open session, TryAcquire, inspect Current, Renew, explicit Release, then ACK |
| `cancel` | Cancelled caller context; CancelPending before admission prevents delayed execution; committed grant wins later cancellation and must be released |
| `with-lease` | WithLease automatically acquires, renews and releases while caching an immutable document checksum; harmless duplicate work needs no destination fencing |
| `with-fencing` | WithLease passes its exact token to an in-memory inventory store; a delayed older stock update is rejected after a newer one |
| `recover` | Maintain/Lost after deliberately discarding a real committed Renew reply; same-attempt status/replay, ACK, Stop and explicit Release |

These are ordered scenarios. No sleep chooses a concurrency winner; the server
response is the grant/commit barrier. The local correctness suite separately
exercises actual race interleavings and after-admission disconnection.

To run one mode against a server you have explicitly chosen and are authorized to
mutate, set `PINTLE_ENDPOINT`, `PINTLE_TOKEN` and optionally `PINTLE_NAMESPACE`
(default `test`) in your environment, then:

```sh
./bin/just example lifecycle
./bin/just example cancel
./bin/just example recover
./bin/just example with-lease
./bin/just example with-fencing
```

Supply the credential through your shell's private input mechanism or secret
manager, not literal shell history or source. HTTPS is required outside loopback.
Each command creates one unique resource, retains its prepared requests in memory
and has a 45-second deadline. Successful runs release owners and ACK resolved
results. Sessions expire normally; fencing metadata remains. Unexpected failure
stops the example; it does not guarantee cleanup or persist a recovery journal.
Repeated runs create more resource histories, so this is not a load generator.

The inventory destination is an in-memory demonstration, with atomic token
comparison and data update under one mutex. It is not a persistent database
integration. Its high-water mark is per product, compares uint64 exactly and
permits equal-token writes by one owner; it does not deduplicate business effects.
A real database/service must enforce the same check atomically with each write.
Do not infer ownership from client wall time, an old receipt or an open
Lost channel. See the [SDK guide](../sdk/go/pintle/README.md) for errors, deadlines,
crash recovery requirements and the exact scope of these examples.

## Semaphore: a bounded render pool

[`semaphore/main.go`](semaphore/main.go) runs six cooperative jobs through a
capacity-two semaphore. It uses WithPermit for automatic renewal, cancellation,
release and recovery cleanup. Channels demonstrate two simultaneous callbacks;
there are no sleeps deciding admission. All jobs and cleanup are joined.

Use the same endpoint/token/namespace environment as the mutex examples:

```sh
./bin/just example-semaphore
```

Or run `./bin/just example-local` to build and execute both examples against an
automatically managed local Wrangler. The package-consumer check builds both
examples outside this repository's module. No extra dependency or database is
needed. This example performs disposable work; irreversible external actions
require an appropriate destination protocol. Optional semaphore fencing is per
resource and permit slot. Capacity cannot be resized after the first grant.

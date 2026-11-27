# Singleflight hot-key load and ticket recovery on Cloudflare staging

On 2026-11-27, Pintle tested the Go SDK and the normal Worker build on
`pintle-staging` for development candidate `rc-2026-11-27.3` (commit
`225bba1`). The earlier staging smoke test covered cache fills, producer
crashes, cancellation races and the inline size limit. [Cache-fill contract and smoke results](adr/ADR-022-singleflight-cache-fill.md#cloudflare-staging)

## Polling one hot key

The SDK ran six successive cache misses on the same resource. For each miss,
the probe waited for all intended waiters to join before starting the producer
hold. **Every trial had one origin fill**, and every waiter that stayed attached
received the producer's value.

| Trial | Waiters admitted | Producer hold | Waiters cancelled | Polls | All gateway RPCs | Poll RPC p95 | End of hold to last result |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 4 | 4 s | 0 | 24 | 35 | 156 ms | 1.76 s |
| 2 | 15 | 4 s | 0 | 90 | 123 | 200 ms | 1.79 s |
| 3 | 16 | 4 s | 0 | 96 | 133 | 111 ms | 1.59 s |
| 4 | 16 | 12 s | 0 | 160 | 197 | 242 ms | 2.13 s |
| 5 | 16 | 12 s | 0 | 160 | 197 | 94 ms | 1.93 s |
| 6 | 16 | 12 s | 8 at 2 s | 112 | 159 | 206 ms | 2.27 s |

In trials 4 and 5, each waiter made ten `ObserveFlight` calls, including the
poll that delivered its result. During seconds 4–12 of each hold, the waiters
sent **8 polls per second** in aggregate. Cancelling half the waiters in trial
6 reduced that rate to **4 polls per second** over the same interval. Across
the whole trial, this saved 48 polls and added eight cancellation RPCs.

The client counted 844 gateway RPCs with distinct Cloudflare Ray IDs, all at
the BEG location. Of these, 642 were polls. The totals include the overflow
probes. Each full trial rejected a seventeenth waiter. Trial 6 rejected another
admission after eight waiters had cancelled, confirming that cancelled tickets
retain their slots for recovery within the generation. These five probes
returned HTTP 429 (`resource_exhausted`). The other 839 requests returned 200.

Each poll also invokes the resource's Durable Object. The live trace provided
the following per-invocation samples at millisecond precision.

| Where the poll ran | Samples | CPU p50 / p95 | Wall time p50 / p95 |
| --- | ---: | ---: | ---: |
| Worker gateway, matched by Ray ID | 65 | 1 / 2 ms | 54 / 63 ms |
| Durable Object for the load key | 53 | 0 / 1 ms | 27 / 42 ms |

Persisted logs were unavailable for this run. The client counts cover every
RPC, while CPU and wall-time percentiles cover only the traced samples. Total
CPU, SQL usage, billed cost and sustained or multi-region throughput remain
unmeasured.

## Original tickets across a Worker redeploy

Both handoffs began with a producer and waiter holding distinct tickets for
one pending flight. The probes retained those tickets and their original join
requests. All three Worker versions ran the same RC against the same staging
namespace. Version IDs below are abbreviated.

| Handoff | What the trace and the clients verified |
| --- | --- |
| `539c94c7` → `dd47618f` | After Cloudflare assigned 100% of traffic to the new version, a reconstructed Durable Object still running the **old version** handled the first completion. The original waiter had the value 12.216 s after admission began. Later, an authority running the new version served completion replays and waiter reads with the same bytes. |
| `dd47618f` → `17677feb` | The probe held the producer until the trace showed the **new-version authority** observing the original pending flight, 4.251 s after admission began. Both gateway and authority traces placed the first completion on the new version. The original producer ticket completed the flight and the original waiter read the exact value by 4.588 s, within the 30 s deadline. |

In both handoffs, replaying the original joins returned byte-for-byte identical
tickets with the same roles. Repeating completion and observing or cancelling
the waiter after completion returned the committed value. The second handoff
verifies that an active flight can finish on the updated authority. Cross-schema
upgrades and regional failover remain unverified.

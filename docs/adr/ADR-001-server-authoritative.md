# ADR-001 — Authoritative server, thin-but-intelligent SDKs

**Status:** ACCEPTED (2026-10-24, updated to reflect Cloudflare-first runtime)

## Decision

Centralize all authoritative coordination algorithms in Rust server-side components. SDKs express user intent and manage local concerns; they never implement independent lock grant or waiting/queue logic. Public boundary is a versioned, backend-independent RPC contract.

First deployment is Cloudflare Worker gateway -> Rust Durable Object authoritative server -> object-local SQLite, with shared provider-neutral Rust domain/state-machine logic. Future native Rust server backed by FDB/etcd should implement same protocol and semantic guarantees. The Cloudflare adapter is runtime-specific and does **not** run unchanged on AWS.

No mandatory sidecar. Core, protocols, SDKs and deployable adapters should be open source.

## Why

- One authoritative implementation of fencing/leases/idempotency and future helper algorithms instead of duplicating them in Go/Kotlin/TS.
- Natural hosted API-key experience without exposing database credentials.
- Explicit testable failure and backend-capability contracts.
- Cloudflare first avoids hosting an initial DB while we retain long-term portable core.

## Tradeoffs

- Additional network boundary compared with an app library running directly against a DB.
- First runtime is dependent on Cloudflare; portable Rust core alone does not offer turnkey standalone deployment.
- Multiple storage backends will require substantial distinct transaction, clock, watch and notification glue plus conformance.
- Strict per-key authority imposes remote latency and hot-key limits; cannot promise cross-object atomicity.

## Superseded ideas

- Client SDK holding whole coordination algorithm.
- etcd-first/native-server-first implementation ordering (an interim design, now superseded).
- Treating DO as a raw database accessible directly by AWS Rust server.
- Duplicating domain helper methods inside every storage adapter.

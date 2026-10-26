# ADR-004 — Conditional multi-record transactions as a storage-contract hypothesis

**Status:** PROPOSED, NOT ACCEPTED/FROZEN (2026-10-24)

## Background

`ubgo/lock` puts a common domain-level `Locker` interface over independent Redis/PostgreSQL/etcd algorithms; this makes API easy but can hide different safety properties. Its Redis fencing path separates lock grant from counter increment, illustrating why a fence and grant must commit together. etcd's concurrency library uses transactional compares, lease-backed contender keys and revision-aware watches.

## Candidate design

Expose backend-neutral atomic *read snapshot* + *conditional multi-record commit* + *resource-change observation*, with explicit capabilities/transaction scopes, rather than a `CoordinationStore::acquire_lock()` per backend or one huge opaque CAS blob. Services own algorithms, backend adapters own consistent atomic commit and durability.

Candidate sketch:

```rust
trait CoordinationStore {
    async fn read_snapshot(&self, keys: &[Key]) -> Result<Snapshot, StoreError>;
    async fn commit(&self, batch: ConditionalBatch) -> Result<CommitOutcome, StoreError>;
    async fn wait_for_change(&self, key: &Key, after: Revision) -> Result<ChangeOutcome, StoreError>;
}
```

Not final signatures. An unknown commit outcome requires an explicit result; watch may be a separate notifier component. A DO only provides cross-record atomicity *inside one object*. Compare/version predicates including absent-key checks need genuine atomic enforcement.

## Validation rule

Do not freeze until the first Do SQLite transition is implemented and a later backend validates the transaction semantics. Retain clear write-conflict, retries, time-source and capability specifications. Never pretend multiple independent CAS calls are a transaction.

# Provisional P0b compatibility baseline

`p0b.binpb` is a Buf-built FileDescriptorSet for the first experimental wire
schema. `just check-proto` checks source compatibility against it, verifies
format/lint, and regenerates Rust/Go into a temporary directory to detect drift.
CI also compares with the fetched PR base if that base contains `buf.yaml`.

This is not a promise that `pintle.v1` is production-stable. Any deliberate
breaking redesign needs a documented decision and explicit baseline update:

```sh
./bin/buf build --as-file-descriptor-set -o proto-baseline/p0b.binpb
```

Do not refresh the baseline just to silence a compatibility failure.

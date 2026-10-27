# Pintle's development commands. Use ./bin/just, or activate Hermit and use just.
# Recipes run as single Bash scripts; arguments are passed without interpolation.
set default-script
set script-interpreter := ['bash', '-euo', 'pipefail']
set positional-arguments

export SHELL := '/bin/bash'
export PATH := justfile_directory() / 'bin' + ':' + env('PATH')

# List the supported development commands.
[default]
help:
    ./bin/just --list

# Install pinned Hermit tools and the Rust toolchain (also used by core-only CI).
bootstrap-core:
    ./bin/hermit install
    ./bin/rustup toolchain install 1.99.1 --no-self-update --profile minimal --component rustfmt --component clippy --target wasm32-unknown-unknown

# Install all pinned dependencies; requires download access on a fresh checkout.
bootstrap: bootstrap-core
    source ./bin/activate-hermit
    npm ci --ignore-scripts
    # worker-build has no upstream Hermit package; install into project CARGO_HOME.
    cargo install worker-build --version 0.8.7 --locked
    go mod download

# Build release Wasm; optional worker-build arguments follow the recipe name.
build *args:
    source ./bin/activate-hermit
    cd crates/cloudflare
    worker-build --release --locked "$@"

# Generate Go/Connect and Rust Protobuf types into the checkout or an output directory.
generate out_dir='.':
    mkdir -p "$1"
    out_dir="$(cd "$1" && pwd)"
    descriptor_file="$(mktemp)"
    trap 'rm -f "$descriptor_file"' EXIT
    ./bin/buf lint
    ./bin/buf format --diff --exit-code
    ./bin/buf generate --output "$out_dir"
    ./bin/buf build --as-file-descriptor-set -o "$descriptor_file"
    ./bin/cargo run --locked --quiet -p pintle-proto-gen -- "$descriptor_file" "$out_dir/crates/protocol/src/generated"
    ./bin/rustfmt --edition 2024 "$out_dir/crates/protocol/src/generated/pintle.v1.rs"

# Check schema compatibility, formatting and generated-code drift without rewriting sources.
check-proto:
    scratch_dir="$(mktemp -d)"
    trap 'rm -rf "$scratch_dir"' EXIT
    ./bin/just generate "$scratch_dir"
    diff -ru gen/go "$scratch_dir/gen/go"
    diff -ru crates/protocol/src/generated "$scratch_dir/crates/protocol/src/generated"
    ./bin/buf breaking --against proto-baseline/p0b.binpb
    if [[ -n "${PINTLE_PROTO_BASE_REF:-}" ]] && git cat-file -e "${PINTLE_PROTO_BASE_REF}:buf.yaml" 2>/dev/null; then
      ./bin/buf breaking --against ".git#ref=${PINTLE_PROTO_BASE_REF}"
    fi

# Check Rust formatting and portable core/auth lints (no Wasm runtime required).
check-core:
    ./bin/cargo fmt --all -- --check
    ./bin/cargo clippy --locked -p pintle-core -p pintle-auth --all-targets -- -D warnings

# Check all formatting, schema/codegen, Rust lints and Go vet, including test hooks.
check: check-proto check-core
    ./bin/just --fmt --check
    ./bin/cargo clippy --locked -p pintle-protocol -p pintle-proto-gen --all-targets -- -D warnings
    ./bin/cargo clippy --locked -p pintle-cloudflare --target wasm32-unknown-unknown -- -D warnings
    ./bin/cargo clippy --locked -p pintle-cloudflare --target wasm32-unknown-unknown --features test-hooks -- -D warnings
    ./bin/go vet ./sdk/go/... ./tests/integration
    ./bin/go vet -tags=testhooks ./tests/integration

# Run native Rust core/auth tests; optionally select a test-name substring.
test-core filter='':
    ./bin/cargo test --locked -p pintle-core -p pintle-auth "$1" -- --nocapture

# Run generated Go package and SDK tests under the race detector; optional Go regexp.
test-sdk filter='.*':
    ./bin/go test -race -count=1 -timeout=3m -v ./gen/go/... ./sdk/go/... -run "$1"

# Build normal Wasm and test real local Wrangler/SQLite; optional Go regexp.
test-runtime filter='.*':
    ./bin/just build
    ./bin/go test -race -count=1 -timeout=3m -v ./tests/integration -run "$1"

# Run controlled-clock/fault tests; always restore normal Wasm, including on failure.
test-hardening filter='TestMutexStorage|TestMutexSchedules|TestP1Deterministic|TestP11Deterministic|TestP2|TestP3CredentialTimeBoundaries':
    restore() {
      status=$?
      trap - EXIT
      ./bin/just build || status=$?
      exit "$status"
    }
    trap restore EXIT
    trap 'exit 130' INT
    trap 'exit 143' TERM
    ./bin/cargo clippy --locked -p pintle-cloudflare --target wasm32-unknown-unknown --features test-hooks -- -D warnings
    ./bin/just build --features test-hooks
    ./bin/go test -race -tags=testhooks -count=1 -timeout=3m -v ./tests/integration -run "$1"

# Run the complete local verification suite, sequentially against shared Wasm artifacts.
test:
    ./bin/just check
    ./bin/just test-core
    ./bin/just test-sdk
    ./bin/just test-runtime
    ./bin/just test-hardening

# Query online npm and Rust/Go advisory databases; separate from correctness tests.
audit:
    mkdir -p test-results
    ./bin/npm audit --json > test-results/npm-audit.json
    ./bin/node tools/security-audit.mjs test-results/security-audit.json

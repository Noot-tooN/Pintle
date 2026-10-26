# ADR-012 — One Just command interface

Status: accepted, 2026-10-26; requested by the project owner.

## Decision

Use a root `justfile` for project command orchestration. Install and pin Just 1.59.0
through Hermit, with no global installation requirement. CI, Wrangler's build hook
and contributor documentation use the same recipes. Remove the seven replaced
shell scripts and npm command wrappers. Keep the standalone advisory-query program
under `tools/`; coordination tests and assertions remain Rust and Go.

Just supplies discoverable documented commands and whole-script recipes without
Make's file-target semantics. Cargo and Go still own compilation and incremental
build decisions. The extra pinned runner is an accepted cost for one maintained
command interface. Generated Hermit launchers and activation scripts remain managed
by Hermit; they are toolchain plumbing, not duplicate project orchestration.

## Execution and failure behavior

Recipes use Bash with `-euo pipefail`. Arguments use positional parameters rather
than insertion into shell source, including generation directories and test filters.
The checkout's `bin` directory leads PATH for Buf plugins and nested tool calls.
Use `./bin/just` from the root, or activate Hermit and invoke `just` from a subdirectory.

`test` runs checks, native core/auth, SDK, normal runtime and instrumented hardening
sequentially. Focused runtime tests always build normal Wasm first. Hardening installs
an EXIT trap before the instrumented build, restores normal Wasm on test failure,
and reports either the test failure or a failed restoration. INT/TERM request exit
through cleanup; SIGKILL or machine failure cannot guarantee restoration. Concurrent
separate invocations still share artifacts and must use separate checkouts or run
sequentially. This migration does not introduce an interprocess lock.

Dependency installation (`bootstrap`) and online advisory queries (`audit`) remain
separate from correctness verification (`test`). This does not promise an offline
build if caches are missing, or change sandbox/cache/network permissions. No new
coordination behavior, protocol, deployment workflow or push action is introduced.

## Evidence

See [Just migration results](../just-migration-results.md) for executed commands,
full-suite output, parameter handling and failure-cleanup verification. Historical
milestone transcripts are retained unchanged; their reproduction command names
have been updated to the new interface.

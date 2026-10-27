# ADR-019: Package the Go SDK and exercise public examples

Accepted for the unpublished v0 API, 2026-10-27.

The Git remote is `github.com/Noot-tooN/Pintle`, but the Go module and generated
imports still used `pintle.local/spike`. Use the repository's canonical module
path, preserving its case, and ship the SDK and generated messages/client in one
root module. This needs no nested module, extra release process or dependency.
Downstream Go builds compile imported Go packages; they do not build Rust/Workers
or execute the repository's integration tests. Root LICENSE remains applicable.

This is an intentional **Go source/import metadata change before publication**.
The SDK package is `github.com/Noot-tooN/Pintle/sdk/go/pintle`; the wire service
remains `pintle.v1.LockService`. No message fields, methods, stored formats,
authority routing or core algorithms change. Keep Go 1.27.3 as the currently
required/tested toolchain; do not claim an untested older minimum.

Update the schema's `go_package` and regenerate through Hermit/Buf. Preserve the
original P0b descriptor as historical evidence. For compatibility comparisons,
`tools/proto-compat` rewrites only the exact historical `go_package` value on
`pintle/v1/lock.proto` to the canonical path, in a temporary baseline copy. Apply
that same normalization to a Git base descriptor during CI. All FILE rules stay
enabled, and any different/future Go package change still fails. Tests verify
unrelated descriptor content and options are preserved. No blanket exclusion or
wire baseline reset is allowed.

`just check-sdk-package` builds the real example from an independent temporary
Go module using a local replacement, cached dependencies, `GOWORK=off` and network
fetching disabled. It proves local consumer dependency closure, not public module
proxy downloadability. Publishing a tag or commit and testing remote fetch remain
future explicitly approved release actions.

The example command has lifecycle, cancellation and renewal-recovery modes.
`just example-local` runs the real binaries against normal local Wrangler with
ephemeral credentials, and default CI/full-suite execution includes them.
Cancellation order is established by cancelled context/committed grant; renewal
reply loss is injected after a successful response. No sleep determines winners.
The examples perform no business side effects and retain requests only in memory;
they are not a production journal, fenced database integration or retry framework.

Keep Client/Session responsibilities and the existing low-level request helpers
for focused earlier protocol tests. Document the Session constructors as the
current service entry point rather than introducing another convenience API or
silently attaching sessions. Validate malformed resource/capability input before
renew/release preparation consumes a sequence; valid prepared identities remain
immutable. Document ACK, explicit Release, Lost, errors and recovery limits at
the SDK boundary. Broad API redesign and storage recovery features remain out of scope.

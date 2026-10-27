# Separate P3 security review and remediation

2026-10-27. Review was completed and findings recorded **before implementation
changes**. This is a separate security-focused pass by the same agent, not an
independent external audit. The independent-review/release gate remains open.
No deployment, hosted test, credential change, commit or push was performed.

## Review phase and scope

Reviewed the working tree at baseline
`74cdb6694456256b258377b92e8aaf56b5428ff0`, including the uncommitted staging,
SDK/packaging and documentation work. Existing changes were preserved. Before
review, 187 Git-visible file hashes and copies of relevant code were captured
under ignored `test-results/security-review-before/`; ignored credentials were
not copied. Manifest SHA-256:
`b21b9b3d89de2354776699b3d2ca405698172098ad3a05f317613a6c0410631d`.
The review findings were frozen in `findings.json` at **20:25:53.280 UTC** before
editing SDK code or regression tests.

Requirements came from the canonical specification, guarantee matrix, recovery
policy and OSS release checklist. The review covered gateway/authority trust,
authentication and principal isolation, durable result access, ownership/fencing,
transaction/error boundaries, input and resource limits, SDK transport/response
handling, secret formatting/logging, test-hook isolation, dependency advisories
and CI/deployment configuration. There was no CodeGraph index; scoped source reads
were used without creating one.

## Confirmed finding: Client values bypassed credential redaction

**SEC-20261027-1 — moderate, conditional on application debug logging.**
`Client.String` and `Client.GoString` used pointer receivers. Formatting the
returned pointer was safe, but `fmt.Sprintf("%+v", *client)` exposed its unexported
bearer credential. The same applied to `%v`, `%#v`, slices of Client values and
exported struct fields holding a Client value. A reader of those application logs
could reuse a still-valid credential within its authorized scope. No remote
unauthenticated endpoint exposing credentials was established.

A pre-fix probe used only a synthetic credential and printed exposure booleans.
The expanded deterministic regression then failed for all nine value/container
format combinations; pointer cases remained redacted. The transcript contains
type/format information, not the credential value.

**Fix:** make the two constant redacted formatting methods value receivers.
Both Client and *Client then implement the formatting interfaces. Client contains
no mutex or owned lifecycle state, so this does not copy locks or change RPC
behavior. The test covers pointers, values, slices and struct fields with all
three formats. This extends the existing redaction test rather than adding a
parallel test framework. Reflection/custom serializers and raw request payloads
remain outside the debug-formatting guarantee.

## Other reviewed boundaries

| Boundary | Code-path evidence and validation | Limit |
| --- | --- | --- |
| Identity and resource isolation | Gateway reconstructs subject/deadline headers; authority name derives from authenticated tenant plus encoded namespace/type/key; core checks session and owner subject | No new independent platform isolation proof; operator configuration remains trusted |
| Ownership despite known capability | New P2 runtime test gives another principal the complete lease and saved request, then denies replay/status/ACK/Renew/Release/CancelPending; original owner remains active | Same-tenant principals share resource availability; tenant-wide quotas stay deferred |
| Forged internal metadata | Attacker replays the owner's actual acquisition envelope with a forged owner-subject header and still receives PermissionDenied; public internal route returns 404 | Other Workers with trusted authority bindings remain an operator access-control concern |
| Atomicity, fencing and history | Synchronous multi-record transaction, bound SQL parameters, checked counters, same-attempt receipts, owner capability+subject checks; existing rollback/restart/model suites | No snapshot rollback, physical replica-loss or cross-authority migration guarantee |
| Input/resource bounds | Bounded auth configuration, streamed body limit, deadlines, fixed session/window/waiter/subscription limits; existing malformed-wire/auth/capacity tests | Does not bound distinct resources across a tenant |
| SDK uncertainty/transport | HTTPS outside loopback, redirects refused by default, response size/correlation checks, explicit Lost/recovery and ACK semantics; existing SDK/example tests | Custom transports and application persistence/effects are caller responsibilities |
| Logging and build isolation | Bounded allowlisted application metrics, constant public errors, production build ignores test hooks; existing metrics and poison-controller tests | Does not certify every provider log or application serializer |
| Build/release configuration | Pinned dependencies, read-only CI permissions, no stored Git credentials, no default hosted tests/deployments; static staging auth | No artifact-signing or whole supply-chain assessment |

No further exploitable auth bypass or cross-tenant access defect was established
in these paths. This is a scoped conclusion, not proof of absence. The new runtime
test strengthens evidence rather than correcting a server authorization defect.
It deliberately uses the owner's saved session envelope for the forged-header
case: using the attacker's session would be rejected under either identity and
would not detect header trust. Ordered calls and a held owner establish the state;
no sleep or race winner determines the expected result.

## Remediation and validation

Only executable production change in this pass: the two SDK formatting receivers.
Regression coverage changed in `sdk/go/pintle/validation_test.go` and
`tests/integration/security_test.go`. Rust core, auth, Cloudflare adapter, schema,
stored format, dependencies and safety limits are unchanged. Existing Hermit and
Just entry points ran all build/tests; no dependency was installed.

The targeted redaction test failed before the fix and passed after it. An initial
runtime test/check failed to compile because the new harness passed a lease pointer
where a value was required; that test-only error was corrected before final
validation. It was not a server defect. Final suite results are recorded below.

### Final local suite

`./bin/just test` completed with exit status **0**, using Hermit-pinned tools.

| Check | Result |
| --- | --- |
| Formatting, schema/codegen compatibility, Clippy, Go vet and consumer packaging | PASS |
| Native Rust core and auth | PASS: 25 core tests and 2 auth tests, including exhaustive bounded models |
| Go SDK under the race detector | PASS, 1.195 s |
| Protocol compatibility helper / local hosted-test guards | PASS, 1.144 s / 1.198 s |
| Normal compiled Wasm + local Wrangler integration | PASS, 88.387 s |
| New P2 principal-boundary regression | PASS within the normal suite, 0.83 s |
| Runnable SDK examples | PASS within the normal suite, 5.89 s |
| Instrumented rollback, clock, concurrency and restart integration | PASS, 83.942 s |

The targeted `./bin/just test-sdk '^TestClientStringRedactsCredential$'` also
passed after remediation (1.573 s). Remote staging tests were skipped; hosted
scenario reuse in this suite targeted local Wrangler only. Existing real-timer
smoke tests remain smoke evidence, while semantic interleavings and time boundaries
use the deterministic test controller. The new authorization test uses ordered
operations and no sleeps or race-winner assumptions.

The suite restored the normal Wasm build, SHA-256
`86be573f0ef931917bf5d8824f50c6a1e5f6c1b9277a40264e69b3508acc8e1e`.
Local evidence is retained under ignored `test-results/`:
`security-pass-full-suite.log`, `security-pass-before-fix.log`,
`security-pass-after-fix.log`, and the review snapshot. Initial harness compilation
failures remain in `security-pass-principal.log` and
`security-pass-initial-check.log`. Final diff whitespace, Just formatting and
local Markdown file-link checks passed.

### Advisory and scanner evidence

`./bin/just audit` passed at **20:24:27.529 UTC**: npm reported **zero vulnerabilities**;
OSV checked **115 Rust/Go dependency versions with zero findings**. The initial
attempt was blocked by the sandbox proxy's npm allowlist; an approved retry passed.
Only public dependency names/versions were sent. This is a dated database result,
not proof that dependencies have no vulnerabilities. Toolchains and platform
internals are outside this package audit.

The initial Opengrep invocation could not start because the host tool tried to
write `~/.opengrep/semgrep.log` outside the permitted paths. The follow-up below
resolves that tooling gap. No new third-party Go APIs were introduced by the fix,
and no general deprecated-symbol or Rust security-analyzer result is claimed.

### Opengrep follow-up: project-local state and completed scan

2026-10-27. Added `just security-scan rules scanner='opengrep'`. The required rules
argument must be a local file; the optional scanner argument permits an explicit
installed binary. The recipe sets `XDG_CONFIG_HOME`, `XDG_CACHE_HOME`, `TMPDIR`,
`SEMGREP_LOG_FILE` and `SEMGREP_SETTINGS_FILE` under ignored `.hermit/opengrep/`,
creates directories with a restrictive umask, and disables version checks.
Opengrep's [environment implementation](https://github.com/opengrep/opengrep/blob/v1.30.1/cli/src/semgrep/env.py)
confirms the supported log/settings overrides and the requirement that the XDG
config directory exist. Observed execution used the extracted engine, temporary
files and log inside Pintle. No global settings or filesystem permissions changed;
the successful scan ran in the existing sandbox without escalation.

Executed:

```sh
./bin/just security-scan /Users/nirdala/.agents/skills/pr-review/assets/opengrep-go.yaml
```

The installed host **Opengrep 1.30.1** ran **42 Go rules on 8 production files**
with **zero findings, zero reported errors and exit status 0**. Scanned:
`sdk/go/pintle/{client,doc,maintain,session,validation,wait}.go`,
`examples/mutex/main.go`, and `tools/proto-compat/main.go`. The default scanner
ignore patterns skipped six Go test files; two Markdown files had no applicable
Go rules. Generated code, integration/hosted harnesses, Rust, JavaScript and
configuration files are outside this scan. This is bounded pattern-analysis
evidence, not a whole-repository security certification or cross-file proof.

The recipe uses `--strict --error`; a synthetic weak-MD5 fixture in ignored
`test-results/opengrep/probe.go` verified that the same rules detect the expected
finding and return exit status 1. It is a scanner positive control, not a product
finding. No application findings required fixes or suppressions.

Hermit installation attempts for catalog versions 1.32.0 and 1.30.2 returned
HTTP 403, including a sandboxed network-permission retry. No package was installed
or pinned. The existing binary was reused rather than broadening permissions for
an optional tool download. This recipe remains optional and is not a new CI gate;
portable scanner/rules distribution remains separate follow-up work. The external
rules were not copied into the OSS repository.

Reproduction fingerprints (SHA-256):

- Scanner: `7b788794e111ce3fb83f0aa52c100a3b85d37d8b59482262109031f50d4a8d91`.
- Rules: `7c08b9532607dc6f7c78a0e09829f11becead6f4bd293ed7a2fd48b3fe1d3697`.

Evidence: ignored `test-results/opengrep/{version.txt,current.json,current.stderr,probe.json,probe.stderr}`.
The Just recipe and documentation are the only changes in this follow-up. Just
formatting and diff whitespace checks passed. Application code is unchanged since
the full suite recorded above; that suite was not repeated for this tooling-only
follow-up. Independent external review remains open.

## Remaining release work

This phase separation does not satisfy independent external review. Obtain that
assessment, configure a real private vulnerability-reporting channel with maintainer
triage, complete the cross-document release contract review, and validate/approve
the actual release candidate. Public module fetch remains unverified until an
explicitly approved publication. Hosted telemetry/cost gaps remain documented.
SaaS customer controls remain deferred; existing isolation and per-resource bounds
are preserved. See the [release checklist](release-checklist.md).

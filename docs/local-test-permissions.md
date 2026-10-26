# Pintle local test permissions

The project-local `.codex/config.toml` defines and selects `pintle-local` for
trusted Pintle sessions. It does not change the user's global Codex configuration
or other repositories. Configured and checked on macOS, 2026-10-26.

## Scope

The profile extends Codex's workspace sandbox, preserving protected agent/config
paths and the existing developer profile's Git metadata access. Its only added
filesystem write roots are:

- `~/Library/Caches/hermit`
- `~/Library/Caches/worker-build`

These are shared tool caches used by Pintle, not general access to
`~/Library/Caches` or the home directory. Rustup, Cargo, Go, Buf, Wrangler state,
build outputs and test data already live in the project or allowed temporary
locations. Cache permission is limited by directory, not by package name.

Command networking uses Codex's network proxy with only `localhost`, `127.0.0.1`
and `::1` allowed. `mode = "full"` permits HTTP POST for local Connect calls; it
does not add external destinations. `allow_local_binding = true` is required on
this macOS runtime to bind Wrangler/test-controller sockets. The initial profile
with that flag false rejected a loopback bind despite its explicit host allowlist.
The destination allowlist remains restricted to loopback.

Localhost permission includes other services on localhost, since tests allocate
unpredictable ephemeral ports. It is not a per-process or per-test endpoint ACL.
No LAN hosts, external domains or Unix sockets are added. Normal sandbox network
filtering does not control separately authorized web/app/MCP tools or approved
sandbox escalation.

`bootstrap` on a cold cache, tool updates and `audit` need external access and
remain separate approval events. Do not add a wildcard domain, turn off the proxy,
disable approvals globally or approve unrestricted shell/Just runners to make
those commands work. No deployment or push is authorized by this configuration.

## Activation

A running chat can retain its previously selected permission profile. The current
chat did retain the old profile after this file was saved: a plain Hermit command
still failed to write its cache. Select `pintle-local` for the Pintle chat in Codex's
permission controls; reload Codex if the new profile is not listed. Fresh trusted
Pintle sessions load this project's default unless a higher-priority selection or
managed policy overrides it. Do not select full access. The available computer-use
tool cannot operate Codex itself, so activation of this existing chat is a manual
step; editing the file is not evidence that the live chat has switched.

For an explicit local diagnostic from a terminal:

```sh
codex sandbox -C /Users/nirdala/Projects/Pintle -P pintle-local -- ./bin/just test
```

The profile uses macOS cache locations. On another OS, configure that system's
actual tool caches instead of granting the entire home/cache directory. Legacy
`sandbox_mode` settings can supersede named-profile defaults; do not mix the two
configuration mechanisms. Keep protected `.codex` writes approval-gated.

## Validation

Executed with installed `codex-cli 0.164.0`, using its actual macOS sandbox:

- Created and removed temporary probe files in the workspace and both allowed caches.
- Confirmed probe file creation was denied in the project `.codex` directory,
  the parent `~/Library/Caches` directory and the home directory.
- Bound an ephemeral loopback HTTP server and completed a POST request.
- Confirmed an external HTTPS CONNECT was rejected by the proxy with HTTP 403.
- Confirmed a direct socket to a non-loopback documentation address was denied by
  the OS sandbox (rather than merely timing out).

The sandbox tester had to be launched with a one-time outer approval because
macOS rejected nested sandbox creation from the existing chat sandbox. Its child
process and tests were constrained by `pintle-local`; they were not run with full
filesystem/network access. No model task or deployment was started.

`./bin/just test` completed inside that profile with exit code **0**. It passed
32 native core tests, 2 auth tests, the SDK race tests (1.344s), normal Wrangler
integration (35.528s), instrumented hardening (40.446s), schema/codegen checks,
formatting, Clippy and Go vet. Normal Wasm was restored successfully. No per-test
approval was required inside the profile and no external domain was added.
The complete local transcript is retained at
`test-results/local-permissions-validation.log` (ignored development evidence).

The user subsequently selected `pintle-local` for this chat. The same filesystem
and network probes then passed directly through the chat's default command tool,
without additional permissions or a separate sandbox launcher. Live-chat activation
is now verified. A direct `./bin/just test` also exited **0** with no extra
permission requests: core/auth, SDK race tests, normal Wrangler integration,
deterministic hardening, checks and normal Wasm restoration all passed. The
transcript is `test-results/active-profile-tests.log`.

References: [Codex permission profiles](https://learn.chatgpt.com/docs/permissions)
and [project configuration precedence](https://learn.chatgpt.com/docs/config-file/config-basic).

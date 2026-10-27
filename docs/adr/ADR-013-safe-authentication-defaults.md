# ADR-013: Static authentication by default and isolated P0 fixture controls

Accepted for local P3 hardening, 2026-10-27.

The fixture-isolation portion was subsequently superseded by
[ADR-014](ADR-014-retire-p0-fixture.md), which removes the fixture. The static
authentication default remains in force. The rest records the initial decision.

The gateway now treats an omitted `PINTLE_AUTH_MODE` as `static`. It requires
`PINTLE_CREDENTIALS` and the existing mandatory credential-expiry validation;
missing or invalid static configuration fails closed. Test credentials are used
only with explicit `PINTLE_AUTH_MODE=test`. Local generated test configurations
select that mode; deployment examples continue to specify static explicitly.

The JSON `/spike` fixture remains an intentionally insecure local regression tool.
Its gateway and Durable Object handler now reject fixture requests when any
protocol service mode is enabled, or when an explicitly selected auth mode is not
`test`. `LOCAL_SPIKE=true` alone still enables the documented local-only fixture.
This is configuration isolation, not a claim that an operator cannot deliberately
publish a fixture. No live deployment is authorized by these flags.

A separate security-focused pass reproduced two pre-existing configuration bugs:
missing auth mode accepted a fixture credential without expiry, and mixed P2/static
configuration still allowed the public `local-a` fixture identity to seed a P0
resource. P0 routing is separate from mutex routing; the second finding did not
establish access to P2 lease records. Both paths now reject those requests.

`TestSecurityConfigurationBoundaries` exercises these cases with compiled Wasm,
the positive default-static path, and fixture isolation for P0b/P1/P1.1/P2.
Existing standalone P0 regression tests retain coverage of the deliberate fixture.
No core state, persistence layout, public schema or dependency changes are required.

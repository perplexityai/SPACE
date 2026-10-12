# `crates/libs/spaced`

The spaced crates share one wire contract, `proto/spaced`. A guest from an
older release keeps running against newer hosts; the guest-compatibility
section of `docs/versioning.md` is the policy.

## Changes

- Host changes must keep accepting every guest in
  `SUPPORTED_SPACED_GUEST_VERSIONS` (`common/src/compatibility.rs`), including
  guests in templates and dormant snapshots. Raising the floor is its own
  change with a migration plan.
- Change `proto/spaced` additively. Keep field numbers, types and RPCs until
  every version that uses them is retired. A guest-side legacy endpoint also
  needs the minimum host version, and its marker says so, as
  `proto/spaced/bridge/guest/v1/legacy.proto` does.
- `proto/spaced/worker/v1` connects the guest service to a worker spawned
  from its own executable. Any change to it bumps the protocol version the
  worker reports in `Ready`, which the parent checks.
- A feature that needs a newer guest answers older ones with
  `guest_capability_unsupported_status` (`crates/libs/common/grpc`).
- Host fallbacks to legacy guest RPCs fall back only on `Unimplemented`; a
  transport failure never switches protocol or repeats a started stream
  (`host/server/src/compat.rs`). Mark each legacy path with
  `SPACED_COMPAT_REMOVE_WHEN_MIN_GUEST_<version>`.
- New spaced-host RPCs on the host listener are authorized by UDS peer (root
  or the sbx-mgr account) in `HostAuthorizationLayer`. New bridge-host RPCs,
  which the guest calls, call `authorize_bridge_host_peer` and keep the
  service's message-size cap.
- A telemetry-only guest dependency goes behind a non-default feature:
  release images build the guest binaries in their own `cargo build` with no
  features (`docker/Dockerfile.rust-build-common`). A service-wiring
  dependency goes behind `service-entrypoint`, which `spaced-svc` enables.

## Tests

Follow "Privileged tests and the CI host" in `docs/rust/space-conventions.md`.

- `SpacedHostServiceConfig::default()` and `SpacedHostService::new` use the
  production sockets; a test sets every path.

Module moves change log targets: see "Log targets" in
`docs/rust/space-conventions.md`.

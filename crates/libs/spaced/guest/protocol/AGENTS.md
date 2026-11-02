# `pplx-space-spaced-guest-protocol`

The guest's side of the spaced wire contract, `proto/spaced`. A guest from an
older release keeps running against newer hosts; the guest-compatibility
section of `docs/versioning.md` is the policy.

- Change `proto/spaced` additively. Keep field numbers, types and RPCs until
  every version that uses them is retired. A guest-side legacy endpoint also
  needs the minimum host version, and its marker says so, as
  `proto/spaced/bridge/guest/v1/legacy.proto` does.
- `proto/spaced/worker/v1` connects the guest service to a worker spawned
  from the explicitly selected executable. Any change to it bumps the protocol
  version the worker reports in `Ready`, which the parent checks.
- The host and guest services' rules are in `crates/libs/spaced/AGENTS.md`.

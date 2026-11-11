# `pplx-space-spaced-common`

A guest from an older release keeps running against newer hosts; the
guest-compatibility section of `docs/versioning.md` is the policy.

- Host changes must keep accepting every guest in
  `SUPPORTED_SPACED_GUEST_VERSIONS` (`src/compatibility.rs`), including guests
  in templates and dormant snapshots. Raising the floor is its own change with
  a migration plan.
- This crate compiles part of `proto/spaced`; the wire-contract rules are in
  `crates/l20_domain/spaced/guest/protocol/AGENTS.md`, and the host and guest
  services' rules in `crates/l40_api/spaced/AGENTS.md`.

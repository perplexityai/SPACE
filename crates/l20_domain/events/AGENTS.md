# `pplx-space-events` Agent Notes

Before changing this crate or adding sandbox event emission call sites, read
`README.md` in this directory.

Keep the sandbox event vocabulary domain-level. Add implementation identity in
`SandboxEventSource`, causality in `trigger`, and supporting context in
`SandboxEventDetails`; do not encode component names into `SandboxEventType`.

When adding, renaming, or removing an event type, source, or detail field:

- Update `README.md`.
- Update the tests in `src/lib.rs`.
- Keep the PR description's event table and coverage notes aligned with the
  emitted schema.

Do not add high-volume request telemetry here. This crate is for logical
sandbox state transitions, failed state-change requests, CAS/race outcomes,
cleanup paths, network allocation/policy/enforcement outcomes, port exposure
outcomes, and immutable snapshot/restore handoff facts.

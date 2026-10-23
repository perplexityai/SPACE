---
name: space-rust-development
description: Develop, review, or diagnose SPACE Rust code under `crates/` and Rust-facing protobufs.
---

# Develop SPACE Rust

Use `docs/rust/README.md` to locate design guidance when the change needs
architectural context. Consult `docs/rust/space-review-lessons.md` for broad
refactors or PR self-review.

## Select conditional guidance

Read only the references that match the task:

- Domain nouns, primitive boundary values, closed vocabularies, sum types, or
  typestate: [types](references/types.md).
- New or changed fallible functions and error types: [errors](references/errors.md).
- Async functions, spawned tasks, loops, cancellation, channels, locks, or
  shutdown: [async and Tokio](references/async.md).
- API handlers, controller placement, reconcilers, or Kubernetes substrate
  behavior: [API services](references/api-services.md).
- Before a Rust PR, push, or self-review: [review discipline](references/review-discipline.md).

## Model the domain

- Parse raw strings, numbers, UUIDs, proto fields, and JSON once at the wire
  boundary. Pass validated domain types through the interior call graph.
- Reuse a noun from the crate that owns it before creating a local parallel
  type, and extend it there instead of forking it. Put a new shared type in the
  crate that owns the concept or a new single-responsibility crate, never in
  `pplx-space-common-core` or `pplx-space-common-svc`.
- Model closed vocabularies as exhaustive enums. Implement `as_str`, `Display`,
  and `FromStr`; put predicates on the enum; match variants rather than string
  literals. The types reference defines the complete pattern.
- Library and reusable application boundaries return small, operation-specific
  `thiserror` enums. Reserve `anyhow` for human/OS-facing orchestration such as
  `main`, CLI commands, build scripts, benchmarks, and tests. Preserve sources
  and never classify `Display` text. The errors reference defines exceptions.
- Use `tokio_util::sync::CancellationToken` for cooperative service shutdown.
  Read the async reference before adding tasks, loops, or shutdown behavior.

## Repository conventions

- Wrap every SQL statement passed to `sqlx::query*` in the shared `sql!` locator
  macro. Runtime-built SQL uses `sqlx::AssertSqlSafe(sql!(expr))`.
- Bind a control-plane `managed_by` substrate value as a parameter
  (`managed_by = $n` with `.bind(SubstrateKind::Kubernetes)`) instead of
  writing `'k8s'`/`'space'` in SQL text. In Rust, a decision about a row's
  substrate calls the `SubstrateKind` method named for the question, a fact
  about the running substrate reads `capabilities()`, and code never compares
  the label string ("Substrate values are bound parameters" in
  `docs/rust/space-conventions.md`). archcheck's
  `pattern_ratchet.substrate_literals` rule catches the common literal forms
  in production code.
- Use UUIDv7 for new SPACE-owned durable identifiers: `Uuid::now_v7()` in Rust
  and `space_uuid_v7()` in Postgres. A UUIDv4 exception needs a concrete privacy
  or protocol reason.
- Use AWS-LC for cryptography. Do not add direct `ring` dependencies or enable
  `ring` provider features.
- Use `jiff` for new time code and convert protobuf timestamps and durations at
  the boundary.
- Keep platform dependencies optional and behind named Cargo features. Default
  features of base crates must compile on every platform.
- Use `tracing` in libraries and configure `tracing-subscriber` only in apps
  through the shared helpers. Do not use `println!` for diagnostics.
- Prefer `DashMap` for independent concurrent map operations. Use an explicit
  lock when an invariant spans multiple operations.
- Delete dead code instead of adding `#[allow(dead_code)]`.
- The common service initializer (`init_service` in
  `crates/libs/common/svc/src/lib.rs`, which installs the notifier from
  `crates/libs/systemd-watchdog`) arms the systemd watchdog before slow
  startup work and sends an initial notification immediately. Keep that
  ordering when refactoring startup code; direct deployments run
  `WatchdogSec=30s` (also noted in the `$space-validation` local-development
  reference).
- Protobuf definitions live in `proto/` and generate Rust through `tonic-build`;
  `protoc` is required.

`docs/rust/space-conventions.md` carries the rationale for SQL, cryptography,
and test-placement conventions.

## Validation

Use targeted checks during iteration. For a requested commit, push, or PR,
select final checks from [review discipline](references/review-discipline.md).
Use `$space-validation` when proof requires Docker, Linux/KVM, Kind, or another
runtime environment.

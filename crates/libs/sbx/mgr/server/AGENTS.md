# `pplx-space-sbx-mgr` server

## Changes

- The node-local SQLite database belongs to `pplx-space-sbx-mgr-store`
  (`../store`); its `AGENTS.md` has the state-write and child-table rules.
  Map an `update_state_from` outcome to a status with
  `require_state_write_from`, or write it best effort with
  `update_state_from_best_effort` (`src/service/cleanup_support.rs`); a caller
  that answers one case differently matches `state_write_result` and passes
  the rest to `into_status`.
- `src/service/adapter.rs` implements the gRPC trait; most handler bodies live
  in `src/service/grpc_core.rs`, `src/service/grpc_lifecycle/` and the other
  `src/service/grpc_*.rs` modules.
- sbx-mgr runs unprivileged: btrfs, dm-crypt, VM launch and runtime cleanup
  are node-manager RPCs. New RPC fields carry domain identifiers, not host
  paths; existing path fields (`dest_dir`, `artifact_dir`) are validated by
  node-manager.

## Tests

Follow "Privileged tests and the CI host" in `docs/rust/space-conventions.md`.

- Tests must not dial node-manager's fixed socket
  (`pplx_space_node_mgr_client::connect()`, `connect_once()`, `lazy()`); give
  the code under test a test-served node-manager. Cancelling `shutdown_token`
  first protects only code paths that check it before dialing.
- Build test services from `SandboxManagerServiceOptions::for_test()` or
  `for_test_in(dir)` and override only the fields a test exercises.
  `SandboxManagerServiceOptions::default()` is production wiring: it reads
  `SPACE_NODE_ID` or the host's node-id file, uses the production host-control
  and credential-manager sockets, probes `fc.pid` under `SANDBOX_RUN_DIR`, and
  dials the host's node-manager and template-manager. Constructors read env
  regardless.
- Never call `db::open()`; use `db::test_conn()`.

Module moves change log targets: see "Log targets" in
`docs/rust/space-conventions.md`.

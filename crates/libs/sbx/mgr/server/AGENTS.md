# `pplx-space-sbx-mgr` server

## Changes

- Write `sandboxes.state` only through `src/db` writers. A new transition uses
  or adds a compare-and-set writer in `src/db`; no generic writer compares
  against the caller's observed state. A refused write must leave every row
  unchanged.
- Foreign keys are off: register a new sandbox child table in
  `delete_sandbox_child_rows` (`src/db.rs`).
- `src/service/adapter.rs` implements the gRPC trait; most handler bodies live
  in `src/service/grpc_core.rs`, `src/service/grpc_lifecycle/` and the other
  `src/service/grpc_*.rs` modules.
- sbx-mgr runs unprivileged: btrfs, dm-crypt, VM launch and runtime cleanup
  are node-manager RPCs. New RPC fields carry domain identifiers, not host
  paths; existing path fields (`dest_dir`, `artifact_dir`) are validated by
  node-manager.

## Tests

Follow "Tests run as root on the CI host" in `docs/rust/space-conventions.md`.

- Tests must not dial node-manager's fixed socket
  (`pplx_space_node_mgr_client::connect()`, `connect_once()`, `lazy()`); give
  the code under test a test-served node-manager. Cancelling `shutdown_token`
  first protects only code paths that check it before dialing.
- `SandboxManagerServiceOptions::default()` reads `SPACE_NODE_ID` or the
  host's node-id file, uses the production host-control and
  credential-manager sockets, and probes `fc.pid` under `SANDBOX_RUN_DIR`. Set
  every field where a test reaches them; constructors read env regardless.
- Never call `db::open()`; use `db::test_conn()`.

Module moves change log targets: see "Log targets" in
`docs/rust/space-conventions.md`.

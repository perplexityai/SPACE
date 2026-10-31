# `pplx-space-node-mgr` server

`README.md` covers execution and cleanup authority.

## Changes

- New RPCs take domain identifiers and derive host paths here. Validate an
  existing path field against its canonical root before use
  (`src/runtime_paths.rs`, `src/service/validation.rs`); never add an
  unvalidated path field.
- Pin privileged inputs before mutating or mounting them
  (`src/service/trusted_paths.rs`). Mutate pinned subvolumes through the
  pinned-descriptor helpers (`src/btrfs.rs`, `src/clone_cleanup.rs`); where
  the CLI is unavoidable, bind it to the pinned descriptor in its own mount
  namespace, as `src/service/artifact_fs/receive.rs` does. Make that
  namespace a slave, not private, so node-manager's unmounts still reach it.
- Host tools run through `src/host_command.rs`, and `nft` mutations through
  `src/firewall.rs`. Use `host_command::Command` for ordinary tools, including
  the runtime-supervisor activator. Direct `std::process::Command` belongs only
  in the spawn implementation, pinned receive namespace setup, tests, and the
  existing non-Linux `src/net/fallback.rs`. Archcheck's
  `node_manager_commands` ratchet enforces this boundary.
- Supervisor RPCs replay only failures that provably never executed
  (`src/runtime_supervisor_client.rs`): a `get_or_connect` dial error,
  `is_client_connect_failure` and `is_request_refused_before_transmission`.
  Do not widen that class. `is_client_connect_failure` is shared
  (`crates/libs/common/grpc/src/retry.rs`); changing it changes every gRPC
  client.

## Tests

Follow "Privileged tests and the CI host" in `docs/rust/space-conventions.md`.

- Never call `NetworkPool::open`; `NetworkPool::open_in_memory_for_test`
  starts no host-networking worker and records node-status marks instead of
  notifying node-ctl. Never give a test pool `NodeCtlNodeStatusDirtySink`.
- Unit-test services must not rely on host-reading defaults for runtime
  paths, memory admission, the lifecycle limiter or the supervisor client;
  inject test-owned ones.
- Host-mutating tests need an environment opt-in naming a disposable target,
  as `SPACE_NODEMGR_RECEIVE_TEST_BTRFS_DIR` does for receive tests
  (`SPACE_NODEMGR_TEST_BTRFS_DIR` opts into other Btrfs tests).

Module moves change log targets: see "Log targets" in
`docs/rust/space-conventions.md`.

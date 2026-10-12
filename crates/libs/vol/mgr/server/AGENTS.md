# `pplx-space-volmgr`

Tables named here are in volume-manager's own SQLite database.

## Changes

- `pplx-space-common-jobs` owns every `jobs` (`db/common`) status
  transition, including cancellation and restart recovery. New transfers
  submit through the `BackgroundWorker` and run in `JobExecutor`; they never
  write `jobs` themselves.
- An exit path that leaves transfer or recovery tasks running calls
  `hard_exit` while holding `service.mutex` (`src/run.rs`); never release it
  by normal unwind. Startup cleans orphaned staging files before recovering
  jobs, so both see the same jobs.
- Volume-manager runs no btrfs command. A btrfs-send push leases its source
  from node-manager and records the lease in the job payload; cancelling the
  job or failing its submit releases it. The periodic census deletes sources
  no pending, running or cancelling job holds, and nothing if it can't parse
  the census.
- New code must work under both the legacy `space-sbxmgr` account and
  `space-volmgr`, which Kind runs. Files shared with sandbox-manager rely on
  group `space-cache-rw` and umask `0022`. Node-manager's btrfs send-lease
  RPCs admit `space-volmgr` and no other service account. Operator steps are
  in "Separating volume-manager's credential identity" in
  `docs/installation.md`.
- Give a new RPC one of the peer allowlists in `src/service.rs`;
  socket-group membership grants connection, not authority.

## Tests

Follow "Privileged tests and the CI host" in `docs/rust/space-conventions.md`.

- New node-manager callers take the client as a parameter, so tests never
  dial node-manager's fixed socket. Existing exceptions: `src/btrfs_push.rs`
  and `get_push_capabilities`.
- Credential paths dial credential-manager through
  `pplx_space_credmgr_client::connect_from_env()`; tests must not reach them.

Module moves change log targets: see "Log targets" in
`docs/rust/space-conventions.md`.

# Guest execution service

`pplx-space-spaced-guest-execution` owns the guest daemon's process and filesystem
implementation. `pplx-space-spaced-guest-server` is its production consumer and
delegates through the existing generated `Process` and `Filesystem` service
contracts. Requests, responses, streams and failures retain their protobuf/Tonic
representation. The daemon owns guest readiness, listeners, host communication,
cgroup initialization and the overall shutdown deadline.

The guest bridge and worker schemas live in `pplx-space-spaced-guest-protocol`.
`spaced-common` re-exports those types for existing callers. This service is
distinct from `execution/common`, which describes portable sandbox runtimes and
execution metadata.

## Guest process requirements

- Linux filesystem workers re-execute the daemon binary. Its entrypoint must
  dispatch `run_filesystem_worker_from_environment()` before ordinary application
  initialization or runtime threads. The supported daemon entrypoint supplies
  that dispatch; arbitrary executables are not supported worker launchers.
- Linux identity workers require the daemon's privilege to install the requested
  OS account's groups and IDs; an unprivileged worker host is unsupported. Direct mode
  runs filesystem syscalls with the daemon's credentials; its `user` selects path
  interpretation and ownership handling, not impersonation. Neither mode creates
  a filesystem sandbox. The daemon runs inside the guest's isolation boundary.
- The daemon registers `filesystem_grpc_service()` and `process_grpc_service()`
  to retain transport payload admission as well as the execution limits. Calling
  generated traits directly does not install the transport admission layer.
- The daemon cancels the shared token and coordinates listener shutdown with draining work.
  `drain()` joins tracked work; it is not a reusable shutdown-and-reject contract
  for subsequent direct calls. Direct filesystem calls can still have effects.
- Linux PTY descriptors are created with close-on-exec atomically. Other Unix
  targets serialize PTY allocation and workload spawning through a private lock;
  unrelated spawners do not participate in that coordination.
- Process supervision tracks command leaders and their output. Termination uses
  process groups while supervision is active. Background descendants that close
  or redirect inherited output can outlive a completed leader; draining this
  service does not establish that every descendant has exited.

This crate retains the daemon contract. General application embedding requires
an explicit worker launcher, credential/capability policy, native admission and
shutdown semantics, descendant ownership, and an integration exercising those
requirements before exposing a separate supported API.

The proposed support profile, ownership contract, and implementation acceptance
criteria are in the [embedding RFC](../../../../../docs/space/rfcs/2026-10-06-guest-execution-embedding.md).
It is a proposal; the requirements above describe current behavior.

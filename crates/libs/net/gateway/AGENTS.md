# `pplx-space-net-gateway` server and client

## Changes

- A change that needs a namespace, an `nft` rule or a capability grant is
  made in node-manager or the runtime supervisor; the gateway never creates,
  enters or deletes a namespace or runs `nft`. Node-manager installs the
  TPROXY rules (`setup_netgtw_tproxy_rules`) and chooses the inherited
  listeners and capability grant
  (`crates/libs/node/mgr/server/src/service/runtime.rs`). The supervisor
  enters the namespace, binds the listeners and drops to the gateway uid,
  keeping `CAP_NET_ADMIN` and `CAP_NET_BIND_SERVICE`. The gateway's own
  socket options use only those capabilities.
- New egress proxy and resolver sockets carry `OUTBOUND_MARK`
  (`pplx_space_net_common::proxy`), as the TCP relay, UDP proxy and resolver
  do. Node-manager's rules return marked packets; an unmarked one can be
  captured again by the gateway's TPROXY rule. Service-port ingress, which
  dials into the guest, takes no mark.
- New policy input travels in sbx-mgr's host-control `GetNetworkPolicy`
  response, which the gateway pulls; `RefreshNetworkPolicy` only triggers
  another pull.
- New state changes go through `GatewayNetworkState`'s writer lock. `run`
  gives it a hook that prunes DNS caches and cancels DNS-attributed TCP
  connections the new policy denies; an install path adds no reconcile.
- A new control RPC authorizes its peer first, with `ALLOW_NET_SERVICE`
  unless read-only (`ALLOW_NET_OBSERVER`) (`server/src/control.rs`).

## Tests

Follow "Privileged tests and the CI host" in `docs/rust/space-conventions.md`.

- `run` and its policy loader dial the production host-control socket; give
  the code under test its own `PolicyLoader`.
- A test must not reach the relay's marked dial (`SO_MARK` needs a
  capability); pass a test connector where the code takes one, as host-bound
  HTTP does.

Module moves change log targets: see "Log targets" in
`docs/rust/space-conventions.md`.

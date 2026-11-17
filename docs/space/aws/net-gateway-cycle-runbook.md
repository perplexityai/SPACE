# Net-gateway fleet cycle

Use `scripts/netgtw/cycle-fleet.sh --context <ctx>` to preview stale gateways on
regular workers. Add `--node <node-name>` to inspect one node. The script is a
dry run unless `--execute` is supplied; `--refresh-config` selects live gateways
regardless of binary SHA and does not shrink its selection after a cycle.

Before creating a privileged per-node Job, an execute run previews the selection
through that node's sandbox-manager pod. An empty successful preview skips the
Job. A failed preview prints the original error, reports `no Job created`, and
marks the node's sweep failed. Resolve the preview failure before retrying; a
Job cannot repair a state-read failure in the same CLI. The CLI reads sandbox
state in bounded gRPC requests and splits a request further if its decoded
reply exceeds Tonic's limit. If one sandbox's state still exceeds the limit,
the preview fails without cycling any gateway.

For `--refresh-config`, the preview exports the selected IDs and the Job uses
those explicit IDs. `--export-ids <file>` saves a cohort without cycling; use
`--ids-file <file>` with `--node` to retry that exact cohort.

`node did not converge (phase=Failed)` reports the cycle Job pod phase. It does
not describe Kubernetes node health. Inspect the preceding Job logs to find
the actual failure.

## Keep guest traffic during a cycle

A cycle stops the old gateway before its replacement listens, and the sandbox
firewall drops what the guest sends in between, DNS queries included. A lost
DNS query costs the guest a full resolver retry, five seconds with glibc's
defaults. Add `--hand-over-sockets` to hand the old gateway's listening sockets
to its replacement: the runtime supervisor stops the old gateway and starts the
new one on the same kernel sockets, so that traffic waits for the new gateway
instead. Connections the old gateway already relays still drop.

The flag is off unless given. Each cycled line reports whether the sockets were
handed over. A node whose node-manager or runtime supervisor predates the
handover stops and relaunches the gateway as without the flag, and the line says
so. A handover that fails, for example on a gateway without launch evidence,
falls back the same way. The host `space-sbxmgr` that `--execute` runs rejects
the flag until the node's image includes it, so roll the node services out
first.

## Protected control endpoint rollout

Sandbox-manager clients connect directly to the isolated gateway listener under
`/space/run/net-gateway/<sandbox-id>/control.sock`. The compatibility alias in
the sandbox-owned run directory remains for old clients, but new clients never
use it: a compromised sandbox-UID sidecar can replace that alias.

Deploy the isolation-capable runtime supervisor and node-manager first, then
cycle resident gateways using the preview and execute workflow above before
upgrading sandbox-manager clients. Use `--refresh-config` to include gateways
whose binary is current but whose live UID or socket layout is still legacy.
Already-isolated gateways work with both client versions; upgrading only the
gateway preserves old-client connectivity through the alias.

Upgrading clients before cycling shared-UID gateways fails closed with a
protected-endpoint error and a `space-sbxmgr cycle-net-gateway` recovery command.
Resident resume remains paused when its policy confirmation cannot connect;
policy reconciliation retries but cannot repair a legacy listener's location.
An operator must complete the gateway cycle. The client does not automatically
restart a live gateway or fall back to the sandbox-owned alias. Rolling clients
back restores old compatibility behavior, including its alias-spoofing exposure.

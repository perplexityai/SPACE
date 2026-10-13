# Kind node-service pod shape

This library holds what the Kind node-service charts
(`charts/space-<service>-local`) share: the role-derived names, labels and node
pool, the identity environment, the ClusterApi token projection, the `/space`
host mounts, the instance-local health-socket probes, the rollout strategies and
the `prepare-run-dir` init container. It deploys no resources. Each chart
renders one DaemonSet for one node service and states its own privileges, host
namespaces, mount propagation and grace period beside the code that requires
them; see "Node services" in `infra/space/kind/README.md`.

Consumers import the shared `node` defaults (Chart.yaml `import-values`) and link
this chart under `charts/space-node-service`, as they link
`space-kind-telemetry`. Unix identities are read from
`infra/space/identity/node-identities.sh` through each chart's
`identity/node-identities.sh` link, so a pod's UID, GID and supplementary groups
are the ones the worker image and the node layer create.

| Value | Default | Meaning |
| --- | --- | --- |
| `node.role` | `worker` | `worker` or `builder`; selects the node pool and the reported role |
| `node.internalApiEndpoint` | `https://space-internal-api:9800` | ClusterApi |
| `node.credmgrAuthorityEndpoint` | `https://space-credmgr-authority:9810` | credential-manager authority (agent upstream, node-manager volume DEKs) |
| `node.diskAdmission.*` | 1 GiB each | Kind-sized `/space/cache` admission floor and reservation |

# space-observability

One Helm release that installs the observability backend SPACE producers send
to, selected by a single value:

| `global.observability.mode` | Installs |
| --- | --- |
| `off` (default) | Nothing. |
| `datadog` | A Datadog Agent DaemonSet: DogStatsD on host port 8125/UDP, APM on 8126/TCP, pod log collection from SPACE namespaces. Uses an API key Secret you create. |
| `otlp` | One gateway OTLP Collector, Prometheus, Tempo, Alloy and Loki, and Grafana with generated admin credentials and its datasources wired. |

Any other value fails the render. The `global.observability` keys are the same
ones the SPACE services charts read, so one values file can drive both.

## Install

Install this release **before** switching SPACE services to the same mode, in
its own namespace (not a SPACE namespace). Vendor the dependencies first:
Helm does not resolve nested `file://` dependencies, so each signal chart's
`common` library goes in before the umbrella packages them.

```bash
charts=infra/space/observability/charts
for chart in metrics logs traces grafana space-observability; do
  helm dependency build "$charts/$chart"
done
kubectl create namespace space-observability
```

### otlp

```bash
helm upgrade --install space-observability "$charts/space-observability" \
  --namespace space-observability \
  --set global.observability.mode=otlp \
  --set global.observability.cluster=<cluster-name> \
  --set global.observability.environment=<environment> \
  --wait --timeout 10m
```

`global.observability.cluster` is required: it labels every log stream.

SPACE producers do not send to the gateway Service. In otlp mode every
producer (pods, host-network services, the runtime supervisor and the worker
shim) sends OTLP/gRPC to the node it runs on, `http://$(NODE_IP):<nodeLocalPort>`
(default 4317, `NODE_IP` from the pod's `status.hostIP`), and a node-local
Collector there forwards to the gateway at
`collector.<release-namespace>.svc:4317`. **otlp mode therefore requires the
node-local Collector.** This chart does not install it yet: nothing listens on
the node port, and producers switched to otlp fail to export until the
node-local Collector DaemonSet is added to this chart. Install the two
together.

What you get, and what to change:

- **Storage.** Prometheus, Tempo and Loki each claim a PVC from the cluster's
  default StorageClass (2 Gi, 4 Gi, 2 Gi), annotated to survive uninstall.
  Without a default class, either name one per signal
  (`metrics.storage.storageClassName=<class>`, likewise `traces`, `logs`) or
  fall back to `emptyDir`, which **loses all stored data whenever the pod
  leaves its node**:

  ```yaml
  metrics:
    storage: {useDefaultStorageClass: false, ephemeral: true}
    prometheus: {resources: {limits: {ephemeral-storage: 3Gi}}}
  traces:
    storage: {useDefaultStorageClass: false, ephemeral: true}
    tempo: {resources: {limits: {ephemeral-storage: 5Gi}}}
  logs:
    storage: {useDefaultStorageClass: false, ephemeral: true}
    loki: {resources: {limits: {ephemeral-storage: 3Gi}}}
  ```

  In ephemeral mode, the data size and total `ephemeral-storage` limit must
  use positive whole `Mi` or `Gi` quantities, with at most nine digits. The
  limit must cover the data size plus 256Mi for logs and writable layers;
  the chart refuses smaller budgets instead of raising limits silently.
  PVC-backed storage is unaffected.

- **Grafana.** With no Secret of your own, the chart generates
  `space-observability-grafana-admin` (user `admin`, random 32-character
  password), keeps it across upgrades, and leaves it in place on uninstall.
  Read it with
  `kubectl -n space-observability get secret space-observability-grafana-admin -o jsonpath='{.data.admin-password}' | base64 -d`.
  To bring your own, set `grafana.auth.existingSecret` to a Secret with
  `admin-user` and `admin-password`. The UI has no Ingress; use
  `kubectl port-forward --address 127.0.0.1 service/grafana 13000:3000`.
  Prometheus, Loki and Tempo datasources point at the in-namespace Services.
  With `grafana.persistence.enabled=true`, ephemeral storage uses the same
  budget rule; a 1Gi data volume requires at least 1280Mi in
  `grafana.resources.limits.ephemeral-storage`. With persistence disabled,
  the existing `grafana.persistence.emptyDirSizeLimit` behavior is unchanged.
- **Network policy.** Every component gets a NetworkPolicy. The gateway
  accepts OTLP only from pods in `spaceNamespaces` (default `[space]`); the
  backends accept only the gateway, Alloy and Grafana. Rendering a policy does
  not prove the CNI enforces it: test an allowed and a denied producer.
- **Logs.** Alloy tails `/var/log/pods/<namespace>_*` for each SPACE namespace;
  `logs.collection.podTargets` must cover every entry of `spaceNamespaces`, and
  the chart fails if it does not. The gateway also forwards OTLP logs to Loki.
  Host journal collection is off by default because a node without a persistent
  journal at `logs.collection.journal.path` would leave Alloy pending.
- **Dropping a signal.** `metrics.enabled`, `traces.enabled`, `logs.enabled`
  and `grafana.enabled` remove one chart; disable its Grafana datasource with it.

The per-signal charts are the standalone charts in `../metrics`, `../logs`,
`../traces` and `../grafana`; every option in their `values.yaml` is available
under the matching key here. Installed on their own, without
`global.observability.mode`, they behave exactly as before.

### datadog

```bash
kubectl -n space-observability create secret generic datadog-api-key \
  --from-literal=api-key=<your-api-key>
helm upgrade --install space-observability "$charts/space-observability" \
  --namespace space-observability \
  --set global.observability.mode=datadog \
  --set global.observability.cluster=<cluster-name> \
  --set global.observability.environment=<environment> \
  --set global.observability.datadog.apiKeySecret.name=datadog-api-key \
  --set datadog.site=<your-datadog-site>
```

The release fails if `apiKeySecret.name` is unset, or, when installing against
a cluster, if that Secret or its key (`apiKeySecret.key`, default `api-key`)
does not exist. `helm template` cannot see the cluster, so only the first check
runs there. SPACE pods send to `status.hostIP` on the DogStatsD and APM host
ports; host processes can also use the Unix sockets under `/var/run/datadog`.
A NetworkPolicy admits DogStatsD and APM traffic only from pods in
`spaceNamespaces` and `datadog.hostPeers`. Host-network services reach it as
the pod's own node where the CNI exempts that traffic (kindnet: root processes
only); otherwise list node addresses in `datadog.hostPeers`, and only where the
sandbox egress policy denies the node range. Logs are
collected from `spaceNamespaces` only. A SPACE container whose output
must not be collected opts out with an `ad.datadoghq.com/<container>.logs_exclude`
annotation, which outranks the namespace inclusion. Local clusters whose
kubelet certificate does not cover the node IP (Kind) need
`datadog.kubeletTLSVerify=false`. The Agent tolerates every taint by default
(`datadog.tolerations: [{operator: Exists}]`) because producers on tainted
worker and builder nodes send to the Agent on their own node; narrow it to
your nodes' taints if needed.

## Switching modes

Switching is not loss-free. Producers keep sending to the old destination until
they restart with the new mode, and a removed backend drops whatever reaches it
meanwhile. Install the new mode's backend first, switch the services, then
remove the old backend. PVCs and the generated Grafana Secret are retained on
uninstall; delete them explicitly when their data is no longer needed.

## Limits

This is a working default, not a production design. Every backend is a single
replica with local storage and in-memory queues, OTLP receivers are plaintext
and unauthenticated, Loki and Tempo have no tenant separation, and resource
requests are sized for a small cluster. The
[shared-cluster readiness checklist](../../shared-cluster-readiness.md) lists
what a production deployment still needs.

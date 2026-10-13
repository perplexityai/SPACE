# Collector isolation test

Checks that a sandbox cannot reach any telemetry receiver that
`space-observability` installs, including the node host ports, while SPACE
pods and host-network services still reach their node's receivers.

```bash
python3 scripts/tests/observability/collector-isolation/run.py          # create, test, delete
python3 scripts/tests/observability/collector-isolation/run.py --keep   # leave the cluster up
```

Requires `docker`, `kind`, `kubectl` and `helm`. The script creates its own
two-node Kind cluster with a random name and a temporary kubeconfig, never
reads or changes your current kube context, refuses to reuse an existing
cluster name, and deletes the cluster on exit unless `--keep` is given. A run
takes a few minutes, mostly image pulls.

It installs the chart twice, in `otlp` mode (gateway and node-local Collector,
one tracing backend on `emptyDir`) and in `datadog` mode with a dummy key and
the unroutable site `local.invalid`, so nothing leaves the cluster. Then it
probes, from four pods on the worker node:

| Source | Expectation |
| --- | --- |
| pod in `space` | reaches its node's node-local OTLP, APM and DogStatsD ports |
| root host-network pod (as node-manager, the worker shim) | same |
| non-root host-network pod (as node-api) | same, once node addresses are host peers |
| pod in `sandboxes` labelled as a SPACE sandbox | reaches nothing |

Targets: every node's InternalIP at the node-local OTLP port (4317/TCP), the
Agent's APM (8126/TCP) and DogStatsD (8125/UDP) host ports, the gateway
Service, and every Collector and Agent pod address. TCP is a connect probe;
each UDP datagram carries a unique metric name looked up in the Agent's
`dogstatsd-stats`.

Phases: the receivers' NetworkPolicies alone; then with the sandbox
namespace's egress policy from `infra/space/kind/charts/space-sandbox-network-policy`;
then with every node address in `nodeLocal.hostPeers` and `datadog.hostPeers`.
The exit status is non-zero if any asserted expectation fails; reachability
the test does not assert is printed as `not asserted`.

The sandbox stand-in is a pod in the sandbox namespace, which is where a
guest's traffic leaves its microVM. It is not a Firecracker guest, so the
per-sandbox egress gateway, which is a further layer, is not exercised here.

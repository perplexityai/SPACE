# SPACE on a caller-provided cluster

This directory installs the existing SPACE Kind charts
(`infra/space/kind/charts`) on a Kubernetes cluster you already run, with your
images, Secrets, database, object store and key broker. It adds no service
templates. `chart/` is a thin umbrella (`space-cluster`) whose subcharts are
symlinks to the Kind charts. It holds only metadata, values and a validation
template that fails the render on missing inputs.

This is a development and evaluation path. It is not a production deployment:
it provisions none of the infrastructure listed under Prerequisites, and a
successful `helm template` proves nothing about a live cluster.

## Layout

| Path | Purpose |
| --- | --- |
| `chart/` | The `space-cluster` umbrella: `Chart.yaml`, `values.yaml` (release `space`), `values-builder.yaml` (release `space-builder`), `templates/validate.yaml`, and `charts/<name>` symlinks to `../../../kind/charts/<name>` |
| `values.example.yaml` | The one values file both releases read |
| `install.sh` | Renders, checks and installs both releases |
| `install-shim.sh` | Installs the shim and its wrapper on a node |

## Releases

The Kind bring-up renders every chart as release `space`, and the builder
node services as `space-builder`, with `helm template | kubectl apply`. One
Helm release can own only one chart, so this directory installs the same
umbrella twice, keeping those names and therefore every resource name:

| Release | Contents |
| --- | --- |
| `space` | Control plane (`space-api-local`, `space-internal-api-local`, `space-credmgr-authority-local`, `space-data-plane-local`, `space-cluster-controller-local`, `space-sandbox-network-policy`) and the worker node services (`space-{node-manager,runtime-manager,volume-manager,credential-manager,template-manager,sandbox-manager,node-ctl,node-api}-local`, `node.role=worker`) |
| `space-builder` | The same node services with `node.role=builder` and the Kind charts' `values-builder.yaml` settings, plus `space-build-manager-local`; no control plane |

`install.sh` applies `chart/values-builder.yaml` before your file, so your
images, endpoints, Secrets and telemetry apply to both releases.
`templates/validate.yaml` rejects a file that changes a role's shape: the
release name, `role`, the component switches, every node chart's `node.role`,
or the builder node-api's service name and build-manager endpoint.
`install.sh` also fails if any object would be rendered by both releases.

The `space-db-migrate` Job is not part of either release.
`install.sh` renders it from `space-api-local` and runs it to completion before
the releases, as the Kind harness does. A Job's pod template is immutable, so
each run replaces the previous Job.

Not installed: the Kind key broker, Postgres, the object store, Redis, the UI,
metrics-server, the node debug shell, seed publishing and console
provisioning. The control plane here is therefore not at parity with a Kind
bring-up. In particular, without the UI chart there is no web console. The
API, data plane and node services are the same templates.

## Values

Copy `values.example.yaml` and replace every example value. Both releases read
the same file:

```bash
infra/space/cluster/install.sh -f my-values.yaml --template      # no cluster access
infra/space/cluster/install.sh -f my-values.yaml --kube-context CONTEXT [--namespace space]
```

`--template` prints the migrate Job and both releases. The raw renders are:

```bash
helm template space infra/space/cluster/chart -n space -f my-values.yaml
helm template space-builder infra/space/cluster/chart -n space \
  -f infra/space/cluster/chart/values-builder.yaml -f my-values.yaml
```

The render fails, listing every gap, unless the file supplies:

- `global.observability.mode` (`"off"`, `datadog` or `otlp`, quoted: bare YAML
  `off` is a boolean). `datadog` and `otlp` also need `environment` and
  `cluster`; `"off"` renders without them. Ports, protocol and trace sampler
  are optional (space-kind-telemetry README).
- `image.repository` and `image.tag` for every enabled subchart. The Kind
  repositories (`space-api`, `space-data-plane`, `space-cluster-controller`,
  `space-worker`) and the Kind tag `local` are refused.
- The same `clusterId` on every subchart that has one, other than `kind-local`.
- `postgres.rolesSecretName` on `space-api-local`, `space-internal-api-local`,
  `space-cluster-controller-local` and `space-credmgr-authority-local`. With it
  set, every managed role password comes from that Secret instead of the Kind
  development passwords.
- `space-credmgr-authority-local.keyBroker`: an `https://` `url` that is not the
  Kind broker, `caSecretName`, a `tokenSecretName` that is not the Kind
  broker's, and `tenantNamespace` and `grantScheme` without `kind-local`.
- `space-cluster-controller-local.snapshotEndpoint`,
  `space-sandbox-manager-local.snapshots.endpoint` and every node chart's
  `node.objectStore.endpoint`, none of them the Kind object store.
- The Kubernetes substrate: `substrate` on `space-api-local` and
  `space-cluster-controller-local`, and every enabled node chart's
  `node.substrate`, left at `kubernetes`. The Direct substrate lane is
  Kind-only ([Kind README](../kind/README.md#direct-substrate-lane)).

`chart/values.yaml` switches the Kind-only parts off:

- `space-api-local.secrets.create: false`, so the development tokens and
  route-signing key are not rendered.
- `migrate.enabled: false`, because the Job is run separately.
- Both entry Services become `ClusterIP`. `entry.type` takes any Service type,
  and `nodePort` applies only to `NodePort`.
- The Kind ECW test hosts are cleared.
- Managed artifact installs stay off (`artifactInstalls.enabled: false` on
  `space-node-manager-local` and `space-runtime-manager-local`). Kind turns them
  on because its node layer gives every node its own root-owned
  `/space/vendor`. Here node-manager mounts `/space/vendor` read-only and does
  not create the store `/space/vendor/artifacts`, and runtime-manager runs
  `--list-only` with a read-only `/space/cache`. On a node whose
  `/space/vendor` is root-owned, writable by root alone and on a writable
  filesystem, set both to `true` together; `templates/validate.yaml` refuses
  a file that sets only one. With them on, runtime-manager exits at startup
  until node-manager has prepared the store and its staging directory.
- `space-cluster-controller-local.ec2.fakeEndpoint.enabled: false`, so the
  controller gets no `AWS_ENDPOINT_URL_EC2` for the Kind fake EC2 endpoint,
  no trust in the internal RPC CA for it, and no loopback alias for the
  public EC2 name. `templates/validate.yaml` refuses it on. The
  reconcile-queue GC (`SPACE_RECONCILE_QUEUE_GC_ENABLED`), which Kind runs
  against that fake, stays off as it ships.

The Kind bring-up also passes `space-api-local`'s `values-kind.yaml`, which
holds bench-only settings that are not test hooks, such as a 45 s
`SPACE_CREATE_DATA_PLANE_READY_TIMEOUT_SECS`. This chart does not read that
file, so those settings keep their binary defaults here.

Telemetry follows `global.observability` through the space-kind-telemetry
helper. A chart's `telemetry.traceExporter: "off"` still vetoes its traces,
which is how build-manager keeps them off. Producers get no identity from the
helper beyond environment, version and cluster: service names stay each
process's own.

`datadog` and `otlp` need node-local receivers at the configured ports on
every node. When `infra/space/observability/charts/space-observability` is in
the tree, `install.sh` reconciles it before the producers, in every mode
(`off` included). It runs as release `space-observability` in its own
namespace (`--observability-namespace`, default `space-observability`, which
must exist). Before changing anything it checks:

- the Secrets that render references there, a Datadog API key Secret
  included;
- for `otlp`, that the render includes the node-local collector DaemonSet.

It passes the same values file, so the file also carries that chart's
settings. The example includes `spaceNamespaces`, the corresponding
`logs.collection.podTargets`, and a Datadog API-key Secret reference. If you
change `--namespace`, update both the producer namespace list and the log
path/namespace; never add the sandbox namespace. Configure the node-only
host peers required by your CNI before relying on host-network telemetry
(see the observability chart's README).

Without the chart:

- a live install refuses any mode other than `off`;
- it also refuses `off` while a `space-observability` release exists, which
  only the tree that installed it can reconcile;
- `--template` warns and prints only the producers.

After provisioning these receiver prerequisites once, change
`global.observability.mode` in that same file and rerun the installer. The
receivers' own caveats about data retention and mode switches apply; mode
`off` reconciles the managed release even when its chart renders no resources.

## Secrets

`install.sh` lists the Secrets the renders reference and do not create (optional
references excluded). It checks, read-only, that each exists in the release
namespace before installing anything. With the example values those are:

| Secret | Keys |
| --- | --- |
| `space-internal` | `SPACE_INTERNAL_TOKEN`, `SPACE_INTERNAL_CONTROL_TOKEN`, `SPACE_PUBLIC_CURSOR_SECRET`, `ecw-credential-key` |
| `space-route-signing` | `signing-key.pem`, `SPACE_API_ROUTE_ED25519_SIGNING_KEY_ID`, `SPACE_API_ROUTE_ED25519_ISSUER` |
| `space-route-verification` | `verification-bundle.json` |
| `space-postgres` | `POSTGRES_CONNECTION_STRING`, read by the migrate Job and the services; the Job's bootstrap-roles step creates the managed logins, so its login must be able to create roles |
| `space-db-roles` | `SPACE_DB_BOOTSTRAP_{SUPERADMIN,USER,RO,MIGRATOR,CREDS}_PASSWORD` |
| `space-internal-rpc-ca` | `ca.crt` |
| `space-cluster-api-tls`, `space-node-api-tls`, `space-build-manager-tls`, `space-ingress-activation-tls` | `tls.crt`, `tls.key` (names in `kind/scripts/internal-rpc-tls.sh`; `docs/internal-rpc-tls.md`) |
| `space-credmgr-authority-tls` | `tls.crt`, `tls.key` |
| `space-credmgr-authority-trust` | `ca.crt` |
| `space-key-broker-token`, `space-key-broker-ca` | `token`; `ca.crt` |
| `space-object-store` | `SPACE_OBJECT_STORE_STATIC_ACCESS_KEY_ID`, `SPACE_OBJECT_STORE_STATIC_SECRET_ACCESS_KEY` |
| `space-sandbox-ssh` | `sandbox_ed25519`, `sandbox_ed25519.pub` |

`kind/scripts/internal-rpc-tls.sh` issues the TLS Secrets only for a Kind
context. On another cluster, issue the same names, keys and DNS SANs yourself.
The charts' internal RPC TLS, TokenReview callers, ServiceAccounts, RBAC and
NetworkPolicies are unchanged from Kind.

## Prerequisites

None of these are provisioned here:

- Kubernetes 1.30 or later (`chart/Chart.yaml` `kubeVersion`; the charts use
  `admissionregistration.k8s.io/v1` ValidatingAdmissionPolicy).
- RuntimeClass `space` with handler `space`, which every sandbox pod names.
  Kind applies it from `infra/space/kind/manifests/runtimeclass.yaml`, and
  `install.sh` checks it, read-only. Apply the same manifest once per
  cluster:

  ```bash
  kubectl --context CONTEXT apply -f infra/space/kind/manifests/runtimeclass.yaml
  ```

- PriorityClasses `space-control-plane` and `space-replaceable`, which the
  control plane and the migrate Job name (the node services use the built-in
  `system-node-critical`). Kind applies them from
  `infra/space/kind/manifests/priorityclasses.yaml`, and `install.sh` checks
  them, read-only. Apply the same manifest once per cluster:

  ```bash
  kubectl --context CONTEXT apply -f infra/space/kind/manifests/priorityclasses.yaml
  ```

- Nodes labelled `space.pplx.net/infra=true` for the control plane, and
  `space.pplx.net/node-pool=sandbox` (workers) or `builder` (builders) for the
  node services (`node.pools`). Sandbox workers also carry the label the API
  schedules sandboxes with (`k8s.sandboxNodeSelectorKey`).
- On every worker and builder: KVM, the `/space` host layout on btrfs that
  the node services mount, and containerd with the `space` runtime handler
  and `install-shim.sh`'s shim. The RuntimeClass alone does not configure
  containerd, and containerd alone does not create the RuntimeClass.
- The release namespace and the sandbox namespace (`sandboxes` by default).
  `install.sh` checks that every namespace the renders use exists.
- A Postgres reachable through `space-postgres`, an S3-compatible object store
  with the snapshot and template buckets, and the intermediate-key broker with
  its CA and token.
- A CNI that enforces NetworkPolicy. `space-sandbox-network-policy` denies
  sandbox traffic by default and allows egress to `0.0.0.0/0` except
  `internetExcept`. That list must cover every private range sandboxes must
  not reach, including the cluster's pod, Service and node CIDRs.
- Build VMs run on builder hosts, outside that NetworkPolicy. Every guest,
  build VMs included, already drops new connections to `10.0.0.0/8`,
  `172.16.0.0/12` and `192.168.0.0/16`. `space-build-manager-local.egress`
  covers the rest: `denyCidrs` must list the cluster's other private ranges,
  such as pod, Service, node or VPC CIDRs in `100.64.0.0/10`, and
  `allowCidrs` reopens approved package mirrors and registries inside them.

The credential-manager authority admits callers by TokenReview of their
projected token. It runs all of these checks on every request, cache hits
included (`resolve_token_identity` in `k8s_identity.rs`):

- the token's ServiceAccount is one of `allowedServiceAccounts`, in the
  authority's namespace;
- the pod still exists with the token's UID and is Running;
- the token's node (name and UID) is a registered SPACE node in the
  control-plane database;
- the pod lists the connection's source address among its pod IPs.

No other caller policy is added here.

## Nodes

On the `global.observability` path, the `kind-node-env` init container of
`space-node-manager-local` writes the node's telemetry settings for the host
processes:

- `/space/config/space.env`, the runtime supervisor's EnvironmentFile. Only
  the names any mode sets are replaced; other lines (your node bootstrap's)
  are kept. A changed file marks the supervisor for restart, as on Kind.
- `/space/config/shim-telemetry.env`, the same settings as shell `export`
  lines.

`install-shim.sh` installs a built shim and a wrapper that starts with metrics
and traces off, then sources `shim-telemetry.env`. Run it on each node as
root:

```bash
infra/space/cluster/install-shim.sh --binary ./containerd-shim-space-v2 [--version V]
infra/space/cluster/install-shim.sh --binary ./containerd-shim-space-v2 --root /tmp/test-root   # disposable root
infra/space/cluster/install-shim.sh --binary ./containerd-shim-space-v2 --print-wrapper
```

It never edits the containerd configuration or restarts containerd. It prints
the `space` runtime stanza when the configuration lacks it.

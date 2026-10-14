# OTLP observability certification

This opt-in certification installs the OSS receivers from this checkout, builds
SPACE from a separate, pinned source checkout, and drives the real Kind /
Firecracker public API. It needs no Datadog account, API key, or cloud credentials.
It produces `report.json`, `summary.md`, raw backend responses, rendered charts,
image IDs, and command timings. **A successful harness exit is not a claim that
all observability gaps have been closed.** Read the check statuses.

The entrypoint is:

```bash
python3 scripts/tests/observability/certification/run.py run \
  --artifacts "$HOME/space-certification-results/$(date -u +%Y%m%dT%H%M%SZ)"
```

is intended for whoever runs it on a host with Docker and KVM access.

## Revisions and scope

The chart baseline is the head of
[#110](https://github.com/perplexityai/SPACE/pull/110), which contains the
[#85](https://github.com/perplexityai/SPACE/pull/85) →
[#86](https://github.com/perplexityai/SPACE/pull/86) →
[#87](https://github.com/perplexityai/SPACE/pull/87) →
[#88](https://github.com/perplexityai/SPACE/pull/88) →
[#101](https://github.com/perplexityai/SPACE/pull/101) →
[#110](https://github.com/perplexityai/SPACE/pull/110) chart/helper work.
Once that stack merges, the baseline is `main`.

The application default is **`a85c35f98cc9d6628184c1c561ba908fdbc6f0e6`**, the
merged naming revision. Application and chart revisions are independent so the
expected-name contract stays reproducible as the chart stack rebases. The runner
creates a clean, detached worktree at the application SHA without merging or
modifying either branch. The runtime retains the Kind OTLP routing introduced by
[#83](https://github.com/perplexityai/SPACE/pull/83),
[#84](https://github.com/perplexityai/SPACE/pull/84), and
[#245](https://github.com/perplexityai/SPACE/pull/245).

All expected metric names, producer identities, minimum instance counts, source
pins, and fix references live in one file:
[`expectations.json`](../scripts/tests/observability/certification/expectations.json).
Names follow [#363](https://github.com/perplexityai/SPACE/pull/363) and
[#365](https://github.com/perplexityai/SPACE/pull/365). The Go shim was not renamed
by those Rust changes: its latency histogram still has
`space_shim_operation_latency_ms_milliseconds`.

The matrix includes API and cluster controllers, data plane, node services,
host-side gateway and spaced, and the node-hosted Go shim. **Both the guest
service and its filesystem identity worker run inside Firecracker.** They appear
as explicit `skip` rows for every signal; the workload still exercises filesystem
read/write behavior. There is no direct guest-to-Collector route. Do not weaken
the boundary in [observability.md](observability.md#collector-requirements) to
make these rows green. A host-only fixture is not evidence of guest ingestion.
The builder is tested as an excluded log source, not as a required log producer.

## Host prerequisites

Use a disposable **Linux x86_64 host** with nested virtualization enabled, or
bare metal. Docker Desktop on macOS and a container without `/dev/kvm` cannot
supply this proof. Suggested capacity (planning estimates, not measured minima):
16 CPU cores, 32 GiB RAM, and at least 150 GiB free disk on the filesystem holding
Docker, the source worktree, build caches, and sparse Kind storage; 64 GiB RAM and
250 GiB free are more comfortable for a cold build. The existing worker harness
uses 40 GiB sparse cache files. Do not globally prune another developer's caches.

Required tools:

| Tool | Reproducible starting version / requirement |
|---|---|
| Docker Engine | 27.5.1 or compatible Engine with BuildKit/buildx and privileged containers |
| Kind | v0.30.0 or newer; node image `kindest/node:v1.34.0`, resolved to its immutable digest before creation |
| kubectl | v1.34.x, matching the selected Kubernetes node minor |
| Helm | v3.19.2 recommended; Helm 3 required |
| Python | 3.11+; certification itself uses only the standard library |
| Go | 1.25.3+ for the node shim; `shim/go.mod` is authoritative |
| Node.js | 22+ for the existing native API smoke's WebSocket / event-stream client |
| Utilities | git, bash, curl, jq, openssl, coreutils (including timeout), tar, gzip, base64 |

Rust builds run in the repository's existing Docker build path; the source
`rust-toolchain.toml` pins Rust 1.98.0. The existing vendor installer stages
Firecracker v1.15.1, jailer, and the guest kernel. The host kernel must support
KVM, btrfs, loop devices, namespaces, and the existing Kind harness's mounts.
The Docker daemon must see the same host paths as the client. See the
[Kind runbook](../infra/space/kind/README.md) for host setup.

On an already provisioned host, verify rather than reinstall:

```bash
docker info
kind version
kubectl version --client
helm version --short
go version
node --version
python3 --version
ls -l /dev/kvm
df -h "$HOME" /var/lib/docker
```

If the existing prerequisite installer is needed, align its kubectl version with
the certification node image (its historical default is older):

```bash
KIND_VERSION=v0.30.0 KUBECTL_VERSION=v1.34.0 HELM_VERSION=v3.19.2 \
  bash infra/space/kind/scripts/install-prereqs.sh
```

The fixed chart helpers target **`kind-space-local`**. The runner refuses an
existing cluster, including an ordinary developer Tilt cluster. Use a dedicated
host's Docker daemon. Do not delete someone else's `space-local` or run Tilt
concurrently: local image tags belong to the existing build tooling.

## Exact commands

From a clean checkout of the certification branch:

```bash
git fetch origin
# Default runtime SHA must be available locally. No floating main build is used.
git cat-file -e a85c35f98cc9d6628184c1c561ba908fdbc6f0e6^{commit}

# Offline assertions and shell syntax; no Docker/KVM needed.
python3 -m unittest discover -s scripts/tests -p test_observability_certification.py -v
bash -n scripts/tests/observability/certification/workload.sh
# Optional, if installed:
shellcheck scripts/tests/observability/certification/workload.sh

# Render every OSS chart, including the optional Grafana UI.
python3 scripts/tests/observability/certification/run.py render \
  --grafana --artifacts "$HOME/space-certification-results/render-$(date -u +%Y%m%dT%H%M%SZ)"

# Full run; automatic teardown on success, error, or ordinary interruption.
python3 scripts/tests/observability/certification/run.py run \
  --grafana --artifacts "$HOME/space-certification-results/run-$(date -u +%Y%m%dT%H%M%SZ)"
```

Grafana is optional; the assertions query Prometheus, Loki, and Tempo directly
through the explicitly scoped Kubernetes API proxy. The kubeconfig is isolated
inside `--state`; the caller's current context is not changed. Only application
smoke port-forwards listen on loopback. No active-context inference is used.

`--state PATH` selects the source/build/storage ownership directory (default
`~/.cache/space-otlp-certification`). Repeated runs reuse that clean source and
Docker caches, but create a fresh cluster and require a **new artifacts directory**.
They do not append a green run to a previous failure report. The runner uses an
advisory lock to serialize certification sessions on the same user account.

To keep an owned cluster for diagnosis:

```bash
python3 scripts/tests/observability/certification/run.py run --keep-cluster \
  --artifacts "$HOME/space-certification-results/debug-$(date -u +%Y%m%dT%H%M%SZ)"
# Read-only inspection, always explicit context:
KUBECONFIG="$HOME/.cache/space-otlp-certification/kubeconfig" \
  kubectl --context kind-space-local get pods -A
# Later, only the container identity recorded by this state directory is deletable:
python3 scripts/tests/observability/certification/run.py teardown \
  --artifacts "$HOME/space-certification-results/teardown-$(date -u +%Y%m%dT%H%M%SZ)"
```

A complete kept run ends in **Datadog default mode with no Agent installed**,
after its negative control. An interrupted run can end in either mode. Read the
report before using a kept cluster. Port-forwards always stop. Teardown deletes
the owned cluster but retains source, vendor files, image/build caches, and sparse
storage for diagnosis and reuse. Remove only this task's retained data if disk
space is needed, after verifying the cluster is gone; no global cache pruning.
SIGKILL or a host reboot cannot run cleanup: use the ownership-checked teardown.

Expect roughly **45–120 minutes cold** (build/network dependent), and
**15–30 minutes with warm build caches**. These are estimates, not measured
certification timings. Image build timeout defaults to 5400 seconds and can be
raised with `--build-timeout`. Each command's actual elapsed time is in the report.

## What the run proves

1. Stage the existing vendor tree and render the existing multi-node Kind config.
   Create Kind with a digest-resolved node image. Install Collector/Prometheus,
   Alloy/Loki, Collector/Tempo, and optionally Grafana using the guarded helpers.
2. Build and load the pinned runtime with existing image tooling, then bootstrap
   with `SPACE_KIND_TELEMETRY_MODE=otel` and Datadog backend `off`. The subprocess
   environment strips ambient cloud credentials, Datadog/OTel settings, watched
   SPACE overrides, and user-selected Kubernetes context. A random local console
   password is generated in memory; no external credentials are required.
3. Run the existing `smoke-spaced-rest.sh` native journey (process/command streams,
   data plane, files, pause/resume and suspend/restore), followed by three held
   create/start/write/read/delete journeys using the shared public API helpers.
   The independent runtime manager is also exercised through its read-only
   `space-rtmgr list` CLI, following the existing Kind smoke.
   Each sandbox is held for 90 seconds: lifecycle gauges sample every 60 seconds,
   while OTLP exports every 10 seconds. No direct SQL or node RPC drives lifecycle.
4. Query exact metric anchors for every scoped host producer. Query all its
   product series for resource/point identity, exact runtime version, instance
   IDs, and `k8s.cluster.name=space-local`, distinct from platform ID `kind-local`.
   Require multiple instances of node services and three shim/gateway/host
   processes, so one surviving process cannot stand in for all producers.
5. Check API/controller counters using **actual stored range-vector samples**:
   at least three points over 20 seconds, no decrease within any identity, and
   positive growth. Verify positive native histogram bucket/count objects for
   API, controller and shim. Other producers have presence and identity checks;
   this is not a claim that every service implements every instrument kind.
6. Require a lifecycle gauge to be positive during an active sandbox and explicitly
   zero on the **same series** after deletion. Missing/stale data is not zero.
   Counters are monotonicity evidence, not an exact-loss accounting ledger.
7. Query per-service Loki streams, and the shim's containerd journal prefix. Write
   unique synthetic markers to the worker and builder `node-stack` stdout streams;
   worker ingestion is the positive control for builder exclusion. This tests the
   default Kind builder source; it does not certify custom builder layouts or journals.
8. Search Tempo during the workload window and fetch complete traces. Require
   trace evidence for every host producer and a same-trace cross-service
   CLIENT → SERVER parent edge with valid unique IDs and acyclic parenting.
9. Redeploy the same runtime with the mode unset, assert the renderer resolves
   `datadog`, then run one shorter create/use/delete journey. Both Collectors'
   accepted metric/span counters must stay unchanged and their pod identities,
   image IDs and restart counts must remain stable. This proves absence of OTLP
   traffic during that bounded control; it does not certify Datadog SaaS delivery.

OSS backend images are already pinned by chart digest. Runtime images are local
builds from the fixed source SHA: their Docker content IDs are captured before
deployment, and running pod `imageID`s and the shim binary SHA-256 are recorded.
Existing build tooling uses `:local` tags; there must be no concurrent builder or
Tilt watcher. This is known-image provenance, not a claim that rebuilding
unpinned upstream base tags on another date is bit-for-bit deterministic.

## Results and known gaps

| Status | Meaning |
|---|---|
| `pass` | Assertion succeeded on the captured evidence. An expected failure that now passes is reported as pass. |
| `fail` | Unexpected missing/wrong data, workload error, API error, preflight error, or teardown failure. Exit 1. |
| `xfail` | A narrowly scoped assertion failed and its known fix is absent from the selected revision. |
| `skip` | Explicitly unexercised boundary, with a reason. Never count it as evidence. |

Exit 0 means no unexpected failures, including for render-only mode.
`report.json.certified` remains false if any check is failed, expected-failed, or
skipped. The current guest boundary therefore prevents a full-platform green
certification, even when every reachable host producer passes.

| Dependency | Assertion affected | What clears it |
|---|---|---|
| [#361](https://github.com/perplexityai/SPACE/pull/361) | Builder marker is ingested despite a working worker positive control | Use chart revision containing the Alloy source exclusions. |
| [#362](https://github.com/perplexityai/SPACE/pull/362) | Product metric `service` disagrees with emitting `service.name` | Build runtime containing attribution fix; peers use `service.role`. |
| [#376](https://github.com/perplexityai/SPACE/pull/376) | Requested template versions collapse | Runtime fix preserves `version_requested`; see qualification below. |
| [#377](https://github.com/perplexityai/SPACE/pull/377) | Rust metrics replace Kubernetes name with platform ID | Build runtime containing the cluster identity fix. |
| Guest + filesystem identity worker | Metrics/logs/traces are explicit skips | A separately reviewed guest collection path; none of the listed fixes creates one. |

Missing metric families, empty positive controls, missing identity/version fields,
missing services, query errors, and broken parenting are **not** blanket xfails.
The healthy public journey does not deliberately generate failed template fetches
for two different content hashes. The requested-version check is explicitly
skipped when none were emitted; if present it requires at least two nonempty
requested hashes. The existing observability `semantic_parity` real-transport
fixture owns that focused collision reproduction. Do not interpret a healthy
workload as proof that requested hashes remain distinct.

Fix detection checks Git ancestry and squash-merge subjects in the relevant tree
(runtime versus charts), not GitHub's current PR status. A merged PR does not fix
an older pinned binary. For rebased/cherry-picked fixes or to disallow a known
xfail, use `--require-fix 362` (repeat for other fixes). To test a newer runtime:

```bash
# Replace the placeholder with a FULL 40-character commit SHA already fetched.
python3 scripts/tests/observability/certification/run.py run \
  --runtime-revision FULL_COMMIT_SHA \
  --state "$HOME/.cache/space-certification-new-revision" \
  --require-fix 362 --require-fix 376 --require-fix 377 \
  --artifacts "$HOME/space-certification-results/new-$(date -u +%Y%m%dT%H%M%SZ)"
```

Use a chart checkout that actually includes the builder exclusion fix before
requiring fix 361. The harness never edits or cherry-picks other PRs for you.

## Troubleshooting and evidence handling

- **No `/dev/kvm`:** move to a Linux host with KVM. A render or mocked run is not
  Firecracker evidence. Missing-device preflight writes a failure report without
  creating a cluster.
- **Existing `space-local`:** do not adopt it. Use another host or tear down only
  a cluster recorded in this harness's state file. Lost/mismatched ownership
  records are a refusal, not permission to delete.
- **Build or image-load failure:** inspect the `command-NNN.log` named by the
  report (including its `.stderr` companion) and disk usage. Check the existing
  Kind runbook's storage and btrfs guidance. Do not expunge unrelated caches.
- **Runtime-source refusal:** choose a new `--state` for a different revision.
  Keep the pinned source worktree clean. Do not silently build a dirty tree.
- **Collector installation / shim endpoint failure:** receivers must exist before
  shim installation because the shim resolves their ClusterIPs. Inspect backend
  namespaces and chart logs with `--context kind-space-local`.
- **Missing metrics:** inspect query JSON, exact source/image revisions, service
  labels, collector health, and expected names. Do not widen to `space_.*` as a
  replacement for an exact anchor. Startup failures remain failures.
- **Gauge fails intermittently:** preserve the evidence; the 90s hold and 90s final
  settle cover the 60s lifecycle sampler plus export/batching. Check for process
  restarts, changed sampler cadence, and unexpected surviving sandboxes.
- **Missing Loki logs:** check Alloy's node placement, pod file paths and journal
  mount. The shim is host-journal output, not pod JSON. A missing worker marker
  makes the builder exclusion assertion fail, even if the builder marker is absent.
- **Missing Tempo traces:** verify `always_on` selection, resource version and
  workload time bounds. The settle covers Tempo's latest-30s search exclusion.
  Two services in separate trace IDs do not prove propagation.
- **Datadog negative control fails:** compare receiver counters and pod identities;
  investigate unrolled OTLP producers or residual sandboxes. Do not dismiss the
  traffic as a known xfail.

Artifacts are created with owner-only permissions. Workloads and sentinels are
synthetic, but native smoke logs can contain short-lived local attach URLs/tokens.
Keep raw output private and inspect it before sharing. Share `summary.md`,
`report.json`, exact image/revision records, and relevant redacted query responses;
do not commit raw run output or copy workstation environment dumps.

Validation available when this harness was authored: assertion unit tests,
existing helper/telemetry tests, shell syntax/ShellCheck, Python compilation,
and all four chart lint/renders. The implementation environment had Docker but
no `/dev/kvm`; full runtime, signal queries, and the traffic negative control
require the full run. Estimates and source-derived expectations above are not
live-cluster results.

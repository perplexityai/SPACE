# Isolated real-binary runtime provisioning

The opt-in `node.runtime_provisioning` scenario starts a real cluster API,
node-manager, node-ctl, and runtime-manager with disposable PostgreSQL and empty
runtime roots. It checks the ordinary authenticated publication and background
recipe loop, without manually invoking the runtime installer. Its target is the
compiled Firecracker 1.17 environment with Linux 6.1.155.

## Prerequisites

- **Linux Docker:** The caller must see the Docker daemon's cgroup filesystem.
  The fixture rejects a remote or otherwise invisible cgroup layout rather than
  guessing which subtree it may modify.
- **Worker image:** Supply a compatible Linux worker image with the normal SPACE
  service accounts, groups, runtime libraries, and basic mount tools. The tracked
  `infra/space/identity/node-identities.sh` supplies the authoritative node-ctl
  supplementary group names; the fixture does not substitute root for node-ctl.
- **Local tools:** Install `docker` and `grpcurl`, and use the normal Python
  environment for the typed smoke framework.
- **Real binaries:** Build `space-internal-api-svc`, `space-nodemgr-svc`,
  `space-nodectl-svc`, `space-rtmgr-svc`, `space-nodemgr`, `space-nodectl`,
  `space-rtmgr`, `space-db-migrator`, and `space-runtime-provisioning-fixture`.
  Put those nine executables in one directory.

The test-only registration helper requires an explicit disposable-database
argument and a loopback database address. It uses the shared ready-worker fixture
to register a synthetic node, not to create catalog entries.

## Run

From the matching source checkout, set paths and images explicitly:

```sh
export SPACE_SMOKE_BACKEND=deployed
export SPACE_SMOKE_API_SURFACE=node
export SPACE_SMOKE_SUBSTRATE=direct
export SPACE_SMOKE_LOCAL_RUNTIME_PROVISIONING=1
export SPACE_SMOKE_RUNTIME_BINARY_DIR=/absolute/path/to/nine-binaries
export SPACE_SMOKE_RUNTIME_SOURCE_DIR="$(pwd)"
export SPACE_SMOKE_RUNTIME_EVIDENCE_DIR=/absolute/path/to/owned-evidence
export SPACE_SMOKE_RUNTIME_WORKER_IMAGE=your-compatible-worker-image
export SPACE_SMOKE_RUNTIME_POSTGRES_IMAGE=postgres:16-alpine
uv run --frozen --python 3.11 python -P -m space_smoke run node.runtime_provisioning
```

No production credentials are needed. Local authentication and signing material
are generated for the isolated services and passed by environment, not printed.

Each run creates a fresh labeled worker, PostgreSQL container, network, and runtime
root. The worker has a private PID/cgroup namespace, three CPUs and 3 GiB of
memory; PostgreSQL has one CPU and 512 MiB. Before starting node-manager, the
fixture verifies the exact owned cgroup inode and resource limits. It does not
map host devices or expose loop-control. Cleanup checks ownership labels before
removing resources and verifies caller/parent limits and loop indices.

## Oracle and negative control

The starting inventory must report compiled known environments but no usable
environments or installed runtime files. After a normal node-ctl publication is
acknowledged, the scenario reads the desired recipe and waits for automatic
installation of the exact target. It independently hashes the resulting
Firecracker executable and Linux kernel, checks the actual executable version,
and requires the exact environment to become usable in node-manager inventory.

For a matched-schema negative control, keep the final migrator and the other
seven executables identical and replace only `space-internal-api-svc` with the
pre-fix binary. Use a fresh fixture for each arm. The old API accepts publication
but returns two empty recipes; the scenario intentionally fails rather than
treating that outcome as a passing test.

Receipts contain service identities, binary hashes, inventory, explicit recipe
responses, artifact hashes, and isolation checks. Keep the typed runner's result
as well: its `cleanup_failures` must be empty, including for the negative control.

## Scope

This is a target-installation proof, not a complete-catalog convergence test.
The background recipe may contain environments outside the local execution
allowlist, and unrelated artifacts may lack enabled sources. A successful target
check must not be described as every installation operation succeeding.

The fixture uses the supported SHA-256 integrity fallback on tmpfs, not an
fs-verity qualification. It does not create a VM guest, execute a build, qualify
sandbox lifecycle behavior, prove builder end-to-end readiness, or authorize a
production deployment or cohort expansion.

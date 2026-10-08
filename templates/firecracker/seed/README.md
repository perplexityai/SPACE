# Sandbox Template Builder

`seed` and `code-runtime` are SPACE system templates. Build and publish them
through the Admin API using the `space-admin-cli` operator CLI. The deployed API embeds
`recipes/seed.toml` and `recipes/code-runtime.toml`; callers select the template,
not a local recipe file. `seed` builds from scratch; `code-runtime` builds on a
published seed release.

## Build and publish system templates

Install the CLI from the same source version as the deployment:

```bash
cargo install --path crates/apps/admin/cli --locked
```

Set `SPACE_API_URL` to the deployment's API origin, without `/admin/api/v1`,
then log in as an administrator of the cluster's managing organization:

```bash
export SPACE_API_URL=https://api.example.com
SPACE_API_TOKEN="$(space-admin-cli login --print-token)" && export SPACE_API_TOKEN
```

`space-admin-cli login` reads `SPACE_API_URL`, prompts for your email and hidden password,
and verifies admin access. `--print-token` returns only the token on stdout.
The shell captures it in `SPACE_API_TOKEN` for the build commands below; no
session file is saved. Continue only after login succeeds, and keep shell tracing
(`set -x`) disabled when handling tokens.

The deployment owns endpoint discovery, network access, and credentials; keep
those values in private caller configuration. Direct API access needs no
Kubernetes commands. If the admin service is not directly reachable, follow the
[operator CLI port-forward instructions](../../../crates/apps/admin/cli/README.md#kubernetes-access-via-kubectl-port-forward)
first. For separate auth and admin endpoints, pass `--auth-url URL` to
`space-admin-cli login`; see the [login examples](../../../crates/apps/admin/cli/README.md#logging-in).

The builder must already have the guest binaries, containerd, configured
artifact storage and cluster credentials, the pinned guest kernel, and the
cluster's sandbox SSH key. API and builder versions must be compatible. Local
recipe edits take effect only after deploying the updated API; local guest
binary edits require updating the builder.

To publish the system-template chain and intentionally move each `latest` tag:

```bash
SEED_HASH=$(space-admin-cli template build seed --publish-tag latest | jq -er '.content_hash') &&
space-admin-cli template build code-runtime --base "seed@$SEED_HASH" --publish-tag latest
```

Run the second command only if the first succeeds. Each command waits for its
build and publication to finish and prints JSON containing `job_id`, `state`,
and `content_hash`. `code-runtime` uses that exact seed, even if another build
moves `seed:latest` meanwhile. Without `--base`, it uses `seed:latest`, resolved
to an exact digest by the API before dispatch.

Omit `--publish-tag latest` to publish only the immutable digest release. Tags
are assigned after successful publication; promotion of the two templates is
not atomic. If the derived build fails, the already-published seed remains.
Existing sandboxes retain their template release. New requests using `latest`
resolve the promoted release; previously pinned warm pools remain pinned.

For long-running builds or disconnected terminals:

```bash
space-admin-cli template build seed --async
space-admin-cli template status seed JOB_UUID
```

Stopping the CLI or reaching its two-hour wait limit does not cancel the job.
Use the printed status command to inspect it. `failed` and `publish_failed`
exit nonzero; an accepted job alone does not prove a usable release. A server
without the system-build endpoint must be upgraded; the CLI does not fall back
to local builds or pod execution.

The HTTP entry point is `POST /admin/api/v1/system-templates/{template}/builds`
with a JSON object containing optional `base`, `force`, and `publish_tags`.
Only `seed` and `code-runtime` are accepted. `base` is only valid for
`code-runtime` and must reference `seed`. Poll the existing
`GET /admin/api/v1/templates/{template}/builds/{job_id}` endpoint.
The generic recipe-build API remains available for custom templates.

Builders upload artifacts and register releases through the normal publication
path. Workers resolve releases and pull through template-manager; publication
does not eagerly populate every worker cache. Verify sandbox creation on the
target deployment, including kernel availability and SSH trust, before relying
on a new lineage. Keep the previous digest for rollback through the existing
admin release-tag API.

## Local seed construction

The Docker launcher below is for local rootfs development. Kind/Tilt uses its
own [automated seed bootstrap](../../../infra/space/kind/README.md#seed-template)
to make the first seed available. Neither replaces the system-template release
procedure above.

## Guest Networking

Every sandbox guest uses a fixed link-local IP (`169.254.0.21/30`, gateway `169.254.0.22`).
This IP is the same for all sandboxes — per-sandbox identity comes from namespace-level
NAT (SNAT to unique vpeer IP `10.12.x.x`).

The IP is set two ways (belt and suspenders):
- **Kernel boot args**: `ip=169.254.0.21::169.254.0.22:255.255.255.252:instance:eth0:off`
- **systemd-networkd**: `/etc/systemd/network/10-eth.network` in the rootfs (fallback)

This design enables fork, migrate, and checkpoint/restore without changing guest network config.

## Guest Clocksource

SPACE launches x86_64 Firecracker guests with `clocksource=kvm-clock` in the kernel boot args.
The seed rootfs does not compile or contain that guest kernel. For the
CrowdStrike-compatible BPF/tracing kernel build, see
[`../kernel/README.md`](../kernel/README.md).

Firecracker's snapshot-load `clock_realtime` flag advances guest wall clock on x86_64 through
`kvm-clock`; if the guest snapshots while using `tsc`, VMClock still reports the restore event
but realtime remains behind by the suspend interval.

On non-x86_64 hosts, Firecracker 1.15 rejects `clock_realtime`, so SPACE uses VMClock only as
the restore signal and then asks the guest daemon to sync realtime through the existing resume
notification path.

## Building the Seed

The seed builder downloads an Ubuntu Minimal Cloud squashfs, verifies it,
chroots into it, installs minimal packages, and packs it into an ext4 image.
The Docker and node-local containerd launchers use the same rootfs builder.

### Download verification

The maintained seed recipe selects Ubuntu 26.04 `release-20260908`, with
`amd64` or `arm64` selected from the builder architecture. Before extraction,
the builder authenticates that release's `SHA256SUMS` with `gpgv` and the
cloud-image keyring supplied by Ubuntu's `ubuntu-keyring` package and the pinned
signer fingerprint `D2EB44626FDDC30B513D5BB71A5D6C4C7DB87C81`, then checks
the image digest. Both builder launchers pin the same Ubuntu 26.04 image digest.
This follows [Ubuntu's verification procedure](https://ubuntu.com/docs/public-images/public-images-how-to/verify-image-checksum/).

Cached images go through the same check against the currently selected release.
A stale or corrupt cache entry triggers a fresh download. The replacement must
match the authenticated checksum before it replaces the cache entry or reaches
extraction. A missing checksum, invalid signature, or mismatched download stops
the build. Checksum metadata must remain reachable even when the image is cached.

The maintained code-runtime recipe pins Node.js 20.20.2, GitHub CLI 2.101.0,
and uv 0.12.18 (also used by the CI image), with SHA-256 values for both
architectures checked before extraction. uv is installed from its release
archive without executing a remote installer. `scripts/build-code-runtime.py`
reads these installation commands from the recipe. The digest sources are:

- [Node.js release checksums](https://nodejs.org/dist/v20.20.2/SHASUMS256.txt)
- [GitHub CLI release checksums](https://github.com/cli/cli/releases/download/v2.101.0/gh_2.101.0_checksums.txt)
- uv [aarch64 checksum](https://github.com/astral-sh/uv/releases/download/0.12.18/uv-aarch64-unknown-linux-gnu.tar.gz.sha256) and [x86_64 checksum](https://github.com/astral-sh/uv/releases/download/0.12.18/uv-x86_64-unknown-linux-gnu.tar.gz.sha256)

When updating a tool, review the upstream release and update its version and
both digest pins together. When updating Ubuntu, choose a dated release with
both squashfs architectures and signed checksum metadata. Other Ubuntu release
selections use the same upstream signature verification; template and OCI
origins are unchanged.

Run `bash scripts/tests/test-seed-download-verification.sh` on Linux with
`curl`, `gnupg`, and `squashfs-tools` installed. It signs a small local squashfs
fixture and checks that corrupt inputs fail before extraction.

### Local Docker build

```bash
cd templates/firecracker/seed

# Build only
./build.sh --id seed

# Build and deploy to node
./build.sh --id seed --deploy

# Deploy existing build without rebuilding
./build.sh --id seed --deploy-only

# Deploy to a different host
./build.sh --id seed --deploy --host my-other-node

# Overwrite existing build
./build.sh --id seed --force --deploy
```

## Guest authentication

The guest `user` account has a locked password. SSH access uses keys, and SSH
password authentication is disabled. Sandbox commands and passwordless `sudo`
continue to work. The seed builder does not accept a password or password hash.

The seed must trust the SSH key used by its target builders. Pass the matching
public key to `build.sh --pubkey /path/to/sandbox_ed25519.pub`; a seed with a
stale key can boot successfully while SSH-based recipe builds fail.

Existing images and sandboxes retain their previous password state. Rebuild the
seed and its derived templates to remove the old built-in password.

## Direct builder development

Recipes are TOML files in `recipes/` that define steps to run inside a booted VM.
`space-buildmgr` submits the recipe to build-manager, which boots the base template in Firecracker, SSHs in, and executes each step.

```bash
# Code-runtime template (Python, Node.js, dev tools) — builds on seed
space-buildmgr build --recipe recipes/code-runtime.toml --id code-runtime
```

### Available Recipes

| Recipe | Base | Description |
|--------|------|-------------|
| `recipes/code-runtime.toml` | seed | Python 3.12, Node.js 20, gh CLI, uv, git, dev tools |

## Options

| Flag | Description |
|------|-------------|
| `--id <id>` | Template identifier (default: "seed") |
| `--deploy` | SCP result to node after build |
| `--deploy-only` | Skip build, just deploy existing output to node |
| `--host <ssh-host>` | SSH host for deploy (default: `utm-space-dev`) |
| `--force` | Overwrite existing output |

## Output

Build output is in `output/{id}/`:

```
output/{id}/
  rootfs.ext4       ext4 rootfs image
  metadata.json     build metadata, including runtime, state-format, and kernel identities
```

Seed builds default to Firecracker `1.15.1` with state format `9.0.0`.
`SPACE_SEED_RUNTIME_VERSION` and `SPACE_SEED_STATE_FORMAT_VERSION` can override
those values when building against a different installed runtime. Node builds
also record the selected guest kernel's version label and SHA-256 digest.
On nodes with multiple installed kernels, `default` is used for new builds and
`legacy-default` remains pinned for artifacts whose legacy metadata did not
record an exact kernel identity.

On the node after deploy:

```
/space/cache/firecracker/templates/{id}/
  sha256:{content-hash}/
    rootfs.ext4
    metadata.json   build provenance and runtime compatibility identity
    manifest.json   content manifest with mirrored runtime identity
```

## Spaced Version Rollout

The seed template is stamped with `spaced_version` from the workspace `Cargo.toml` and
the derived `spaced_compatibility_family`. For SemVer, `1.x.y` is compatible within
major version `1`, `0.x.y` is compatible within `0.x`, and `0.0.y` is exact-version
compatible. `space-sbxmgr-svc` refuses to create or restore sandboxes from templates
whose compatibility family does not match the manager binary, or whose version metadata
is missing.

Mixed-incompatible host, guest, manager, and seed-template deployments are unsupported.
For each node, deploy the new manager and host binaries together with a seed template
built from the same checkout. Cluster-level rolling replacement is acceptable only when
each node is internally same-family before it starts creating or restoring sandboxes.

If preflight fails with a compatibility mismatch, rebuild and redeploy the seed template
from the same compatible source line as the node binaries, then rebuild derived recipe
templates.

Native application startup is documented in [native template startup](../../../docs/native-template-runtime.md).

---
name: space-template-lifecycle
description: Build, publish, distribute, or troubleshoot SPACE Firecracker templates and guest kernels.
---

# Manage the SPACE template lifecycle

Use the production publish and pull paths. A template is usable only when its
release is registered in the target cluster, its bytes are present on workers,
the required guest kernel is staged, and builders hold a private key trusted by
the seed.

## Recipe builds and transfers

Use `space-admin-cli template build seed` and `space-admin-cli template build code-runtime` through
the Admin API for system-template releases. The API embeds the canonical recipe
files; do not reconstruct their JSON or substitute checkout recipes. Read
`templates/firecracker/seed/README.md#build-and-publish-system-templates` for the
command sequence, polling, explicit tag promotion, and verification.

The caller supplies `SPACE_API_URL` and an authorized `SPACE_API_TOKEN` using
the owning deployment's access instructions. Keep private endpoints and
credentials outside this repository. Do not build system releases by pod exec,
local Docker plus manual uploads, static internal tokens, or direct registration.
Local Docker and Kind seed bootstrap remain development procedures.

Build seed first and pass its exact digest as `--base seed@sha256:...` when
building code-runtime. `--publish-tag latest` intentionally promotes a release;
without it only the immutable digest is published. Check final job status,
then verify sandbox creation and required kernel/SSH compatibility on the
target deployment. Worker template-managers use the normal release pull path;
publication alone does not mean every worker has cached the bytes.

`code-runtime` provides the environment for coding workflows and is built from
`seed`. Workload-specific templates are built and registered by their owning
workflows through the template build API.

A cluster bring-up stops at `seed` and `code-runtime` unless the workload's own
workflow registers more.

## Deploy-phase publishing

Inspect `crates/l50_app/template/publish/` and the owning deployment's publish
configuration before changing release selection. A configured pin is a floor:
if `latest` already resolves, the Job leaves it untouched. Do not turn it into an
assignment that rolls back a newer release.

The projected ServiceAccount token is the credential; static fallback is
disabled. Worker bootstrap does not stage templates. Do not restore direct
database writes or `scripts/bootstrap-templates.sh`.

## Guest kernel

Read `templates/firecracker/kernel/README.md` before changing
`scripts/k8s/stage-guest-kernel.sh`, the node-manager init container, or image
cache mounts. The kernel is a separate artifact, must be a real file on the
cache filesystem, and is hard-linked into each jail. Failure to stage it must
keep the node service unready.

## Builder SSH trust

Read the SSH key guidance in `templates/firecracker/seed/README.md` before
changing key generation, `sandboxSsh`, or seed pubkey injection. The key and seed are deliberately
per-cluster. Enabling `sandboxSsh` is a cutover: rebuild the registered template
chain to trust the secret before pods present it.

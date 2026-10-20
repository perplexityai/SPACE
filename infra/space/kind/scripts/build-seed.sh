#!/usr/bin/env bash
# build-seed.sh — build the production seed template artifacts for the local
# kind stack, using the SAME builder the production seed uses
# (templates/firecracker/seed/Dockerfile + rootfs-builder.sh).
#
# Output: ${SPACE_SEED_OUTPUT_DIR}/ containing
#   rootfs.ext4     — Firecracker-bootable Ubuntu rootfs with a vsock-listening
#                     spaced-svc (identical build path to the prod seed)
#   metadata.json   — schema v2 template metadata (execution block included)
#   manifest.json   — template manifest authored here, exactly the shape the
#                     production builder publishes next to its artifacts
#                     (content_hash = sha256 of rootfs.ext4 — the same digest
#                     template-manager computes on register and the pull path
#                     verifies after download)
#
# The guest binaries (spaced, spaced-svc) come out of the worker Rust build
# image so the seed's guest daemon is byte-identical to the one the node
# services were built against (spaced compatibility gating keys off the
# workspace version).
#
# The sandbox SSH public key is baked into the seed like the Packer AMI build
# does. On a devbox there is no Packer, so we generate a per-devbox keypair
# once under ~/.space-devbox/kind/ssh/ — bootstrap.sh ships the key pair to
# build-manager (Secret mounted at /root/.ssh), which SSHes into build VMs as
# a builder does with its node image's key.
#
# Idempotent: skips the (slow, privileged) rootfs build when the inputs
# (guest binaries, builder script, seed recipe source, pubkey) are unchanged.
#
# The builder image's layers and the recipe's Ubuntu cloud image (its signed
# checksums and the squashfs) come from the Kind registry layer cache
# (infra/space/kind/lib/build-cache.sh, scope kind-seed-builder-e<epoch>) when it holds
# them; rootfs-builder.sh verifies both before use, and downloads whatever the
# cache could not provide.
#
# Usage:
#   bash infra/space/kind/scripts/build-seed.sh
#   bash infra/space/kind/scripts/build-seed.sh --cache-only   # build just the
#       stages the cache keeps; main CI exports them (runtime-image-cache.sh publish)
#
# Env knobs:
#   SPACE_SEED_OUTPUT_DIR       artifact dir (default: ~/.space-devbox/kind/seed/build)
#   SPACE_RUST_BUILD_IMAGE      image holding /artifacts/bin/spaced{,-svc}
#                               (default: space-rust-build-kind:local, the kind
#                               stack's shared artifact image; built here via
#                               build-rust-artifacts.sh if missing)
#   SPACE_SEED_GUEST_BIN_DIR    directory with prebuilt spaced + spaced-svc; skips
#                               the rust build image entirely (e.g. a native
#                               target/release after `cargo build --bin spaced --bin spaced-svc`)
#   SPACE_SEED_REBUILD=1        force a rebuild even if inputs are unchanged
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
SEED_DIR="$REPO_ROOT/templates/firecracker/seed"
SEED_RECIPE="$REPO_ROOT/templates/firecracker/recipes/seed.toml"
OUT_DIR="${SPACE_SEED_OUTPUT_DIR:-$HOME/.space-devbox/kind/seed/build}"
SSH_DIR="$HOME/.space-devbox/kind/ssh"
RUST_BUILD_IMAGE="${SPACE_RUST_BUILD_IMAGE:-space-rust-build-kind:local}"
# Same defaults as templates/firecracker/seed/build.sh — the Firecracker
# version pinned by install-fc-vendor.sh and its snapshot state format.
SEED_RUNTIME_VERSION="${SPACE_SEED_RUNTIME_VERSION:-1.15.1}"
SEED_STATE_FORMAT_VERSION="${SPACE_SEED_STATE_FORMAT_VERSION:-9.0.0}"

log() { printf '[build-seed] %s\n' "$*"; }
die() { printf '[build-seed] ERROR: %s\n' "$*" >&2; exit 1; }

command -v docker >/dev/null 2>&1 || die "docker not on PATH"
command -v python3 >/dev/null 2>&1 || die "python3 not on PATH (needed to author manifest.json)"

# shellcheck source=../lib/build-cache.sh
source "$REPO_ROOT/infra/space/kind/lib/build-cache.sh"
# shellcheck source=../lib/shared-build-inputs.sh
source "$REPO_ROOT/infra/space/kind/lib/shared-build-inputs.sh"
# shellcheck source=../../../../templates/firecracker/seed/rootfs-builder.sh
source "$SEED_DIR/rootfs-builder.sh"

# ── Seed source config, read from the seed recipe like build.sh does ─────────
seed_source_value() {
  awk -F' = ' -v key="$1" '
    /^\[source\]/ { in_source = 1; next }
    /^\[/ { in_source = 0 }
    in_source && $1 == key { gsub(/"/, "", $2); print $2; exit }
  ' "$SEED_RECIPE"
}
SEED_SOURCE_TYPE="$(seed_source_value type)"
SEED_UBUNTU_RELEASE="$(seed_source_value release)"
SEED_UBUNTU_VERSION="$(seed_source_value version)"
SEED_UBUNTU_DATE="$(seed_source_value date)"
for v in SEED_SOURCE_TYPE SEED_UBUNTU_RELEASE SEED_UBUNTU_VERSION SEED_UBUNTU_DATE; do
  [[ -n "${!v}" ]] || die "failed to read ${v#SEED_} from $SEED_RECIPE"
done

# ── Seed builder builds (templates/firecracker/seed/Dockerfile) ──────────────
# In GitHub CI (SPACE_CI_IMAGE_MIRROR set) the builder's base comes from the
# repository's GHCR mirror (ci/mirrored-images.txt); elsewhere this adds nothing.
# The cache options import the builder's layers, and export them only when
# the caller enables writes; main takes its bases from the mirror the same
# way, so the layers' keys agree. A build that fails reading the cache or the
# mirrored base is built again without either; main's export keeps the
# mirrored base, so it never writes layers no reader would match.
seed_builder_build() {
  local -a mirror optional
  space_kind_mirror_contexts "$SEED_DIR/Dockerfile"
  mirror=("${MIRROR_FLAGS[@]}")
  space_kind_cache_flags "$(space_kind_layer_scope seed-builder)" "${SEED_CACHE_OPTION[@]}" \
    || die "invalid build cache configuration"
  if (( ${#CACHE_TO_FLAGS[@]} && MIRROR_MISSING )); then
    die "the mirrored seed builder base was unavailable; not exporting layers no mirrored build would match"
  fi
  optional=("${CACHE_FROM_FLAGS[@]}")
  if (( ${#CACHE_TO_FLAGS[@]} == 0 )); then
    optional+=("${mirror[@]}")
    mirror=()
  fi
  space_kind_build_reading_cache "${#optional[@]}" "${optional[@]}" \
    "${mirror[@]}" "${CACHE_TO_FLAGS[@]}" \
    --build-arg "SPACE_SEED_UBUNTU_RELEASE=$SEED_UBUNTU_RELEASE" \
    --build-arg "SPACE_SEED_UBUNTU_VERSION=$SEED_UBUNTU_VERSION" \
    --build-arg "SPACE_SEED_UBUNTU_DATE=$SEED_UBUNTU_DATE" \
    "$@" "$SEED_DIR"
}
SEED_CACHE_OPTION=()

if [[ "${1:-}" == "--cache-only" ]]; then
  log "building the seed builder's cached stages (the builder and the recipe's cloud image)"
  # The export is this mode's only purpose, so its failure is not ignored.
  SEED_CACHE_OPTION=(--fail-on-export-error)
  seed_builder_build --target cloud-image --output type=cacheonly
  exit 0
fi
(( $# == 0 )) || die "usage: $0 [--cache-only]"

# $OUT_DIR is shared by every checkout on the host; see the library for who
# reads this mark.
space_kind_mark_shared_input_writer "$REPO_ROOT"

# Record the bytes staged for the Kind workers, not a guessed kernel release.
# shellcheck source=lib/kind-host-paths.sh
source "$REPO_ROOT/infra/space/kind/scripts/lib/kind-host-paths.sh"
kind_host_paths__require SPACE_VENDOR_ROOT
kernel_path="$SPACE_VENDOR_ROOT/firecracker/default/kernel/vmlinux.bin"
if [[ ! -f "$kernel_path" ]]; then
  SPACE_VENDOR_ROOT="$SPACE_VENDOR_ROOT" bash "$REPO_ROOT/infra/space/kind/scripts/install-fc-vendor.sh"
fi
[[ -f "$kernel_path" ]] || die "staged guest kernel missing: $kernel_path"
SEED_KERNEL_SHA256="$(sha256sum "$kernel_path" | awk '{print $1}')"
SEED_KERNEL_VERSION="legacy-${SEED_KERNEL_SHA256:0:12}"
SEED_KERNEL_DIGEST="sha256:$SEED_KERNEL_SHA256"

# ── Sandbox SSH keypair (Packer-parity, generated once per devbox) ───────────
if [[ ! -f "$SSH_DIR/sandbox_ed25519" ]]; then
  log "generating sandbox SSH keypair → $SSH_DIR/sandbox_ed25519"
  install -d -m 0755 "$SSH_DIR"
  ssh-keygen -q -t ed25519 -N '' -C 'space-sandbox-local' -f "$SSH_DIR/sandbox_ed25519"
fi

# ── Guest binaries (from the worker Rust build image, or a local dir) ────────
if [[ -n "${SPACE_SEED_GUEST_BIN_DIR:-}" ]]; then
  GUEST_BIN_DIR="$SPACE_SEED_GUEST_BIN_DIR"
  for bin in spaced spaced-svc; do
    [[ -f "$GUEST_BIN_DIR/$bin" ]] || die "SPACE_SEED_GUEST_BIN_DIR is missing $bin"
  done
  log "using prebuilt guest binaries from $GUEST_BIN_DIR"
else
  if ! docker image inspect "$RUST_BUILD_IMAGE" >/dev/null 2>&1; then
    log "building $RUST_BUILD_IMAGE (the kind stack's shared artifact image; long on a cold cache)"
    bash "$REPO_ROOT/infra/space/kind/scripts/build-rust-artifacts.sh"
  fi

  GUEST_BIN_DIR="$OUT_DIR/.guest-bin"
  mkdir -p "$GUEST_BIN_DIR"
  extract_container="space-seed-guest-bin-$$"
  docker create --name "$extract_container" "$RUST_BUILD_IMAGE" /dev/null >/dev/null
  trap 'docker rm -f "$extract_container" >/dev/null 2>&1 || true' EXIT
  for bin in spaced spaced-svc; do
    docker cp "$extract_container:/artifacts/bin/$bin" "$GUEST_BIN_DIR/$bin"
  done
  docker rm -f "$extract_container" >/dev/null
  trap - EXIT
fi

# ── Skip the rootfs build when nothing that feeds it changed ─────────────────
inputs_hash=$( (
  cat "$GUEST_BIN_DIR/spaced" "$GUEST_BIN_DIR/spaced-svc" \
      "$SEED_DIR/rootfs-builder.sh" "$SEED_DIR/Dockerfile" \
      "$SEED_RECIPE" "$SSH_DIR/sandbox_ed25519.pub"
  echo "$SEED_RUNTIME_VERSION $SEED_STATE_FORMAT_VERSION $SEED_KERNEL_VERSION $SEED_KERNEL_DIGEST"
) | sha256sum | awk '{print $1}')
stamp="$OUT_DIR/.inputs-sha256"
if [[ "${SPACE_SEED_REBUILD:-0}" != "1" \
      && -f "$OUT_DIR/rootfs.ext4" && -f "$OUT_DIR/metadata.json" && -f "$OUT_DIR/manifest.json" \
      && -f "$stamp" && "$(cat "$stamp")" == "$inputs_hash" ]]; then
  log "seed artifacts up to date in $OUT_DIR (inputs unchanged); skipping build"
  exit 0
fi

# ── Build the seed with the production seed builder ──────────────────────────
log "building seed builder image (templates/firecracker/seed/Dockerfile)"
SPACE_KIND_BUILD_CACHE_WRITE=0 seed_builder_build -t space-seed-builder:local

mkdir -p "$OUT_DIR" "$OUT_DIR/.cache"

# The builder's /cache holds the cloud image under its name and the release's
# signed checksums under the release URL. When it lacks either, export the
# builder's cloud-image stage into it: from the build cache when that holds
# this release, otherwise downloaded and verified by the stage itself.
seed_image_source "$SEED_UBUNTU_RELEASE" "$SEED_UBUNTU_VERSION" "$SEED_UBUNTU_DATE" "$(uname -m)"
sums_cache="$OUT_DIR/.cache/sums/${SEED_IMAGE_BASE_URL#*://}"
if [[ -f "$OUT_DIR/.cache/$SEED_IMAGE_NAME" && -f "$sums_cache/SHA256SUMS" && -f "$sums_cache/SHA256SUMS.gpg" ]]; then
  log "cloud image already held in $OUT_DIR/.cache"
else
  held="$(mktemp -d "$OUT_DIR/.cloud-image.XXXXXX")"
  if SPACE_KIND_BUILD_CACHE_WRITE=0 seed_builder_build --target cloud-image --output "type=local,dest=$held" \
      && cp -R --remove-destination "$held/." "$OUT_DIR/.cache/"; then
    log "cloud image held in $OUT_DIR/.cache"
  else
    log "WARNING: could not prepare a copy of the cloud image; rootfs-builder.sh downloads it"
  fi
  rm -rf "$held"
fi
rm -f "$OUT_DIR/rootfs.ext4" "$OUT_DIR/metadata.json" "$OUT_DIR/manifest.json"

log "running rootfs-builder.sh (privileged; downloads the Ubuntu minimal squashfs on first run)"
docker run --rm --privileged \
  -v "$OUT_DIR:/output" \
  -v "$OUT_DIR/.cache:/cache" \
  -v "$REPO_ROOT/Cargo.toml:/space/Cargo.toml:ro" \
  -v "$SSH_DIR/sandbox_ed25519.pub:/space/etc/ssh/sandbox_ed25519.pub:ro" \
  -v "$GUEST_BIN_DIR/spaced:/guest-bin/spaced:ro" \
  -v "$GUEST_BIN_DIR/spaced-svc:/guest-bin/spaced-svc:ro" \
  -e "SPACE_SEED_SOURCE_TYPE=$SEED_SOURCE_TYPE" \
  -e "SPACE_SEED_UBUNTU_RELEASE=$SEED_UBUNTU_RELEASE" \
  -e "SPACE_SEED_UBUNTU_VERSION=$SEED_UBUNTU_VERSION" \
  -e "SPACE_SEED_UBUNTU_DATE=$SEED_UBUNTU_DATE" \
  -e "SPACE_SEED_RUNTIME_VERSION=$SEED_RUNTIME_VERSION" \
  -e "SPACE_SEED_STATE_FORMAT_VERSION=$SEED_STATE_FORMAT_VERSION" \
  -e "SPACE_SEED_KERNEL_VERSION=$SEED_KERNEL_VERSION" \
  -e "SPACE_SEED_KERNEL_DIGEST=$SEED_KERNEL_DIGEST" \
  -e SPACE_SEED_REUSE_SIGNED_CHECKSUMS=1 \
  space-seed-builder:local --id seed --force

[[ -f "$OUT_DIR/rootfs.ext4" && -f "$OUT_DIR/metadata.json" ]] \
  || die "seed builder did not produce rootfs.ext4 + metadata.json in $OUT_DIR"

# ── Author manifest.json (the builder-side artifact table of contents) ───────
# Mirrors what the production build-manager publishes next to its S3 artifacts:
# content_hash is the sha256 of the bootable rootfs (the digest the worker's
# template-manager computes on register and the pull path verifies), and the
# files map lists the artifacts that travel with the build. The manifest
# carries the same schema v2 execution block as metadata.json so
# template-manager's normalize step accepts it without patching.
log "authoring manifest.json (content_hash = sha256(rootfs.ext4))"
python3 - "$OUT_DIR" <<'PY'
import hashlib, json, os, sys

out_dir = sys.argv[1]
with open(os.path.join(out_dir, "metadata.json")) as f:
    metadata = json.load(f)

h = hashlib.sha256()
rootfs = os.path.join(out_dir, "rootfs.ext4")
with open(rootfs, "rb") as f:
    for chunk in iter(lambda: f.read(1 << 20), b""):
        h.update(chunk)
digest = f"sha256:{h.hexdigest()}"

manifest = {
    "schema_version": metadata["schema_version"],
    "execution": metadata["execution"],
    "name": metadata["name"],
    "content_hash": digest,
    "build_date": metadata.get("built_at", ""),
    "runtime": "fc",
    "arch": metadata.get("arch", ""),
    "config": {},
    "bootable_artifact": "rootfs.ext4",
    "files": {
        "rootfs.ext4": {"digest": digest, "size": os.path.getsize(rootfs)},
    },
}
with open(os.path.join(out_dir, "manifest.json"), "w") as f:
    json.dump(manifest, f, indent=2, sort_keys=False)
    f.write("\n")
print(f"[build-seed] manifest.json written (content_hash={digest})")
PY

printf '%s\n' "$inputs_hash" >"$stamp"
log "seed artifacts ready:"
ls -lh "$OUT_DIR"/rootfs.ext4 "$OUT_DIR"/metadata.json "$OUT_DIR"/manifest.json

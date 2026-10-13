#!/usr/bin/env bash
# build-seed.sh — node-local seed build wrapper used by space-builder.
#
# This intentionally mirrors templates/firecracker/seed/build.sh closely, but
# swaps the Mac+Docker launcher for a node-local containerd+ctr launcher.
#
# Current source-of-truth references:
#   - templates/firecracker/seed/build.sh
#   - templates/firecracker/seed/rootfs-builder.sh
#   - templates/firecracker/seed/Dockerfile
#
# Current role:
#   - Invoked by `space-builder build` for `from = "scratch"` recipes.
#   - Usable directly on a devbox or builder node for manual validation.
#
# Current tradeoffs:
#   - Uses ctr directly instead of Docker.
#   - Uses a pinned Ubuntu container instead of first building the
#     current Dockerfile image.
#   - Reuses rootfs-builder.sh for the inner rootfs construction steps.
#
# Explicitly denied in this phase:
#   - Adding Docker as a cluster-node dependency
#   - Rewriting the seed build as a dockerless host script first
#   - Converting this into the future builder daemon/RPC flow in this change
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="${SPACE_SEED_REPO_ROOT:-$(cd "${SCRIPT_DIR}/../../../.." 2>/dev/null && pwd || true)}"
ROOTFS_BUILDER_SCRIPT="${SPACE_SEED_ROOTFS_BUILDER_SCRIPT:-${REPO_ROOT}/templates/firecracker/seed/rootfs-builder.sh}"

# Stage with the same identity that asks containerd to mount the inputs.
if [[ $EUID -ne 0 ]]; then
  exec sudo -E env "HOME=${HOME:-}" "SPACE_SEED_REPO_ROOT=$REPO_ROOT" \
    /bin/bash "$0" "$@"
fi

# Walk from / and keep each directory open as cwd before using its children.
# Physical-path checks reject redirects even when a shared entry is exchanged.
enter_directory() {
  local target="$1" create="$2" expected="" part
  local -a parts
  IFS=/ read -r -a parts <<< "$target"
  cd /
  for part in "${parts[@]}"; do
    [[ -z "$part" ]] && continue
    expected="${expected}/$part"
    if [[ "$create" == yes && ! -e "$part" && ! -L "$part" ]]; then
      mkdir -m 0755 -- "$part"
    fi
    cd -P -- "$part"
    if [[ "$(pwd -P)" != "$expected" ]]; then
      echo "ERROR: directory path was redirected: $target" >&2
      exit 1
    fi
  done
}

prepare_root_directory() (
  local target="$1" kind="$2" expected="" part owner mode
  local -a parts
  IFS=/ read -r -a parts <<< "$target"
  cd /
  for part in "${parts[@]}"; do
    [[ -z "$part" ]] && continue
    read -r owner mode < <(stat -c '%u %a' .)
    if [[ "$owner" != 0 ]] || (( (8#$mode & 0022) != 0 && (8#$mode & 01000) == 0 )); then
      echo "ERROR: unsafe parent of $kind directory: $target" >&2
      echo "  $(pwd -P) is owned by uid $owner with mode $mode; it must be root-owned and either sticky or not group/other-writable." >&2
      echo "  Apply the node cache layout from the provisioning scripts (sticky /space/cache, firecracker and builds) before running scratch builds." >&2
      exit 1
    fi
    expected="${expected}/$part"
    if [[ ! -e "$part" && ! -L "$part" ]]; then
      mkdir -m 0755 -- "$part"
    fi
    cd -P -- "$part"
    if [[ "$(pwd -P)" != "$expected" ]]; then
      echo "ERROR: redirected $kind directory: $target" >&2
      exit 1
    fi
  done
  read -r owner mode < <(stat -c '%u %a' .)
  if [[ "$owner" != 0 ]] || { [[ "$kind" == staging ]] && (( (8#$mode & 0022) != 0 )); }; then
    local requirement="root-owned"
    [[ "$kind" != staging ]] || requirement="root-owned and not group/other-writable"
    echo "ERROR: unsafe $kind directory: $target" >&2
    echo "  It is owned by uid $owner with mode $mode; it must be $requirement." >&2
    exit 1
  fi
  if [[ "$kind" == cache ]]; then
    install -d -m 00755 -o 0 -g 0 .
  fi
)

ID="seed"
PROFILE="release"
FORCE=""
PUBKEY=""
ARCH=""

IMAGE_REF="docker.io/library/ubuntu:26.04@sha256:da6fc2be547864451aa253836dd926da33623312df4a9a243e35dc877c378a78"
CTR_SNAPSHOTTER="${CTR_SNAPSHOTTER:-native}"
DEFAULT_SEED_RUNTIME_VERSION="1.15.1"
DEFAULT_SEED_STATE_FORMAT_VERSION="9.0.0"

# Where the space-build-manager chart mounts the SPACE MITM root CA, and the
# fixed path rootfs-builder.sh reads it from. Keep in sync with the chart's
# mitmRootCa.mountPath and rootfs-builder.sh.
MITM_ROOT_CA_MOUNT="/space/etc/space-mitm/root-ca.pem"

normalize_arch() {
  case "$1" in
    aarch64|arm64) echo "aarch64" ;;
    x86_64|amd64) echo "x86_64" ;;
    *) return 1 ;;
  esac
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --id)
      ID="$2"
      shift 2
      ;;
    --profile)
      PROFILE="$2"
      shift 2
      ;;
    --force)
      FORCE="--force"
      shift
      ;;
    --pubkey)
      PUBKEY="$2"
      shift 2
      ;;
    --arch)
      ARCH="$2"
      shift 2
      ;;
    *)
      echo "Unknown arg: $1" >&2
      exit 1
      ;;
  esac
done

if [[ "$PROFILE" != "release" && "$PROFILE" != "debug" ]]; then
  echo "ERROR: --profile must be 'release' or 'debug'" >&2
  exit 1
fi

if [[ -z "$ARCH" ]]; then
  ARCH="$(uname -m)"
fi

ARCH="$(normalize_arch "$ARCH")" || {
  echo "ERROR: unsupported architecture: $ARCH (expected aarch64 or x86_64)" >&2
  exit 1
}

case "$ARCH" in
  aarch64)
    CONTAINER_PLATFORM="linux/arm64"
    RUST_TARGET="aarch64-unknown-linux-musl"
    ;;
  x86_64)
    CONTAINER_PLATFORM="linux/amd64"
    RUST_TARGET="x86_64-unknown-linux-musl"
    ;;
esac

echo "Architecture: ${ARCH} (containerd: ${CONTAINER_PLATFORM}, rust: ${RUST_TARGET})"

OUTDIR="${SPACE_SEED_OUTPUT_DIR:-${REPO_ROOT}/templates/firecracker/seed/output/${ID}}"
CACHEDIR="$(realpath -ms -- "${SPACE_SEED_CACHE_DIR:-/space/cache/firecracker/builds/seed-cache}")"
OUTDIR="$(realpath -ms -- "$OUTDIR")"
GUEST_BINS=(spaced-svc spaced)
# containerd rejects container IDs longer than 76 characters. Embedding the raw
# template ID made the name depend on both the ID length and the PID digit count,
# so long IDs failed once a builder's PID counter passed 1,000,000. A short
# digest keeps the name unique per ID and bounded (19 + 16 + 1 + <=7 digits).
CONTAINER_ID_DIGEST="$(printf '%s' "$ID" | sha256sum | cut -c1-16)"
CONTAINER_NAME="space-seed-builder-${CONTAINER_ID_DIGEST}-$$"
BUNDLE_DIR=""
INPUT_DIR=""
GUEST_BIN_STAGE_DIR=""
ROOTFS_BUILDER_STAGE_PATH=""

# containerd needs host-visible paths, so use the shared root-owned runtime
# directory. mktemp creates a 0700 bundle even with a permissive caller umask.
STAGE_ROOT="$(realpath -ms -- "${SPACE_SEED_STAGE_ROOT:-/space/run/build-manager}")"
prepare_root_directory "$STAGE_ROOT" staging
prepare_root_directory "$CACHEDIR" cache
BUNDLE_DIR="$(mktemp -d "$STAGE_ROOT/space-seed-build.XXXXXX")"
cleanup_container() {
  ctr tasks rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
  ctr containers rm "$CONTAINER_NAME" >/dev/null 2>&1 || true
  rm -rf "$BUNDLE_DIR"
}
trap cleanup_container EXIT

INPUT_DIR="${BUNDLE_DIR}/inputs"
GUEST_BIN_STAGE_DIR="${BUNDLE_DIR}/guest-bin"
ROOTFS_BUILDER_STAGE_PATH="${BUNDLE_DIR}/rootfs-builder.sh"
BUILD_OUTPUT="${BUNDLE_DIR}/output"
mkdir -p "$BUILD_OUTPUT" "$INPUT_DIR" "$GUEST_BIN_STAGE_DIR"

if [[ ! -f "$ROOTFS_BUILDER_SCRIPT" ]]; then
  echo "ERROR: rootfs builder script not found at ${ROOTFS_BUILDER_SCRIPT}" >&2
  exit 1
fi

cp "$ROOTFS_BUILDER_SCRIPT" "$ROOTFS_BUILDER_STAGE_PATH"
chmod 755 "$ROOTFS_BUILDER_STAGE_PATH"

if [[ -f "${OUTDIR}/rootfs.ext4" && -z "$FORCE" ]]; then
  echo "Output already exists at ${OUTDIR}/rootfs.ext4. Use --force to overwrite." >&2
  exit 1
fi

if ! ctr version >/dev/null 2>&1; then
  echo "ERROR: ctr cannot talk to containerd. Is containerd running locally?" >&2
  exit 1
fi

BUILDER_PUBKEY="${PUBKEY}"
if [[ -z "$BUILDER_PUBKEY" ]]; then
  PUBKEY_CANDIDATES=("${OUTDIR}/sandbox_ed25519.pub" /space/etc/ssh/sandbox_ed25519.pub)
  if [[ -n "${HOME:-}" ]]; then
    PUBKEY_CANDIDATES+=("${HOME}/.ssh/sandbox_ed25519.pub")
  fi
  for candidate in "${PUBKEY_CANDIDATES[@]}"; do
    if [[ -f "$candidate" ]]; then
      BUILDER_PUBKEY="$candidate"
      break
    fi
  done
fi

if [[ -n "$BUILDER_PUBKEY" && -f "$BUILDER_PUBKEY" ]]; then
  echo "Injecting sandbox SSH public key: ${BUILDER_PUBKEY}"
  cp "$BUILDER_PUBKEY" "${INPUT_DIR}/sandbox_ed25519.pub"
else
  echo "ERROR: No sandbox SSH public key found." >&2
  echo "  Checked:" >&2
  echo "    ${OUTDIR}/sandbox_ed25519.pub" >&2
  echo "    /space/etc/ssh/sandbox_ed25519.pub" >&2
  if [[ -n "${HOME:-}" ]]; then
    echo "    ${HOME}/.ssh/sandbox_ed25519.pub" >&2
  fi
  echo "  Or provide one manually: --pubkey /path/to/key.pub" >&2
  exit 1
fi

CARGO_TOML_SOURCE=""
for candidate in "${SPACE_SEED_CARGO_TOML:-}" "${REPO_ROOT}/Cargo.toml" /space/Cargo.toml; do
  [[ -z "$candidate" ]] && continue
  if [[ -f "$candidate" ]]; then
    CARGO_TOML_SOURCE="$candidate"
    break
  fi
done

GUEST_BIN_DIR=""
for candidate in "${SPACE_SEED_GUEST_BIN_DIR:-}" "${REPO_ROOT}/target/${RUST_TARGET}/${PROFILE}" /space/data/spaced; do
  [[ -z "$candidate" ]] && continue
  missing=false
  for bin in "${GUEST_BINS[@]}"; do
    if [[ ! -f "${candidate}/${bin}" ]]; then
      missing=true
      break
    fi
  done
  if [[ "$missing" == false ]]; then
    GUEST_BIN_DIR="$candidate"
    break
  fi
done

if [[ -z "$GUEST_BIN_DIR" ]]; then
  echo "ERROR: Could not find guest binaries (${GUEST_BINS[*]})." >&2
  echo "  Checked:" >&2
  if [[ -n "${SPACE_SEED_GUEST_BIN_DIR:-}" ]]; then
    echo "    ${SPACE_SEED_GUEST_BIN_DIR}" >&2
  fi
  echo "    ${REPO_ROOT}/target/${RUST_TARGET}/${PROFILE}" >&2
  echo "    /space/data/spaced" >&2
  exit 1
fi

for bin in "${GUEST_BINS[@]}"; do
  cp "${GUEST_BIN_DIR}/${bin}" "${GUEST_BIN_STAGE_DIR}/${bin}"
  chmod 755 "${GUEST_BIN_STAGE_DIR}/${bin}"
done

: "${SPACE_SEED_SOURCE_TYPE:?SPACE_SEED_SOURCE_TYPE is required}"
: "${SPACE_SEED_UBUNTU_RELEASE:?SPACE_SEED_UBUNTU_RELEASE is required}"
: "${SPACE_SEED_UBUNTU_VERSION:?SPACE_SEED_UBUNTU_VERSION is required}"
: "${SPACE_SEED_UBUNTU_DATE:?SPACE_SEED_UBUNTU_DATE is required}"

if [[ -n "${SPACE_SEED_RUNTIME_VERSION:-}" || -n "${SPACE_SEED_STATE_FORMAT_VERSION:-}" ]]; then
  if [[ -z "${SPACE_SEED_RUNTIME_VERSION:-}" || -z "${SPACE_SEED_STATE_FORMAT_VERSION:-}" ]]; then
    echo "ERROR: seed execution metadata requires both SPACE_SEED_RUNTIME_VERSION and SPACE_SEED_STATE_FORMAT_VERSION when overriding the defaults" >&2
    exit 1
  fi
fi
if [[ -z "${SPACE_SEED_RUNTIME_VERSION:-}" && -z "${SPACE_SEED_STATE_FORMAT_VERSION:-}" ]]; then
  latest_firecracker_dir=""
  while IFS= read -r candidate; do
    if [[ -x "${candidate}/bin/firecracker" ]] &&
      [[ -x "${candidate}/bin/jailer" ]] &&
      [[ -f "${candidate}/kernel/vmlinux.bin" ]]; then
      latest_firecracker_dir="$candidate"
      break
    fi
  done < <(find /space/vendor/firecracker -mindepth 1 -maxdepth 1 -type d -name 'v[0-9]*' 2>/dev/null | sort -Vr)
  if [[ -n "$latest_firecracker_dir" && -x "${latest_firecracker_dir}/bin/firecracker" ]]; then
    detected_runtime_version="$("${latest_firecracker_dir}/bin/firecracker" --version | sed -n 's/^Firecracker v\([^[:space:]]*\).*$/\1/p' | head -n1)"
    detected_state_format_version="$("${latest_firecracker_dir}/bin/firecracker" --snapshot-version | sed -n 's/^v\([0-9][^[:space:]]*\)$/\1/p' | head -n1)"
    if [[ -z "$detected_runtime_version" || -z "$detected_state_format_version" ]]; then
      echo "ERROR: could not inspect runtime metadata from ${latest_firecracker_dir}" >&2
      exit 1
    fi
    SPACE_SEED_RUNTIME="${SPACE_SEED_RUNTIME:-firecracker}"
    SPACE_SEED_RUNTIME_VERSION="${detected_runtime_version}"
    SPACE_SEED_STATE_FORMAT_VERSION="${detected_state_format_version}"
  else
    SPACE_SEED_RUNTIME_VERSION="${DEFAULT_SEED_RUNTIME_VERSION}"
    SPACE_SEED_STATE_FORMAT_VERSION="${DEFAULT_SEED_STATE_FORMAT_VERSION}"
  fi
fi
SPACE_SEED_RUNTIME="${SPACE_SEED_RUNTIME:-firecracker}"

if [[ -n "${SPACE_SEED_KERNEL_VERSION:-}" && -z "${SPACE_SEED_KERNEL_DIGEST:-}" ]] ||
  [[ -z "${SPACE_SEED_KERNEL_VERSION:-}" && -n "${SPACE_SEED_KERNEL_DIGEST:-}" ]]; then
  echo "ERROR: seed execution metadata requires both SPACE_SEED_KERNEL_VERSION and SPACE_SEED_KERNEL_DIGEST when overriding the defaults" >&2
  exit 1
fi

default_kernel="/space/cache/firecracker/kernels/default/vmlinux.bin"
if [[ ! -f "$default_kernel" ]]; then
  default_kernel="/space/cache/firecracker/assets/default/kernel/vmlinux.bin"
fi
if [[ -f "$default_kernel" ]]; then
  resolved_kernel="$(readlink -f "$default_kernel")"
  kernel_digest="sha256:$(sha256sum "$default_kernel" | awk '{print $1}')"
  case "$resolved_kernel" in
    /space/cache/firecracker/kernels/*/vmlinux.bin)
      kernel_version="$(basename "$(dirname "$resolved_kernel")")"
      ;;
    *)
      kernel_hex="${kernel_digest#sha256:}"
      kernel_version="legacy-${kernel_hex:0:12}"
      ;;
  esac
  SPACE_SEED_KERNEL_VERSION="${SPACE_SEED_KERNEL_VERSION:-${kernel_version}}"
  SPACE_SEED_KERNEL_DIGEST="${SPACE_SEED_KERNEL_DIGEST:-${kernel_digest}}"
fi

# The build container sources this file as root, so recipe and host values
# must stay data: %q quotes each one for bash, whatever characters it holds.
ROOTFS_BUILDER_ENV="${INPUT_DIR}/rootfs-builder.env"
: >"$ROOTFS_BUILDER_ENV"
stage_env() {
  local name
  for name in "$@"; do
    if [[ -n "${!name:-}" ]]; then
      printf 'export %s=%q\n' "$name" "${!name}" >>"$ROOTFS_BUILDER_ENV"
    fi
  done
}

stage_env \
  SPACE_SEED_SOURCE_TYPE \
  SPACE_SEED_UBUNTU_RELEASE \
  SPACE_SEED_UBUNTU_VERSION \
  SPACE_SEED_UBUNTU_DATE \
  SPACE_SEED_RUNTIME \
  SPACE_SEED_RUNTIME_VERSION \
  SPACE_SEED_STATE_FORMAT_VERSION \
  SPACE_SEED_KERNEL_VERSION \
  SPACE_SEED_KERNEL_DIGEST

# Prefer an explicit version from the outer launcher when available. Otherwise
# rootfs-builder.sh will fall back to the workspace Cargo.toml copied below.
stage_env SPACE_SEED_SPACED_VERSION

if [[ -n "$CARGO_TOML_SOURCE" ]]; then
  cp "$CARGO_TOML_SOURCE" "${INPUT_DIR}/Cargo.toml"
fi

# Stage the mounted SPACE MITM root CA into the bundle; the container preamble
# copies it to the fixed path rootfs-builder.sh reads. Only the read-only mount
# is honored — no writable fallback — so `mitmRootCa.enabled: false` (mount
# gone) deterministically yields a seed that trusts no SPACE root.
if [[ -f "$MITM_ROOT_CA_MOUNT" ]]; then
  echo "Injecting SPACE MITM root CA: ${MITM_ROOT_CA_MOUNT}"
  cp "$MITM_ROOT_CA_MOUNT" "${INPUT_DIR}/root-ca.pem"
fi

echo "Guest binaries (${PROFILE}): ${GUEST_BINS[*]}"

echo "=== Pulling builder image via containerd ==="
ctr images pull --platform "${CONTAINER_PLATFORM}" "${IMAGE_REF}"

MOUNTS=(
  "--mount" "type=bind,src=${BUILD_OUTPUT},dst=/output,options=rbind:rw"
  "--mount" "type=bind,src=${CACHEDIR},dst=/cache,options=rbind:rw"
  "--mount" "type=bind,src=${ROOTFS_BUILDER_STAGE_PATH},dst=/workspace/rootfs-builder.sh,options=rbind:ro"
  "--mount" "type=bind,src=${INPUT_DIR},dst=/workspace/inputs,options=rbind:ro"
  "--mount" "type=bind,src=${GUEST_BIN_STAGE_DIR},dst=/guest-bin,options=rbind:ro"
)

RUN_ARGS=(bash --id "$ID")
if [[ -n "$FORCE" ]]; then
  RUN_ARGS+=(--force)
fi

echo "=== Building seed template '${ID}' with ctr ==="
ctr run \
  --rm \
  --privileged \
  --net-host \
  --snapshotter "${CTR_SNAPSHOTTER}" \
  --platform "${CONTAINER_PLATFORM}" \
  "${MOUNTS[@]}" \
  "${IMAGE_REF}" \
  "${CONTAINER_NAME}" \
  /bin/bash -lc '
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y --no-install-recommends \
  ca-certificates \
  curl \
  e2fsprogs \
  gpgv \
  squashfs-tools \
  ubuntu-keyring \
  wget
source /workspace/inputs/rootfs-builder.env
mkdir -p /space /space/etc/ssh
cp /workspace/inputs/sandbox_ed25519.pub /space/etc/ssh/sandbox_ed25519.pub
if [[ -f /workspace/inputs/Cargo.toml ]]; then
  cp /workspace/inputs/Cargo.toml /space/Cargo.toml
fi
if [[ -f /workspace/inputs/root-ca.pem ]]; then
  mkdir -p /space/etc/space-mitm
  cp /workspace/inputs/root-ca.pem /space/etc/space-mitm/root-ca.pem
fi
exec /bin/bash /workspace/rootfs-builder.sh "$@"
' "${RUN_ARGS[@]}"

echo ""
# Publish only the completed artifacts, with the destination pinned by cwd.
# --force replaces leaf entries; mv -T never follows a destination symlink.
(
  for artifact in rootfs.ext4 metadata.json; do
    [[ -f "$BUILD_OUTPUT/$artifact" && ! -L "$BUILD_OUTPUT/$artifact" ]] || {
      echo "ERROR: build artifact is not a regular file: $artifact" >&2
      exit 1
    }
  done
  enter_directory "$(dirname "$OUTDIR")" yes
  if [[ -n "${SPACE_SEED_OUTPUT_GROUP:-}" ]]; then
    chown "root:${SPACE_SEED_OUTPUT_GROUP}" .
    chmod g+w .
    chown "root:${SPACE_SEED_OUTPUT_GROUP}" "$BUILD_OUTPUT/rootfs.ext4" "$BUILD_OUTPUT/metadata.json"
    chmod g+w "$BUILD_OUTPUT/rootfs.ext4" "$BUILD_OUTPUT/metadata.json"
  fi
  output_name="$(basename "$OUTDIR")"
  if [[ ! -e "$output_name" || -L "$output_name" ]]; then
    mkdir -m 0700 -- "$output_name"
  fi
  cd -P -- "$output_name"
  # Standalone sudo callers may retry in their own existing output directory.
  allowed_output_owner=0
  if [[ -z "${SPACE_SEED_OUTPUT_GROUP:-}" ]]; then
    allowed_output_owner="${SUDO_UID:-0}"
  fi
  output_owner="$(stat -c %u .)"
  if [[ "$(pwd -P)" != "$OUTDIR" ]] ||
    { [[ "$output_owner" != 0 && "$output_owner" != "$allowed_output_owner" ]]; }; then
    echo "ERROR: unsafe output directory: $OUTDIR" >&2
    exit 1
  fi
  for artifact in rootfs.ext4 metadata.json; do
    mv -T -- "$BUILD_OUTPUT/$artifact" "./$artifact"
  done
  if [[ -n "${SPACE_SEED_OUTPUT_GROUP:-}" ]]; then
    chown "root:${SPACE_SEED_OUTPUT_GROUP}" .
    chmod 2775 .
  else
    chmod go+rx .
  fi
)
echo "=== Build complete: ${OUTDIR} ==="
if [[ -z "${SPACE_SEED_OUTPUT_DIR:-}" ]]; then
  echo ""
  echo "If you are running this helper directly, register the result with:"
  echo "  sudo /space/bin/space-tmplmgr register --template ${ID}"
fi

#!/usr/bin/env bash
# build-template.sh — Build the seed Firecracker rootfs using Docker, optionally deploy to node.
#
# The seed is the minimal bootable Ubuntu image (SSH + networking). All
# application layers such as code-runtime are built on top by space-builder
# using TOML recipes.
#
# Template chain:  seed (this script) → code-runtime (recipe)
#
# Usage:
#   ./build-template.sh --id seed [options]
#
# Options:
#   --id <id>           Template identifier (default: "seed")
#   --arch <arch>       Target architecture: aarch64 or x86_64 (default: host arch)
#   --profile <p>       Build profile: release or debug (default: release)
#   --deploy            SCP result to node after build
#   --deploy-only       Skip build, just deploy existing output to node
#   --host <ssh-host>   SSH host for deploy (default: deploy@utm-space-dev)
#   --force             Overwrite existing output
#
# Prerequisites:
#   cargo build --workspace --release --target <arch>-unknown-linux-musl
#
# Examples:
#   ./build.sh --id seed                              # build for host arch (arm64 on Mac)
#   ./build.sh --id seed --arch x86_64                # build for EKS/x86_64
#   ./build.sh --id seed --deploy                     # build and deploy to UTM VM
#   ./build.sh --id seed --deploy-only                # deploy existing build
#   ./build.sh --id seed --deploy --host my-other-node
set -euo pipefail

if [[ "${SPACE_SEED_RUNTIME:-firecracker}" != firecracker ]]; then
  echo "ERROR: this standalone helper builds Firecracker seeds; use space-buildmgr with a qualified environment for other runtimes" >&2
  exit 1
fi

cd "$(dirname "$0")"

REPO_ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
SEED_RECIPE="${REPO_ROOT}/templates/firecracker/recipes/seed.toml"

ID="seed"
PROFILE="release"
DEPLOY=false
DEPLOY_ONLY=false
HOST="deploy@utm-space-dev"
FORCE=""
PUBKEY=""
ARCH=""
SSH_OPTS="-C -o StrictHostKeyChecking=no -o ConnectTimeout=5"
DEFAULT_SEED_RUNTIME_VERSION="1.15.1"
DEFAULT_SEED_STATE_FORMAT_VERSION="9.0.0"

seed_source_value() {
  local key="$1"
  awk -F' = ' -v key="$key" '
    /^\[source\]/ { in_source = 1; next }
    /^\[/ { in_source = 0 }
    in_source && $1 == key {
      gsub(/"/, "", $2)
      print $2
      exit
    }
  ' "$SEED_RECIPE"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --id)          ID="$2"; shift 2 ;;
    --profile)     PROFILE="$2"; shift 2 ;;
    --deploy)      DEPLOY=true; shift ;;
    --deploy-only) DEPLOY_ONLY=true; DEPLOY=true; shift ;;
    --host)        HOST="$2"; shift 2 ;;
    --force)       FORCE="--force"; shift ;;
    --pubkey)      PUBKEY="$2"; shift 2 ;;
    --arch)        ARCH="$2"; shift 2 ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
done

if [[ "$PROFILE" != "release" && "$PROFILE" != "debug" ]]; then
  echo "ERROR: --profile must be 'release' or 'debug'" >&2
  exit 1
fi

normalize_arch() {
  case "$1" in
    aarch64|arm64) echo "aarch64" ;;
    x86_64|amd64) echo "x86_64" ;;
    *) return 1 ;;
  esac
}

# Resolve target architecture. Default to host arch; --arch overrides.
# Supported: aarch64 (arm64) and x86_64 (amd64).
if [[ -z "$ARCH" ]]; then
  if [[ "$DEPLOY" == true ]]; then
    echo "Detecting target architecture from ${HOST}..."
    ARCH="$(ssh $SSH_OPTS "$HOST" uname -m)"
  else
    ARCH="$(uname -m)"
  fi
fi

ARCH="$(normalize_arch "$ARCH")" || {
  echo "ERROR: unsupported architecture: $ARCH (expected aarch64 or x86_64)" >&2
  exit 1
}

case "$ARCH" in
  aarch64)
    DOCKER_PLATFORM="linux/arm64"
    RUST_TARGET="aarch64-unknown-linux-musl"
    ;;
  x86_64)
    DOCKER_PLATFORM="linux/amd64"
    RUST_TARGET="x86_64-unknown-linux-musl"
    ;;
esac

echo "Architecture: ${ARCH} (docker: ${DOCKER_PLATFORM}, rust: ${RUST_TARGET})"

OUTDIR="./output/${ID}"

# ── Helpers ───────────────────────────────────────────────────────────────────
metadata_arch() {
  sed -n 's/.*"arch": "\([^"]*\)".*/\1/p' "$1" | head -1
}

remote_arch() {
  normalize_arch "$(ssh $SSH_OPTS "$HOST" uname -m)"
}

# Deploy template files to node. SCP to the flat layout under the template name dir,
# then call RegisterTemplate which moves them into a content-addressed digest dir.
# Previous builds and snapshots for other hashes are preserved.
deploy_template() {
  local template_dir="/space/cache/firecracker/templates/${ID}"
  local staging="/tmp/space-template-staging-${ID}"
  local built_arch target_arch
  built_arch="$(normalize_arch "$(metadata_arch "${OUTDIR}/metadata.json")")"
  target_arch="$(remote_arch)"
  if [[ "$built_arch" != "$target_arch" ]]; then
    echo "ERROR: refusing to deploy ${built_arch} seed template to ${target_arch} node ${HOST}" >&2
    echo "       Rebuild with: $0 --id ${ID} --arch ${target_arch} --force --deploy --host ${HOST}" >&2
    exit 1
  fi
  if ! ssh $SSH_OPTS "$HOST" "test -x /space/bin/space-tmplmgr"; then
    echo "ERROR: /space/bin/space-tmplmgr is not installed on ${HOST}" >&2
    echo "       Deploy the worker or builder feature before deploying the seed template." >&2
    exit 1
  fi

  echo "Deploying template to ${template_dir}/"
  ssh $SSH_OPTS "$HOST" "rm -rf ${staging} && mkdir -p ${staging}"
  scp $SSH_OPTS "${OUTDIR}/rootfs.ext4" "${HOST}:${staging}/rootfs.ext4"
  scp $SSH_OPTS "${OUTDIR}/metadata.json" "${HOST}:${staging}/metadata.json"

  # Stop pool and delete pool sandboxes before replacing the template
  echo "Stopping pool for ${ID} (if any)..."
  ssh $SSH_OPTS "$HOST" "sudo /space/bin/space-tmplmgr delete-pool --template ${ID} 2>/dev/null || true"

  # Place flat files in the template name dir (alongside any existing digest dirs).
  # RegisterTemplate will move them into a new sha256:{hash}/ digest dir.
  ssh $SSH_OPTS "$HOST" "sudo mkdir -p ${template_dir} && sudo mv ${staging}/rootfs.ext4 ${template_dir}/rootfs.ext4 && sudo mv ${staging}/metadata.json ${template_dir}/metadata.json && sudo rm -rf ${staging} && sudo chown -R root:space-cache-rw ${template_dir} && sudo chmod -R g+w ${template_dir}"

  echo ""
  echo "=== Deployed ==="
  echo "  Template: ${template_dir}/"
  ssh $SSH_OPTS "$HOST" "ls -lh ${template_dir}/"

  # Register with template-manager: computes content hash, moves files into
  # digest dir (sha256:{hash}/), creates manifest.json, tags as "latest".
  echo "Registering template with template-manager..."
  ssh $SSH_OPTS "$HOST" "sudo /space/bin/space-tmplmgr register --template ${ID}" || echo "  WARNING: registration failed (will be indexed on next restart)"

  # Clean up old builds: get the latest hash, then delete any other builds.
  echo "Cleaning up old builds..."
  ssh $SSH_OPTS "$HOST" "
    latest_hash=\$(sudo /space/bin/space-tmplmgr get --template ${ID} --tag latest 2>/dev/null | grep 'content_hash:' | awk '{print \$2}')
    if [ -n \"\$latest_hash\" ]; then
      for dir in ${template_dir}/sha256:*; do
        hash=\$(basename \"\$dir\")
        if [ \"\$hash\" != \"\$latest_hash\" ] && [ -d \"\$dir\" ]; then
          echo \"  removing old build: \$hash\"
          sudo /space/bin/space-tmplmgr delete-build --name ${ID} --content-hash \"\$hash\" 2>/dev/null || sudo rm -rf \"\$dir\"
        fi
      done
    fi
  " || true

  echo ""
  echo "NOTE: Run 'space-tmplmgr base-snapshot --template ${ID} --state true' to request a snapshot,"
  echo "      then 'space-tmplmgr create-pool --template ${ID} --boot-mode snapshot' for the pool."
}

# ── Deploy only ───────────────────────────────────────────────────────────────
if [[ "$DEPLOY_ONLY" == true ]]; then
  if [[ ! -f "${OUTDIR}/rootfs.ext4" ]]; then
    echo "Error: ${OUTDIR}/rootfs.ext4 not found. Build first." >&2
    exit 1
  fi
  echo "=== Deploying existing build to ${HOST} ==="
  deploy_template
  exit 0
fi

# ── Build validation ──────────────────────────────────────────────────────────
if [[ -d "$OUTDIR" && -z "$FORCE" ]]; then
  echo "Output already exists at ${OUTDIR}. Use --force to overwrite." >&2
  exit 1
fi

if [[ -n "$FORCE" ]]; then
  rm -rf "$OUTDIR"
fi
mkdir -p "$OUTDIR"

# ── Mount builder SSH public key if available ────────────────────────────────
CACHEDIR="$(pwd)/.cache"
mkdir -p "$CACHEDIR"

DOCKER_ARGS=(
  --rm --privileged
  --platform "${DOCKER_PLATFORM}"
  -v "$(pwd)/${OUTDIR}:/output"
  -v "${CACHEDIR}:/cache"
  -v "${REPO_ROOT}/Cargo.toml:/space/Cargo.toml:ro"
)

# Keep the legacy Docker-based seed path aligned with the explicit scratch
# source config used by space-builder's seed recipe.
SPACE_SEED_SOURCE_TYPE="$(seed_source_value type)"
SPACE_SEED_UBUNTU_RELEASE="$(seed_source_value release)"
SPACE_SEED_UBUNTU_VERSION="$(seed_source_value version)"
SPACE_SEED_UBUNTU_DATE="$(seed_source_value date)"

for value_name in \
  SPACE_SEED_SOURCE_TYPE \
  SPACE_SEED_UBUNTU_RELEASE \
  SPACE_SEED_UBUNTU_VERSION \
  SPACE_SEED_UBUNTU_DATE
do
  if [[ -z "${!value_name}" ]]; then
    echo "ERROR: failed to read ${value_name} from ${SEED_RECIPE}" >&2
    exit 1
  fi
done

DOCKER_ARGS+=(
  -e "SPACE_SEED_SOURCE_TYPE=${SPACE_SEED_SOURCE_TYPE}"
  -e "SPACE_SEED_UBUNTU_RELEASE=${SPACE_SEED_UBUNTU_RELEASE}"
  -e "SPACE_SEED_UBUNTU_VERSION=${SPACE_SEED_UBUNTU_VERSION}"
  -e "SPACE_SEED_UBUNTU_DATE=${SPACE_SEED_UBUNTU_DATE}"
)

if [[ -n "${SPACE_SEED_RUNTIME_VERSION:-}" || -n "${SPACE_SEED_STATE_FORMAT_VERSION:-}" ]]; then
  if [[ -z "${SPACE_SEED_RUNTIME_VERSION:-}" || -z "${SPACE_SEED_STATE_FORMAT_VERSION:-}" ]]; then
    echo "ERROR: SPACE_SEED_RUNTIME_VERSION and SPACE_SEED_STATE_FORMAT_VERSION must be set together when overriding the defaults" >&2
    exit 1
  fi
else
  SPACE_SEED_RUNTIME_VERSION="${DEFAULT_SEED_RUNTIME_VERSION}"
  SPACE_SEED_STATE_FORMAT_VERSION="${DEFAULT_SEED_STATE_FORMAT_VERSION}"
fi

for value_name in \
  SPACE_SEED_RUNTIME \
  SPACE_SEED_RUNTIME_VERSION \
  SPACE_SEED_STATE_FORMAT_VERSION \
  SPACE_SEED_KERNEL_VERSION \
  SPACE_SEED_KERNEL_DIGEST
do
  if [[ -n "${!value_name:-}" ]]; then
    DOCKER_ARGS+=(-e "${value_name}=${!value_name}")
  fi
done

# Sandbox SSH public key — baked into the rootfs so space-builder and space-ssh
# can SSH into VMs. The key is generated by Packer during image build and lives
# on the node at /space/etc/ssh/sandbox_ed25519.pub.
#
# Priority: --pubkey flag > deploy target key > local cache.
# A seed baked with a stale key still boots, but space-ssh and recipe builds
# fail on the target node. Prefer the deploy target whenever we know it.
BUILDER_PUBKEY="${PUBKEY}"

if [[ -z "$BUILDER_PUBKEY" && "$DEPLOY" == true ]]; then
  local_cache="${OUTDIR}/sandbox_ed25519.pub"
  echo "Fetching sandbox SSH public key from ${HOST}..."
  if scp $SSH_OPTS "${HOST}:/space/etc/ssh/sandbox_ed25519.pub" "$local_cache" 2>/dev/null; then
    BUILDER_PUBKEY="$local_cache"
    echo "  Cached at $local_cache"
  else
    echo "WARNING: could not fetch sandbox key from deploy target ${HOST}." >&2
  fi
fi

if [[ -z "$BUILDER_PUBKEY" ]]; then
  # Check local cache and well-known paths first
  for candidate in "${OUTDIR}/sandbox_ed25519.pub" /space/etc/ssh/sandbox_ed25519.pub "$HOME/.ssh/sandbox_ed25519.pub"; do
    if [[ -f "$candidate" ]]; then
      BUILDER_PUBKEY="$candidate"
      break
    fi
  done
fi

if [[ -z "$BUILDER_PUBKEY" || ! -f "$BUILDER_PUBKEY" ]]; then
  # Not found locally — try fetching from the node
  local_cache="${OUTDIR}/sandbox_ed25519.pub"
  echo "Fetching sandbox SSH public key from ${HOST}..."
  if scp $SSH_OPTS "${HOST}:/space/etc/ssh/sandbox_ed25519.pub" "$local_cache" 2>/dev/null; then
    BUILDER_PUBKEY="$local_cache"
    echo "  Cached at $local_cache"
  else
    echo "WARNING: could not fetch sandbox key from node. Is the node running?" >&2
  fi
fi

if [[ -n "$BUILDER_PUBKEY" && -f "$BUILDER_PUBKEY" ]]; then
  echo "Injecting sandbox SSH public key: ${BUILDER_PUBKEY}"
  DOCKER_ARGS+=(-v "$(cd "$(dirname "$BUILDER_PUBKEY")" && pwd)/$(basename "$BUILDER_PUBKEY"):/space/etc/ssh/sandbox_ed25519.pub:ro")
else
  echo "ERROR: No sandbox SSH public key found." >&2
  echo "  The key is generated during Packer image build (06-sysctl.sh)." >&2
  echo "  If deploying, ensure the node is running and accessible via ${HOST}." >&2
  echo "  Or provide one manually: --pubkey /path/to/key.pub" >&2
  exit 1
fi

# ── Mount guest binaries (spaced-svc, spaced) ────────────────────────────────
GUEST_BIN_DIR="${REPO_ROOT}/target/${RUST_TARGET}/${PROFILE}"
GUEST_BINS=(spaced-svc spaced)
MISSING_BINS=()
for bin in "${GUEST_BINS[@]}"; do
  if [[ -f "${GUEST_BIN_DIR}/${bin}" ]]; then
    DOCKER_ARGS+=(-v "${GUEST_BIN_DIR}/${bin}:/guest-bin/${bin}:ro")
  else
    MISSING_BINS+=("$bin")
  fi
done

if [[ ${#MISSING_BINS[@]} -gt 0 ]]; then
  echo "ERROR: Missing guest binaries in ${GUEST_BIN_DIR}:" >&2
  printf "  %s\n" "${MISSING_BINS[@]}" >&2
  echo "" >&2
  echo "Build first:" >&2
  echo "  cargo build --workspace --${PROFILE} --target ${RUST_TARGET}" >&2
  exit 1
fi
echo "Guest binaries (${PROFILE}): ${GUEST_BINS[*]}"

# ── Build Docker image ────────────────────────────────────────────────────────
echo "=== Building Docker image ==="
docker build --platform "${DOCKER_PLATFORM}" -t space-template-builder .

# ── Run builder ───────────────────────────────────────────────────────────────
echo "=== Building seed template '${ID}' ==="

docker run "${DOCKER_ARGS[@]}" space-template-builder \
  --id "$ID" $FORCE

echo ""
echo "=== Build complete ==="
ls -lh "${OUTDIR}/"
cat "${OUTDIR}/metadata.json"

# ── Deploy ────────────────────────────────────────────────────────────────────
if [[ "$DEPLOY" == true ]]; then
  echo ""
  echo "=== Deploying to ${HOST} ==="
  deploy_template
fi

echo ""
echo "Next steps:"
echo "  1. Deploy seed to node:  ./build.sh --id ${ID} --deploy-only"
echo "  2. Build code-runtime:   space-builder build --recipe /space/templates/firecracker/recipes/code-runtime.toml --id code-runtime"

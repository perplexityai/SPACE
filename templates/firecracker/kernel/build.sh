#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

readonly ARCH="${ARCH:-x86_64}"
readonly KERNEL_VERSION="${KERNEL_VERSION:-6.1.155}"
readonly FIRECRACKER_CI_VERSION="${FIRECRACKER_CI_VERSION:-v1.15}"
readonly KERNEL_SOURCE_COMMIT="${KERNEL_SOURCE_COMMIT:-069c93d2b7bddda87c472453c38c450faf86f999}"
readonly BASE_CONFIG_SHA256="${BASE_CONFIG_SHA256:-024b2aae62fe7131f9a9ab80f27619c882c0a37265dd105adae01d2867bef7c3}"
readonly OUTPUT_DIR="${OUTPUT_DIR:-${PWD}/output}"
readonly CONFIG_FRAGMENT="${CONFIG_FRAGMENT:-}"
# Further docker build options, such as a builder, base-image build contexts and
# caches (ci.sh): whitespace-separated, none containing spaces.
readonly KERNEL_DOCKER_BUILD_OPTIONS="${KERNEL_DOCKER_BUILD_OPTIONS:-}"

if [[ -n "$CONFIG_FRAGMENT" ]]; then
  if [[ "$CONFIG_FRAGMENT" == */* || ! -f "$CONFIG_FRAGMENT" || -L "$CONFIG_FRAGMENT" ]]; then
    echo 'ERROR: CONFIG_FRAGMENT must name an existing regular file in the kernel build directory' >&2
    exit 1
  fi
fi

if [[ "$ARCH" != "x86_64" ]]; then
  echo "ERROR: only x86_64 guest kernels are supported; aarch64 restore behavior is not validated" >&2
  exit 1
fi
if [[ ! "$KERNEL_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "ERROR: KERNEL_VERSION must be a three-part numeric version" >&2
  exit 1
fi
if [[ ! "$FIRECRACKER_CI_VERSION" =~ ^v[0-9]+\.[0-9]+$ ]]; then
  echo "ERROR: FIRECRACKER_CI_VERSION must look like v1.15" >&2
  exit 1
fi
if [[ ! "$KERNEL_SOURCE_COMMIT" =~ ^[0-9a-f]{40}$ ]]; then
  echo "ERROR: KERNEL_SOURCE_COMMIT must be a full lowercase Git commit" >&2
  exit 1
fi
if [[ ! "$BASE_CONFIG_SHA256" =~ ^[0-9a-f]{64}$ ]]; then
  echo "ERROR: BASE_CONFIG_SHA256 must be a lowercase SHA-256 digest" >&2
  exit 1
fi

mkdir -p "$OUTPUT_DIR"
# shellcheck disable=SC2086 # one option per word
DOCKER_BUILDKIT=1 docker build \
  --platform linux/amd64 \
  --target export \
  --build-arg "KERNEL_VERSION=${KERNEL_VERSION}" \
  --build-arg "FIRECRACKER_CI_VERSION=${FIRECRACKER_CI_VERSION}" \
  --build-arg "KERNEL_SOURCE_COMMIT=${KERNEL_SOURCE_COMMIT}" \
  --build-arg "BASE_CONFIG_SHA256=${BASE_CONFIG_SHA256}" \
  --build-arg "CONFIG_FRAGMENT=${CONFIG_FRAGMENT}" \
  --output "type=local,dest=${OUTPUT_DIR}" \
  $KERNEL_DOCKER_BUILD_OPTIONS \
  .

echo "Guest kernel artifacts written to ${OUTPUT_DIR}/vmlinux-${KERNEL_VERSION}${CONFIG_FRAGMENT:+-bpf-modsig}"

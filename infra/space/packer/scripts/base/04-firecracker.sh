#!/usr/bin/env bash
# Download and install Firecracker + Jailer from GitHub Releases.
#
# Bake the fixed execution pair into the vendor/cache paths used by launches.
# Runtime-manager candidates live in a separate managed store, initially empty.
# No execution path links into that store.
#
# Layout:
#   /space/vendor/firecracker/{version}/bin/{firecracker,jailer}
#   /space/vendor/kernels/{kernel-version}/vmlinux.bin
#   /space/vendor/kernels/{default,legacy-default} -> {kernel-version}
#   /space/cache/firecracker/kernels/{kernel-version}/vmlinux.bin
#   /space/cache/firecracker/kernels/default -> {kernel-version}
#   /space/cache/firecracker/kernels/legacy-default -> {kernel-version}
#   compatibility aliases remain under firecracker/{version}/kernel and assets/{version}/kernel
#   /space/cache/firecracker/assets/default -> {version}
#   /space/vendor/firecracker/default -> {version}    (symlink to active version)
#   /space/bin/firecracker -> ../vendor/firecracker/default/bin/firecracker
#   /space/bin/jailer -> ../vendor/firecracker/default/bin/jailer
set -euo pipefail

# The AWS recipes build x86_64 images, and only x86_64 artifacts are pinned
# below; another architecture fails on its first unpinned download.
ARCH=$(uname -m)

curl_retry() {
  curl --retry 5 --retry-delay 2 --retry-max-time 180 --connect-timeout 20 --retry-all-errors "$@"
}

# Every artifact this script downloads goes into every node and every sandbox,
# so each is checked against a SHA-256 pinned here. Adding a Firecracker
# release or a guest kernel means adding its pin. A Firecracker release carries
# a .sha256.txt to check the download against before pinning it; Firecracker's
# CI bucket publishes no checksums for its guest kernels, so a kernel's pin
# records the file as first downloaded and reviewed. A download with no pin, or
# a different hash, fails the bake. Guest kernels are keyed by their path in
# the spec.ccfc.min bucket.
declare -A PINNED_SHA256=(
  ["firecracker-v1.15.1-x86_64.tgz"]="d4a32ab2322d887ca1bc4a4e7afa9cc35393e6362dfc2b3becb389d362e4275a"
  ["firecracker-ci/v1.15/x86_64/vmlinux-6.1.155"]="e20e46d0c36c55c0d1014eb20576171b3f3d922260d9f792017aeff53af3d4f2"
)

verify_pinned() {
  local name="$1" file="$2" want got
  got="$(sha256sum "$file" | awk '{print $1}')"
  want="${PINNED_SHA256[$name]:-}"
  if [[ -z "$want" ]]; then
    echo "ERROR: no pinned SHA-256 for ${name} (downloaded ${got}); add it to PINNED_SHA256" >&2
    exit 1
  fi
  if [[ "$got" != "$want" ]]; then
    echo "ERROR: ${name} has SHA-256 ${got}, pinned ${want}" >&2
    exit 1
  fi
}

normalize_version() {
  printf 'v%s\n' "${1#v}"
}

FIRECRACKER_ROOT=/space/vendor/firecracker
KERNEL_ROOT=/space/vendor/kernels
# Baking is not a live-layout migration. Never follow a prior prototype's
# redirection of execution paths into a mutable store.
for root in "$FIRECRACKER_ROOT" "$KERNEL_ROOT"; do
  if [[ -L "$root" ]]; then
    echo "ERROR: rebuild from a clean base; $root is a symlink" >&2
    exit 1
  fi
  install -d -m 0755 -o root -g root "$root"
done

install_firecracker_release() {
  local version="$1"
  local version_dir="${FIRECRACKER_ROOT}/${version}"
  local staging_dir="${FIRECRACKER_ROOT}/.${version}.tmp.$$"
  local asset_dir="/space/cache/firecracker/assets/${version}"
  local tmpdir

  tmpdir=$(mktemp -d)
  echo "Installing Firecracker release: ${version}"
  curl_retry -fsSL -o "$tmpdir/firecracker.tgz" \
    "https://github.com/firecracker-microvm/firecracker/releases/download/${version}/firecracker-${version}-${ARCH}.tgz"
  verify_pinned "firecracker-${version}-${ARCH}.tgz" "$tmpdir/firecracker.tgz"
  tar xzf "$tmpdir/firecracker.tgz" -C "$tmpdir"

  rm -rf "${staging_dir}"
  mkdir -p "${staging_dir}/bin"
  install -o root -g root -m 0755 \
    "$tmpdir/release-${version}-${ARCH}/firecracker-${version}-${ARCH}" "${staging_dir}/bin/firecracker"
  install -o root -g root -m 0755 \
    "$tmpdir/release-${version}-${ARCH}/jailer-${version}-${ARCH}" "${staging_dir}/bin/jailer"

  rm -rf "$tmpdir"

  "${staging_dir}/bin/firecracker" --version
  "${staging_dir}/bin/firecracker" --snapshot-version
  "${staging_dir}/bin/jailer" --version

  rm -rf "${version_dir}"
  mv "${staging_dir}" "${version_dir}"
  mkdir -p "${asset_dir}/kernel"
}

install_guest_kernel() {
  local ci_version="$1"
  local kernel_key kernel_version kernel_image vendor_dir asset_dir tmpdir

  # The newest pinned kernel of this CI series, not the newest the bucket
  # lists: a kernel published upstream reaches an image only once it's pinned.
  echo "Resolving guest kernel (CI_VERSION=${ci_version}, arch=${ARCH})..."
  kernel_key=$(printf '%s\n' "${!PINNED_SHA256[@]}" \
    | grep -E "^firecracker-ci/${ci_version}/${ARCH}/vmlinux-[0-9]+\.[0-9]+\.[0-9]{1,3}$" \
    | sort -V | tail -1 || true)
  if [[ -z "$kernel_key" ]]; then
    echo "ERROR: no pinned guest kernel for CI_VERSION=${ci_version} ARCH=${ARCH}" >&2
    exit 1
  fi

  # Keep version labels consistent across vendor and cache copies.
  kernel_version="$(basename "$kernel_key")"
  kernel_version="${kernel_version#vmlinux-}"
  if [[ ! "$kernel_version" =~ ^[0-9]+(\.[0-9]+)*(-[a-z0-9]+)*$ ]]; then
    echo "ERROR: ${kernel_key} does not name a kernel version the runtime manager accepts" >&2
    exit 1
  fi
  if [[ " ${FIRECRACKER_KERNEL_VERSIONS[*]} " == *" ${kernel_version} "* ]]; then
    return
  fi
  tmpdir=$(mktemp -d)
  kernel_image="${tmpdir}/vmlinux.bin"
  curl_retry -fSL -o "$kernel_image" "https://s3.amazonaws.com/spec.ccfc.min/${kernel_key}"
  verify_pinned "$kernel_key" "$kernel_image"
  vendor_dir="${KERNEL_ROOT}/${kernel_version}"
  asset_dir="/space/cache/firecracker/kernels/${kernel_version}"
  mkdir -p "$vendor_dir" "$asset_dir"
  install -o root -g root -m 0644 "$kernel_image" "${vendor_dir}/vmlinux.bin"
  install -o root -g root -m 0644 "$kernel_image" "${asset_dir}/vmlinux.bin"
  rm -rf "$tmpdir"
  FIRECRACKER_KERNEL_VERSIONS+=("$kernel_version")
  echo "Installed guest kernel: ${kernel_version} ($(sha256sum "${asset_dir}/vmlinux.bin" | awk '{print $1}'))"
}

echo "=== Installing Firecracker ==="

FIRECRACKER_DEFAULT_VERSION=$(normalize_version "${FIRECRACKER_DEFAULT_VERSION:-${FIRECRACKER_VERSION:-v1.15.1}}")
FIRECRACKER_VERSIONS_RAW="${FIRECRACKER_VERSIONS:-v1.15.1}"
FIRECRACKER_KERNEL_CI_VERSIONS_RAW="${FIRECRACKER_KERNEL_CI_VERSIONS:-v1.15}"
FIRECRACKER_VERSIONS=()
FIRECRACKER_KERNEL_VERSIONS=()

for raw_version in $FIRECRACKER_VERSIONS_RAW "$FIRECRACKER_DEFAULT_VERSION"; do
  version=$(normalize_version "$raw_version")
  if [[ " ${FIRECRACKER_VERSIONS[*]} " != *" ${version} "* ]]; then
    FIRECRACKER_VERSIONS+=("$version")
  fi
done

for version in "${FIRECRACKER_VERSIONS[@]}"; do
  install_firecracker_release "$version"
done

for ci_version in $FIRECRACKER_KERNEL_CI_VERSIONS_RAW; do
  install_guest_kernel "$ci_version"
done
if [[ ${#FIRECRACKER_KERNEL_VERSIONS[@]} -eq 0 ]]; then
  echo "ERROR: no guest kernels installed" >&2
  exit 1
fi
FIRECRACKER_DEFAULT_KERNEL_VERSION="${FIRECRACKER_DEFAULT_KERNEL_VERSION:-$(printf '%s\n' "${FIRECRACKER_KERNEL_VERSIONS[@]}" | sort -V | tail -1)}"
FIRECRACKER_LEGACY_KERNEL_VERSION="${FIRECRACKER_LEGACY_KERNEL_VERSION:-${FIRECRACKER_KERNEL_VERSIONS[0]}}"
if [[ ! -f "/space/cache/firecracker/kernels/${FIRECRACKER_DEFAULT_KERNEL_VERSION}/vmlinux.bin" ]]; then
  echo "ERROR: default guest kernel ${FIRECRACKER_DEFAULT_KERNEL_VERSION} is not installed" >&2
  exit 1
fi
if [[ ! -f "/space/cache/firecracker/kernels/${FIRECRACKER_LEGACY_KERNEL_VERSION}/vmlinux.bin" ]]; then
  echo "ERROR: legacy guest kernel ${FIRECRACKER_LEGACY_KERNEL_VERSION} is not installed" >&2
  exit 1
fi
ln -sfn "${FIRECRACKER_DEFAULT_KERNEL_VERSION}" "${KERNEL_ROOT}/default"
ln -sfn "${FIRECRACKER_DEFAULT_KERNEL_VERSION}" /space/cache/firecracker/kernels/default
ln -sfn "${FIRECRACKER_LEGACY_KERNEL_VERSION}" "${KERNEL_ROOT}/legacy-default"
ln -sfn "${FIRECRACKER_LEGACY_KERNEL_VERSION}" /space/cache/firecracker/kernels/legacy-default

# Compatibility aliases preserve the historical per-release asset paths used by
# operator tooling. Cache aliases are hard links, so every release points at one kernel inode.
for version in "${FIRECRACKER_VERSIONS[@]}"; do
  mkdir -p "${FIRECRACKER_ROOT}/${version}/kernel" "/space/cache/firecracker/assets/${version}/kernel"
  ln -sfn "${KERNEL_ROOT}/${FIRECRACKER_DEFAULT_KERNEL_VERSION}/vmlinux.bin" \
    "${FIRECRACKER_ROOT}/${version}/kernel/vmlinux.bin"
  rm -f "/space/cache/firecracker/assets/${version}/kernel/vmlinux.bin"
  ln "/space/cache/firecracker/kernels/${FIRECRACKER_DEFAULT_KERNEL_VERSION}/vmlinux.bin" \
    "/space/cache/firecracker/assets/${version}/kernel/vmlinux.bin"
done

# New cold boots use the default release. Restores prefer their recorded
# release, then a compatible default, then another compatible installed release.
ln -sfn "${FIRECRACKER_DEFAULT_VERSION}" "${FIRECRACKER_ROOT}/default"
ln -sfn "${FIRECRACKER_DEFAULT_VERSION}" /space/cache/firecracker/assets/default

mkdir -p /space/bin
ln -sf ../vendor/firecracker/default/bin/firecracker /space/bin/firecracker
ln -sf ../vendor/firecracker/default/bin/jailer /space/bin/jailer

tee /etc/profile.d/space.sh > /dev/null <<'EOF'
export PATH="/space/bin:$PATH"
EOF

echo "=== Firecracker installed ==="
echo "  Releases:     ${FIRECRACKER_VERSIONS[*]}"
echo "  Guest kernels: ${FIRECRACKER_KERNEL_VERSIONS[*]}"
echo "  Kernel default: /space/cache/firecracker/kernels/default -> ${FIRECRACKER_DEFAULT_KERNEL_VERSION}"
echo "  Legacy kernel:  /space/cache/firecracker/kernels/legacy-default -> ${FIRECRACKER_LEGACY_KERNEL_VERSION}"
echo "  Asset default: /space/cache/firecracker/assets/default -> ${FIRECRACKER_DEFAULT_VERSION}"
echo "  Default:       ${FIRECRACKER_ROOT}/default -> ${FIRECRACKER_DEFAULT_VERSION}"
/space/bin/firecracker --version
/space/bin/firecracker --snapshot-version
/space/bin/jailer --version

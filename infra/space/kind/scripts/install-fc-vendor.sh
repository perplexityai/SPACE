#!/usr/bin/env bash
# install-fc-vendor.sh — fetch Firecracker binaries + guest kernel into the
# vendor tree the kind cluster mounts into each sandbox worker.
#
# Without this, the kind workers crash-loop because the node services'
# /space/vendor hostPath mounts point at a directory that has not been staged. This
# installer stages only the artifacts the kind harness needs:
#
#   $VENDOR_ROOT/firecracker/v1.15.1/bin/{firecracker,jailer}
#   $VENDOR_ROOT/firecracker/default → v1.15.1 (symlink)
#   $VENDOR_ROOT/firecracker/default/kernel/vmlinux.bin
#
# The target is $SPACE_VENDOR_ROOT, resolved by scripts/lib/kind-host-paths.sh
# — the same library that stamps it into the cluster config `kind create`
# reads, so a caller that leaves the default alone gets one directory for
# both. Staging with an explicit SPACE_VENDOR_ROOT and then rendering without
# it still splits them; the bring-up scripts pass one resolved value to both.
#
# Idempotent: re-running with the binaries already installed is a no-op fast
# path. Everything lands under $SPACE_VENDOR_ROOT. There is one escalation
# below, with two triggers: a vendor root that exists but belongs to root (the
# shape the devbox platform provisions), and a root that cannot be created
# because something above it is unwritable. In the second case the chown
# covers the directories this script had to create, which can include one
# above the vendor root — see the `created_root` walk.
set -euo pipefail

FC_VERSION="${SPACE_FC_VERSION:-v1.15.1}"
# firecracker-ci publishes upstream guest kernels keyed by CI version. v1.15
# ships kernel 5.10.x and 6.1.x; pick 6.1 (matches the kernel digest the
# production seed manifests reference).
FC_CI_VERSION="${SPACE_FC_CI_VERSION:-v1.15}"
KERNEL_SERIES="${SPACE_FC_KERNEL_SERIES:-vmlinux-6.1}"
ARCH="${SPACE_FC_ARCH:-x86_64}"
# shellcheck source=lib/kind-host-paths.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/kind-host-paths.sh"
kind_host_paths__require SPACE_VENDOR_ROOT
VENDOR_ROOT="$SPACE_VENDOR_ROOT"
# Prefer preinstalled firecracker/jailer and guest kernels over downloading.
IMAGE_VENDOR_ROOT="${SPACE_IMAGE_VENDOR_ROOT:-/opt/space/vendor}"

log() { printf '[install-fc-vendor] %s\n' "$*"; }
die() { printf '[install-fc-vendor] ERROR: %s\n' "$*" >&2; exit 1; }

require_bin() {
  for bin in "$@"; do
    command -v "$bin" >/dev/null 2>&1 || die "required binary missing: $bin"
  done
}
require_bin curl tar

# On some devbox images the parent ~/.space-devbox tree is pre-provisioned
# with `vendor/` owned by root. Sudo-chown to the current user once so this script
# (and re-runs) can write without escalation. Falls through silently if
# the path is already writable.
#
# Creating it is attempted first, unprivileged: `-w` alone is false for a root
# whose path merely doesn't exist yet — every box on its first run — and
# escalating to make a directory inside the caller's own home broke that run
# wherever sudo is absent or wants a password. `mkdir -p` succeeds on an
# already-existing directory, so the root-owned tree still reaches the chown
# below, and a root-owned parent makes it fail and reach it too.
if ! mkdir -p "$VENDOR_ROOT" 2>/dev/null || [[ ! -w "$VENDOR_ROOT" ]]; then
  # id -un, not $USER: non-interactive contexts (e.g. the devbox setup
  # agent's bash -l -c) don't set USER, and this script runs under set -u.
  current_user="$(id -un)"
  if command -v sudo >/dev/null 2>&1; then
    log "VENDOR_ROOT unusable ($VENDOR_ROOT); escalating once to create and chown it"
    # The topmost directory that does not exist yet. `sudo mkdir -p` creates
    # it and everything under it as root, and chowning only the leaf is what
    # left a root-owned ~/.space-devbox behind — the state the smoke-test
    # runbook tells operators to fix by hand. Hand back exactly what this
    # branch creates; a pre-existing ancestor is the platform's and stays put.
    created_root="$VENDOR_ROOT"
    while [[ ! -d "$(dirname "$created_root")" ]]; do
      created_root="$(dirname "$created_root")"
    done
    # Bound the escalation to the trees this harness owns. The walk above
    # leaves created_root at $VENDOR_ROOT whenever the root already exists, so
    # a pre-existing unwritable root would be chowned recursively whatever it
    # is: SPACE_VENDOR_ROOT=/usr/local rewrites ownership of /usr/local/bin,
    # which is privilege-escalation-shaped rather than merely destructive.
    #
    # Refusing such a root outright is the wrong knob — "everything on the big
    # disk" (SPACE_KIND_DATA_ROOT=/mnt/kind with the vendor root beneath it) is
    # a layout this harness documents. So the escalation is refused instead,
    # and the operator is handed the command to run once, where they can see
    # what they are about to chown. Both documented triggers stay inside the
    # bound: the devbox platform pre-provisions ~/.space-devbox/space/vendor,
    # and a first run creates directories under $HOME.
    # PHYSICAL space, via the library's single resolver: a symlinked
    # *component* of the path is resolved by ordinary pathname resolution, so
    # the recursion starts inside whatever it points at. `$HOME/mount/usr/local`
    # with `$HOME/mount -> /` passed a string-prefix check and reached sudo.
    # chown's -H/-L/-P only govern symlinks met as entries during the walk, so
    # none of them helps here — verified: `chmod -R -P` through a symlinked
    # component still changes the target tree. The bound has to be the resolved
    # path.
    owned_prefix="${SPACE_DEVBOX_PERSIST_ROOT:-${HOME:-}}"
    owned_real="$(kind_host_paths__physical "$owned_prefix")"
    target_real="$(kind_host_paths__physical "$created_root")"
    # Equality counts as well as descent, but only for a target that does not
    # exist yet. Two cases collapse onto equality and they need opposite
    # answers:
    #
    #   - SPACE_DEVBOX_PERSIST_ROOT that does not exist under an unwritable
    #     parent. The walk stopped there precisely because its parent exists
    #     and it does not, so created_root == owned_real and this is the
    #     documented one-time mkdir/chown. Allow it.
    #   - SPACE_VENDOR_ROOT=$HOME itself, which already exists. Allowing
    #     equality unconditionally made this `sudo chown -R <user> $HOME` —
    #     the whole home tree, `.ssh` included.
    #
    # `! -d "$created_root"` is what separates them, and it is the same fact
    # the walk used to stop.
    if [[ "$target_real" == "$owned_real" && -n "$owned_real" && -d "$created_root" ]]; then
      # Equality with an existing target. Naming the prefix as the
      # disqualifier here read as nonsense — "not a directory this run would
      # create under <itself>" — because the prefix is not what disqualifies
      # it. Existing is.
      die "$VENDOR_ROOT is not writable. Refusing to escalate: $target_real already exists, so this run did not create it, and a recursive chown of it would reach everything inside — for \$HOME that is .ssh included.
     If that is what you intend, run it yourself:
       sudo chown -R $current_user:$(id -gn) $target_real"
    fi
    if [[ -z "$owned_real" \
          || ( "$target_real" != "$owned_real" && "$target_real" != "$owned_real"/* ) ]]; then
      die "$VENDOR_ROOT is not writable. Refusing to escalate: $created_root resolves to $target_real, which is not a directory this run would create under ${owned_real:-\$HOME}.
     If that is what you intend, run it yourself:
       sudo chown -R $current_user:$(id -gn) $target_real"
    fi
    sudo mkdir -p "$VENDOR_ROOT"
    sudo chown -R "$current_user":"$(id -gn)" "$created_root"
  else
    die "$VENDOR_ROOT is not writable by $current_user and sudo is unavailable"
  fi
fi
mkdir -p "$VENDOR_ROOT/firecracker"

# ── Firecracker binaries ────────────────────────────────────────────────────
version_dir="$VENDOR_ROOT/firecracker/$FC_VERSION"
if [[ -x "$version_dir/bin/firecracker" && -x "$version_dir/bin/jailer" ]]; then
  installed=$("$version_dir/bin/firecracker" --version 2>&1 | awk '/^Firecracker/ {print $2}' | head -1)
  if [[ "$installed" == "$FC_VERSION" ]]; then
    log "firecracker $FC_VERSION already installed ($version_dir)"
  else
    log "firecracker version mismatch (have $installed, want $FC_VERSION); re-installing"
    rm -rf "$version_dir"
  fi
fi

if [[ ! -x "$version_dir/bin/firecracker" && -x "$IMAGE_VENDOR_ROOT/firecracker/$FC_VERSION/bin/firecracker" \
      && -x "$IMAGE_VENDOR_ROOT/firecracker/$FC_VERSION/bin/jailer" ]]; then
  log "staging firecracker $FC_VERSION from the devbox-space image ($IMAGE_VENDOR_ROOT)"
  mkdir -p "$version_dir/bin"
  install -m 0755 "$IMAGE_VENDOR_ROOT/firecracker/$FC_VERSION/bin/firecracker" "$version_dir/bin/firecracker"
  install -m 0755 "$IMAGE_VENDOR_ROOT/firecracker/$FC_VERSION/bin/jailer" "$version_dir/bin/jailer"
fi

if [[ ! -x "$version_dir/bin/firecracker" ]]; then
  log "downloading firecracker $FC_VERSION + jailer for $ARCH"
  tmp=$(mktemp -d)
  trap "rm -rf $tmp" EXIT
  # GitHub's release downloads can answer 500 for longer than fixed 2 s
  # retries last: six attempts over 13 s have all failed. curl's doubling
  # backoff spreads seven attempts over about a minute, and --retry-all-errors
  # also retries a reset connection.
  curl -fsSL --retry 6 --retry-all-errors \
    "https://github.com/firecracker-microvm/firecracker/releases/download/${FC_VERSION}/firecracker-${FC_VERSION}-${ARCH}.tgz" \
    -o "$tmp/fc.tgz"
  tar -xzf "$tmp/fc.tgz" -C "$tmp"
  mkdir -p "$version_dir/bin"
  install -m 0755 "$tmp/release-${FC_VERSION}-${ARCH}/firecracker-${FC_VERSION}-${ARCH}" "$version_dir/bin/firecracker"
  install -m 0755 "$tmp/release-${FC_VERSION}-${ARCH}/jailer-${FC_VERSION}-${ARCH}" "$version_dir/bin/jailer"
  rm -rf "$tmp"
  trap - EXIT
  log "firecracker $FC_VERSION installed ($version_dir)"
fi

# default symlink so /space/vendor/firecracker/default works
ln -sfn "$FC_VERSION" "$VENDOR_ROOT/firecracker/default"

# ── Guest kernel (vmlinux) ──────────────────────────────────────────────────
# Pulled from the upstream firecracker-ci public S3 bucket. The bucket is
# anonymously readable (no creds required); we use plain curl.
#
# Note: `firecracker/default` is a symlink to firecracker/$FC_VERSION (set
# above), so writing the kernel under $FC_VERSION/kernel/ also makes it
# resolvable at default/kernel/. We don't create a separate
# default/kernel/vmlinux.bin — that would be a self-referencing symlink.
kernel_dir="$VENDOR_ROOT/firecracker/$FC_VERSION/kernel"
mkdir -p "$kernel_dir"
# If a previous bad run left a self-referencing symlink at the destination,
# remove it before the actual download.
if [[ -L "$kernel_dir/vmlinux.bin" && ! -e "$kernel_dir/vmlinux.bin" ]]; then
  log "removing dangling symlink at $kernel_dir/vmlinux.bin"
  rm -f "$kernel_dir/vmlinux.bin"
fi
image_kernel="$(ls -d "$IMAGE_VENDOR_ROOT"/kernels/"$KERNEL_SERIES".*/vmlinux.bin 2>/dev/null | sort -V | tail -1 || true)"
if [[ -f "$kernel_dir/vmlinux.bin" && ! -L "$kernel_dir/vmlinux.bin" ]]; then
  log "vmlinux already present at $kernel_dir/vmlinux.bin"
elif [[ -n "$image_kernel" && -f "$image_kernel" ]]; then
  log "staging image-provided kernel $(basename "$(dirname "$image_kernel")") from $IMAGE_VENDOR_ROOT"
  rm -f "$kernel_dir/vmlinux.bin"
  install -m 0644 "$image_kernel" "$kernel_dir/vmlinux.bin"
else
  log "discovering latest $KERNEL_SERIES kernel under firecracker-ci/$FC_CI_VERSION/$ARCH"
  # firecracker-ci publishes kernels as flat files keyed by vmlinux-MAJ.MIN.PATCH.
  # Pull the bucket listing (XML) and pick the newest patch matching our series.
  listing=$(curl -fsSL "https://s3.amazonaws.com/spec.ccfc.min/?prefix=firecracker-ci/${FC_CI_VERSION}/${ARCH}/${KERNEL_SERIES}.&list-type=2") \
    || die "failed to list firecracker-ci S3 bucket; check network egress to s3.amazonaws.com"
  kernel_key=$(echo "$listing" \
    | grep -Eo "firecracker-ci/${FC_CI_VERSION}/${ARCH}/${KERNEL_SERIES}\\.[0-9]+" \
    | sort -V | uniq | tail -1)
  [[ -n "$kernel_key" ]] || die "no $KERNEL_SERIES kernel found in firecracker-ci/$FC_CI_VERSION/$ARCH"
  log "downloading $kernel_key"
  rm -f "$kernel_dir/vmlinux.bin"   # in case a stale symlink survived
  curl -fsSL "https://s3.amazonaws.com/spec.ccfc.min/${kernel_key}" -o "$kernel_dir/vmlinux.bin"
  log "vmlinux installed at $kernel_dir/vmlinux.bin"
fi

log "done. vendor tree at $VENDOR_ROOT/firecracker:"
ls -la "$VENDOR_ROOT/firecracker/" 2>/dev/null | head -10

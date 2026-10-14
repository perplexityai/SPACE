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
# path. `--check` writes nothing and exits 0 only when that is so: the
# Firecracker release and the guest kernel at its pinned sha256 are both
# staged. The bring-up scripts run the installer when it does not. Everything lands under $SPACE_VENDOR_ROOT. There is one escalation
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
ARCH="${SPACE_FC_ARCH:-x86_64}"
# One pinned object and its sha256, rather than the newest 6.1 patch in the
# bucket listing, so a kernel firecracker-ci publishes cannot change an
# unchanged bring-up. To upgrade, set SPACE_FC_KERNEL_VERSION and
# SPACE_FC_KERNEL_SHA256 together; a version without its checksum is refused,
# so it cannot pass as the default kernel. The bucket lists what exists:
#   curl -fsSL 'https://s3.amazonaws.com/spec.ccfc.min/?list-type=2&prefix=firecracker-ci/v1.15/x86_64/vmlinux-6.1.'
KERNEL_VERSION="${SPACE_FC_KERNEL_VERSION:-6.1.155}"
if [[ -n "${SPACE_FC_KERNEL_VERSION:-}" ]]; then
  KERNEL_SHA256="${SPACE_FC_KERNEL_SHA256:-}"
else
  KERNEL_SHA256="${SPACE_FC_KERNEL_SHA256:-e20e46d0c36c55c0d1014eb20576171b3f3d922260d9f792017aeff53af3d4f2}"
fi
# Candidates among the devbox image's kernels are matched by series; only one
# at the pinned sha256 is staged.
KERNEL_SERIES="${SPACE_FC_KERNEL_SERIES:-vmlinux-${KERNEL_VERSION%.*}}"
# shellcheck source=lib/kind-host-paths.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/kind-host-paths.sh"
kind_host_paths__require SPACE_VENDOR_ROOT
VENDOR_ROOT="$SPACE_VENDOR_ROOT"
# Prefer preinstalled firecracker/jailer and guest kernels over downloading.
IMAGE_VENDOR_ROOT="${SPACE_IMAGE_VENDOR_ROOT:-/opt/space/vendor}"

log() { printf '[install-fc-vendor] %s\n' "$*"; }
die() { printf '[install-fc-vendor] ERROR: %s\n' "$*" >&2; exit 1; }
warn() { printf '[install-fc-vendor] WARNING: %s\n' "$*" >&2; }

require_bin() {
  for bin in "$@"; do
    command -v "$bin" >/dev/null 2>&1 || die "required binary missing: $bin"
  done
}

# `firecracker/default` is a symlink to firecracker/$FC_VERSION, so the kernel
# staged under $FC_VERSION/kernel/ also resolves at default/kernel/.
kernel_dir="$VENDOR_ROOT/firecracker/$FC_VERSION/kernel"
# The pinned sha256 with the inode, size and mtime of the file it was checked
# against, so an unchanged kernel is not hashed again on every run and any
# other file, even one renamed into place since, is. A dotfile, which the node
# services' kernel scans skip.
kernel_stamp="$kernel_dir/.vmlinux.bin.sha256"

[[ "$KERNEL_SHA256" =~ ^[0-9a-f]{64}$ ]] \
  || die "SPACE_FC_KERNEL_VERSION=$KERNEL_VERSION needs SPACE_FC_KERNEL_SHA256, the sha256 of firecracker-ci/$FC_CI_VERSION/$ARCH/vmlinux-$KERNEL_VERSION"

file_sha256() { sha256sum "$1" | awk '{print $1}'; }
kernel_fingerprint() { stat -c '%i %s %.9Y' "$1"; }

firecracker_current() {
  local bin_dir="$VENDOR_ROOT/firecracker/$FC_VERSION/bin" installed
  [[ -x "$bin_dir/firecracker" && -x "$bin_dir/jailer" ]] || return 1
  installed=$("$bin_dir/firecracker" --version 2>&1 | awk '/^Firecracker/ {print $2}' | head -1)
  [[ "$installed" == "$FC_VERSION" ]] || return 1
  [[ "$(readlink "$VENDOR_ROOT/firecracker/default" 2>/dev/null)" == "$FC_VERSION" ]]
}

# The staged kernel is the pinned one: by its stamp while the file is
# unchanged, else by hashing it.
kernel_current() {
  local kernel="$kernel_dir/vmlinux.bin"
  [[ -f "$kernel" && ! -L "$kernel" ]] || return 1
  if [[ -f "$kernel_stamp" \
        && "$(cat "$kernel_stamp")" == "$KERNEL_SHA256 $(kernel_fingerprint "$kernel")" ]]; then
    return 0
  fi
  [[ "$(file_sha256 "$kernel")" == "$KERNEL_SHA256" ]]
}

if [[ "${1:-}" == --check ]]; then
  firecracker_current && kernel_current
  exit
fi

require_bin curl tar sha256sum stat

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
# Every kernel staged here, whatever its source, is the pinned one: a kernel
# already present, one the devbox image provides, or one downloaded from the
# upstream firecracker-ci public S3 bucket (anonymously readable; plain curl).
# Each is checked against KERNEL_SHA256, and one that does not match is
# replaced, so a host staged before a pin change stages the new kernel.
mkdir -p "$kernel_dir"
# If a previous bad run left a self-referencing symlink at the destination,
# remove it before the actual download.
if [[ -L "$kernel_dir/vmlinux.bin" && ! -e "$kernel_dir/vmlinux.bin" ]]; then
  log "removing dangling symlink at $kernel_dir/vmlinux.bin"
  rm -f "$kernel_dir/vmlinux.bin"
fi

# Installers that share this vendor root, such as two worktrees' bring-ups
# with different pins, stage one at a time: each checks, replaces and stamps
# the kernel under this lock. The stamp alone is already exact (it describes
# the file that was verified); the lock keeps two runs from replacing the
# kernel under each other.
if command -v flock >/dev/null 2>&1; then
  exec {kernel_lock_fd}>"$kernel_dir/.vmlinux.bin.lock"
  flock "$kernel_lock_fd"
fi

# Install $kernel_tmp as the kernel if it is the pinned one; return 1 if its
# checksum differs. Every other failure ends the run with no stamp vouching for
# a file it was not computed from. Up to the rename, the previous kernel stays
# in place, since a rename replaces it only once it succeeds; if only the stamp
# write fails, the new, verified kernel is in place unstamped, and the next
# run hashes it.
kernel_tmp=""
trap 'rm -f "${kernel_tmp:-}"' EXIT
stage_kernel() {
  local source="$1" got fingerprint
  got="$(file_sha256 "$kernel_tmp")" || die "could not hash $kernel_tmp"
  [[ "$got" == "$KERNEL_SHA256" ]] || return 1
  chmod 0644 "$kernel_tmp" || die "could not make $kernel_tmp readable"
  # Before the rename, which keeps the inode, size and mtime: the stamp then
  # describes the file that was verified, not whatever is at the path later.
  fingerprint="$(kernel_fingerprint "$kernel_tmp")" || die "could not stat $kernel_tmp"
  rm -f "$kernel_stamp" || die "could not remove $kernel_stamp"
  mv -f "$kernel_tmp" "$kernel_dir/vmlinux.bin" \
    || die "could not move the verified kernel to $kernel_dir/vmlinux.bin"
  kernel_tmp=""
  printf '%s %s\n' "$KERNEL_SHA256" "$fingerprint" >"$kernel_stamp" \
    || die "could not write $kernel_stamp"
  log "vmlinux $KERNEL_VERSION (sha256 $got) installed at $kernel_dir/vmlinux.bin from $source"
}

# Fetch the pinned object into $kernel_tmp. Each attempt has connection and
# transfer deadlines, and aborts a transfer that stalls, so a hung S3
# connection costs minutes rather than the job's timeout. A 4xx answer other
# than 408 and 429, such as the 404 of a mistyped SPACE_FC_KERNEL_VERSION,
# fails at once. Otherwise the HTTP status decides only when curl completed,
# because `%{http_code}` is the status already received: a body cut short or
# timed out after its 200 is a curl error. A 408, 429 or 5xx, and the curl
# errors a flaky network causes (a DNS or connect failure, a timeout, a body
# cut short or reset, a failed TLS handshake), are retried with a doubling
# backoff; any other curl error, such as a certificate that does not verify,
# fails at once.
download_kernel() {
  local url="$1" attempts=5 delay=2 attempt status code
  for ((attempt = 1; ; attempt++)); do
    status=0
    code="$(curl -sSL --connect-timeout 20 --max-time 300 --speed-limit 10240 --speed-time 60 \
      -w '%{http_code}' -o "$kernel_tmp" "$url")" || status=$?
    if [[ "$code" =~ ^4[0-9][0-9]$ && "$code" != 408 && "$code" != 429 ]]; then
      die "$url answered HTTP $code; check SPACE_FC_KERNEL_VERSION"
    fi
    if [[ "$status" -eq 0 ]]; then
      [[ "$code" == 200 ]] && return 0
      [[ "$code" == 408 || "$code" == 429 || "$code" =~ ^5[0-9][0-9]$ ]] \
        || die "$url answered HTTP $code"
    else
      case "$status" in
        6 | 7 | 18 | 28 | 35 | 52 | 56) ;;
        *) die "could not download $url (curl exit $status); check network egress to s3.amazonaws.com" ;;
      esac
    fi
    if ((attempt >= attempts)); then
      die "failed to download $url after $attempts attempts (curl exit $status, HTTP $code); check network egress to s3.amazonaws.com"
    fi
    log "kernel download failed (curl exit $status, HTTP $code; attempt $attempt/$attempts); retrying in ${delay}s"
    sleep "$delay"
    delay=$((delay * 2))
  done
}

if kernel_current; then
  # The kernel is verified, so a stamp that cannot be refreshed costs only a
  # hash on the next run, not this one.
  printf '%s %s\n' "$KERNEL_SHA256" "$(kernel_fingerprint "$kernel_dir/vmlinux.bin")" >"$kernel_stamp" \
    || warn "could not refresh $kernel_stamp; the next run hashes $kernel_dir/vmlinux.bin again"
  log "vmlinux already present at $kernel_dir/vmlinux.bin (sha256 $KERNEL_SHA256)"
else
  if [[ -e "$kernel_dir/vmlinux.bin" ]]; then
    log "replacing $kernel_dir/vmlinux.bin: it is not the pinned vmlinux $KERNEL_VERSION (sha256 $KERNEL_SHA256)"
  fi
  staged=""
  while IFS= read -r image_kernel; do
    [[ -f "$image_kernel" ]] || continue
    kernel_tmp="$(mktemp "$kernel_dir/.vmlinux.bin.XXXXXX")"
    cp "$image_kernel" "$kernel_tmp"
    if stage_kernel "image-provided $image_kernel"; then
      staged=1
      break
    fi
    log "not staging image-provided $image_kernel: its sha256 is not the pinned $KERNEL_SHA256"
    rm -f "$kernel_tmp"
    kernel_tmp=""
  done < <(ls -d "$IMAGE_VENDOR_ROOT"/kernels/"$KERNEL_SERIES".*/vmlinux.bin 2>/dev/null | sort -rV || true)
  if [[ -z "$staged" ]]; then
    kernel_key="firecracker-ci/${FC_CI_VERSION}/${ARCH}/vmlinux-${KERNEL_VERSION}"
    log "downloading $kernel_key"
    kernel_tmp="$(mktemp "$kernel_dir/.vmlinux.bin.XXXXXX")"
    download_kernel "https://s3.amazonaws.com/spec.ccfc.min/${kernel_key}"
    stage_kernel "$kernel_key" \
      || die "$kernel_key has sha256 $(file_sha256 "$kernel_tmp"), not the pinned $KERNEL_SHA256; set SPACE_FC_KERNEL_SHA256 only after checking the new object"
  fi
fi
trap - EXIT

log "done. vendor tree at $VENDOR_ROOT/firecracker:"
ls -la "$VENDOR_ROOT/firecracker/" 2>/dev/null | head -10

#!/usr/bin/env bash
# stage-guest-kernel.sh — put the vendored Firecracker guest kernel where the
# node's runtime inventory reads it.
#
# `/space/cache` is a fresh filesystem on every boot: the instance-store NVMe
# is reformatted before kubelet starts, so whatever the node image baked under
# `/space/cache` is gone by the time any service runs. The kernel itself ships
# on the root volume under `/space/vendor` and survives. Something therefore
# has to re-stage it after each mount, and doing it here — from the
# node-manager init container — makes the kernel a property of the node
# service release instead of the node image generation. That is what lets a
# new cluster reach a bootable worker from its Terraform plus the deploy
# pipeline, with no image rebuild and no hand-applied stopgap.
#
# Two vendor layouts are supported, matching what node images ship:
#
#   versioned  `<vendor>/kernels/<version>/vmlinux.bin`, plus `default` and
#              `legacy-default` symlinks naming a version
#              -> `<cache>/firecracker/kernels/<version>/vmlinux.bin`
#
#   flat       `<vendor>/firecracker/default/kernel/vmlinux.bin`
#              -> `<cache>/firecracker/assets/default/kernel/vmlinux.bin`
#
# The versioned tree wins when the vendor has one, because the inventory reads
# the flat `assets/default/kernel` path only when `kernels/` is absent. Keeping
# both branches faithful to their own layout matters: the flat path's component
# version is derived from the kernel digest, so synthesizing a versioned tree
# out of a flat vendor would rename the kernel that existing snapshots recorded.
#
# Staged kernels have to be real files. Jail setup hard-links the kernel into
# each sandbox's chroot, and hard-linking a symlink links the symlink itself,
# which Firecracker cannot open once it has chrooted.
#
# Exiting non-zero when nothing can be staged is deliberate. A worker with no
# guest kernel fails every cold activation, so refusing to start node-manager
# is the outcome we want: the node never reports its services ready, placement
# skips it, and the failure surfaces once on the node instead of once per
# sandbox create.
#
# The node-manager chart can also request one immutable kernel from the
# cluster's bootstrap bucket. The version, bucket, object key, and SHA-256 are
# an all-or-none set. The download is authenticated through the AWS CLI and is
# installed only after the vendored default has been restored, so the added
# kernel never changes `default` or `legacy-default` on a versioned node.
#
# A flat-layout node needs one extra transition before it can advertise both
# kernels: its existing kernel is copied into the versioned tree under the
# exact `legacy-<digest12>` identity RuntimeInventory already assigns it, and
# both previously nonexistent aliases point there. The outgoing node-manager
# can keep using the flat file during a rolling update; the incoming process
# sees the equivalent versioned identity plus the downloaded kernel.
#
# Staged kernels are written only by root: this script and the node boot
# scripts that rehydrate the cache after formatting it. Shared-cache services
# can create entries in the cache's group-writable parents, and the boot
# scripts have applied `chmod -R 2775` to the whole cache, so before and after
# staging the kernel trees are held to the staging account's ownership (root on
# a node) and 0755/0644 modes. A directory another account owns was planted,
# not staged, and staging stops rather than chowning it into something trusted.
# A file another account owns is usually ownership drift: jail setup hard-links
# the cache kernel into each sandbox's chroot, and node-managers that chowned
# the jail after linking it handed the shared inode to the sandbox's uid, which
# could then rewrite it. Such a file, or one any other account could write, may
# still be open for writing, and no chown or chmod revokes that descriptor, so
# it is never kept in place: when this run can verify its bytes (its vendor
# copy, the pinned remote digest, or the digest its `legacy-<digest12>` name
# carries), a fresh copy checked against that source is renamed over it, and
# the cache paths that shared its inode are linked to the copy. A foreign-owned
# file it cannot verify, such as a remote kernel from an earlier pin, is moved
# into `firecracker/.untrusted/` rather than trusted or deleted.
#
# Overridable for tests and for the kind harness:
#   SPACE_VENDOR_ROOT  (default /space/vendor)
#   SPACE_CACHE_ROOT   (default /space/cache)
#   SPACE_STAGED_KERNEL_OWNER_UID  (default: this script's uid)
#
# Optional immutable S3 kernel:
#   SPACE_GUEST_KERNEL_VERSION
#   SPACE_GUEST_KERNEL_S3_BUCKET
#   SPACE_GUEST_KERNEL_S3_KEY
#   SPACE_GUEST_KERNEL_SHA256
#   SPACE_GUEST_KERNEL_AWS_REGION  (default us-east-1)
set -euo pipefail

VENDOR_ROOT="${SPACE_VENDOR_ROOT:-/space/vendor}"
CACHE_ROOT="${SPACE_CACHE_ROOT:-/space/cache}"

KERNEL_VENDOR_DIR="${VENDOR_ROOT}/kernels"
KERNEL_CACHE_DIR="${CACHE_ROOT}/firecracker/kernels"
FLAT_KERNEL_SRC="${VENDOR_ROOT}/firecracker/default/kernel/vmlinux.bin"
FLAT_KERNEL_DST="${CACHE_ROOT}/firecracker/assets/default/kernel/vmlinux.bin"
ASSET_CACHE_DIR="${CACHE_ROOT}/firecracker/assets"
STAGED_OWNER_UID="${SPACE_STAGED_KERNEL_OWNER_UID:-$(id -u)}"

staged=0
reused=0
staging_tmp=""
# Newline-separated paths this run renamed a fresh copy over.
replaced_files=""
# Newline-separated cache images another account owned or could write when
# this run started. Such an image may still be open for writing, and no chown
# or chmod revokes that descriptor, so it is never kept: a fresh copy of its
# verified bytes is renamed over it.
exposed_files=""

log() { printf 'stage-guest-kernel: %s\n' "$*"; }
die() {
  printf 'stage-guest-kernel: ERROR: %s\n' "$*" >&2
  exit 1
}

cleanup() {
  if [[ -n "$staging_tmp" ]]; then
    rm -f "$staging_tmp"
  fi
}
trap cleanup EXIT

# Copy $1 to $2 as an independent regular file, skipping the write when the
# destination already holds the same bytes and was never exposed. `cp`
# dereferences a symlinked source, and the rename is atomic within the
# destination directory, so a concurrent launch never observes a partially
# written kernel.
stage_kernel_file() {
  local src="$1" dst="$2" dst_dir
  dst_dir="$(dirname "$dst")"
  install -d -m 0755 "$dst_dir"
  if [[ -f "$dst" && ! -L "$dst" ]] && ! is_exposed "$dst" && cmp -s "$src" "$dst"; then
    reused=$((reused + 1))
    return 0
  fi
  replace_with_copy "$src" "$dst" cmp_matches "$src"
  staged=$((staged + 1))
}

cmp_matches() {
  cmp -s "$1" "$2"
}

# Rename a fresh copy of $1 over $2 once `$3 <copy> $4` accepts the copy, and
# link the other cache paths that shared $2's old inode, such as a release's
# kernel alias, to the new one. The copy is what gets trusted, so the copy is
# what gets checked.
replace_with_copy() {
  local src="$1" dst="$2" check="$3" expected="$4" dst_dir old_inode=""
  dst_dir="$(dirname "$dst")"
  if [[ -f "$dst" && ! -L "$dst" ]]; then
    old_inode="$(inode_of "$dst")"
  fi
  staging_tmp="$(mktemp "${dst_dir}/.vmlinux.XXXXXXXX")"
  cp -f "$src" "$staging_tmp"
  chmod 0644 "$staging_tmp"
  "$check" "$staging_tmp" "$expected" || die "the copy staged for ${dst} does not match its source"
  mv -f "$staging_tmp" "$dst"
  staging_tmp=""
  replaced_files+="$dst"$'\n'
  if [[ -n "$old_inode" ]]; then
    relink_aliases "$old_inode" "$dst"
  fi
}

is_replaced() {
  printf '%s' "$replaced_files" | grep -Fxq -- "$1"
}

# Point every other cache path still on inode $1 at $2's inode.
relink_aliases() {
  local old_inode="$1" replacement="$2" tree path link
  for tree in "$KERNEL_CACHE_DIR" "$ASSET_CACHE_DIR"; do
    [[ -d "$tree" && ! -L "$tree" ]] || continue
    while IFS= read -r -d '' path; do
      [[ "$path" != "$replacement" ]] || continue
      link="$(mktemp "$(dirname "$path")/.vmlinux.XXXXXXXX")"
      rm -f "$link"
      ln "$replacement" "$link"
      mv -f "$link" "$path"
      replaced_files+="$path"$'\n'
    done < <(find "$tree" -xdev -type f -inum "$old_inode" -print0)
  done
}

# GNU `mv -T` replaces a symlink through rename(2) — atomic, and it does not
# follow the symlink. BSD `mv` has no `-T` and would move the temp *into* the
# directory the alias points at. Production is Linux, so it takes the atomic
# path; the fallback keeps this runnable on a dev machine.
if mv --version >/dev/null 2>&1; then
  atomic_symlink_replace=true
else
  atomic_symlink_replace=false
fi

# Point $2 at $1, refusing to touch a path that is something other than a
# symlink. `ln -sfn` against an existing directory silently creates a link
# *inside* it, which would leave the alias unresolvable and the failure far
# from its cause.
#
# The replacement is atomic where the platform allows it. `ln -sfn` unlinks
# before it symlinks, and the reader is not necessarily idle during that
# window: node-manager builds its runtime inventory lazily on the first
# launch, not at startup, and the node-manager DaemonSet rolls with
# `maxSurge`, so a new pod's init container can be rewriting these aliases
# while the outgoing pod is still serving launches. A reader landing in the
# gap sees no `default` and fails to resolve a boot kernel.
link_alias() {
  local target="$1" path="$2" tmp
  if [[ -e "$path" && ! -L "$path" ]]; then
    die "${path} exists and is not a symlink; refusing to replace it"
  fi
  if [[ "$atomic_symlink_replace" == true ]]; then
    tmp="${path}.tmp.$$"
    ln -sfn "$target" "$tmp"
    mv -Tf "$tmp" "$path"
  else
    ln -sfn "$target" "$path"
  fi
}

# Echo the version directory a vendor alias names, or fail if it resolves to
# nothing.
resolve_alias() {
  local path="$1" resolved
  resolved="$(readlink -f "$path" 2>/dev/null || true)"
  if [[ -z "$resolved" || ! -d "$resolved" ]]; then
    return 1
  fi
  basename "$resolved"
}

# Print one version name per installed vendor kernel, skipping the alias
# entries. Both the layout decision and the staging loop read the vendor tree
# through this, so there is one definition of "which directories count".
vendor_kernel_versions() {
  local candidate version
  if [[ ! -d "$KERNEL_VENDOR_DIR" ]]; then
    return 0
  fi
  for candidate in "$KERNEL_VENDOR_DIR"/*/vmlinux.bin; do
    if [[ ! -f "$candidate" ]]; then
      continue
    fi
    version="$(basename "$(dirname "$candidate")")"
    case "$version" in
      default | legacy-default) continue ;;
    esac
    printf '%s\n' "$version"
  done
}

stage_versioned_kernels() {
  local version default_version legacy_version
  while IFS= read -r version; do
    stage_kernel_file \
      "${KERNEL_VENDOR_DIR}/${version}/vmlinux.bin" \
      "${KERNEL_CACHE_DIR}/${version}/vmlinux.bin"
  done < <(vendor_kernel_versions)

  default_version="$(resolve_alias "${KERNEL_VENDOR_DIR}/default" || true)"
  if [[ -z "$default_version" ]]; then
    die "${KERNEL_VENDOR_DIR}/default does not resolve to a kernel version"
  fi
  link_alias "$default_version" "${KERNEL_CACHE_DIR}/default"

  # A vendor without a legacy alias pins both to the same version, matching how
  # the node image lays the tree out when only one kernel is installed.
  legacy_version="$(resolve_alias "${KERNEL_VENDOR_DIR}/legacy-default" || true)"
  if [[ -z "$legacy_version" ]]; then
    legacy_version="$default_version"
  fi
  link_alias "$legacy_version" "${KERNEL_CACHE_DIR}/legacy-default"

  log "versioned kernels under ${KERNEL_CACHE_DIR}: default=${default_version} legacy-default=${legacy_version}"
}

stage_flat_kernel() {
  stage_kernel_file "$FLAT_KERNEL_SRC" "$FLAT_KERNEL_DST"
  log "flat kernel at ${FLAT_KERNEL_DST}"
}

# Hold one staged kernel tree to root-only writes. Symlinks are the version
# aliases and are left alone; find does not follow them.
protect_kernel_tree() {
  local tree="$1" foreign
  if [[ -L "$tree" ]]; then
    die "${tree} is a symlink; refusing to stage kernels through it"
  fi
  [[ -d "$tree" ]] || return 0
  foreign="$(find "$tree" -xdev -type d ! -uid "$STAGED_OWNER_UID" -print -quit)"
  if [[ -n "$foreign" ]]; then
    die "${foreign} is not owned by uid ${STAGED_OWNER_UID}; refusing to stage kernels into a tree another account wrote"
  fi
  # Symbolic modes: a numeric chmod keeps a directory's setgid bit, which the
  # cache-wide 2775 has set.
  find "$tree" -xdev -type d ! -perm 0755 -exec chmod u=rwx,go=rx,ug-s,a-t {} +
  find "$tree" -xdev -type f ! -perm 0644 -exec chmod u=rw,go=r,ug-s,a-t {} +
}

protect_kernel_trees() {
  protect_kernel_tree "$KERNEL_CACHE_DIR"
  protect_kernel_tree "$ASSET_CACHE_DIR"
}

# GNU stat on nodes, BSD stat on a dev machine.
uid_of() {
  stat -c '%u' "$1" 2>/dev/null || stat -f '%u' "$1"
}

inode_of() {
  stat -c '%i' "$1" 2>/dev/null || stat -f '%i' "$1"
}

# Record, before anything is re-owned or re-moded, which cache images another
# account owns or any group or other account can write.
record_exposed_files() {
  local tree path
  for tree in "$KERNEL_CACHE_DIR" "$ASSET_CACHE_DIR"; do
    [[ -d "$tree" && ! -L "$tree" ]] || continue
    while IFS= read -r -d '' path; do
      exposed_files+="$path"$'\n'
    done < <(find "$tree" -xdev -type f \( ! -uid "$STAGED_OWNER_UID" -o -perm -020 -o -perm -002 \) -print0)
  done
}

is_exposed() {
  printf '%s' "$exposed_files" | grep -Fxq -- "$1"
}

# `kernels/legacy-<digest12>/vmlinux.bin` names its own content: the promoted
# flat kernel has no vendor copy, but its digest prefix can still be checked.
digest_named_kernel_matches() {
  local path="$1" version prefix
  [[ "$(basename "$path")" == vmlinux.bin ]] || return 1
  [[ "$(dirname "$(dirname "$path")")" == "$KERNEL_CACHE_DIR" ]] || return 1
  version="$(basename "$(dirname "$path")")"
  [[ "$version" =~ ^legacy-([0-9a-f]{12})$ ]] || return 1
  prefix="${BASH_REMATCH[1]}"
  [[ "$(sha256_file "$path")" == "$prefix"* ]]
}

digest_prefix_matches() {
  [[ "$(sha256_file "$1")" == "$2"* ]]
}

# The digest prefix a `kernels/legacy-<digest12>/vmlinux.bin` path names.
digest_named_prefix() {
  local version
  version="$(basename "$(dirname "$1")")"
  printf '%s' "${version#legacy-}"
}

# Deal with the exposed images staging did not replace, then set aside every
# foreign-owned file left. Runs after staging, so restaged files are already
# fresh root-owned copies.
settle_foreign_files() {
  local tree path owner relative quarantine="" untrusted="${CACHE_ROOT}/firecracker/.untrusted"
  while IFS= read -r path; do
    [[ -n "$path" && -f "$path" && ! -L "$path" ]] || continue
    is_replaced "$path" && continue
    owner="$(uid_of "$path")"
    if digest_named_kernel_matches "$path"; then
      # The copy is checked against the digest the name carries, not against
      # bytes a retained descriptor may have changed since.
      replace_with_copy "$path" "$path" digest_prefix_matches "$(digest_named_prefix "$path")"
      log "WARNING: ${path} was exposed (owner uid ${owner}); replaced with a fresh copy of its verified bytes"
    elif [[ "$owner" == "$STAGED_OWNER_UID" ]]; then
      log "WARNING: ${path} was writable by other accounts and has nothing to verify it against; a descriptor opened then can still write it"
    fi
  done <<<"$exposed_files"
  for tree in "$KERNEL_CACHE_DIR" "$ASSET_CACHE_DIR"; do
    [[ -d "$tree" && ! -L "$tree" ]] || continue
    while IFS= read -r -d '' path; do
      # A path replaced earlier in this loop is no longer foreign.
      [[ -L "$path" || "$(uid_of "$path")" != "$STAGED_OWNER_UID" ]] || continue
      if [[ -z "$quarantine" ]]; then
        # The parent directory is writable by the cache's services, so a
        # `.untrusted` someone else planted is refused rather than moved into.
        if [[ -L "$untrusted" ]] || { [[ -e "$untrusted" ]] \
          && { [[ ! -d "$untrusted" ]] || [[ "$(uid_of "$untrusted")" != "$STAGED_OWNER_UID" ]]; }; }; then
          die "${untrusted} is not a directory owned by uid ${STAGED_OWNER_UID}; refusing to quarantine into it"
        fi
        install -d -m 0700 "$untrusted"
        # Symbolic, as in protect_kernel_tree: drop the setgid bit inherited
        # from the cache-wide 2775.
        chmod u=rwx,go=,ug-s,a-t "$untrusted"
        quarantine="${untrusted}/$(date -u +%Y%m%dT%H%M%SZ)-$$"
        install -d -m 0700 "$quarantine"
      fi
      relative="${path#"${CACHE_ROOT}/firecracker/"}"
      install -d -m 0700 "$quarantine/$(dirname "$relative")"
      mv -f "$path" "$quarantine/$relative"
      log "WARNING: ${path} is owned by uid $(uid_of "$quarantine/$relative") and could not be verified; moved to ${quarantine}/${relative}"
    done < <(find "$tree" -xdev ! -type d ! -uid "$STAGED_OWNER_UID" -print0)
  done
}

sha256_file() {
  sha256sum "$1" | awk '{print $1}'
}

# A versioned kernel directory makes RuntimeInventory stop consulting the flat
# path. Preserve the flat kernel under the exact identity the legacy discovery
# path derives before creating that directory, so enabling an additional
# kernel does not remove the node's current default from its inventory.
promote_flat_kernel() {
  local digest legacy_version
  digest="$(sha256_file "$FLAT_KERNEL_DST")"
  legacy_version="legacy-${digest:0:12}"

  stage_kernel_file \
    "$FLAT_KERNEL_DST" \
    "${KERNEL_CACHE_DIR}/${legacy_version}/vmlinux.bin"
  link_alias "$legacy_version" "${KERNEL_CACHE_DIR}/default"
  link_alias "$legacy_version" "${KERNEL_CACHE_DIR}/legacy-default"
  log "promoted flat kernel into versioned inventory as ${legacy_version}"
}

stage_remote_kernel() {
  local version="${SPACE_GUEST_KERNEL_VERSION:-}"
  local bucket="${SPACE_GUEST_KERNEL_S3_BUCKET:-}"
  local key="${SPACE_GUEST_KERNEL_S3_KEY:-}"
  local expected_sha256="${SPACE_GUEST_KERNEL_SHA256:-}"
  local region="${SPACE_GUEST_KERNEL_AWS_REGION:-us-east-1}"
  local expected_key destination destination_dir actual_sha256

  if [[ -z "$version" && -z "$bucket" && -z "$key" && -z "$expected_sha256" ]]; then
    return 0
  fi
  if [[ -z "$version" || -z "$bucket" || -z "$key" || -z "$expected_sha256" ]]; then
    die "guest kernel version, S3 bucket, S3 key, and SHA-256 must be set together"
  fi
  if [[ ! "$version" =~ ^[a-zA-Z0-9._+-]{1,128}$ ]] \
    || [[ "$version" == "." || "$version" == ".." ]] \
    || [[ "$version" == "default" || "$version" == "legacy-default" ]]; then
    die "invalid guest kernel version: ${version}"
  fi
  if [[ ! "$bucket" =~ ^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$ ]]; then
    die "invalid guest kernel S3 bucket: ${bucket}"
  fi
  if [[ ! "$expected_sha256" =~ ^[0-9a-f]{64}$ ]]; then
    die "guest kernel SHA-256 must be 64 lowercase hexadecimal characters"
  fi

  expected_key="guest-kernels/${version}/sha256:${expected_sha256}/vmlinux.bin"
  if [[ "$key" != "$expected_key" ]]; then
    die "guest kernel S3 key must be ${expected_key}"
  fi

  if [[ ! -d "$KERNEL_CACHE_DIR" ]]; then
    promote_flat_kernel
  fi

  destination_dir="${KERNEL_CACHE_DIR}/${version}"
  destination="${destination_dir}/vmlinux.bin"
  if [[ -f "$destination" && ! -L "$destination" ]]; then
    actual_sha256="$(sha256_file "$destination")"
    if [[ "$actual_sha256" == "$expected_sha256" ]]; then
      if is_exposed "$destination"; then
        replace_with_copy "$destination" "$destination" digest_prefix_matches "$expected_sha256"
        staged=$((staged + 1))
        log "guest kernel ${version} matched sha256:${expected_sha256} but was exposed; replaced with a fresh copy"
        return 0
      fi
      reused=$((reused + 1))
      log "guest kernel ${version} already matches sha256:${expected_sha256}"
      return 0
    fi
    die "installed guest kernel ${version} has sha256:${actual_sha256}, expected sha256:${expected_sha256}"
  fi
  if [[ -e "$destination" || -L "$destination" ]]; then
    die "guest kernel destination ${destination} exists and is not a regular file"
  fi
  if [[ -e "$destination_dir" && ! -d "$destination_dir" ]] || [[ -L "$destination_dir" ]]; then
    die "guest kernel destination ${destination_dir} is not a regular directory"
  fi
  command -v aws >/dev/null 2>&1 \
    || die "aws CLI is required to download configured guest kernel ${version}"

  install -d -m 0755 "$destination_dir"
  staging_tmp="$(mktemp "${destination_dir}/.vmlinux.XXXXXXXX")"
  if ! aws s3 cp \
    "s3://${bucket}/${key}" \
    "$staging_tmp" \
    --region "$region" \
    --only-show-errors; then
    die "failed to download guest kernel from s3://${bucket}/${key}"
  fi
  actual_sha256="$(sha256_file "$staging_tmp")"
  if [[ "$actual_sha256" != "$expected_sha256" ]]; then
    die "downloaded guest kernel has sha256:${actual_sha256}, expected sha256:${expected_sha256}"
  fi
  chmod 0644 "$staging_tmp"
  mv -f "$staging_tmp" "$destination"
  staging_tmp=""
  staged=$((staged + 1))
  log "installed guest kernel ${version} from s3://${bucket}/${key}"
}

# Re-derive what the runtime inventory will find, using the same branch
# `RuntimeInventory::discover` takes (crates/l40_api/node/runtime/core/src/
# runtime_inventory.rs): a `kernels/` directory selects the versioned tree
# outright, and the flat asset path is consulted only in its absence.
# Asserting the post-state rather than the steps catches a half-populated
# cache left behind by an earlier boot. Keep this in step with that function —
# it is the one place the two implementations have to agree.
verify_inventory_source() {
  local alias
  if [[ -d "$KERNEL_CACHE_DIR" ]]; then
    # Both aliases, not just `default`. `discover_kernels` marks
    # `default_for_new` and `legacy_default` from these two independently, and
    # a launch that is not a new cold boot resolves through `legacy_default`.
    # An alias pointing at a version directory that was never staged — or
    # staged without its `vmlinux.bin` — canonicalizes fine, matches no
    # installed kernel, and leaves that selection with nothing to resolve. The
    # `-f` test follows the symlink, so it catches both the dangling and the
    # present-but-empty case.
    for alias in default legacy-default; do
      if [[ ! -f "${KERNEL_CACHE_DIR}/${alias}/vmlinux.bin" ]]; then
        die "${KERNEL_CACHE_DIR}/${alias} does not resolve to a kernel; launches selecting it would find none"
      fi
    done
    return 0
  fi
  if [[ ! -f "$FLAT_KERNEL_DST" ]]; then
    die "no kernel at ${FLAT_KERNEL_DST} and no ${KERNEL_CACHE_DIR} tree"
  fi
}

# A vendor `kernels/` directory is authoritative on its own, empty or not. A
# node image that ships one and fails to populate it is broken, and every path
# that creates the directory populates it in the same step. Falling back to the
# flat layout there would be worse than failing: the flat layout's component
# version is the kernel digest, so the node would come up healthy while
# reporting a different kernel identity from the rest of the fleet, and the
# mismatch would only surface later as a restore that finds no matching kernel.
#
# The condition is a plain file test rather than a function call because
# calling a function as an `if` condition suppresses errexit for its whole call
# tree, which would let a staging failure fall through to the other layout.
record_exposed_files
protect_kernel_trees
if [[ -d "$KERNEL_VENDOR_DIR" ]]; then
  stage_versioned_kernels
elif [[ -f "$FLAT_KERNEL_SRC" ]]; then
  stage_flat_kernel
else
  die "no guest kernel under ${VENDOR_ROOT}: expected ${KERNEL_VENDOR_DIR}/<version>/vmlinux.bin or ${FLAT_KERNEL_SRC}"
fi

stage_remote_kernel
settle_foreign_files
protect_kernel_trees
verify_inventory_source

log "complete staged=${staged} unchanged=${reused}"

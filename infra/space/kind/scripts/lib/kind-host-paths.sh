#!/usr/bin/env bash
# kind-host-paths.sh — the single place that resolves the host paths the kind
# harness and the kind cluster config have to agree on, and the renderer that
# puts them into that config.
#
# Why a renderer rather than a static path: `kind create` reads plain YAML and
# expands nothing, so a checked-in config can only name one host layout. It
# named `/home/dev/...`, while the script that stages the Firecracker vendor
# tree defaulted to `$HOME/...`. Under any other username the writer and the
# two readers pointed at different directories, and Docker answers a missing
# bind-mount source by creating an empty directory instead of refusing — so
# the workers came up Ready with an empty `/space/vendor` and the first
# sandbox failed as if Firecracker were broken.
#
# Sourcing this file resolves the roots; `kind_render_cluster_config` stamps
# them into the config `kind create` reads, and `kind_prepare_host_mounts`
# creates or verifies every bind-mount source that config names. A caller that
# stages the vendor tree must pass `$SPACE_VENDOR_ROOT` to the staging script
# rather than letting it re-derive a default.
#
# Both roots default to the literals the config carried before when `$HOME` is
# `/home/dev`, so a devbox renders byte-identical mounts. The vendor installer
# sources this same library, so staging and mounts agree under any username
# and with explicit path overrides.
#
# Env knobs:
#   SPACE_VENDOR_ROOT     Firecracker vendor tree, mounted at /space/vendor
#                         (default: $HOME/.space-devbox/space/vendor)
#   SPACE_KIND_DATA_ROOT  per-node containerd storage backing
#                         (default: $HOME/.kind-data)
#   SPACE_KIND_CONFIG     where the rendered cluster config is written
#                         (default: $HOME/.space-devbox/kind/kind-cluster.yaml)
#   SPACE_KIND_NODE_IMAGE / SPACE_KIND_BASE_NODE_IMAGE
#                         node images the config names (default: what
#                         ../kind-node-image.sh prints for `ref` / `base`)

# The binary a sandbox worker executes through /space/vendor. The staging
# check in bootstrap.sh gates on this same path; this is the one definition.
KIND_HOST_PATHS_VENDOR_PROBE="firecracker/default/bin/firecracker"

# The node image script, found from this file so every caller resolves the same
# references (the render also runs from tests with a scrubbed environment).
KIND_HOST_PATHS_NODE_IMAGE_SCRIPT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/kind-node-image.sh"

# Two spaces, and every comparison in this library belongs to exactly one.
# Mixing them is what each spelling-related defect here has been, so they are
# named once rather than re-derived per predicate:
#
#   RENDERED — the literal string stamped into the cluster config. Docker
#     reports a bind source back verbatim (verified: a `//` in a source comes
#     back as `//`, unnormalized), and `kind create` reads the literal. So
#     comparisons against a live cluster's mounts belong here, and
#     canonicalising them would break them.
#   PHYSICAL — what the path denotes after symlinks and `..` are followed.
#     `chown`, `mkdir` and the filesystem act here, so any comparison that
#     gates a side effect belongs here. `kind_host_paths__physical` is the
#     one implementation; do not add a second.
kind_host_paths__physical() {
  local target="$1"
  # An empty path has no physical form. Returning one anyway is worse than
  # returning nothing: `dirname ""` is `.`, so the old code answered "$PWD/"
  # and any caller bounding a side effect by this value silently bounded it
  # by the process's working directory instead. Callers test for empty.
  [[ -n "$target" ]] || return 0
  if [[ -d "$target" ]]; then
    (cd -- "$target" 2>/dev/null && pwd -P)
    return
  fi
  # Not created yet: walk up to the deepest ancestor that does exist and
  # re-append everything below it. Walking rather than taking one dirname,
  # because when the parent is missing too a single dirname leaves the leaf
  # rooted at `/` — `/tmp/a/b/c` with `/tmp/a` absent answered `/c`, a path
  # under `/` that no comparison against $HOME could match.
  local rest="" parent="$target" base resolved
  while :; do
    base="$(basename -- "$parent")"
    parent="$(dirname -- "$parent")"
    rest="$base${rest:+/$rest}"
    if resolved="$(cd -- "$parent" 2>/dev/null && pwd -P)"; then
      # Normalized because an ancestor of `/` resolves to `/` and would
      # otherwise produce a leading `//`.
      kind_host_paths__normalize "$resolved/$rest"
      return
    fi
    [[ "$parent" == / || "$parent" == . ]] && break
  done
  # Nothing above it exists either: hand back the normalized input rather
  # than a path rooted at `/`.
  kind_host_paths__normalize "$target"
}

kind_host_paths__die() {
  printf '[kind-host-paths] ERROR: %s\n' "$*" >&2
  exit 1
}

# `kind create` hands each hostPath to Docker verbatim, which rejects a
# relative source ("includes invalid characters for a local volume name") far
# from the assignment that produced it. The other two rejections are about the
# renderer: `|` is its sed delimiter, and a newline has no representation in a
# one-line replacement. `\` and `&` are escaped rather than rejected, in
# kind_host_paths__sed_escape.
kind_host_paths__assert_mountable() {
  local name="$1" value="$2"
  [[ "$value" == /* ]] || kind_host_paths__die "$name must be an absolute path (got: ${value:-<empty>})"
  # `/` itself, which `$UNSET_VAR/` and the trailing-slash normalization both
  # produce. It is absolute and contains no rejected character, so it reaches
  # install-fc-vendor.sh, where `[[ ! -w / ]]` is true for any non-root user
  # and the escalation becomes `sudo chown -R <user> /` — a recursive chown of
  # the root filesystem, which also strips setuid from sudo itself.
  [[ "$value" != / ]] || kind_host_paths__die "$name must name a directory below /, not the filesystem root itself"
  # Spellings that are not literally `/` but land there. `/tmp/..` is absolute
  # and passes the test above, and the installer's `created_root != /` guard
  # compares literally, so `sudo chown -R <user> /tmp/..` still rewrote the
  # whole filesystem. Rejecting `..` outright is simpler than canonicalising
  # every path, and no legitimate mount root needs one.
  case "/$value/" in
    */../*) kind_host_paths__die "$name must not contain a '..' component (got: $value)" ;;
  esac
  # And resolve, when the directory exists: a symlink to `/` is absolute, has
  # no `..`, and is not literally `/`. `pwd -P` is what settles it.
  if [[ -d "$value" ]]; then
    # PHYSICAL: a symlink to `/` has no `..` and is not literally `/`.
    [[ "$(kind_host_paths__physical "$value")" != / ]] \
      || kind_host_paths__die "$name resolves to the filesystem root (got: $value)"
  fi 
  [[ "$value" != *'|'* ]] || kind_host_paths__die "$name must not contain '|' (got: $value)"
  # A root may contain `${...}` — an unknown placeholder renders literally and
  # that is tested. What it must not contain is a placeholder the renderer
  # *knows*, because the substitutions run as two `sed` expressions over one
  # stream and the second rescans what the first inserted: a data root reading
  # `/mnt/${SPACE_VENDOR_ROOT}/data` renders the vendor root into it, then
  # gets created and compared against the literal spelling forever. Only one
  # ordering is exposed today, so both are refused — a reorder of those two
  # expressions must not be able to reopen this.
  case "$value" in
    *'${SPACE_VENDOR_ROOT}'* | *'${SPACE_KIND_DATA_ROOT}'*)
      kind_host_paths__die "$name must not contain a placeholder the renderer substitutes (\${SPACE_VENDOR_ROOT} or \${SPACE_KIND_DATA_ROOT}); it would be expanded inside this value (got: $value)" ;;
  esac
  [[ "$value" != *$'\n'* ]] || kind_host_paths__die "$name must not contain a newline"
  # The root is substituted into a YAML plain scalar and read back by a
  # line-oriented parser, so the shapes YAML would silently reshape are
  # rejected rather than escaped. Each one recreates this library's own defect:
  # the mount would use the reshaped path while the installer staged the tree
  # at the original.
  #
  # Verified against PyYAML: `hostPath: /mnt/vendor #1` loads as
  # `/mnt/vendor`, and `hostPath: /a/b ` loads as `/a/b`. Note the `#` rule is
  # deliberately broader than YAML's — truncation needs whitespace before the
  # `#`, and `/has#hash` survives both YAML and the hand parser intact — but
  # the difference is one space and the failure is silent, so the whole
  # character goes. A `:` is a hard ScannerError and is Docker's own `-v`
  # separator; a tab is a ScannerError too, and is the separator
  # kind_config_mounts emits.
  case "$value" in
    *'#'*) kind_host_paths__die "$name must not contain '#' — YAML reads it as a comment when a space precedes it (got: $value)" ;;
    *:*)   kind_host_paths__die "$name must not contain ':' — invalid YAML and Docker's mount separator (got: $value)" ;;
    *'"'* | *"'"*) kind_host_paths__die "$name must not contain a quote (got: $value)" ;;
    *$'\t'*) kind_host_paths__die "$name must not contain a tab (got: $value)" ;;
    *$'\r'*) kind_host_paths__die "$name must not contain a carriage return" ;;
    *[[:space:]]) kind_host_paths__die "$name must not end with whitespace — YAML trims it from a plain scalar, so the mount would differ from the staged path (got: '$value')" ;;
  esac
  # No leading-whitespace arm: a value that starts with whitespace does not
  # start with `/` and is already refused by the absolute-path check above.
}

# Deduplicate a list for reporting. The config names the vendor root and
# /dev/kvm on four nodes each, so a raw failure list repeats them; the mount
# *counts* elsewhere need the undeduplicated stream, which is why
# kind_config_mounts does not sort -u for everyone.
kind_host_paths__unique() {
  printf '%s\n' "$@" | sort -u
}

# Strip trailing slashes so `/mnt/data` and `/mnt/data/` resolve to one value.
# Both spellings are legitimate input, and without this they compare unequal
# against a live cluster's recorded mount — so bootstrapping once with the
# slash and again without it would report drift and demand a recreate for a
# cluster that is correct. Docker was observed *not* to normalize `//` in a
# bind source, so canonicalising the input is what makes the two spellings
# interchangeable. A bare `/` is preserved.
kind_host_paths__normalize() {
  local value="$1"
  # Internal duplicates and `/.` segments too, not just the tail: `/a//b` and
  # `/a/./b` name the same directory as `/a/b`, and leaving them meant
  # bootstrapping with one spelling and re-running with another still compared
  # unequal against the live cluster's recorded mount — the recreate-for-a-
  # healthy-cluster outcome this function exists to prevent.
  while [[ "$value" == *//* ]]; do
    value="${value//\/\///}"
  done
  while [[ "$value" == */./* ]]; do
    value="${value//\/.\///}"
  done
  if [[ "$value" == */. ]]; then
    value="${value%/.}"
    # `/.` is the filesystem root, not an empty path. Emptying it here made
    # `kind_host_paths__require` report an explicitly-set root as "unset and
    # HOME is not set to derive its default", telling the operator to set the
    # variable they had just set. Its siblings `/./`, `//` and `/.//` all
    # collapse to `/` in the loops above and reach the root refusal; this one
    # has to arrive there too.
    [[ -n "$value" ]] || value=/
  fi
  while [[ "$value" == */ && "$value" != / ]]; do
    value="${value%/}"
  done
  printf '%s' "$value"
}

# Make a path safe to use as sed replacement text. `&` re-inserts the whole
# match and `\` starts an escape, so an unescaped root containing either
# renders a corrupted hostPath — and then fails the leftover-placeholder check
# below, reporting a template defect that doesn't exist.
kind_host_paths__sed_escape() {
  local value="$1"
  value="${value//\\/\\\\}"
  value="${value//&/\\&}"
  printf '%s' "$value"
}

# Each default is derived from $HOME, but an unset HOME leaves the value
# *empty* rather than failing here — `${HOME:-}`, not `${HOME:?}`. Sourcing
# this library must not fail over a root the caller never reads.
#
# The concrete regression: `install-fc-vendor.sh` used to read
# `${SPACE_VENDOR_ROOT:-$HOME/...}`, so an explicit root meant `$HOME` was
# never evaluated and the script ran with HOME unset. Resolving all three
# roots eagerly took that away — measured on this branch, the same invocation
# exits 0 before the change and 1 after, failing on SPACE_KIND_DATA_ROOT,
# which the installer never touches. No in-repo caller runs without HOME
# today; every one is an interactive or CI shell. The point is that a shared
# library should not add a requirement the scripts it absorbed did not have.
# `kind_host_paths__require` re-imposes the check at each point of use, so the
# failure lands on the root that is actually missing.
#
# Empty, not `/.space-devbox/...`: a bare `${HOME}` would silently stage a
# vendor tree at the filesystem root.
#
# Preserve the established SPACE_DEVBOX_SPACE_TARGET and
# SPACE_DEVBOX_PERSIST_ROOT overrides in the vendor default. Resolving them
# here keeps the installer and cluster mount on the same path when an
# operator moves the persisted state.
#
# The `:+` guards chain deliberately: an underivable value stays empty all the
# way down rather than collapsing to an absolute path like `/space/vendor`.
kind_host_paths__home="${HOME:-}"
kind_host_paths__persist="${SPACE_DEVBOX_PERSIST_ROOT:-${kind_host_paths__home:+$kind_host_paths__home/.space-devbox}}"
kind_host_paths__space="${SPACE_DEVBOX_SPACE_TARGET:-${kind_host_paths__persist:+$kind_host_paths__persist/space}}"
SPACE_VENDOR_ROOT="${SPACE_VENDOR_ROOT:-${kind_host_paths__space:+$kind_host_paths__space/vendor}}"
SPACE_KIND_DATA_ROOT="${SPACE_KIND_DATA_ROOT:-${kind_host_paths__home:+$kind_host_paths__home/.kind-data}}"
SPACE_KIND_CONFIG="${SPACE_KIND_CONFIG:-${kind_host_paths__persist:+$kind_host_paths__persist/kind/kind-cluster.yaml}}"
SPACE_VENDOR_ROOT="$(kind_host_paths__normalize "$SPACE_VENDOR_ROOT")"
SPACE_KIND_DATA_ROOT="$(kind_host_paths__normalize "$SPACE_KIND_DATA_ROOT")"
export SPACE_VENDOR_ROOT SPACE_KIND_DATA_ROOT SPACE_KIND_CONFIG

# Assert one root at the point it is about to be used. Callers that touch a
# single root — the vendor installer, the bench stager — require only that one.
kind_host_paths__require() {
  local name
  for name in "$@"; do
    local value="${!name}"
    [[ -n "$value" ]] \
      || kind_host_paths__die "$name is unset and HOME is not set to derive its default"
    case "$name" in
      SPACE_KIND_CONFIG) ;;   # a file path, not a bind-mount source
      *) kind_host_paths__assert_mountable "$name" "$value" ;;
    esac
  done
}

# Every (containerPath, hostPath) pair the given cluster config bind-mounts,
# tab-separated.
#
# Hand-parsed because this library is sourced by scripts that are allowed to
# assume nothing but bash — and correct only for one-key-per-line block
# mappings whose `hostPath:`/`containerPath:` keys are all node extraMounts.
# Four shapes it gets wrong, all valid YAML:
#
#   - a `hostPath:` inside a `kubeadmConfigPatches` block is read as a host
#     mount, then demanded to exist on the host, which a node-internal path
#     never will;
#   - `extraMounts: [{hostPath: ...}]`, a flow sequence, is invisible;
#   - `- {hostPath: ..., containerPath: ...}`, a flow mapping inside a block
#     sequence, is invisible;
#   - two keys on one line are read as one.
#
# A *dropped* mount is the dangerous direction: `kind_prepare_host_mounts`
# never checks it and `kind_assert_live_cluster_mounts` expects zero of it, so
# any cluster passes — the original defect, reopened.
#
# The enforcement is `test_hand_parsed_host_paths_match_a_yaml_parse` in
# `scripts/tests/test_kind_host_paths.py`, which holds this output against a
# real PyYAML walk of the same file, pairs and multiplicity included. The
# companion shape test rejects the idioms outright so the failure names the
# cause rather than a count mismatch.
kind_config_mounts() {
  sed -e 's/[[:space:]]\{1,\}#.*$//' "$1" \
    | awk -F': *' '
        /^[[:space:]]*-?[[:space:]]*hostPath:/      { host = $2 }
        /^[[:space:]]*-?[[:space:]]*containerPath:/ { container = $2 }
        host != "" && container != "" {
          print container "\t" host
          host = ""
          container = ""
        }
      ' \
    | sed -e 's/"//g' -e "s/'//g"
}


# Render the cluster config template into $SPACE_KIND_CONFIG (or $2).
kind_render_cluster_config() {
  kind_host_paths__require SPACE_VENDOR_ROOT SPACE_KIND_DATA_ROOT
  local template="$1" dest="${2:-$SPACE_KIND_CONFIG}"
  [[ -n "$dest" ]] || kind_host_paths__die "SPACE_KIND_CONFIG is unset and HOME is not set to derive its default"
  [[ -f "$template" ]] || kind_host_paths__die "cluster config template not found: $template"
  mkdir -p "$(dirname "$dest")" \
    || kind_host_paths__die "cannot create $(dirname "$dest") for the rendered cluster config; if $(dirname "$(dirname "$dest")") is root-owned, chown it to $(id -un)"
  # A placeholder the substitutions below do not cover would reach
  # `kind create` verbatim and be bind-mounted as a directory named after
  # itself. Checked on the *template*, before substitution: scanning the
  # rendered output blamed the template for a root that legitimately contains
  # `${`, the same misdiagnosis kind_host_paths__sed_escape exists to avoid.
  # Comments are skipped, since the template documents its own knobs.
  local unknown
  unknown="$(sed 's/[[:space:]]*#.*$//' "$template" \
    | grep -oE '\$\{[A-Za-z_][A-Za-z0-9_]*\}' \
    | sort -u \
    | grep -vxE '\$\{SPACE_KIND_DATA_ROOT\}|\$\{SPACE_VENDOR_ROOT\}|\$\{SPACE_KIND_NODE_IMAGE\}|\$\{SPACE_KIND_BASE_NODE_IMAGE\}' || true)"
  if [[ -n "$unknown" ]]; then
    kind_host_paths__die "unknown placeholder(s) in $template: $(printf '%s' "$unknown" | tr '\n' ' ')— add them to kind_render_cluster_config"
  fi

  # A state dir can be root-owned, and a bare redirection failure
  # here would report a path with no way to act on it.
  local data_root vendor_root node_image base_image name value
  data_root="$(kind_host_paths__sed_escape "$SPACE_KIND_DATA_ROOT")"
  vendor_root="$(kind_host_paths__sed_escape "$SPACE_VENDOR_ROOT")"
  # Image references, not paths: resolved here rather than at source time so
  # sourcing stays free of a subprocess, and validated as one YAML scalar.
  node_image="${SPACE_KIND_NODE_IMAGE:-$(bash "$KIND_HOST_PATHS_NODE_IMAGE_SCRIPT" ref)}"
  base_image="${SPACE_KIND_BASE_NODE_IMAGE:-$(bash "$KIND_HOST_PATHS_NODE_IMAGE_SCRIPT" base)}"
  for name in node_image base_image; do
    value="${!name}"
    [[ "$value" =~ ^[A-Za-z0-9][A-Za-z0-9._/:@-]*$ ]] \
      || kind_host_paths__die "$name is not an image reference: '$value'"
  done
  # PID-suffixed: $SPACE_KIND_CONFIG defaults to a per-user path, not a
  # per-worktree one, so two bring-ups in sibling worktrees would otherwise
  # write the same temp file. The `mv` below is atomic, so the loser of that
  # race still leaves a complete config rather than a torn one.
  local partial="$dest.$$.partial"
  sed \
    -e "s|\${SPACE_KIND_DATA_ROOT}|$data_root|g" \
    -e "s|\${SPACE_VENDOR_ROOT}|$vendor_root|g" \
    -e "s|\${SPACE_KIND_NODE_IMAGE}|$node_image|g" \
    -e "s|\${SPACE_KIND_BASE_NODE_IMAGE}|$base_image|g" \
    "$template" >"$partial" \
    || kind_host_paths__die "cannot write $partial; if $(dirname "$dest") is root-owned, chown it to $(id -un)"
  mv "$partial" "$dest"
}

# Refuse a cluster that already exists but bind-mounts different host paths
# than the config now renders.
#
# Docker fixes a container's bind mounts when it creates the container, and
# `kind create` is the only thing that reads the config — so a cluster created
# before these paths were parameterised keeps mounting the old literals for its
# whole life. Without this check the repair looks complete and isn't: the tree
# gets staged at the resolved root, the preflight above passes because that
# root now exists, `kind create` is skipped because the cluster is there, and
# the workers go on reading an empty /space/vendor. Recreating the cluster is
# the only remedy, so say that.
#
# Both roots are checked, not just the vendor tree. A changed
# SPACE_KIND_DATA_ROOT that this ignored would leave the nodes on the old
# containerd storage — the documented knob silently doing nothing, which is
# the failure this whole library exists to remove.
#
# Silent when the paths agree, which is every devbox: `$HOME` is /home/dev
# there, so the rendered mounts equal the literals the live cluster was
# created with.
kind_assert_live_cluster_mounts() {
  kind_host_paths__require SPACE_VENDOR_ROOT SPACE_KIND_DATA_ROOT
  local cluster="$1" config="${2:-$SPACE_KIND_CONFIG}"
  command -v docker >/dev/null 2>&1 || return 0
  [[ -f "$config" ]] || kind_host_paths__die "rendered cluster config not found: $config (render it first)"

  local -a drifted=()
  local node dest src
  # Newline-separated list of every destination the live cluster mounts, so
  # the count comparison below can cover destinations this switch has no
  # special arm for — /dev/kvm was ignored by a hardcoded pair.
  local live_dests=""
  # Only destinations this config declares get judged. The provider adds binds
  # of its own — kind ships /dev/mapper and /lib/modules — and asserting
  # anything about those would report drift whose printed remedy, recreating
  # the cluster, cannot fix: the provider adds them again on the next create.
  local declared_dests
  declared_dests="$(kind_config_mounts "$config" | cut -f1 | sort -u)"
  while read -r node; do
    [[ -n "$node" ]] || continue
    while read -r dest src; do
      [[ -n "$dest" ]] || continue
      printf '%s\n' "$declared_dests" | grep -qFx "$dest" || continue
      live_dests+="$dest"$'\n'
      case "$dest" in
        /space/vendor)
          # RENDERED space: $src is what Docker was given and reports back.
          [[ "$src" == "$SPACE_VENDOR_ROOT" ]] \
            || drifted+=("$node: /space/vendor <- $src (renders $SPACE_VENDOR_ROOT)")
          ;;
        /var/lib/containerd)
          # Per-node subdirectory, so the root is a prefix rather than equal.
          case "$src" in
            "$SPACE_KIND_DATA_ROOT"/*) ;;
            *) drifted+=("$node: /var/lib/containerd <- $src (renders under $SPACE_KIND_DATA_ROOT)") ;;
          esac
          ;;
        /dev/*)
          # A device is mounted from itself, so the config's own source is the
          # expected value.
          [[ "$src" == "$dest" ]] \
            || drifted+=("$node: $dest <- $src (renders $dest)")
          ;;
      esac
    done < <(docker inspect "$node" \
      --format '{{range .Mounts}}{{.Destination}} {{.Source}}{{"\n"}}{{end}}' 2>/dev/null)
  done < <(docker ps -a --format '{{.Names}}' \
    --filter "label=io.x-k8s.kind.cluster=$cluster" 2>/dev/null)

  # The loop above can only disagree with a mount that exists, so a same-named
  # cluster created without this config — no extraMounts at all — passes it
  # untouched. bootstrap.sh does fail eventually, but two minutes later and
  # blaming node labels: `apply_node_labels` waits for pools that never appear.
  # Count what the config asks for instead: a per-destination count is what
  # makes a wholly missing mount visible, and it covers every destination the
  # config declares, so adding a mount to the template extends this
  # automatically. Hardcoding the pair silently exempted /dev/kvm: a cluster
  # built without it passed, bootstrap skipped creation, and the node-manager
  # pods could not start because the chart requires that path as a CharDevice.
  #
  # What a count does not catch: the right number of mounts distributed over
  # the wrong nodes. kind names workers from the config's node order
  # (`worker`, `worker2`, … for nodes 2..N), which is why install-shim.sh can
  # key off `worker3` for infra; a per-node comparison is therefore possible
  # and would be stricter. It isn't done here, so a pre-existing cluster whose
  # node order differs from the template's passes this check and fails later
  # at the node-service rollout instead.
  local dest_expected dest_live
  while read -r dest; do
    [[ -n "$dest" ]] || continue
    dest_expected="$(kind_config_mounts "$config" | cut -f1 | grep -cFx "$dest" || true)"
    dest_live="$(printf '%s' "$live_dests" | grep -cFx "$dest" || true)"
    (( dest_live >= dest_expected )) \
      || drifted+=("cluster has $dest_live of $dest_expected expected $dest mounts")
  done < <(kind_config_mounts "$config" | cut -f1 | sort -u)

  if (( ${#drifted[@]} > 0 )); then
    printf '[kind-host-paths] ERROR: kind cluster %s was created against different host paths:\n' "$cluster" >&2
    printf '       %s\n' "${drifted[@]}" >&2
    printf '       Bind mounts are fixed when the node container is created, so this\n' >&2
    printf '       cluster cannot pick up the new paths. Recreate it once:\n' >&2
    printf '           kind delete cluster --name %s\n' "$cluster" >&2
    printf '       then re-run this script. A delete leaves the containerd image\n' >&2
    printf '       caches on disk, so the rebuild does not re-pull.\n' >&2
    exit 1
  fi
}

kind_prepare_host_mounts() {
  kind_host_paths__require SPACE_VENDOR_ROOT SPACE_KIND_DATA_ROOT
  local config="${1:-$SPACE_KIND_CONFIG}"
  [[ -f "$config" ]] || kind_host_paths__die "rendered cluster config not found: $config (render it first)"

  local container host
  local -a missing=()
  local -a not_a_device=()
  local -a unstaged=()
  while IFS="$(printf '\t')" read -r container host; do
    [[ -n "$host" ]] || continue
    kind_host_paths__assert_mountable "hostPath in $config" "$host"
    # Keyed on where the mount lands inside the node, not on where its source
    # sits on the host. A source-prefix test looked equivalent and was not: a
    # SPACE_VENDOR_ROOT nested under SPACE_KIND_DATA_ROOT — `/mnt/kind` and
    # `/mnt/kind/vendor`, a reasonable "everything on the big disk" layout —
    # matched the containerd arm, so the vendor tree was created empty and the
    # workers mounted it. That is the defect this library exists to prevent,
    # re-entered through its own two knobs.
    case "$container" in
      /var/lib/containerd)
        install -d -m 0755 "$host" \
          || kind_host_paths__die "cannot create containerd storage dir $host"
        ;;
      /dev/*)
        # `-e` is not enough for a device. Docker answers a missing bind
        # source by creating a *directory* — the behaviour this whole library
        # exists to defend against — so a bring-up on a host without KVM is
        # itself how /dev/kvm becomes a directory. That directory then passes
        # an existence test forever, while the node-manager chart's
        # `type: CharDevice` (charts/space-node-manager-local/templates/daemonset.yaml)
        # makes kubelet refuse the volume and the pod never starts.
        if [[ ! -e "$host" ]]; then
          missing+=("$host")
        elif [[ ! -c "$host" && ! -b "$host" ]]; then
          not_a_device+=("$host")
        fi
        ;;
      /space/vendor)
        # Existence is not the property that matters. The message below names
        # the path, and the obvious response to "does not exist" is `mkdir -p`
        # — which an existence test would then accept, putting an empty
        # /space/vendor back on the workers. That is the defect this library
        # exists to remove, so check the binary the node will execute.
        if [[ ! -e "$host" ]]; then
          missing+=("$host")
        elif [[ ! -x "$host/$KIND_HOST_PATHS_VENDOR_PROBE" ]]; then
          unstaged+=("$host")
        fi
        ;;
      *)
        [[ -e "$host" ]] || missing+=("$host")
        ;;
    esac
  done < <(kind_config_mounts "$config")

  if (( ${#missing[@]} > 0 )); then
    printf '[kind-host-paths] ERROR: %s bind-mounts host paths that do not exist:\n' "$config" >&2
    # One remedy per missing path. Printing both unconditionally told a host
    # with no KVM to stage a vendor tree, and a host with an unstaged tree
    # that it cannot run Firecracker.
    while read -r host; do
      printf '       %s\n' "$host" >&2
      case "$host" in
        "$SPACE_VENDOR_ROOT" | "$SPACE_VENDOR_ROOT"/*)
          printf '         the Firecracker vendor tree (SPACE_VENDOR_ROOT). Stage it with\n' >&2
          printf '         `bash infra/space/kind/scripts/install-fc-vendor.sh`.\n' >&2
          ;;
        /dev/*)
          printf '         a host device. Without it this host cannot run Firecracker.\n' >&2
          ;;
      esac
    done < <(kind_host_paths__unique "${missing[@]}")
    exit 1
  fi

  if (( ${#unstaged[@]} > 0 )); then
    printf '[kind-host-paths] ERROR: %s mounts a vendor tree with no Firecracker binary:\n' "$config" >&2
    while read -r host; do
      printf '       %s exists but has no %s\n' "$host" "$KIND_HOST_PATHS_VENDOR_PROBE" >&2
    done < <(kind_host_paths__unique "${unstaged[@]}")
    printf '       Creating the directory is not enough — the workers would mount it\n' >&2
    printf '       empty and the first sandbox would fail as if Firecracker were\n' >&2
    printf '       broken. Stage it with\n' >&2
    printf '         `bash infra/space/kind/scripts/install-fc-vendor.sh`.\n' >&2
    exit 1
  fi

  if (( ${#not_a_device[@]} > 0 )); then
    printf '[kind-host-paths] ERROR: %s bind-mounts device paths that are not devices:\n' "$config" >&2
    while read -r host; do
      printf '       %s is a %s\n' "$host" \
        "$([[ -d "$host" ]] && printf directory || printf 'regular file')" >&2
    done < <(kind_host_paths__unique "${not_a_device[@]}")
    printf '       A previous bring-up on a host without the device leaves a\n' >&2
    printf '       directory here, because Docker creates a missing bind source.\n' >&2
    printf '       Remove it; if the device never appears, this host cannot run\n' >&2
    printf '       Firecracker and the kind harness is not usable on it.\n' >&2
    exit 1
  fi
}

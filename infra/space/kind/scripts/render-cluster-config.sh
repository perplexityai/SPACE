#!/usr/bin/env bash
# render-cluster-config.sh — render the `space-local` kind cluster config for
# this host and print the path of a copy private to this run.
#
# The front door for a hand-run `kind create`. Two steps, and the assignment
# matters: inline as `--config "$(...)"` a failure here is unchecked, and
# `kind create --config ""` does not error — it builds a default one-node
# cluster with no extraMounts and no labels, which is the silent-success
# failure this whole library exists to remove. An assignment keeps the exit
# status, so `&&` stops before kind runs.
#
#   config="$(bash infra/space/kind/scripts/render-cluster-config.sh)" \
#     && kind create cluster --config "$config"
#
# `kind create` expands nothing, so the config it reads is generated rather
# than checked in: see `lib/kind-host-paths.sh` for the roots and why. This
# also creates the per-node containerd-storage directories and refuses a
# missing vendor tree or /dev/kvm, because Docker's answer to a missing
# bind-mount source is an empty directory rather than an error. Once that
# passes, it makes the SPACE Kind node image the config names available locally
# (`kind-node-image.sh ensure`: local, else GHCR, else a local build), so
# `kind create` never falls back to pulling a tag that was never published.
#
# `bootstrap.sh` and the smoke suites' `scripts/tests/lib/bootstrap-kind-stack.sh`
# source the library themselves and do not need this wrapper.
#
# Env knobs: SPACE_VENDOR_ROOT, SPACE_KIND_DATA_ROOT, SPACE_KIND_CONFIG —
# all documented in `lib/kind-host-paths.sh`.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/kind-host-paths.sh
source "$SCRIPT_DIR/lib/kind-host-paths.sh"

# `$SPACE_KIND_CONFIG` is per-user, not per-run, and this script hands its
# output to a `kind create` that runs after the script has exited. Those two
# facts together need both halves of what follows:
#
#   - What gets PRINTED is a copy private to this run. Publishing atomically
#     is not enough by itself, because atomicity only rules out reading a
#     half-written file; a second run can still replace the contents of a
#     shared path between this one printing it and the operator's `kind
#     create` reading it, and the first caller then builds a cluster on the
#     second's host roots, silently, with every mount it declares present.
#   - What gets PUBLISHED is that copy, renamed into place only after the
#     preflight passes. An operator who hits a missing vendor tree, fixes it,
#     and reuses the path from their scrollback must not find the render that
#     preceded the fix. Publishing by rename rather than deleting on failure
#     also leaves a sibling worktree's `bootstrap.sh` config alone — deleting
#     it was the previous approach and could take a config that run had
#     rendered and not yet used.
#
# `bootstrap.sh` renders and publishes separately because it stages the vendor
# tree between its render and its preflight, so its config has to exist across
# that step. This wrapper does both in one go, so publishing last costs it
# nothing.
mkdir -p "$(dirname "$SPACE_KIND_CONFIG")"
# In $TMPDIR because the caller reads it after this script is gone, so nothing
# here can clean it up on the success path — $TMPDIR is reaped, the published
# directory is not, and a stale run file sitting next to the published config
# is exactly the thing an operator would paste by mistake.
run_config="$(mktemp "${TMPDIR:-/tmp}/kind-cluster-run.XXXXXX")"
# Staged for publication in the destination's own directory, not $TMPDIR: `mv`
# is atomic only within a filesystem and degrades to copy-then-unlink across
# one, which would reintroduce the torn read. Writing that directory is
# required either way.
staged="$(mktemp "$SPACE_KIND_CONFIG.XXXXXX.partial")"
trap 'rm -f "$staged" "$run_config"' EXIT
kind_render_cluster_config "$SCRIPT_DIR/../kind-cluster.yaml.tmpl" "$run_config"
kind_prepare_host_mounts "$run_config"
# The reference the render used, made present. stdout is reserved for the path.
bash "$SCRIPT_DIR/kind-node-image.sh" ensure >/dev/null
# Published only once it has verified, so an operator reusing the documented
# path from their scrollback never picks up a render that failed its preflight.
cp -f "$run_config" "$staged"
mv "$staged" "$SPACE_KIND_CONFIG"
# The run file has to outlive this script; only the staging copy is reaped.
trap 'rm -f "$staged"' EXIT

# Only the path goes to stdout, so the command substitution above works.
printf '%s\n' "$run_config"

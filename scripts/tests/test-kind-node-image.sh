#!/usr/bin/env bash
# The SPACE Kind node image's tag, the configs that name it, and the check that
# refuses a cluster on another node image. Hermetic: docker, kind and kubectl
# are stubs, and no image is built.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
image_script="$repo_root/infra/space/kind/scripts/kind-node-image.sh"
check_script="$repo_root/infra/space/kind/scripts/check-kind-node-image.sh"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

# ── The tag is a function of the listed inputs, and only of them ─────────────
ref="$(bash "$image_script" ref)"
[[ "$ref" =~ ^ghcr\.io/perplexityai/space-kind-node:[0-9a-f]{16}$ ]] || fail "unexpected reference: $ref"
[[ "$(bash "$image_script" ref)" == "$ref" ]] || fail "the reference changed between two runs"

# A copy of exactly the inputs and the script gives the same tag; editing an
# input changes it; editing anything else does not.
copy="$work/tree"
while IFS= read -r input; do
  install -D -m 0644 "$repo_root/$input" "$copy/$input"
done < <(bash "$image_script" inputs)
install -D -m 0755 "$image_script" "$copy/infra/space/kind/scripts/kind-node-image.sh"
copy_ref() { bash "$copy/infra/space/kind/scripts/kind-node-image.sh" ref; }
[[ "$(copy_ref)" == "$ref" ]] || fail "the tag depends on something besides the listed inputs"
echo "unrelated" >"$copy/README.md"
[[ "$(copy_ref)" == "$ref" ]] || fail "an unlisted file changed the tag"
while IFS= read -r input; do
  cp "$copy/$input" "$work/saved"
  echo "# changed" >>"$copy/$input"
  [[ "$(copy_ref)" != "$ref" ]] || fail "editing $input left the tag unchanged"
  cp "$work/saved" "$copy/$input"
done < <(bash "$image_script" inputs)
[[ "$(copy_ref)" == "$ref" ]] || fail "restoring the inputs did not restore the tag"

# Every file the Dockerfile copies is an input, so none can change the image
# without changing the tag.
inputs="$(bash "$image_script" inputs)"
while read -r source; do
  if [[ "$source" == */ ]]; then
    for file in "$repo_root/$source"*; do
      grep -qxF "${file#"$repo_root/"}" <<<"$inputs" \
        || fail "the Dockerfile copies ${file#"$repo_root/"}, which the tag does not cover"
    done
  else
    grep -qxF "$source" <<<"$inputs" || fail "the Dockerfile copies $source, which the tag does not cover"
  fi
done < <(awk 'toupper($1) == "COPY" { for (i = 2; i < NF; i++) if ($i !~ /^--/) print $i }' \
  "$repo_root/infra/space/kind/node/Dockerfile")

# ── One base for every node, and the mirror carries it ───────────────────────
base="$(bash "$image_script" base)"
[[ "$base" =~ ^kindest/node:v[0-9.]+@sha256:[0-9a-f]{64}$ ]] || fail "the base is not pinned by digest: $base"
bash "$repo_root/ci/scripts/mirror-images.sh" listed "$base" \
  || fail "$base is not in ci/mirrored-images.txt"

# In GitHub CI the other nodes name the base by the digest the mirror holds,
# the one a pull from the mirror resolves under, but only while the mirror's
# record of that pairing names it; otherwise, and outside GitHub CI, by the
# base's own pin. The list is synthetic, so the mirrored digest is known here.
mirror_digest="sha256:$(printf 'b%.0s' {1..64})"
printf '%s %s ghcr.io/example/mirror:kindest-node\n' "$base" "$mirror_digest" >"$work/mirrored-images.txt"
mirrored_base="${base%%@*}@$mirror_digest"
export SPACE_MIRRORED_IMAGES="$work/mirrored-images.txt"
mirror_ref="ghcr.io/example/mirror@$mirror_digest"
source_ref="${base%%:*}@$mirror_digest"
index_ref="${base%%:*}@${base##*@}"
# The base is pinned by digest, so jobs take it through the tag publish wrote
# when it verified the pairing (mirror-images.sh).
pin_ref="ghcr.io/example/mirror:pin-$(printf '%s %s' "$base" "$mirror_digest" | sha256sum | cut -c1-40)"
stale_digest="sha256:$(printf 'e%.0s' {1..64})"

# A docker that has the SPACE node image, pulls what FAKE_PULLABLE lists (a
# `tag=digest-ref` entry pulls a tag that resolves to that digest), reads the
# pairing records FAKE_RECORDS lists (`record=digest`), and resolves a tag
# with a digest as the containerd image store does: to the image tagged with
# that name, if it was pulled by that digest.
mkdir -p "$work/stub-bin"
cat >"$work/stub-bin/docker" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"$FAKE_CALLS"
case "$1 $2" in
  "image inspect") [[ "$3" == "$FAKE_NODE_REF" ]] || grep -qxF -- "$3" "$FAKE_LOCAL" ;;
  "pull "*)
    for entry in ${FAKE_PULLABLE:-}; do
      [[ "${entry%%=*}" == "${*: -1}" ]] || continue
      printf '%s\n' "${entry%%=*}" "${entry#*=}" >>"$FAKE_LOCAL"
      exit 0
    done
    echo "unreachable: ${*: -1}" >&2
    exit 1
    ;;
  "tag "*) printf '%s\n%s@%s\n' "$3" "$3" "${2#*@}" >>"$FAKE_LOCAL" ;;
  "buildx imagetools")
    for entry in ${FAKE_RECORDS:-}; do
      [[ "${entry%%=*}" == "${*: -1}" ]] || continue
      printf '{"digest":"%s"}\n' "${entry#*=}"
      exit 0
    done
    echo "manifest unknown: ${*: -1}" >&2
    exit 1
    ;;
  *) echo "unexpected docker call: $*" >&2; exit 99 ;;
esac
EOF
chmod +x "$work/stub-bin/docker"
: >"$work/local"
# Runs its arguments (settings, then the command) under env with the stub
# docker; an empty SPACE_CI_IMAGE_MIRROR is unset as far as the scripts go.
with_stub() {
  env -u GHCR_TOKEN -u SPACE_KIND_NODE_IMAGE -u SPACE_KIND_BASE_NODE_IMAGE \
    PATH="$work/stub-bin:$PATH" FAKE_CALLS="$work/calls" FAKE_LOCAL="$work/local" \
    FAKE_NODE_REF="$ref" SPACE_DOCKER_PULL_ATTEMPTS=1 "$@"
}
cluster_base() { with_stub "$@" bash "$image_script" cluster-base 2>/dev/null; }

[[ "$(cluster_base SPACE_CI_IMAGE_MIRROR= FAKE_RECORDS="$pin_ref=$mirror_digest")" == "$base" ]] \
  || fail "outside GitHub CI the cluster does not run the pinned base"
[[ "$(cluster_base SPACE_CI_IMAGE_MIRROR=ghcr FAKE_RECORDS="$pin_ref=$mirror_digest")" == "$mirrored_base" ]] \
  || fail "in GitHub CI the cluster does not name the verified mirrored digest of the base"
# An index bumped without its digest has no record: the cluster keeps the index
# rather than run the stale listed digest.
[[ "$(cluster_base SPACE_CI_IMAGE_MIRROR=ghcr FAKE_RECORDS=)" == "$base" ]] \
  || fail "a pairing without a record put the cluster on the listed digest instead of the pinned index"
[[ "$(cluster_base SPACE_CI_IMAGE_MIRROR=ghcr FAKE_RECORDS="$pin_ref=$stale_digest")" == "$base" ]] \
  || fail "a record that names another digest put the cluster on the listed digest"
# A malformed line after the base's: the list is rejected, and the config
# still gets one reference, the base's own.
cp "$work/mirrored-images.txt" "$work/malformed-images.txt"
echo "registry.example/unpinned sha256:short ghcr.io/example/mirror" >>"$work/malformed-images.txt"
[[ "$(cluster_base SPACE_MIRRORED_IMAGES="$work/malformed-images.txt" SPACE_CI_IMAGE_MIRROR=ghcr \
  FAKE_RECORDS="$pin_ref=$mirror_digest")" == "$base" ]] \
  || fail "a malformed list did not leave the cluster on exactly the pinned base"

# The rendered space-local config: SPACE nodes on the image, the rest on its
# base. Arguments after the expected base are settings for with_stub.
rendered="$work/kind-cluster.yaml"
check_render() {
  local base_want="$1" images role pool image want
  shift
  # shellcheck disable=SC2016 # expanded by the inner shell
  with_stub "$@" HOME="$work/home" bash -c \
    'source "$1"; kind_render_cluster_config "$2" "$3"' _ \
    "$repo_root/infra/space/kind/scripts/lib/kind-host-paths.sh" \
    "$repo_root/infra/space/kind/kind-cluster.yaml.tmpl" "$rendered"
  images="$(awk '
    /^  - role:/ { if (role != "") print role, pool, image; role = $3; pool = "-"; image = "-" }
    /^    image:/ { image = $2 }
    /space.pplx.net\/node-pool:/ { pool = $2 }
    END { print role, pool, image }' "$rendered")"
  while read -r role pool image; do
    case "$pool" in
      sandbox | builder) want="$ref" ;;
      *) want="$base_want" ;;
    esac
    [[ "$image" == "$want" ]] || fail "the $role node in pool $pool renders image $image, not $want"
  done <<<"$images"
  [[ "$(grep -c "image: $ref\$" "$rendered")" == 4 ]] || fail "expected four SPACE nodes on $ref"
}
check_render "$base" SPACE_CI_IMAGE_MIRROR=
check_render "$mirrored_base" SPACE_CI_IMAGE_MIRROR=ghcr FAKE_RECORDS="$pin_ref=$mirror_digest"
check_render "$base" SPACE_CI_IMAGE_MIRROR=ghcr FAKE_RECORDS=

# ── ensure takes the cluster's base from the mirror in GitHub CI ─────────────
run_ensure() {
  : >"$work/calls"
  : >"$work/local"
  with_stub "$@" bash "$image_script" ensure >"$work/out" 2>"$work/err"
}
run_ensure SPACE_CI_IMAGE_MIRROR=ghcr FAKE_RECORDS="$pin_ref=$mirror_digest" FAKE_PULLABLE="$pin_ref=$mirror_ref" \
  || fail "ensure from the mirror failed: $(cat "$work/err")"
[[ "$(cat "$work/out")" == "$ref" ]] || fail "ensure printed more than the node image: $(cat "$work/out")"
grep -qxF "tag $mirror_ref ${base%%@*}" "$work/calls" || fail "ensure did not take the base from the mirror"
grep -qxF -- "$mirrored_base" "$work/local" \
  || fail "the mirrored base does not resolve under the name the cluster config gives it"
grep -q "^pull .*$source_ref" "$work/calls" && fail "ensure pulled the base from its source although the mirror had it"
# The stale-digest regression: an index bumped without its digest. Nothing
# runs the listed digest; ensure takes the pinned index, which is what the
# config then names and finds locally.
run_ensure SPACE_CI_IMAGE_MIRROR=ghcr FAKE_RECORDS= FAKE_PULLABLE="$mirror_ref $source_ref $index_ref" \
  || fail "ensure without a verified pairing failed: $(cat "$work/err")"
grep -qxF "tag $mirror_ref ${base%%@*}" "$work/calls" && fail "ensure took a digest publish never verified from the mirror"
grep -q "^pull .*$source_ref" "$work/calls" && fail "ensure pulled the unverified listed digest from the source"
grep -qxF "tag $index_ref ${base%%@*}" "$work/calls" || fail "an unverified pairing did not fall back to the pinned index"
grep -qxF -- "$base" "$work/local" || fail "the pinned index does not resolve under the name the cluster config gives it"
run_ensure SPACE_CI_IMAGE_MIRROR=ghcr FAKE_RECORDS="$pin_ref=$mirror_digest" FAKE_PULLABLE="$index_ref" \
  || fail "ensure with the mirror down failed: $(cat "$work/err")"
grep -qxF "tag $index_ref ${base%%@*}" "$work/calls" || fail "ensure did not fall back to the pinned index"
run_ensure SPACE_CI_IMAGE_MIRROR=ghcr FAKE_RECORDS= FAKE_PULLABLE= \
  || fail "ensure failed although kind create can still pull the base: $(cat "$work/err")"
[[ "$(cat "$work/out")" == "$ref" ]] || fail "ensure printed more than the node image: $(cat "$work/out")"
grep -q "kind create pulls it" "$work/err" || fail "ensure did not say that kind create pulls the base"
run_ensure SPACE_CI_IMAGE_MIRROR= FAKE_RECORDS="$pin_ref=$mirror_digest" FAKE_PULLABLE="$pin_ref=$mirror_ref" \
  || fail "ensure outside GitHub CI failed"
grep -q "^pull " "$work/calls" && fail "ensure pulled the base outside GitHub CI"
unset SPACE_MIRRORED_IMAGES

# ── check-kind-node-image.sh ─────────────────────────────────────────────────
# Stubs: one cluster with two SPACE nodes whose image labels come from
# TEST_TITLE / TEST_TAG.
mkdir -p "$work/bin"
cat >"$work/bin/kind" <<'EOF'
#!/usr/bin/env bash
[[ "$*" == "get clusters" ]] && [[ -n "${TEST_CLUSTER:-}" ]] && echo "$TEST_CLUSTER"
exit 0
EOF
cat >"$work/bin/kubectl" <<'EOF'
#!/usr/bin/env bash
printf 'space-local-worker\nspace-local-worker4\n'
EOF
cat >"$work/bin/docker" <<'EOF'
#!/usr/bin/env bash
case "$*" in
  *org.opencontainers.image.title*) echo "${TEST_TITLE} ${TEST_TAG}" ;;
  *Config.Image*) echo "kindest/node:v0.0.0" ;;
  *) exit 1 ;;
esac
EOF
chmod +x "$work/bin/"*
run_check() { env PATH="$work/bin:$PATH" "$@" bash "$check_script" space-local >"$work/out" 2>&1; }

run_check TEST_CLUSTER= TEST_TITLE= TEST_TAG= || fail "a missing cluster was refused: $(cat "$work/out")"
run_check TEST_CLUSTER=space-local TEST_TITLE=space-kind-node TEST_TAG="${ref##*:}" \
  || fail "a cluster on this tree's image was refused: $(cat "$work/out")"
if run_check TEST_CLUSTER=space-local TEST_TITLE= TEST_TAG=; then
  fail "a cluster from before the node image was accepted"
fi
grep -q "kind delete cluster --name space-local" "$work/out" || fail "the refusal does not name the remedy"
grep -q "not the SPACE Kind node image" "$work/out" || fail "the refusal does not say why"
if run_check TEST_CLUSTER=space-local TEST_TITLE=space-kind-node TEST_TAG=0123456789abcdef; then
  fail "a cluster on an older build of the node image was accepted"
fi
grep -q "runs SPACE Kind node image 0123456789abcdef; this tree builds ${ref##*:}" "$work/out" \
  || fail "the stale-image refusal does not name both tags: $(cat "$work/out")"

echo "ok: the Kind node image tag covers exactly its inputs, every config names it or its base, CI takes the base from the mirror, and clusters on another node image are refused with the remedy"

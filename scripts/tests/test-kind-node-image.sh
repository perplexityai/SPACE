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
static="$repo_root/infra/space/kind/kind-cluster-3486.yaml"
[[ "$(grep -c "image: $base\$" "$static")" == 2 ]] \
  || fail "kind-cluster-3486.yaml must run $base on its control-plane and infra nodes"
grep -q 'image: space-kind-node:local$' "$static" \
  || fail "kind-cluster-3486.yaml must run the SPACE Kind node image on its sandbox worker"

# The rendered space-local config: SPACE nodes on the image, the rest on its base.
rendered="$work/kind-cluster.yaml"
# shellcheck disable=SC2016 # expanded by the inner shell
env -u SPACE_KIND_NODE_IMAGE -u SPACE_KIND_BASE_NODE_IMAGE HOME="$work/home" bash -c \
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
    *) want="$base" ;;
  esac
  [[ "$image" == "$want" ]] || fail "the $role node in pool $pool renders image $image, not $want"
done <<<"$images"
[[ "$(grep -c "image: $ref\$" "$rendered")" == 4 ]] || fail "expected four SPACE nodes on $ref"

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

echo "ok: the Kind node image tag covers exactly its inputs, every config names it or its base, and clusters on another node image are refused with the remedy"

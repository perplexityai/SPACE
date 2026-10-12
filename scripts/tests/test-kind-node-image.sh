#!/usr/bin/env bash
# The SPACE Kind node image's tag, the Tilt resource that watches its inputs,
# the loop-device wiring its node layer bakes in, the configs that name it, and
# the check that refuses a cluster on another node image. Hermetic: docker, kind
# and kubectl are stubs, and no image is built.
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

# "<repository source> <image path>" for every file the Dockerfile copies; a
# directory COPY means every file under it, at any depth.
dockerfile="$repo_root/infra/space/kind/node/Dockerfile"
copied="$work/copied"
while read -r -a words; do
  dest="${words[${#words[@]} - 1]}"
  for source in "${words[@]:0:${#words[@]}-1}"; do
    if [[ -d "$repo_root/$source" ]]; then
      while IFS= read -r file; do
        printf '%s %s\n' "$file" "${dest%/}/${file#"${source%/}/"}"
      done < <(cd "$repo_root" && find "${source%/}" -type f | sort)
    elif [[ $dest == */ ]]; then
      printf '%s %s%s\n' "$source" "$dest" "${source##*/}"
    else
      printf '%s %s\n' "$source" "$dest"
    fi
  done
done < <(awk 'toupper($1) == "COPY" {
  line = ""; for (i = 2; i <= NF; i++) if ($i !~ /^--/) line = line " " $i; print line }' "$dockerfile") \
  >"$copied"
[[ -s $copied ]] || fail "found no COPY in the Dockerfile"
image_path_of() { awk -v source="$1" '$1 == source { print $2 }' "$copied"; }
source_of() { awk -v path="$1" '$2 == path { print $1 }' "$copied"; }

# The Dockerfile and every file it copies are inputs, so none can change the
# image without changing the tag.
inputs="$(bash "$image_script" inputs)"
grep -qxF infra/space/kind/node/Dockerfile <<<"$inputs" || fail "the tag does not cover the Dockerfile"
while read -r source _; do
  grep -qxF "$source" <<<"$inputs" || fail "the Dockerfile copies $source, which the tag does not cover"
done <"$copied"

# Tilt re-runs check-kind-node-image when any input changes: each one is a dep
# or under a directory dep.
tilt_deps="$(sed -n '/^    "check-kind-node-image",$/,/^)/p' "$repo_root/Tiltfile" \
  | sed -n '/deps = \[/,/\]/p' | grep -o '"[^"]*"' | tr -d '"')"
[[ -n $tilt_deps ]] || fail "found no deps for Tilt's check-kind-node-image"
while IFS= read -r input; do
  watched=0
  while IFS= read -r dep; do
    [[ $input == "$dep" || $input == "${dep%/}/"* ]] && watched=1
  done <<<"$tilt_deps"
  ((watched)) || fail "Tilt's check-kind-node-image does not watch $input"
done <<<"$inputs"

# ── The node layer's loop wiring ─────────────────────────────────────────────
node_script="$repo_root/infra/space/kind/node/space-kind-node.sh"
loop_lib=scripts/lib/space-loop-devices.sh

# Every library the node script sources from the image is copied there. Paths
# are resolved through the script's NAME=/dir assignments.
declare -A dirs=()
while IFS='=' read -r name value; do
  dirs[$name]="$value"
done < <(sed -nE 's#^[[:space:]]*([A-Za-z_][A-Za-z0-9_]*)="?(/[^"$[:space:]]*)"?[[:space:]]*$#\1=\2#p' "$node_script")
sourced=()
while IFS= read -r path; do
  path="${path//\"/}"
  if [[ $path =~ ^\$\{?([A-Za-z_][A-Za-z0-9_]*)\}?/(.+)$ ]] && [[ -n ${dirs[${BASH_REMATCH[1]}]:-} ]]; then
    path="${dirs[${BASH_REMATCH[1]}]%/}/${BASH_REMATCH[2]}"
  fi
  [[ $path == /* ]] || fail "space-kind-node.sh sources $path, which does not resolve to an image path"
  [[ -n "$(source_of "$path")" ]] || fail "space-kind-node.sh sources $path, which the Dockerfile does not copy"
  sourced+=("$path")
done < <(sed -nE 's/^[[:space:]]*(\.|source)[[:space:]]+([^[:space:];]+).*/\2/p' "$node_script")
loop_lib_path="$(image_path_of "$loop_lib")"
[[ -n $loop_lib_path ]] || fail "the Dockerfile does not copy $loop_lib"
[[ " ${sourced[*]} " == *" $loop_lib_path "* ]] || fail "space-kind-node.sh does not source $loop_lib_path"

# Every space_loop_* function the node script calls is one the library defines.
defined="$(sed -n 's/^\(space_loop_[A-Za-z0-9_]*\)() {.*/\1/p' "$repo_root/$loop_lib")"
called="$(grep -v '^[[:space:]]*#' "$node_script" | sed 's/[[:space:]]#.*//' \
  | grep -o 'space_loop_[A-Za-z0-9_]*' | sort -u)"
grep -qx space_loop_stage_pool <<<"$called" || fail "space-kind-node.sh does not stage the loop pool"
while IFS= read -r fn; do
  grep -qxF "$fn" <<<"$defined" || fail "space-kind-node.sh calls $fn, which $loop_lib does not define"
done <<<"$called"

# space-loop-nodes.service is installed and enabled, runs the staged entrypoint's
# loop-nodes step, and finishes before kubelet and the cache that attaches a
# loop device.
unit_source() {
  local source
  source="$(source_of "/etc/systemd/system/$1")"
  [[ -n $source ]] || fail "the Dockerfile does not install $1"
  printf '%s\n' "$repo_root/$source"
}
unit_values() { sed -n "s/^$2=//p" "$1" | tr ' ' '\n' | sed '/^$/d'; }
enabled="$(awk '{ if (sub(/\\$/, "")) { buf = buf $0 " "; next } print buf $0; buf = "" }' "$dockerfile" \
  | awk 'toupper($1) == "RUN" && /systemctl enable/' | grep -o '[A-Za-z0-9@._-]*\.service')"
grep -qx space-loop-nodes.service <<<"$enabled" || fail "the Dockerfile does not enable space-loop-nodes.service"
loop_unit="$(unit_source space-loop-nodes.service)"
cache_unit="$(unit_source space-cache.service)"
unit_values "$loop_unit" WantedBy | grep -q . || fail "space-loop-nodes.service has no WantedBy=, so enabling it does nothing"
read -r exec_path exec_step _ <<<"$(unit_values "$loop_unit" ExecStart | xargs)"
[[ $exec_path == "$(image_path_of infra/space/kind/node/space-kind-node.sh)" ]] \
  || fail "space-loop-nodes.service runs $exec_path, not the staged space-kind-node.sh"
grep -qE "^[[:space:]]*$exec_step\) loop_nodes ;;" "$node_script" \
  || fail "space-kind-node.sh does not dispatch $exec_step to loop_nodes"
unit_values "$loop_unit" Before | grep -qx kubelet.service \
  || fail "space-loop-nodes.service is not ordered before kubelet.service"
unit_values "$cache_unit" After | grep -qx space-loop-nodes.service \
  || unit_values "$loop_unit" Before | grep -qx space-cache.service \
  || fail "space-cache.service is not ordered after space-loop-nodes.service"

# ── One base for every node, and the mirror carries it ───────────────────────
base="$(bash "$image_script" base)"
[[ "$base" =~ ^kindest/node:v[0-9.]+@sha256:[0-9a-f]{64}$ ]] || fail "the base is not pinned by digest: $base"
bash "$repo_root/ci/scripts/mirror-images.sh" listed "$base" \
  || fail "$base is not in ci/mirrored-images.txt"

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

echo "ok: the Kind node image tag covers exactly its inputs, Tilt watches them, the node layer sources and enables its loop wiring, every config names it or its base, and clusters on another node image are refused with the remedy"

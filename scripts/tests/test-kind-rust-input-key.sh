#!/usr/bin/env bash
# The Rust input key (build-rust-artifacts.sh --input-key) decides when Kind CI
# reuses an artifact image main built from another commit, so a change the
# build reads but the key misses would test stale binaries. On a scratch copy
# of this checkout's tracked files, each case commits one change and compares
# the key with the base commit's:
#
#   1. Changes the Docker build never reads keep the key.
#   2. Changes to what it reads (a crate, Cargo.lock, either Dockerfile, db/,
#      a copied script or recipe, a file mode, a base image pin) change it.
#   3. Uncommitted or untracked inputs, and Dockerfiles that read the context
#      in ways the key cannot list, have no key at all.
#   4. The key reads base pins from the list the build uses.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
repo="$work/repo"
mkdir -p "$repo"
git -C "$repo_root" ls-files -z | (cd "$repo_root" && tar --null -T - -cf -) | tar -xf - -C "$repo"
git init -q -b main "$repo"
git -C "$repo" config user.name test
git -C "$repo" config user.email test@example.com
# No background gc racing the cleanup trap.
git -C "$repo" config gc.auto 0
git -C "$repo" config maintenance.auto false
git -C "$repo" add -A
git -C "$repo" commit -q -m base
base="$(git -C "$repo" rev-parse HEAD)"

key() { bash "$repo/infra/space/kind/scripts/build-rust-artifacts.sh" --input-key; }
base_key="$(key)"
[[ "$base_key" =~ ^[a-f0-9]{64}$ ]] || { echo "FAIL: invalid key '$base_key'" >&2; exit 1; }

status=0
first_file() { git -C "$repo" ls-files -- "$1" | head -1; }
# Commit one change made by "$@" in the scratch checkout, then compare its key.
case_key() {
  local expect="$1" name="$2" got
  shift 2
  git -C "$repo" checkout -q --detach "$base"
  (cd "$repo" && "$@")
  git -C "$repo" add -A
  git -C "$repo" commit -q -m "$name"
  got="$(key)"
  if [[ "$expect" == same && "$got" != "$base_key" ]]; then
    echo "FAIL: $name changed the key" >&2
    status=1
  elif [[ "$expect" == changed && "$got" == "$base_key" ]]; then
    echo "FAIL: $name kept the key" >&2
    status=1
  fi
}
# shellcheck disable=SC2329 # run through case_key and no_key
append() { printf '\n# changed\n' >>"$1"; }
# shellcheck disable=SC2329 # run through case_key
bump_pin() {
  sed -i -E "s|^($1[[:space:]]+sha256:)[a-f0-9]{64}|\\1$(printf '0123456789abcdef%.0s' 1 2 3 4)|" ci/mirrored-images.txt
}

# Smoke Python, charts, the Tiltfile, workflows, a runtime image's Dockerfile,
# the Kind scripts beside the build, and the pin of an image no Rust stage uses.
case_key same "a smoke scenario" append "$(first_file 'space_smoke/scenarios/*.py')"
case_key same "a Kind chart" append "$(first_file 'infra/space/kind/charts/*/values.yaml')"
case_key same "the Tiltfile" append Tiltfile
case_key same "the Kind workflow" append .github/workflows/kind-ci.yml
case_key same "a runtime Dockerfile" append docker/Dockerfile.api
case_key same "the artifact handoff" append infra/space/kind/scripts/rust-artifact-handoff.py
case_key same "a test-only image pin" sed -i 's/postgres-16-alpine/postgres-16-alpine-moved/' ci/mirrored-images.txt

case_key changed "a crate source file" append "$(first_file 'crates/*.rs')"
case_key changed "a crate manifest" append "$(first_file 'crates/*/Cargo.toml')"
case_key changed "Cargo.lock" append Cargo.lock
case_key changed "the toolchain pin" append rust-toolchain.toml
case_key changed "the cargo config" append .cargo/config.toml
case_key changed "a proto file" append "$(first_file 'proto/*.proto')"
case_key changed "a db/ file" append "$(first_file 'db/*')"
case_key changed "the Kind Rust Dockerfile" append docker/Dockerfile.rust-build-kind
case_key changed "the common Rust Dockerfile" append docker/Dockerfile.rust-build-common
case_key changed "the cargo wrapper" append docker/run-cargo-with-sccache.sh
case_key changed "the supervisor build script" append scripts/build-runtime-supervisor.sh
case_key changed "the seed rootfs builder" append templates/firecracker/seed/rootfs-builder.sh
case_key changed "a code-runtime recipe file" append "$(first_file 'templates/firecracker/recipes/code-runtime/*')"
case_key changed "the node identities script" append infra/space/identity/node-identities.sh
case_key changed "the .dockerignore" append .dockerignore
case_key changed "the artifact build script" append infra/space/kind/scripts/build-rust-artifacts.sh
case_key changed "a crate file's mode" chmod +x "$(first_file 'crates/*.rs')"
case_key changed "the Rust base image pin" bump_pin 'public.ecr.aws/docker/library/rust:1-bookworm'
case_key changed "the health probe base pin" bump_pin 'public.ecr.aws/docker/library/debian:bookworm-slim'
# BuildKit prefers a Dockerfile's own ignore file to the root one, and
# attributes can rewrite checked-out bytes anywhere from the root down.
case_key changed "a Kind Dockerfile ignore file" append docker/Dockerfile.rust-build-kind.dockerignore
case_key changed "a common Dockerfile ignore file" append docker/Dockerfile.rust-build-common.dockerignore
case_key changed "a root .gitattributes" append .gitattributes
case_key changed "a .gitattributes above an input" append infra/.gitattributes
case_key changed "a .gitattributes inside an input" append crates/.gitattributes
case_key same "a .gitattributes beside no input" append space_smoke/.gitattributes

# Uncommitted inputs have no key: the build would read bytes no commit names.
no_key() {
  local name="$1"
  shift
  git -C "$repo" checkout -q --detach "$base"
  (cd "$repo" && "$@")
  if key >/dev/null 2>&1; then
    echo "FAIL: $name still had a key" >&2
    status=1
  fi
  git -C "$repo" reset -q --hard "$base"
  git -C "$repo" clean -qfdx
}
no_key "a modified crate file" append "$(first_file 'crates/*.rs')"
no_key "an untracked crate file" touch crates/untracked.rs
no_key "a modified Dockerfile" append docker/Dockerfile.rust-build-kind
no_key "an untracked Dockerfile ignore file" touch docker/Dockerfile.rust-build-kind.dockerignore
no_key "an untracked root .gitattributes" touch .gitattributes

# Context reads the key cannot list refuse a key even when committed.
refused() {
  local name="$1" line="$2"
  git -C "$repo" checkout -q --detach "$base"
  printf '%s\n' "$line" >>"$repo/docker/Dockerfile.rust-build-kind"
  git -C "$repo" commit -qam "$name"
  if key >/dev/null 2>&1; then
    echo "FAIL: a Dockerfile with $name still had a key" >&2
    status=1
  fi
}
refused "ADD" 'ADD extra /extra'
refused "a lowercase add" 'add extra /extra'
refused "a lowercase copy" 'copy extra /extra'
refused "an indented COPY" '  COPY extra /extra'
refused "a continued COPY" "COPY Cargo.toml \\"
refused "a bind mount" 'RUN --mount=type=bind,source=extra,target=/extra true'
refused "an untyped mount" 'RUN --mount=source=extra,target=/extra true'
refused "a cache mount seeded from the context" 'RUN --mount=type=cache,from=.,target=/extra true'
git -C "$repo" checkout -q --detach "$base"

# mirror-images.sh honours SPACE_MIRRORED_IMAGES; the key reads the same list.
list="$work/mirrored-images.txt"
cp "$repo/ci/mirrored-images.txt" "$list"
[[ "$(SPACE_MIRRORED_IMAGES="$list" key)" == "$base_key" ]] \
  || { echo "FAIL: an override with the same pins changed the key" >&2; status=1; }
sed -i -E "s|^(public.ecr.aws/docker/library/rust:1-bookworm[[:space:]]+sha256:)[a-f0-9]{64}|\\1$(printf 'f%.0s' {1..64})|" "$list"
[[ "$(SPACE_MIRRORED_IMAGES="$list" key)" != "$base_key" ]] \
  || { echo "FAIL: an override with another Rust base pin kept the key" >&2; status=1; }

if (( status == 0 )); then
  echo "PASS: the Rust input key follows exactly the Docker build's committed inputs"
fi
exit "$status"

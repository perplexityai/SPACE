#!/usr/bin/env bash
# Exercise the CI handoff at the Docker boundary; never touch the host daemon.
# The checkout is a committed scratch copy of this one's tracked files, so its
# Rust input key exists whatever this worktree has uncommitted.
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
repo="$work/repo"
mkdir -p "$repo" "$work/bin"
git -C "$repo_root" ls-files -z | (cd "$repo_root" && tar --null -T - -cf -) | tar -xf - -C "$repo"
git init -q -b main "$repo"
git -C "$repo" config user.name test
git -C "$repo" config user.email test@example.com
git -C "$repo" config gc.auto 0
git -C "$repo" add -A
git -C "$repo" commit -q -m base
cat >"$work/bin/docker" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
case "$*" in
  'image inspect -f {{index .Config.Labels "space.pplx.net/kind-rust-build"}} space-rust-build-kind:local')
    echo test-hooks ;;
  "image inspect -f {{.Id}} space-rust-build-kind:local")
    [[ -n "$STUB_IMAGE_ID" ]] || exit 1
    echo "$STUB_IMAGE_ID" ;;
  'image inspect -f {{index .Config.Labels "space.pplx.net/kind-rust-input-key"}} space-rust-build-kind:local')
    echo "${STUB_KEY_LABEL:-<no value>}" ;;
  *)
    echo "unexpected Docker operation: $*" >&2
    exit 99 ;;
esac
EOF
chmod +x "$work/bin/docker"
export PATH="$work/bin:$PATH"
export SPACE_TILT_STATE_DIR="$work/state"
export SPACE_KIND_RUST_ARTIFACTS=prebuilt
SPACE_KIND_PREBUILT_RUST_SHA="$(git -C "$repo" rev-parse HEAD)"
export SPACE_KIND_PREBUILT_RUST_SHA
export SPACE_KIND_PREBUILT_RUST_IMAGE_ID=sha256:fixture-image
export STUB_IMAGE_ID="$SPACE_KIND_PREBUILT_RUST_IMAGE_ID"
script="$repo/infra/space/kind/scripts/build-rust-artifacts.sh"
key="$(bash "$script" --input-key)"
other_key="$(printf '0%.0s' {1..64})"

# Tilt's repeated invocations must all reuse the image without building it.
bash "$script"
bash "$script"
# An image main built from another commit, labelled and handed off with this
# checkout's input key, is this checkout's build.
env SPACE_KIND_PREBUILT_RUST_SHA=another-commit SPACE_KIND_PREBUILT_RUST_KEY="$key" \
  STUB_KEY_LABEL="$key" bash "$script"

reject() {
  local expected="$1"; shift
  if "$@" >"$work/output" 2>&1; then
    echo "FAIL: accepted invalid prebuilt artifact" >&2
    exit 1
  fi
  grep -q "$expected" "$work/output" || { cat "$work/output" >&2; exit 1; }
}
reject 'requires an image ID and source SHA' \
  env -u SPACE_KIND_PREBUILT_RUST_IMAGE_ID bash "$script"
reject 'names no input key' \
  env SPACE_KIND_PREBUILT_RUST_SHA=another-commit bash "$script"
reject 'neither does its input key' \
  env SPACE_KIND_PREBUILT_RUST_SHA=another-commit SPACE_KIND_PREBUILT_RUST_KEY="$other_key" \
  STUB_KEY_LABEL="$other_key" bash "$script"
reject 'does not carry the input key' \
  env SPACE_KIND_PREBUILT_RUST_SHA=another-commit SPACE_KIND_PREBUILT_RUST_KEY="$key" \
  STUB_KEY_LABEL="$other_key" bash "$script"
reject 'does not carry the input key' \
  env SPACE_KIND_PREBUILT_RUST_SHA=another-commit SPACE_KIND_PREBUILT_RUST_KEY="$key" bash "$script"
reject 'missing or does not match' \
  env STUB_IMAGE_ID=sha256:another-image bash "$script"
reject 'missing or does not match' \
  env STUB_IMAGE_ID= bash "$script"
# A key match never excuses a different image.
reject 'missing or does not match' \
  env SPACE_KIND_PREBUILT_RUST_SHA=another-commit SPACE_KIND_PREBUILT_RUST_KEY="$key" \
  STUB_KEY_LABEL="$key" STUB_IMAGE_ID=sha256:another-image bash "$script"
# The checkout's key is recomputed: an input changed since the handoff rejects.
printf '\n// changed\n' >>"$repo/$(git -C "$repo" ls-files -- 'crates/*.rs' | head -1)"
git -C "$repo" commit -qam 'change a crate'
reject 'neither does its input key' \
  env SPACE_KIND_PREBUILT_RUST_SHA=another-commit SPACE_KIND_PREBUILT_RUST_KEY="$key" \
  STUB_KEY_LABEL="$key" bash "$script"
echo 'PASS: prebuilt Rust artifact reuse and fail-closed identity checks'

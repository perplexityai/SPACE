#!/usr/bin/env bash
set -euo pipefail

if [[ "$(uname -s)" != Linux || $EUID -ne 0 ]]; then
  if [[ "${SPACE_CI_REQUIRE_TEST_PRIVILEGE:-0}" == 1 ]]; then
    echo "Run these tests as root in the Linux CI container." >&2
    exit 1
  fi
  echo "SKIP: seed-build staging tests require Linux and root"
  exit 0
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
script="${repo_root}/crates/libs/build/mgr/scripts/build-seed.sh"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT
chmod 755 "$tmp_dir"

fake_bin="${tmp_dir}/bin"
mkdir -p "${fake_bin}"

cat >"${fake_bin}/sudo" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
if [[ -n "${CAPTURED_SUDO:-}" ]]; then
  printf '%s\n' "$@" >"$CAPTURED_SUDO"
  exit 0
fi
echo "unexpected sudo invocation" >&2
exit 1
SH
chmod +x "${fake_bin}/sudo"

cat >"${fake_bin}/find" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == "/space/vendor/firecracker" ]]; then
  if [[ -n "${FAKE_VENDOR_ROOT:-}" ]]; then
    exec /usr/bin/find "${FAKE_VENDOR_ROOT}" "${@:2}"
  fi
  exit 0
fi
exec /usr/bin/find "$@"
SH
chmod +x "${fake_bin}/find"

cat >"${fake_bin}/ctr" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
case "${1:-}" in
  version)
    exit 0
    ;;
  images)
    [[ "${ALLOW_CTR_RUN:-}" == "1" ]] || {
      echo "ctr images should not run in this case" >&2
      exit 97
    }
    [[ "${2:-}" == "pull" ]] || {
      echo "unexpected ctr images invocation: $*" >&2
      exit 1
    }
    if [[ "${EXERCISE_OUTPUT_SWAP:-}" == pull ]]; then
      setpriv --reuid=65534 --regid=0 --clear-groups \
        ln -s "$REDIRECT_TARGET" "$SPACE_SEED_OUTPUT_DIR"
    fi
    exit 0
    ;;
  run)
    [[ "${ALLOW_CTR_RUN:-}" == "1" ]] || {
      echo "ctr run should not run in this case" >&2
      exit 97
    }
    env_file=""
    rootfs_source=""
    guest_source=""
    output_source=""
    cache_source=""
    container_id=""
    prev=""
    while [[ $# -gt 0 ]]; do
      if [[ "$1" == type=bind,src=*,dst=/workspace/inputs,* ]]; then
        src="${1#type=bind,src=}"
        src="${src%%,dst=*}"
        env_file="${src}/rootfs-builder.env"
      fi
      case "$1" in
        type=bind,src=*,dst=/output,*)
          output_source="${1#type=bind,src=}"
          output_source="${output_source%%,dst=*}"
          ;;
        type=bind,src=*,dst=/cache,*)
          cache_source="${1#type=bind,src=}"
          cache_source="${cache_source%%,dst=*}"
          ;;
        type=bind,src=*,dst=/workspace/rootfs-builder.sh,*)
          rootfs_source="${1#type=bind,src=}"
          rootfs_source="${rootfs_source%%,dst=*}"
          ;;
        type=bind,src=*,dst=/guest-bin,*)
          guest_source="${1#type=bind,src=}"
          guest_source="${guest_source%%,dst=*}"
          ;;
      esac
      # ctr run <options> <image-ref> <container-id> <cmd...>
      if [[ -z "$container_id" && "$prev" == docker.io/*@sha256:* ]]; then
        container_id="$1"
      fi
      prev="$1"
      shift
    done
    if [[ "${CHECK_STAGE_OWNERSHIP:-}" == 1 ]]; then
      bundle="$(dirname "$(readlink -f "$rootfs_source")")"
      [[ "$bundle" == "${SPACE_SEED_STAGE_ROOT}/"* ]] || {
        echo "scratch bundle escaped the private test staging root" >&2
        exit 1
      }
      [[ "$output_source" == "$bundle/output" ]]
      [[ "$(stat -c '%u:%a' "$cache_source")" == '0:755' ]]
      # A cache-group member must not be able to rename the bundle away.
      if setpriv --reuid=65534 --regid=0 --clear-groups \
        mv "$bundle" "${bundle}.moved" 2>/dev/null; then
        mv "${bundle}.moved" "$bundle"
        echo "another user replaced the scratch bundle" >&2
        exit 1
      fi
      [[ "$(stat -c '%u:%a' "$bundle")" == '0:700' ]]
      for src in "$rootfs_source" "$env_file" "${guest_source}/spaced"; do
        if setpriv --reuid=65534 --regid=0 --clear-groups \
          touch "$src" 2>/dev/null; then
          echo "another user modified a scratch input" >&2
          exit 1
        fi
      done
      cmp "$rootfs_source" "$SPACE_SEED_ROOTFS_BUILDER_SCRIPT"
      cmp "${guest_source}/spaced" "${SPACE_SEED_GUEST_BIN_DIR}/spaced"
      cmp "${guest_source}/spaced-svc" "${SPACE_SEED_GUEST_BIN_DIR}/spaced-svc"
    fi
    # Mirror containerd's identifier validation (identifiers.Validate).
    [[ -n "$container_id" ]] || {
      echo "ctr run: no container ID after the image ref" >&2
      exit 1
    }
    if (( ${#container_id} > 76 )); then
      echo "ctr: create container failed validation: container.ID: identifier \"${container_id}\" greater than maximum length (76 characters): invalid argument" >&2
      exit 1
    fi
    [[ "$container_id" =~ ^[A-Za-z0-9]+([._-][A-Za-z0-9]+)*$ ]] || {
      echo "ctr: container.ID: identifier \"${container_id}\" must match ^[A-Za-z0-9]+(?:[._-](?:[A-Za-z0-9]+))*\$: invalid argument" >&2
      exit 1
    }
    printf '%s\n' "$container_id" >"${CAPTURED_CONTAINER_ID:?CAPTURED_CONTAINER_ID is required}"
    [[ -n "$env_file" && -f "$env_file" ]] || {
      echo "rootfs-builder env file was not staged" >&2
      exit 1
    }
    cp "$env_file" "${CAPTURED_ENV:?CAPTURED_ENV is required}"
    cmp "$(dirname "$env_file")/sandbox_ed25519.pub" "$EXPECTED_PUBKEY"
    [[ -d "$output_source" ]]
    if [[ "${BAD_OUTPUT:-}" == symlink ]]; then
      ln -s "$REDIRECT_TARGET" "$output_source/rootfs.ext4"
    else
      printf 'rootfs\n' >"${output_source}/rootfs.ext4"
    fi
    printf '{}\n' >"${output_source}/metadata.json"
    exit 0
    ;;
  *)
    echo "unexpected ctr invocation: $*" >&2
    exit 1
    ;;
esac
SH
chmod +x "${fake_bin}/ctr"

cat >"${fake_bin}/mv" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
if [[ "${EXERCISE_OUTPUT_SWAP:-}" == publish && "$*" == *./rootfs.ext4 ]]; then
  setpriv --reuid=65534 --regid=0 --clear-groups \
    /bin/mv "$SPACE_SEED_OUTPUT_DIR" "${SPACE_SEED_OUTPUT_DIR}.original"
  setpriv --reuid=65534 --regid=0 --clear-groups \
    ln -s "$REDIRECT_TARGET" "$SPACE_SEED_OUTPUT_DIR"
fi
exec /bin/mv "$@"
SH
chmod +x "${fake_bin}/mv"

vendor_root="${tmp_dir}/vendor/firecracker"
vendor_version_dir="${vendor_root}/v1.16.0"
mkdir -p "${vendor_version_dir}/bin" "${vendor_version_dir}/kernel"
touch "${vendor_version_dir}/kernel/vmlinux.bin" "${vendor_version_dir}/bin/jailer"
chmod +x "${vendor_version_dir}/bin/jailer"
cat >"${vendor_version_dir}/bin/firecracker" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
if [[ "${FAIL_IF_FIRECRACKER_CALLED:-}" == "1" ]]; then
  echo "firecracker should not be inspected in this case" >&2
  exit 98
fi
case "${1:-}" in
  --version)
    echo "Firecracker v1.16.0"
    echo "2026-06-22T00:58:19.212313456 [anonymous-instance:main] Firecracker exiting successfully. exit_code=0"
    ;;
  --snapshot-version)
    echo "v10.1.0"
    echo "2026-06-22T00:58:19.215689465 [anonymous-instance:main] Firecracker exiting successfully. exit_code=0"
    ;;
  *)
    echo "unexpected firecracker invocation: $*" >&2
    exit 1
    ;;
esac
SH
chmod +x "${vendor_version_dir}/bin/firecracker"

make_case_dirs() {
  local name="$1"
  case_root="${tmp_dir}/${name}"
  out_dir="${case_root}/out/seed"
  cache_dir="${case_root}/cache"
  stage_root="${case_root}/stage"
  guest_bin_dir="${case_root}/guest-bin"
  rootfs_builder="${case_root}/rootfs-builder.sh"
  captured_env="${case_root}/rootfs-builder.env"
  captured_container_id="${case_root}/container-id"
  pubkey="${case_root}/sandbox_ed25519.pub"
  cargo_toml="${case_root}/Cargo.toml"

  mkdir -p "$(dirname "$out_dir")" "$cache_dir" "$stage_root" "$guest_bin_dir"
  chmod 755 "$case_root"
  chmod 700 "$stage_root"
  chmod 2775 "$(dirname "$out_dir")"
  touch "${guest_bin_dir}/spaced" "${guest_bin_dir}/spaced-svc" "$pubkey"
  printf 'trusted-public-key\n' >"$pubkey"
  if [[ "$name" == recovered-output || "$name" == foreign-output || "$name" == force-output-* ]]; then
    mkdir "$out_dir"
  fi
  if [[ "$name" == recovered-output || "$name" == foreign-output || "$name" == force-output-caller ]]; then
    chown 65534 "$out_dir"
  fi
  if [[ "$name" == force-output-* ]]; then
    printf 'old-rootfs\n' >"$out_dir/rootfs.ext4"
    printf 'other-public-key\n' >"$out_dir/sandbox_ed25519.pub"
    mkdir "$out_dir/sha256:old-build"
    printf 'old-artifact\n' >"$out_dir/sha256:old-build/rootfs.ext4"
    chown 65534:65534 "$out_dir/sha256:old-build/rootfs.ext4"
    chmod 600 "$out_dir/sha256:old-build/rootfs.ext4"
  fi
  printf '[workspace.package]\nversion = "0.1.2"\n' >"$cargo_toml"
  printf '#!/usr/bin/env bash\n' >"$rootfs_builder"
  chmod +x "$rootfs_builder" "${guest_bin_dir}/spaced" "${guest_bin_dir}/spaced-svc"
}

run_success() {
  local name="$1"
  shift
  make_case_dirs "$name"
  local -a args=()
  [[ "$name" != force-output-* ]] || args+=(--force)
  env \
    PATH="${fake_bin}:${PATH}" \
    ALLOW_CTR_RUN=1 \
    CAPTURED_ENV="$captured_env" \
    EXPECTED_PUBKEY="$pubkey" \
    SPACE_SEED_OUTPUT_GROUP=0 \
    CAPTURED_CONTAINER_ID="$captured_container_id" \
    SPACE_SEED_OUTPUT_DIR="$out_dir" \
    SPACE_SEED_CACHE_DIR="$cache_dir" \
    SPACE_SEED_STAGE_ROOT="$stage_root" \
    SPACE_SEED_GUEST_BIN_DIR="$guest_bin_dir" \
    SPACE_SEED_ROOTFS_BUILDER_SCRIPT="$rootfs_builder" \
    SPACE_SEED_CARGO_TOML="$cargo_toml" \
    SPACE_SEED_SOURCE_TYPE="ubuntu-minimal-cloud-squashfs" \
    SPACE_SEED_UBUNTU_RELEASE="resolute" \
    SPACE_SEED_UBUNTU_VERSION="26.04" \
    SPACE_SEED_UBUNTU_DATE="release" \
    "$@" \
    "$script" --id "$name" --pubkey "$pubkey" "${args[@]}" >/dev/null
  [[ -z "$(find "$stage_root" -mindepth 1 -print -quit)" ]]
}

run_failure() {
  local name="$1"
  local expected_error="$2"
  shift 2
  make_case_dirs "$name"
  set +e
  env \
    PATH="${fake_bin}:${PATH}" \
    CAPTURED_CONTAINER_ID="$captured_container_id" \
    CAPTURED_ENV="$captured_env" \
    EXPECTED_PUBKEY="$pubkey" \
    SPACE_SEED_OUTPUT_GROUP=0 \
    SPACE_SEED_OUTPUT_DIR="$out_dir" \
    SPACE_SEED_CACHE_DIR="$cache_dir" \
    SPACE_SEED_STAGE_ROOT="$stage_root" \
    SPACE_SEED_GUEST_BIN_DIR="$guest_bin_dir" \
    SPACE_SEED_ROOTFS_BUILDER_SCRIPT="$rootfs_builder" \
    SPACE_SEED_CARGO_TOML="$cargo_toml" \
    SPACE_SEED_SOURCE_TYPE="ubuntu-minimal-cloud-squashfs" \
    SPACE_SEED_UBUNTU_RELEASE="resolute" \
    SPACE_SEED_UBUNTU_VERSION="26.04" \
    SPACE_SEED_UBUNTU_DATE="release" \
    "$@" \
    "$script" --id "$name" --pubkey "$pubkey" >"${case_root}/stdout" 2>"${case_root}/stderr"
  exit_code=$?
  set -e

  [[ "$exit_code" -ne 0 ]] || {
    echo "${name}: expected failure" >&2
    exit 1
  }
  grep -F "$expected_error" "${case_root}/stderr" >/dev/null || {
    echo "${name}: expected error containing: ${expected_error}" >&2
    sed 's/^/  /' "${case_root}/stderr" >&2
    exit 1
  }
}

# Compare what the build container's `source` assigns, not the file's spelling.
assert_export() {
  local file="$1"
  local name="$2"
  local value="$3"
  local actual
  # shellcheck disable=SC2016 # the inner shell expands its own arguments
  actual="$(env -i bash --noprofile --norc -c \
    'set -euo pipefail; source "$1"; printf "%s." "${!2}"' _ "$file" "$name")" || {
    echo "sourcing ${file} failed" >&2
    exit 1
  }
  [[ "${actual%.}" == "$value" ]] || {
    echo "expected ${name}=${value@Q} in ${file}, got ${actual%.}" >&2
    echo "actual exports:" >&2
    sed 's/^/  /' "$file" >&2
    exit 1
  }
}

# An unprivileged caller keeps its arguments, home and repository when the
# wrapper asks sudo to restart it before creating any staging directory.
captured_sudo="${tmp_dir}/sudo-args"
touch "$captured_sudo"
chmod 666 "$captured_sudo"
setpriv --reuid=65534 --regid=65534 --clear-groups env \
  PATH="${fake_bin}:${PATH}" CAPTURED_SUDO="$captured_sudo" \
  HOME="${tmp_dir}/caller-home" SPACE_SEED_REPO_ROOT="$repo_root" \
  "$script" --id manual-seed --pubkey '/tmp/key with spaces.pub'
printf '%s\n' -E env "HOME=${tmp_dir}/caller-home" \
  "SPACE_SEED_REPO_ROOT=$repo_root" /bin/bash "$script" \
  --id manual-seed --pubkey '/tmp/key with spaces.pub' >"${tmp_dir}/expected-sudo"
cmp "$captured_sudo" "${tmp_dir}/expected-sudo"

run_success private-staging CHECK_STAGE_OWNERSHIP=1
assert_export "$captured_env" SPACE_SEED_UBUNTU_RELEASE resolute

# Unsafe roots and output redirects must fail before touching their targets.
redirect_target="$tmp_dir/redirect-target"
mkdir "$redirect_target"
for kind in staging cache; do
  for form in symlink foreign writable-parent; do
    unsafe="$tmp_dir/unsafe-$kind-$form"
    case "$form" in
      symlink) ln -s "$redirect_target" "$unsafe" ;;
      foreign) mkdir "$unsafe"; chown 65534 "$unsafe" ;;
      writable-parent) mkdir -m 0777 "$unsafe"; unsafe="$unsafe/child" ;;
    esac
    if [[ "$kind" == staging ]]; then
      run_failure "$kind-$form" "$kind directory" SPACE_SEED_STAGE_ROOT="$unsafe"
    else
      run_failure "$kind-$form" "$kind directory" SPACE_SEED_CACHE_DIR="$unsafe"
    fi
  done
done
mkdir -m 0775 "$tmp_dir/unsafe-writable-stage"
run_failure writable-stage 'unsafe staging directory' SPACE_SEED_STAGE_ROOT="$tmp_dir/unsafe-writable-stage"
mkdir -m 2775 "$tmp_dir/legacy-cache"
run_success repaired-cache SPACE_SEED_CACHE_DIR="$tmp_dir/legacy-cache" CHECK_STAGE_OWNERSHIP=1
[[ "$(stat -c '%u:%a' "$tmp_dir/legacy-cache")" == '0:755' ]]
run_failure output-swap 'File exists' ALLOW_CTR_RUN=1 EXERCISE_OUTPUT_SWAP=pull REDIRECT_TARGET="$redirect_target" CHECK_STAGE_OWNERSHIP=1
[[ -z "$(find "$redirect_target" -mindepth 1 -print -quit)" ]]
run_success pinned-publication EXERCISE_OUTPUT_SWAP=publish REDIRECT_TARGET="$redirect_target"
[[ "$(cat "$out_dir.original/rootfs.ext4")" == rootfs ]]
[[ "$(stat -c '%g:%a' "$out_dir.original")" == '0:2775' ]]
[[ -z "$(find "$redirect_target" -mindepth 1 -print -quit)" ]]
run_failure output-symlink 'not a regular file' ALLOW_CTR_RUN=1 BAD_OUTPUT=symlink REDIRECT_TARGET="$redirect_target"
run_success recovered-output SPACE_SEED_OUTPUT_GROUP= SUDO_UID=65534
run_failure foreign-output 'unsafe output directory' ALLOW_CTR_RUN=1 SUDO_UID=65534
for owner in service caller; do
  output_group=0
  [[ "$owner" != caller ]] || output_group=""
  run_success "force-output-$owner" SPACE_SEED_OUTPUT_GROUP="$output_group" SUDO_UID=65534
  [[ "$(cat "$out_dir/rootfs.ext4")" == rootfs ]]
  [[ "$(cat "$out_dir/sha256:old-build/rootfs.ext4")" == old-artifact ]]
  [[ "$(stat -c '%u:%g:%a' "$out_dir/sha256:old-build/rootfs.ext4")" == '65534:65534:600' ]]
done

run_success no-vendor-defaults
assert_export "$captured_env" SPACE_SEED_RUNTIME firecracker
assert_export "$captured_env" SPACE_SEED_RUNTIME_VERSION 1.15.1
assert_export "$captured_env" SPACE_SEED_STATE_FORMAT_VERSION 9.0.0

runtime_pair_error='requires both SPACE_SEED_RUNTIME_VERSION and SPACE_SEED_STATE_FORMAT_VERSION'
run_failure no-vendor-one-sided "$runtime_pair_error" SPACE_SEED_RUNTIME_VERSION=1.16.0

run_failure vendor-one-sided "$runtime_pair_error" \
  FAKE_VENDOR_ROOT="$vendor_root" \
  FAIL_IF_FIRECRACKER_CALLED=1 \
  SPACE_SEED_RUNTIME_VERSION=1.16.0

run_success vendor-explicit \
  FAKE_VENDOR_ROOT="$vendor_root" \
  FAIL_IF_FIRECRACKER_CALLED=1 \
  SPACE_SEED_RUNTIME_VERSION=9.9.9 \
  SPACE_SEED_STATE_FORMAT_VERSION=8.8.8
assert_export "$captured_env" SPACE_SEED_RUNTIME_VERSION 9.9.9
assert_export "$captured_env" SPACE_SEED_STATE_FORMAT_VERSION 8.8.8

run_success vendor-detected FAKE_VENDOR_ROOT="$vendor_root"
assert_export "$captured_env" SPACE_SEED_RUNTIME_VERSION 1.16.0
assert_export "$captured_env" SPACE_SEED_STATE_FORMAT_VERSION 10.1.0

kernel_version="vmlinux-6.1.200-custom"
kernel_digest="sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
run_success guest-kernel-explicit \
  SPACE_SEED_KERNEL_VERSION="$kernel_version" \
  SPACE_SEED_KERNEL_DIGEST="$kernel_digest"
assert_export "$captured_env" SPACE_SEED_KERNEL_VERSION "$kernel_version"
assert_export "$captured_env" SPACE_SEED_KERNEL_DIGEST "$kernel_digest"

kernel_pair_error='requires both SPACE_SEED_KERNEL_VERSION and SPACE_SEED_KERNEL_DIGEST'
run_failure guest-kernel-version-only "$kernel_pair_error" \
  SPACE_SEED_KERNEL_VERSION="$kernel_version"
run_failure guest-kernel-digest-only "$kernel_pair_error" \
  SPACE_SEED_KERNEL_DIGEST="$kernel_digest"

# The privileged build container sources the env file as root, so every
# staged value must stay data however the caller spelled it.
marker="${tmp_dir}/shell-metacharacters/sourced-command-ran"
declare -A hostile=(
  [SPACE_SEED_UBUNTU_RELEASE]="resolute\"; touch '${marker}'; echo \""
  [SPACE_SEED_UBUNTU_VERSION]="\$(touch '${marker}')"
  [SPACE_SEED_UBUNTU_DATE]="\`touch '${marker}'\`"
  [SPACE_SEED_RUNTIME]=$'firecracker\ntouch '"'${marker}'"
  [SPACE_SEED_RUNTIME_VERSION]="1.16.0'; touch '${marker}'; '"
  [SPACE_SEED_STATE_FORMAT_VERSION]="10.1.0\\\"; touch '${marker}' #"
  [SPACE_SEED_KERNEL_VERSION]="\${PATH:+\$(touch '${marker}')}"
  [SPACE_SEED_KERNEL_DIGEST]=$'sha256:0\r\n$(touch '"'${marker}'"')'
  [SPACE_SEED_SPACED_VERSION]="0.1.2\$IFS\$(touch '${marker}')"
)
hostile_env=()
for name in "${!hostile[@]}"; do
  hostile_env+=("${name}=${hostile[$name]}")
done
run_success shell-metacharacters "${hostile_env[@]}"
# shellcheck disable=SC2016 # the inner shell expands its own argument
env -i bash --noprofile --norc -c 'source "$1"' _ "$captured_env" >/dev/null 2>&1 || true
[[ ! -e "$marker" ]] || {
  echo "shell-metacharacters: sourcing the staged env file ran a command" >&2
  sed 's/^/  /' "$captured_env" >&2
  exit 1
}
for name in "${!hostile[@]}"; do
  assert_export "$captured_env" "$name" "${hostile[$name]}"
done

# The ctr container ID is bounded by containerd's 76-character identifier
# limit. It must fit for any template ID and for any PID the kernel can hand
# out (pid_max is at most 2^22, i.e. 7 digits), so a long-lived builder whose
# PID counter passed 1,000,000 cannot push a long template ID over the limit.
assert_container_id_fits() {
  local case_name="$1"
  local container_id pid_suffix base
  container_id="$(cat "$captured_container_id")"
  pid_suffix="${container_id##*-}"
  base="${container_id%-*}"
  [[ "$pid_suffix" =~ ^[0-9]+$ ]] || {
    echo "${case_name}: expected a PID suffix in container ID ${container_id}" >&2
    exit 1
  }
  (( ${#base} + 1 + 7 <= 76 )) || {
    echo "${case_name}: container ID ${container_id} exceeds 76 characters with a 7-digit PID" >&2
    exit 1
  }
}

# Shape used by the template recipe build smoke: seed-bk-smoke-<job uuid>.
smoke_shaped_id="seed-bk-smoke-00000000-0000-4000-8000-000000000000"
run_success "$smoke_shaped_id"
assert_container_id_fits smoke-shaped-id

long_id="seed-$(printf 'x%.0s' {1..120})"
run_success "$long_id"
assert_container_id_fits long-id

rootfs_stderr="${tmp_dir}/rootfs-kernel-pair.stderr"
set +e
env \
  SPACE_SEED_SOURCE_TYPE=ubuntu-minimal-cloud-squashfs \
  SPACE_SEED_UBUNTU_RELEASE=resolute \
  SPACE_SEED_UBUNTU_VERSION=26.04 \
  SPACE_SEED_UBUNTU_DATE=release \
  SPACE_SEED_SPACED_VERSION=0.1.2 \
  SPACE_SEED_RUNTIME_VERSION=1.16.0 \
  SPACE_SEED_STATE_FORMAT_VERSION=10.1.0 \
  SPACE_SEED_KERNEL_VERSION="$kernel_version" \
  bash "${repo_root}/templates/firecracker/seed/rootfs-builder.sh" \
  --id guest-kernel-version-only >"${tmp_dir}/rootfs-kernel-pair.stdout" 2>"$rootfs_stderr"
rootfs_exit_code=$?
set -e
[[ "$rootfs_exit_code" -ne 0 ]] || {
  echo "rootfs-builder: expected one-sided guest kernel failure" >&2
  exit 1
}
grep -F "$kernel_pair_error" "$rootfs_stderr" >/dev/null || {
  echo "rootfs-builder: expected error containing: ${kernel_pair_error}" >&2
  sed 's/^/  /' "$rootfs_stderr" >&2
  exit 1
}

echo "seed build metadata tests passed"

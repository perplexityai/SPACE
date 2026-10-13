#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
script="${repo_root}/crates/libs/build/mgr/scripts/build-seed.sh"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

fake_bin="${tmp_dir}/bin"
mkdir -p "${fake_bin}"

cat >"${fake_bin}/sudo" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == "-E" ]]; then
  shift
fi
exec "$@"
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
    exit 0
    ;;
  run)
    [[ "${ALLOW_CTR_RUN:-}" == "1" ]] || {
      echo "ctr run should not run in this case" >&2
      exit 97
    }
    env_file=""
    container_id=""
    prev=""
    while [[ $# -gt 0 ]]; do
      if [[ "$1" == type=bind,src=*,dst=/workspace/inputs,* ]]; then
        src="${1#type=bind,src=}"
        src="${src%%,dst=*}"
        env_file="${src}/rootfs-builder.env"
      fi
      # ctr run <options> <image-ref> <container-id> <cmd...>
      if [[ -z "$container_id" && "$prev" == docker.io/*@sha256:* ]]; then
        container_id="$1"
      fi
      prev="$1"
      shift
    done
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
    mkdir -p "${SPACE_SEED_OUTPUT_DIR:?SPACE_SEED_OUTPUT_DIR is required}"
    printf 'rootfs\n' >"${SPACE_SEED_OUTPUT_DIR}/rootfs.ext4"
    printf '{}\n' >"${SPACE_SEED_OUTPUT_DIR}/metadata.json"
    exit 0
    ;;
  *)
    echo "unexpected ctr invocation: $*" >&2
    exit 1
    ;;
esac
SH
chmod +x "${fake_bin}/ctr"

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

  mkdir -p "$out_dir" "$cache_dir" "$stage_root" "$guest_bin_dir"
  touch "${guest_bin_dir}/spaced" "${guest_bin_dir}/spaced-svc" "$pubkey"
  printf '[workspace.package]\nversion = "0.1.2"\n' >"$cargo_toml"
  printf '#!/usr/bin/env bash\n' >"$rootfs_builder"
  chmod +x "$rootfs_builder" "${guest_bin_dir}/spaced" "${guest_bin_dir}/spaced-svc"
}

run_success() {
  local name="$1"
  shift
  make_case_dirs "$name"
  env \
    PATH="${fake_bin}:${PATH}" \
    ALLOW_CTR_RUN=1 \
    CAPTURED_ENV="$captured_env" \
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
    "$script" --id "$name" --pubkey "$pubkey" >/dev/null
}

run_failure() {
  local name="$1"
  local expected_error="$2"
  shift 2
  make_case_dirs "$name"
  set +e
  env \
    PATH="${fake_bin}:${PATH}" \
    CAPTURED_ENV="$captured_env" \
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

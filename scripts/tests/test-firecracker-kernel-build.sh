#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
kernel_dir="${repo_root}/templates/firecracker/kernel"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

# shellcheck source=/dev/null
source "${kernel_dir}/build-kernel.sh"

valid_config="${tmp_dir}/valid.config"
cat >"$valid_config" <<'EOF'
CONFIG_DEBUG_INFO_BTF=y
CONFIG_DEBUG_INFO_BTF_MODULES=y
CONFIG_DEBUG_INFO_DWARF4=y
CONFIG_TRACING=y
CONFIG_KPROBE_EVENTS=y
CONFIG_UPROBE_EVENTS=y
CONFIG_BPF_JIT=y
CONFIG_KALLSYMS_ALL=y
CONFIG_NET_CLS_BPF=y
CONFIG_NET_SCH_INGRESS=y
CONFIG_MODULES=y
CONFIG_KPROBES=y
CONFIG_FTRACE=y
CONFIG_MODULE_SIG=y
CONFIG_MODULE_SIG_FORCE=y
CONFIG_MODULE_SIG_ALL=y
CONFIG_MODULE_SIG_SHA512=y
CONFIG_MODULE_SIG_HASH="sha512"
CONFIG_MODULE_SIG_KEY="certs/signing_key.pem"
# CONFIG_FUSE_FS is not set
# CONFIG_MODULE_UNLOAD is not set
# CONFIG_VSOCKETS_LOOPBACK is not set
EOF

verify_config "$valid_config"

for setting in 'CONFIG_VSOCKETS_LOOPBACK=y' 'CONFIG_VSOCKETS_LOOPBACK=m'; do
  sed "s/# CONFIG_VSOCKETS_LOOPBACK is not set/$setting/" "$valid_config" >"${tmp_dir}/loopback.config"
  if verify_config "${tmp_dir}/loopback.config" >/dev/null 2>&1; then
    echo "verify_config accepted guest vsock loopback" >&2
    exit 1
  fi
done

missing_btf_modules="${tmp_dir}/missing-btf-modules.config"
grep -v '^CONFIG_DEBUG_INFO_BTF_MODULES=' "$valid_config" >"$missing_btf_modules"
if verify_config "$missing_btf_modules" >/dev/null 2>&1; then
  echo "verify_config accepted a config without split BTF" >&2
  exit 1
fi

unsigned_modules="${tmp_dir}/unsigned-modules.config"
grep -v '^CONFIG_MODULE_SIG_FORCE=' "$valid_config" >"$unsigned_modules"
if verify_config "$unsigned_modules" >/dev/null 2>&1; then
  echo "verify_config accepted a config without forced module signatures" >&2
  exit 1
fi

modular_config="${tmp_dir}/modular.config"
cp "$valid_config" "$modular_config"
printf '%s\n' 'CONFIG_TEST_MODULE=m' >>"$modular_config"
if verify_config "$modular_config" >/dev/null 2>&1; then
  echo "verify_config accepted a loadable module" >&2
  exit 1
fi

fake_bin="${tmp_dir}/bin"
docker_args="${tmp_dir}/docker-args"
mkdir -p "$fake_bin"
cat >"${fake_bin}/objdump" <<'EOF'
#!/usr/bin/env bash
case "$*" in
  *missing-btf*) exit 0 ;;
  *)
    printf '%s\n' \
      '  1 .BTF          0047a5c7  0000000000000000  0000000000000000  00001000  2**0' \
      '  2 .BTF_ids      000002e8  0000000000000000  0000000000000000  0047b000  2**0'
    ;;
esac
EOF
cat >"${fake_bin}/docker" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$@" >"$DOCKER_ARGS"
EOF
chmod +x "${fake_bin}/docker" "${fake_bin}/objdump"

# Exercise both build profiles without compiling Linux. The Linux build-tool
# boundary is substituted; main still selects/merges/verifies config.
mkdir -p "${tmp_dir}/linux/scripts/kconfig"
printf '1234567890\n' >"${tmp_dir}/source-date-epoch"
cat >"${fake_bin}/make" <<'EOF'
#!/usr/bin/env bash
case "$*" in
  '-s kernelversion'|'-s kernelrelease') echo 6.1.155 ;;
  *vmlinux*) printf 'kernel\n' >vmlinux ;;
esac
EOF
cat >"${fake_bin}/objcopy" <<'EOF'
#!/usr/bin/env bash
cp vmlinux "${@: -1}"
EOF
cat >"${tmp_dir}/linux/scripts/kconfig/merge_config.sh" <<'EOF'
#!/usr/bin/env bash
cp "$3" .config
EOF
cat >"${tmp_dir}/linux/scripts/config" <<'EOF'
#!/usr/bin/env bash
[[ "$1" == --enable ]] || exit 1
printf 'CONFIG_%s=y\n' "$2" >>.config
EOF
chmod +x "${fake_bin}/make" "${fake_bin}/objcopy" "${tmp_dir}/linux/scripts/kconfig/merge_config.sh" \
  "${tmp_dir}/linux/scripts/config"
printf 'CONFIG_BASE_ONLY=y\n' >"${tmp_dir}/base.config"
for fragment in '' "$valid_config" "$unsigned_modules"; do
  if env PATH="${fake_bin}:${PATH}" SOURCE_DIR="${tmp_dir}/linux" \
      BASE_CONFIG="${tmp_dir}/base.config" CONFIG_FRAGMENT="$fragment" \
      SOURCE_TIMESTAMP_FILE="${tmp_dir}/source-date-epoch" OUTPUT_DIR="${tmp_dir}/compiled" \
      bash "${kernel_dir}/build-kernel.sh" >"${tmp_dir}/build.log" 2>&1; then
    [[ "$fragment" != "$unsigned_modules" ]] || {
      echo 'fragment build accepted missing module-signing enforcement' >&2; exit 1;
    }
    while IFS= read -r setting; do
      grep -Fxq "$setting" "${tmp_dir}/compiled/config"
    done <"${fragment:-${tmp_dir}/base.config}"
  else
    [[ "$fragment" == "$unsigned_modules" ]] || { cat "${tmp_dir}/build.log" >&2; exit 1; }
  fi
done

PATH="${fake_bin}:${PATH}" verify_btf_section valid-image .BTF
PATH="${fake_bin}:${PATH}" verify_btf_section valid-image .BTF_ids
if PATH="${fake_bin}:${PATH}" verify_btf_section missing-btf-image .BTF >/dev/null 2>&1; then
  echo "verify_btf_section accepted an image without BTF" >&2
  exit 1
fi

test_version="6.1.999"
test_ci_version="v1.99"
test_commit="1111111111111111111111111111111111111111"
test_config_sha="2222222222222222222222222222222222222222222222222222222222222222"

env \
  PATH="${fake_bin}:${PATH}" \
  DOCKER_ARGS="$docker_args" \
  OUTPUT_DIR="${tmp_dir}/output" \
  KERNEL_VERSION="$test_version" \
  FIRECRACKER_CI_VERSION="$test_ci_version" \
  KERNEL_SOURCE_COMMIT="$test_commit" \
  BASE_CONFIG_SHA256="$test_config_sha" \
  "${kernel_dir}/build.sh" >/dev/null

for expected_arg in \
  "KERNEL_VERSION=${test_version}" \
  "FIRECRACKER_CI_VERSION=${test_ci_version}" \
  "KERNEL_SOURCE_COMMIT=${test_commit}" \
  "BASE_CONFIG_SHA256=${test_config_sha}"; do
  grep -Fxq "$expected_arg" "$docker_args" || {
    echo "build.sh did not forward ${expected_arg}" >&2
    exit 1
  }
done

if env ARCH=aarch64 PATH="${fake_bin}:${PATH}" "${kernel_dir}/build.sh" >/dev/null 2>&1; then
  echo "build.sh accepted an aarch64 build" >&2
  exit 1
fi

if env CONFIG_FRAGMENT=missing-review-fixture.config PATH="${fake_bin}:${PATH}" \
    "${kernel_dir}/build.sh" >/dev/null 2>&1; then
  echo 'build.sh silently accepted a missing requested fragment' >&2
  exit 1
fi

echo "Firecracker kernel build guards passed"

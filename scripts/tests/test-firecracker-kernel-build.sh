#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
kernel_dir="${repo_root}/templates/firecracker/kernel"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

# shellcheck disable=SC2034 # read by the sourced build-kernel.sh
VARIANT_DIR="${kernel_dir}/variants"
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

# Exercise both build profiles without compiling Linux. Only the compiler and
# binutils boundary is substituted; main still selects/merges/verifies config.
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
chmod +x "${fake_bin}/make" "${fake_bin}/objcopy" "${tmp_dir}/linux/scripts/kconfig/merge_config.sh"
printf 'CONFIG_BASE_ONLY=y\n' >"${tmp_dir}/base.config"
for fragment in '' "$valid_config" "$unsigned_modules"; do
  if env PATH="${fake_bin}:${PATH}" SOURCE_DIR="${tmp_dir}/linux" \
      BASE_CONFIG="${tmp_dir}/base.config" CONFIG_FRAGMENT="$fragment" \
      SOURCE_TIMESTAMP_FILE="${tmp_dir}/source-date-epoch" OUTPUT_DIR="${tmp_dir}/compiled" \
      bash "${kernel_dir}/build-kernel.sh" >"${tmp_dir}/build.log" 2>&1; then
    [[ "$fragment" != "$unsigned_modules" ]] || {
      echo 'fragment build accepted missing module-signing enforcement' >&2; exit 1;
    }
    cmp "${fragment:-${tmp_dir}/base.config}" "${tmp_dir}/compiled/config"
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

# In-repository variants. Each has a fragment, a required list and a disabled
# list; a fragment only enables built-in symbols, and nothing is both required
# and disabled.
for fragment in "${kernel_dir}"/variants/*.config; do
  variant="$(basename "$fragment" .config)"
  for list in required disabled; do
    [[ -f "${kernel_dir}/variants/${variant}.${list}" ]] || {
      echo "variant ${variant} has no ${list} list" >&2; exit 1;
    }
  done
  if variant_lines "$fragment" | grep -Ev '^CONFIG_[A-Z0-9_]+=y$' >&2; then
    echo "variant ${variant} sets something other than a built-in symbol" >&2
    exit 1
  fi
  grep -qx 'CONFIG_MODULES' "${kernel_dir}/variants/${variant}.disabled" || {
    echo "variant ${variant} does not keep CONFIG_MODULES disabled" >&2; exit 1;
  }
  while IFS= read -r symbol; do
    if { variant_lines "$fragment"; variant_lines "${kernel_dir}/variants/${variant}.required"; } |
        grep -q "^${symbol}="; then
      echo "variant ${variant} both requires and disables ${symbol}" >&2
      exit 1
    fi
  done < <(variant_lines "${kernel_dir}/variants/${variant}.disabled")

  # A config holding exactly the variant's promises passes; breaking any one
  # of them fails.
  variant_config="${tmp_dir}/${variant}-resolved.config"
  {
    variant_lines "$fragment"
    variant_lines "${kernel_dir}/variants/${variant}.required"
    variant_lines "${kernel_dir}/variants/${variant}.disabled" | sed 's/.*/# & is not set/'
  } >"$variant_config"
  verify_variant_config "$variant_config" "$variant"
  first_symbol="$(variant_lines "$fragment" | head -n1)"
  grep -vxF "$first_symbol" "$variant_config" >"${tmp_dir}/unresolved.config"
  if verify_variant_config "${tmp_dir}/unresolved.config" "$variant" >/dev/null 2>&1; then
    echo "verify_variant_config accepted an unresolved ${variant} symbol" >&2
    exit 1
  fi
  sed 's/^# CONFIG_MODULES is not set$/CONFIG_MODULES=y/' "$variant_config" >"${tmp_dir}/modules.config"
  if verify_variant_config "${tmp_dir}/modules.config" "$variant" >/dev/null 2>&1; then
    echo "verify_variant_config accepted CONFIG_MODULES for ${variant}" >&2
    exit 1
  fi
  cp "$variant_config" "${tmp_dir}/variant-modular.config"
  printf '%s\n' 'CONFIG_TEST_MODULE=m' >>"${tmp_dir}/variant-modular.config"
  if verify_variant_config "${tmp_dir}/variant-modular.config" "$variant" >/dev/null 2>&1; then
    echo "verify_variant_config accepted a loadable module for ${variant}" >&2
    exit 1
  fi
  # The bpf-modsig profile's own checks do not apply to a variant.
  if verify_config "$variant_config" >/dev/null 2>&1; then
    echo "the ${variant} fixture unexpectedly satisfies the bpf-modsig profile" >&2
    exit 1
  fi
done

# build-kernel.sh with a variant: the fragment comes from VARIANT_DIR, the
# output is named after the variant, and the variant's lists decide success.
fixture_variants="${tmp_dir}/variants"
mkdir -p "$fixture_variants"
printf 'CONFIG_FIXTURE_ON=y\n# CONFIG_FIXTURE_OFF is not set\n' >"${fixture_variants}/fixture.config"
printf 'CONFIG_FIXTURE_ON=y\n' >"${fixture_variants}/fixture.required"
printf 'CONFIG_FIXTURE_OFF\n' >"${fixture_variants}/fixture.disabled"
cp "${fixture_variants}/fixture.config" "${fixture_variants}/broken.config"
cp "${fixture_variants}/fixture.required" "${fixture_variants}/broken.required"
printf 'CONFIG_FIXTURE_OFF\nCONFIG_FIXTURE_ABSENT\n' >"${fixture_variants}/broken.disabled"
build_variant() {
  env PATH="${fake_bin}:${PATH}" SOURCE_DIR="${tmp_dir}/linux" \
    BASE_CONFIG="${tmp_dir}/base.config" KERNEL_VARIANT="$1" CONFIG_FRAGMENT="${2:-}" \
    VARIANT_DIR="$fixture_variants" SOURCE_TIMESTAMP_FILE="${tmp_dir}/source-date-epoch" \
    OUTPUT_DIR="${3:-}" bash "${kernel_dir}/build-kernel.sh" >"${tmp_dir}/variant-build.log" 2>&1
}
build_variant fixture "" "${tmp_dir}/variant-out" || { cat "${tmp_dir}/variant-build.log" >&2; exit 1; }
cmp "${fixture_variants}/fixture.config" "${tmp_dir}/variant-out/config"
if build_variant broken "" "${tmp_dir}/broken-out"; then
  echo "build-kernel.sh accepted a variant whose disabled list did not resolve" >&2
  exit 1
fi
if build_variant fixture "$valid_config" "${tmp_dir}/both-out"; then
  echo "build-kernel.sh accepted both KERNEL_VARIANT and CONFIG_FRAGMENT" >&2
  exit 1
fi
# The default output directory carries the variant's name, not bpf-modsig.
default_output_dir() {
  # SC2016: the nested shell expands $1 and OUTPUT_DIR.
  # shellcheck disable=SC2016
  env -u OUTPUT_DIR -u KERNEL_VARIANT -u CONFIG_FRAGMENT "$@" \
    bash -c 'source "$1"; echo "$OUTPUT_DIR"' _ "${kernel_dir}/build-kernel.sh"
}
[[ "$(default_output_dir KERNEL_VARIANT=fixture)" == /output/vmlinux-6.1.155-fixture ]]
[[ "$(default_output_dir CONFIG_FRAGMENT=custom.config)" == /output/vmlinux-6.1.155-bpf-modsig ]]
[[ "$(default_output_dir)" == /output/vmlinux-6.1.155 ]]

# build.sh forwards an in-repository variant and rejects anything else.
env PATH="${fake_bin}:${PATH}" DOCKER_ARGS="$docker_args" OUTPUT_DIR="${tmp_dir}/output" \
  KERNEL_VARIANT=nft "${kernel_dir}/build.sh" >"${tmp_dir}/build-sh.log"
grep -Fxq 'KERNEL_VARIANT=nft' "$docker_args" || { echo 'build.sh did not forward KERNEL_VARIANT' >&2; exit 1; }
grep -Fq 'vmlinux-6.1.155-nft' "${tmp_dir}/build-sh.log" || { echo 'build.sh did not name the nft output' >&2; exit 1; }
for rejected in missing-variant bpf-modsig 'Nft' '../nft'; do
  if env KERNEL_VARIANT="$rejected" PATH="${fake_bin}:${PATH}" DOCKER_ARGS="$docker_args" \
      "${kernel_dir}/build.sh" >/dev/null 2>&1; then
    echo "build.sh accepted KERNEL_VARIANT=${rejected}" >&2
    exit 1
  fi
done
if env KERNEL_VARIANT=nft CONFIG_FRAGMENT=build.sh PATH="${fake_bin}:${PATH}" DOCKER_ARGS="$docker_args" \
    "${kernel_dir}/build.sh" >/dev/null 2>&1; then
  echo 'build.sh accepted both KERNEL_VARIANT and CONFIG_FRAGMENT' >&2
  exit 1
fi

echo "Firecracker kernel build guards passed"

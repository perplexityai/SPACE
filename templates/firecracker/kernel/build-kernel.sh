#!/usr/bin/env bash
set -euo pipefail

readonly SOURCE_DIR="${SOURCE_DIR:-/build/linux}"
readonly BASE_CONFIG="${BASE_CONFIG:-/build/base.config}"
readonly CONFIG_FRAGMENT="${CONFIG_FRAGMENT:-}"
readonly SOURCE_TIMESTAMP_FILE="${SOURCE_TIMESTAMP_FILE:-/build/source-date-epoch}"
readonly KERNEL_VERSION="${KERNEL_VERSION:-6.1.155}"
readonly OUTPUT_DIR="${OUTPUT_DIR:-/output/vmlinux-${KERNEL_VERSION}${CONFIG_FRAGMENT:+-bpf-modsig}}"

# Modern iptables uses nf_tables even when a container only requests NET_ADMIN.
# Keep filtering and NAT built in for both frontends, including inet-family
# nft rules and the legacy raw tables and matches used by firewall tools.
readonly -a REQUIRED_NETFILTER_CONFIGS=(
  CONFIG_NF_TABLES
  CONFIG_NF_TABLES_IPV4
  CONFIG_NF_TABLES_IPV6
  CONFIG_NF_TABLES_INET
  CONFIG_NFT_CT
  CONFIG_NFT_COMPAT
  CONFIG_NFT_REJECT
  CONFIG_NFT_REJECT_IPV4
  CONFIG_NFT_REJECT_IPV6
  CONFIG_NFT_NAT
  CONFIG_NFT_MASQ
  CONFIG_NFT_REDIR
  CONFIG_IP_NF_RAW
  CONFIG_IP6_NF_RAW
  CONFIG_NETFILTER_XT_MATCH_COMMENT
  CONFIG_NETFILTER_XT_MATCH_MULTIPORT
  CONFIG_NETFILTER_XT_MATCH_OWNER
  CONFIG_NETFILTER_XT_MATCH_MARK
)

readonly -a REQUIRED_ENABLED_CONFIGS=(
  CONFIG_DEBUG_INFO_BTF
  CONFIG_DEBUG_INFO_BTF_MODULES
  CONFIG_DEBUG_INFO_DWARF4
  CONFIG_TRACING
  CONFIG_KPROBE_EVENTS
  CONFIG_UPROBE_EVENTS
  CONFIG_BPF_JIT
  CONFIG_KALLSYMS_ALL
  CONFIG_NET_CLS_BPF
  CONFIG_NET_SCH_INGRESS
  CONFIG_MODULES
  CONFIG_KPROBES
  CONFIG_FTRACE
  CONFIG_MODULE_SIG
  CONFIG_MODULE_SIG_FORCE
  CONFIG_MODULE_SIG_ALL
  CONFIG_MODULE_SIG_SHA512
)
readonly -a REQUIRED_DISABLED_CONFIGS=(
  CONFIG_FUSE_FS
  CONFIG_MODULE_UNLOAD
  CONFIG_VSOCKETS_LOOPBACK
)

verify_config() {
  local config="$1"
  local symbol

  for symbol in "${REQUIRED_ENABLED_CONFIGS[@]}"; do
    if ! grep -qx "${symbol}=y" "$config"; then
      echo "ERROR: Kconfig did not resolve ${symbol}=y" >&2
      return 1
    fi
  done

  if ! grep -qx 'CONFIG_MODULE_SIG_HASH="sha512"' "$config"; then
    echo 'ERROR: Kconfig did not resolve CONFIG_MODULE_SIG_HASH="sha512"' >&2
    return 1
  fi
  if ! grep -qx 'CONFIG_MODULE_SIG_KEY="certs/signing_key.pem"' "$config"; then
    echo 'ERROR: Kconfig did not retain the ephemeral module-signing key path' >&2
    return 1
  fi
  for symbol in "${REQUIRED_DISABLED_CONFIGS[@]}"; do
    if ! grep -qx "# ${symbol} is not set" "$config"; then
      echo "ERROR: Kconfig did not keep ${symbol} disabled" >&2
      return 1
    fi
  done
  if grep -Eq '=m$' "$config"; then
    echo "ERROR: resolved config contains loadable modules" >&2
    grep -E '=m$' "$config" >&2
    return 1
  fi
}

verify_btf_section() {
  local image="$1"
  local section="$2"
  local size

  size="$(objdump --section-headers "$image" | awk -v section="$section" '$2 == section { print $3; exit }')"
  if [[ ! "$size" =~ ^[0-9a-fA-F]*[1-9a-fA-F][0-9a-fA-F]*$ ]]; then
    echo "ERROR: ${image} has no non-empty ${section} section" >&2
    return 1
  fi
}

main() {
  local resolved_version source_version source_timestamp symbol

  cd "$SOURCE_DIR"
  source_version="$(make -s kernelversion)"
  if [[ "$source_version" != "$KERNEL_VERSION" ]]; then
    echo "ERROR: source is Linux ${source_version}, expected ${KERNEL_VERSION}" >&2
    return 1
  fi

  if [[ -n "$CONFIG_FRAGMENT" ]]; then
    scripts/kconfig/merge_config.sh -m "$BASE_CONFIG" "$CONFIG_FRAGMENT"
  else
    cp "$BASE_CONFIG" .config
  fi
  for symbol in "${REQUIRED_NETFILTER_CONFIGS[@]}"; do
    scripts/config --enable "${symbol#CONFIG_}"
  done
  make olddefconfig
  for symbol in "${REQUIRED_NETFILTER_CONFIGS[@]}"; do
    if ! grep -qx "${symbol}=y" .config; then
      echo "ERROR: Kconfig did not resolve ${symbol}=y" >&2
      return 1
    fi
  done
  if [[ -n "$CONFIG_FRAGMENT" ]]; then
    verify_config .config
  fi

  resolved_version="$(make -s kernelrelease)"
  if [[ "$resolved_version" != "$KERNEL_VERSION" ]]; then
    echo "ERROR: resolved kernel release is ${resolved_version}, expected ${KERNEL_VERSION}" >&2
    return 1
  fi

  source_timestamp="$(<"$SOURCE_TIMESTAMP_FILE")"
  if [[ ! "$source_timestamp" =~ ^[0-9]+$ ]]; then
    echo "ERROR: source timestamp is not a Unix timestamp" >&2
    return 1
  fi
  export KBUILD_BUILD_TIMESTAMP="@${source_timestamp}"
  export SOURCE_DATE_EPOCH="$source_timestamp"
  make -j"$(nproc)" vmlinux

  mkdir -p "$OUTPUT_DIR"
  objcopy \
    --preserve-dates \
    --strip-debug \
    --keep-section=.BTF \
    --keep-section=.BTF_ids \
    vmlinux "${OUTPUT_DIR}/vmlinux.bin"
  if [[ -n "$CONFIG_FRAGMENT" ]]; then
    verify_btf_section "${OUTPUT_DIR}/vmlinux.bin" .BTF
    verify_btf_section "${OUTPUT_DIR}/vmlinux.bin" .BTF_ids
  fi
  chmod 0644 "${OUTPUT_DIR}/vmlinux.bin"
  cp .config "${OUTPUT_DIR}/config"
  (
    cd "$OUTPUT_DIR"
    sha256sum vmlinux.bin > vmlinux.bin.sha256
  )

  if [[ -n "$CONFIG_FRAGMENT" ]]; then
    verify_config "${OUTPUT_DIR}/config"
  fi

  # The generated private key is a build input, not an artifact. Removing it
  # in the build step keeps it out of both the scratch export and this layer.
  rm -f certs/signing_key.pem certs/signing_key.x509
  echo "Built ${OUTPUT_DIR}/vmlinux.bin"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi

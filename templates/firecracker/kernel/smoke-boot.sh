#!/usr/bin/env bash
set -euo pipefail

readonly KERNEL_IMAGE="${1:?usage: smoke-boot.sh KERNEL_IMAGE KERNEL_VERSION [KERNEL_VARIANT]}"
readonly KERNEL_VERSION="${2:?usage: smoke-boot.sh KERNEL_IMAGE KERNEL_VERSION [KERNEL_VARIANT]}"
# A variant may list extended regular expressions its boot log must match in
# variants/<name>.boot-log. Such a variant boots with initcall_debug so that
# every built-in subsystem it adds reports its initcall result.
readonly KERNEL_VARIANT="${3:-}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly SCRIPT_DIR
readonly BOOT_LOG_EXPECTATIONS="${SCRIPT_DIR}/variants/${KERNEL_VARIANT}.boot-log"
KERNEL_CMDLINE='console=ttyS0 root=/dev/vda reboot=t panic=1'
if [[ -n "$KERNEL_VARIANT" && -f "$BOOT_LOG_EXPECTATIONS" ]]; then
  KERNEL_CMDLINE+=' initcall_debug ignore_loglevel'
fi
readonly KERNEL_CMDLINE
SMOKE_IMAGE="space-guest-kernel-smoke:$(sha256sum "${SCRIPT_DIR}/Dockerfile" | cut -c1-16)"
readonly SMOKE_IMAGE

if [[ -n "$KERNEL_VARIANT" && ! -f "${SCRIPT_DIR}/variants/${KERNEL_VARIANT}.config" ]]; then
  echo "ERROR: unknown kernel variant: ${KERNEL_VARIANT}" >&2
  exit 1
fi
if [[ ! -f "$KERNEL_IMAGE" ]]; then
  echo "ERROR: guest kernel does not exist: ${KERNEL_IMAGE}" >&2
  exit 1
fi

# KERNEL_DOCKER_BUILD_OPTIONS adds docker build options, as for build.sh.
# shellcheck disable=SC2086 # one option per word
docker build --target smoke --tag "$SMOKE_IMAGE" ${KERNEL_DOCKER_BUILD_OPTIONS:-} "$SCRIPT_DIR"

boot_log="$(mktemp)"
container_id=""
cleanup() {
  if [[ -n "$container_id" ]]; then
    docker rm --force "$container_id" >/dev/null 2>&1 || true
  fi
  rm -f "$boot_log"
}
trap cleanup EXIT

# The Buildkite Docker daemon is remote from the command container, so a bind
# mount would resolve on the daemon host. docker cp transfers the client-side
# build output into the smoke container explicitly.
container_id="$(docker create \
  "$SMOKE_IMAGE" \
  timeout 90 \
    qemu-system-x86_64 \
      -machine pc,accel=tcg \
      -cpu max \
      -m 512M \
      -nodefaults \
      -no-reboot \
      -display none \
      -serial stdio \
      -kernel /tmp/vmlinux.bin \
      -append "$KERNEL_CMDLINE")"
docker cp "$KERNEL_IMAGE" "${container_id}:/tmp/vmlinux.bin"

status=0
docker start --attach "$container_id" >"$boot_log" 2>&1 || status=$?

cat "$boot_log"
if [[ "$status" -ne 0 && "$status" -ne 124 ]]; then
  echo "ERROR: QEMU exited unexpectedly with status ${status}" >&2
  exit "$status"
fi
if ! grep -Fq "Linux version ${KERNEL_VERSION}" "$boot_log"; then
  echo "ERROR: guest kernel did not reach the Linux entry point" >&2
  exit 1
fi
if ! grep -Fq 'Kernel panic - not syncing: VFS: Unable to mount root fs' "$boot_log"; then
  echo "ERROR: guest kernel did not reach the expected root-filesystem handoff" >&2
  exit 1
fi

if [[ -n "$KERNEL_VARIANT" && -f "$BOOT_LOG_EXPECTATIONS" ]]; then
  while IFS= read -r expected; do
    if ! grep -Eq -- "$expected" "$boot_log"; then
      echo "ERROR: the ${KERNEL_VARIANT} boot log does not match: ${expected}" >&2
      exit 1
    fi
  done < <(grep -Ev '^[[:space:]]*(#|$)' "$BOOT_LOG_EXPECTATIONS" || true)
fi

echo "Guest kernel ${KERNEL_VERSION}${KERNEL_VARIANT:+ (${KERNEL_VARIANT})} reached the VFS handoff under QEMU"

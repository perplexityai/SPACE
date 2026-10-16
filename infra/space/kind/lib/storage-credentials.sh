#!/usr/bin/env bash
# storage-credentials.sh — how the Kind harness's node services get
# object-store credentials, selected by SPACE_KIND_STORAGE_CREDENTIALS for
# every bring-up path (the Tiltfile, bootstrap.sh and the smoke bring-up):
#
#   exchange  (default) as deployed: volume-manager on the sandbox workers
#             holds no object-store keys. The credential-manager authority
#             vends each transfer a session scoped to its one object through
#             STS AssumeRole, which the Kind-only STS bridge answers from the
#             object store's IAM service. See "Object-store credentials" in
#             infra/space/kind/README.md.
#   static    the opt-out: worker volume-manager reads the store's static keys
#             and the authority's storage exchange is off.
#
# Both modes run the same object store, IAM service and bridge over the same
# data, so an existing cluster switches on its next bring-up. Builders,
# build-manager and cluster-controller read the static keys in both.
#
# Source it for the function, or run it to print the mode.

# Prints the validated mode; fails on any other value.
space_kind_storage_credentials() {
  local mode="${SPACE_KIND_STORAGE_CREDENTIALS:-exchange}"
  case "$mode" in
    static|exchange) printf '%s\n' "$mode" ;;
    *)
      printf 'SPACE_KIND_STORAGE_CREDENTIALS must be exchange or static, not %q\n' "$mode" >&2
      return 1
      ;;
  esac
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  set -euo pipefail
  space_kind_storage_credentials
fi

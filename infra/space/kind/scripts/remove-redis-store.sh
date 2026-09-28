#!/usr/bin/env bash
#
# Remove the opt-in Redis limiter store from a default stack.
#
#   bash infra/space/kind/scripts/remove-redis-store.sh <kube-context> <namespace>
#
# A default bring-up holds no store, whatever an earlier `--with-redis` /
# `SPACE_LOCAL_REDIS=1` bring-up or a killed smoke left behind. Tilt never
# deletes what an earlier session applied, so every default path calls this:
# `bootstrap.sh` without `--with-redis`, the Tiltfile's `remove-redis-store`
# resource, and `bootstrap_kind_stack` in `scripts/tests/lib/`.
#
# Skipped while any smoke holds the public-API mutation Lease
# (`space-smoke-public-api-*`). A rate-limiter smoke (holder `rate-limiter;...`)
# may be using a store it created, and removes that store itself; any other
# holder (the node-group or node-selector-extras smoke) could hand the Lease to
# one before the delete lands. A Lease a killed run still holds counts until it
# expires, and the next rate-limiter run is refused until then too. After that,
# the next default bring-up removes the store; the next rate-limiter run also
# does, through its run record, when the killed run had created it.
#
# Otherwise the delete runs under that same Lease, taken for a minute and handed
# back, so a rate-limiter run can't start between the check and the delete and
# lose a store it just created: the scenario refuses to start while the Lease is
# held.
#
# A no-op before the namespace exists, as on a fresh cluster where Tilt starts
# this alongside the namespace manifest: there is no store to remove, and no
# Lease could be created there to fence the delete.
set -euo pipefail

kctx="${1:?kube context}"
ns="${2:?namespace}"
record_py="$(dirname "${BASH_SOURCE[0]}")/lib/limiter_run_record.py"
k=(-n "$ns" --context "$kctx")
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
if ! kubectl get namespace "$ns" --context "$kctx" >/dev/null 2>"$tmp/err"; then
  grep -qi notfound "$tmp/err" && exit 0
  cat "$tmp/err" >&2
  exit 1
fi
lease="$(python3 "$record_py" lease-name "$kctx")"
holder="kind-remove-redis-store;host=$(hostname);pid=$$;token=$(od -An -N8 -tx1 /dev/urandom | tr -d ' \n')"

# Read the record and the Lease, then act on the plan. A take that loses a race
# (the Lease changed since it was read) is re-read and re-planned.
for _ in 1 2 3; do
  if ! kubectl get lease "$lease" "${k[@]}" -o json >"$tmp/lease.json" 2>"$tmp/err"; then
    grep -qi notfound "$tmp/err" || { cat "$tmp/err" >&2; exit 1; }
    : >"$tmp/lease.json"
  fi
  plan="$(python3 "$record_py" fence "$tmp/lease.json" "$holder" "$lease" "$ns")"
  case "$plan" in
    aside\ *)
      read -r _ held kind <<<"$plan"
      if [[ "$kind" == rate-limiter ]]; then
        echo "[remove-redis-store] leaving the Redis store in place: a rate-limiter smoke holds $held"
      else
        echo "[remove-redis-store] leaving the Redis store in place while another smoke holds $held; the next bring-up removes it"
      fi
      exit 0
      ;;
    take\ *)
      verb=replace
      [[ -s "$tmp/lease.json" ]] || verb=create
      if printf '%s' "${plan#take }" | kubectl "$verb" -f - "${k[@]}" >/dev/null 2>"$tmp/err"; then
        release() {
          kubectl get lease "$lease" "${k[@]}" -o json >"$tmp/held.json" &&
            python3 "$record_py" release "$tmp/held.json" "$holder" >"$tmp/release.json" &&
            { [[ ! -s "$tmp/release.json" ]] || kubectl replace -f "$tmp/release.json" "${k[@]}" >/dev/null; } ||
            echo "[remove-redis-store] WARNING: could not hand back $lease; it expires on its own within a minute" >&2
        }
        trap 'release; rm -rf "$tmp"' EXIT
        # Both the request and the wait for deletion are bounded well inside the
        # fence Lease's minute, so the delete can't outlive the fence.
        kubectl delete deploy/space-redis svc/space-redis --ignore-not-found --timeout=30s \
          --request-timeout=20s "${k[@]}"
        exit 0
      fi
      ;;
  esac
done
echo "[remove-redis-store] could not take $lease to remove the Redis store:" >&2
cat "$tmp/err" >&2
exit 1

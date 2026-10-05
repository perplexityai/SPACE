#!/usr/bin/env bash
#
# Hand the Redis limiter store to the bench on an opt-in bring-up.
#
#   bash infra/space/kind/scripts/claim-redis-store.sh <kube-context> <namespace>
#
# The rate-limiter smoke scenario records on the public API Deployment
# (`space.pplx.net/smoke-limiter-run`) whether it created the store, so that the
# next run can remove a store a killed run left behind. An opt-in bring-up
# (`bootstrap.sh --with-redis`, or the Tiltfile with `SPACE_LOCAL_REDIS=1`)
# applies the chart over whatever is there, keeping the same objects, so without
# this the next run would still read that store as its own and delete it. This
# flips the record's `redis_created` to false; the recorded env is untouched, so
# the next run still restores it.
#
# Runs even while a rate-limiter smoke holds its Lease: the flip only ever keeps
# a store, so it can't hurt a running smoke, and it is what saves the store from
# a killed one whose Lease hasn't expired. A live run still removes a store it
# created when it finishes (its removal is armed in memory), and says so here;
# re-run the opt-in afterwards. The write is guarded by the Deployment's
# resourceVersion, so it can't clobber a record the running smoke rewrote.
#
# A no-op when there is no public API Deployment yet or no record on it.
set -euo pipefail

kctx="${1:?kube context}"
ns="${2:?namespace}"
record_py="$(dirname "${BASH_SOURCE[0]}")/lib/limiter_run_record.py"
k=(-n "$ns" --context "$kctx")
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
lease="$(python3 "$record_py" lease-name "$kctx")"

for _ in 1 2 3; do
  kubectl get deploy -l app.kubernetes.io/name=space-api "${k[@]}" -o json >"$tmp/deploy.json"
  claim="$(python3 "$record_py" claim "$tmp/deploy.json")"
  [[ -n "$claim" ]] || exit 0
  read -r name version record <<<"$claim"
  if kubectl annotate --overwrite --resource-version="$version" "deploy/$name" \
      "space.pplx.net/smoke-limiter-run=$record" "${k[@]}" >/dev/null 2>"$tmp/err"; then
    echo "[claim-redis-store] the Redis store now belongs to the bench; a smoke run won't remove it"
    if kubectl get lease "$lease" "${k[@]}" -o json >"$tmp/lease.json" 2>/dev/null &&
        [[ "$(python3 "$record_py" fence "$tmp/lease.json" - "$lease" "$ns")" == aside\ *\ rate-limiter ]]; then
      echo "[claim-redis-store] a running rate-limiter smoke still removes a store it created when it finishes; re-run the opt-in after it"
    fi
    exit 0
  fi
  grep -qi conflict "$tmp/err" || { cat "$tmp/err" >&2; exit 1; }
done
echo "[claim-redis-store] the record kept changing under the claim:" >&2
cat "$tmp/err" >&2
exit 1

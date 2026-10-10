#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/../lib/release.sh"
action=${1:-status}
shift || true
# Namespace ownership alone does not establish ownership of a Helm release.
# Use Helm's stored identity/manifest and actual proposed upgrade, not a second
# YAML parser or an assumption that --reuse-values preserves rendered objects.
logs_release() {
  python3 - "$1" "$release" "$chart_dir" "$context" "$namespace" <<'PY'
import collections
import json
import re
import subprocess
import sys

action, release, chart, context, namespace = sys.argv[1:]
scope = ["--kube-context", context, "--namespace", namespace]

def helm(*args, values=None):
    return subprocess.run(
        ["helm", *args, *scope], input=values, text=True,
        capture_output=True, check=True,
    ).stdout

def documents(manifest):
    # Preserve every payload byte; only document framing/source comments vary
    # between Helm get/template/dry-run. Conservative refusal beats adoption.
    return collections.Counter(
        re.sub(r"\A# Source: [^\n]*\n", "", part.strip())
        for part in re.split(r"(?m)^---\s*$", manifest) if part.strip()
    )

def alloy(document):
    return bool(re.search(r"(?m)^kind: DaemonSet$", document)
                and re.search(r"(?m)^metadata:\n  name: alloy\n", document))

def without_toggle(values):
    values = json.loads(json.dumps(values or {}))
    if isinstance(values.get("alloy"), dict):
        values["alloy"].pop("enabled", None)
        if not values["alloy"]:
            values.pop("alloy")
    return values

try:
    entries = json.loads(helm("list", "--all", "--filter", f"^{release}$", "-o", "json"))
    if not entries and action == "up":
        print("absent")
        sys.exit(0)
    if (len(entries) != 1 or entries[0].get("name") != release
            or entries[0].get("namespace") != namespace
            or not re.fullmatch(r"space-logs-[0-9]+\.[0-9]+\.[0-9]+", entries[0].get("chart", ""))
            or entries[0].get("status") != "deployed"):
        raise ValueError("Refusing missing, unowned or non-deployed logs release")
    stored = documents(helm("get", "manifest", release))
    all_values = helm("get", "values", release, "--all", "-o", "json")
    expected = documents(helm("template", release, chart, "--is-upgrade", "-f", "-", values=all_values))
    if not stored or stored != expected:
        raise ValueError("Refusing logs release whose stored manifest does not match this chart and its values")
    if action in ("up", "check"):
        print("present")
        sys.exit(0)
    enabled = "true" if action == "resume" else "false"
    upgrade = ["upgrade", release, chart, "--reuse-values", "--wait", "--timeout", "5m",
               "--set", f"alloy.enabled={enabled}"]
    proposed = json.loads(helm(*upgrade, "--dry-run=client", "-o", "json"))
    desired = documents(proposed["manifest"])
    previous_values = json.loads(helm("get", "values", release, "-o", "json"))
    if without_toggle(previous_values) != without_toggle(proposed["config"]):
        raise ValueError("Refusing logs transition that changes values other than alloy.enabled")
    old_other = collections.Counter({doc: count for doc, count in stored.items() if not alloy(doc)})
    new_other = collections.Counter({doc: count for doc, count in desired.items() if not alloy(doc)})
    old_alloy, new_alloy = stored - old_other, desired - new_other
    if (old_other != new_other or sum(old_alloy.values()) > 1
            or sum(new_alloy.values()) != (1 if action == "resume" else 0)
            or (old_alloy and new_alloy and old_alloy != new_alloy)):
        raise ValueError("Refusing logs transition with a non-Alloy render delta")
    desired_values = json.loads(all_values)
    desired_values.setdefault("alloy", {})["enabled"] = action == "resume"
    expected_new = documents(helm("template", release, chart, "--is-upgrade", "-f", "-", values=json.dumps(desired_values)))
    if desired != expected_new:
        raise ValueError("Refusing logs transition with an unrecognized Alloy manifest")
    # Recheck revision immediately before mutation. Never silently apply a plan
    # calculated for another release revision; concurrent operators must retry.
    if json.loads(helm("list", "--all", "--filter", f"^{release}$", "-o", "json")) != entries:
        raise ValueError("Logs release changed during profile reconciliation")
    helm(*upgrade)
except (ValueError, KeyError, TypeError, subprocess.CalledProcessError) as error:
    sys.exit(f"Logs profile guard: {error}")
PY
}
requested_action=$action
[[ $action != up ]] || action=preflight
release_init logs
if [[ $requested_action == up ]]; then
  existing=absent
  [[ ${namespace_absent:-false} == true ]] || existing=$(logs_release up)
  action=up
  namespace_absent=false
  release_init logs
fi
case "$action" in
  up)
    # Fail rather than silently deploying a journal receiver at an absent path.
    # kind reports a cluster with no nodes on stderr and exits 0, so an empty
    # list fails too instead of checking nothing.
    nodes=$(kind get nodes --name "$cluster")
    [[ -n $nodes ]] || { echo "No kind nodes found for cluster \"$cluster\"" >&2; exit 1; }
    while IFS= read -r node; do
      docker exec "$node" test -d /run/log/journal
      docker exec "$node" test -d /var/log/pods
    done <<<"$nodes"
    helm_options "$@"
    if [[ $existing == present && ${#helm_options[@]} == 0 ]]; then
      # Automatic profile bring-up must retain an existing release's values,
      # not reset Loki/storage to this checkout's defaults.
      logs_release resume
      exit 0
    fi
    # Do not advance source cursors while a brand-new Loki is still starting.
    if [[ $existing == absent ]]; then
      # A concurrent same-name install must fail, not become an adoption.
      helm install "$release" "$chart_dir" --kube-context "$context" \
        --namespace "$namespace" --wait --timeout 5m \
        -f "$chart_dir/profiles/kind.yaml" "${helm_options[@]}" --set alloy.enabled=false
    fi
    helm_upgrade 5m
    ;;
  pause|resume|check|preflight)
    # These transitions retain Loki and its values/data. Never adopt another
    # release or install a missing backend when installation was disabled.
    [[ ${namespace_absent:-false} == false ]] || exit 0
    if [[ $action == preflight ]]; then logs_release up; else logs_release "$action"; fi
    ;;
  status) "${kube[@]}" -n "$namespace" get pods,deploy,ds,job,svc,pvc ;;
  ui)
    exec "${kube[@]}" -n "$namespace" port-forward --address 127.0.0.1 svc/loki 13101:3100
    ;;
  restart)
    "${kube[@]}" -n "$namespace" rollout restart daemonset/alloy
    "${kube[@]}" -n "$namespace" rollout status daemonset/alloy --timeout=3m
    ;;
  cleanup)
    confirm_cleanup "${1:-}" 'Requires --confirm-delete-local-data; destroys the local Loki PVC'
    "${kube[@]}" delete namespace "$namespace" --wait=false
    echo 'Node-local Alloy cursors remain at /var/lib/space-logs/alloy; shared logs and other stacks are untouched.'
    ;;
  *) echo 'Usage: local.sh up [Helm options] | preflight | pause | resume | check | status | ui | restart | cleanup --confirm-delete-local-data' >&2; exit 2 ;;
esac

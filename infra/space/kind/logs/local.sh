#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/../lib/release.sh"
action=${1:-status}
shift || true
# Namespace ownership alone does not establish ownership of a Helm release.
# A chart update is not an ownership change. Normal up reconciles the current
# chart; collector-only transitions retain the recorded backend resources.
logs_release() {
  python3 - "$1" "$release" "$chart_dir" "$context" "$namespace" <<'PY'
import collections
import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path

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
    metadata = re.search(r"(?m)^metadata:\n((?:[ \t]+[^\n]*\n)+)", document)
    return bool(re.search(r"""(?m)^kind: ["']?DaemonSet["']?\s*$""", document)
                and metadata
                and re.search(r"""(?m)^  name: ["']?alloy["']?\s*$""", metadata[1]))

def non_collector(manifest):
    return collections.Counter({doc: count for doc, count in manifest.items() if not alloy(doc)})

def manifest_text(manifest):
    return "---\n" + "\n---\n".join(manifest.elements()) + "\n"

def ready(manifest):
    for document in manifest:
        kind = re.search(r"(?m)^kind: (Deployment|DaemonSet|StatefulSet)$", document)
        name = re.search(r"(?m)^metadata:\n  name: ([^\n]+)$", document)
        if kind and name:
            result = subprocess.run(
                ["kubectl", "--context", context, "-n", namespace, "rollout", "status",
                 f"{kind[1].lower()}/{name[1]}", "--timeout=1s"],
                capture_output=True, text=True,
            )
            if result.returncode:
                return False
    return True

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
            or not re.fullmatch(r"space-logs-[0-9]+\.[0-9]+\.[0-9]+", entries[0].get("chart", ""))):
        raise ValueError("Refusing missing or unowned logs release")
    if action in ("up", "check"):
        if action == "check" and entries[0].get("status") != "deployed":
            raise ValueError("Owned logs release is not deployed; run local.sh up to reconcile it")
        print("present")
        sys.exit(0)
    stored = documents(helm("get", "manifest", release))
    all_values = helm("get", "values", release, "--all", "-o", "json")
    if not stored:
        raise ValueError("Owned logs release has no recorded resources; run local.sh up to reconcile it")
    old_other = non_collector(stored)
    old_alloy = stored - old_other
    if sum(old_alloy.values()) > 1:
        raise ValueError("Refusing logs release with multiple Alloy DaemonSets")
    if json.loads(all_values).get("alloy", {}).get("enabled", True) and not old_alloy:
        raise ValueError("Unrecognized stored collector; run local.sh up to reconcile the owned chart")
    enabled = "true" if action == "resume" else "false"
    collector = old_alloy if action == "resume" else collections.Counter()
    if action == "resume" and not collector:
        # A pause retained all backend resources. Prefer the collector from a
        # successful revision with those exact resources, not a new checkout's
        # potentially different config/checksum or security policy.
        history = json.loads(helm("history", release, "--max", "10", "-o", "json"))
        for revision in sorted(history, key=lambda item: int(item["revision"]), reverse=True):
            if (revision.get("status") not in ("deployed", "superseded")
                    or not re.fullmatch(r"space-logs-[0-9]+\.[0-9]+\.[0-9]+", revision.get("chart", ""))):
                continue
            previous = documents(helm("get", "manifest", release, "--revision", str(revision["revision"])))
            candidate = previous - non_collector(previous)
            if non_collector(previous) == old_other and sum(candidate.values()) == 1:
                collector = candidate
                break
        if not collector:
            values = json.loads(all_values)
            values.setdefault("alloy", {})["enabled"] = True
            current = documents(helm("template", release, chart, "--is-upgrade", "-f", "-", values=json.dumps(values)))
            collector = current - non_collector(current)
            if non_collector(current) != old_other or sum(collector.values()) != 1:
                raise ValueError("No matching collector revision; run local.sh up to upgrade the owned logs chart")
    desired = old_other + collector
    if desired == stored and entries[0].get("status") == "deployed" and (action == "pause" or ready(desired)):
        if json.loads(helm("list", "--all", "--filter", f"^{release}$", "-o", "json")) != entries:
            raise ValueError("Logs release changed during profile reconciliation")
        print("unchanged")
        sys.exit(0)
    # Freeze non-collector resources during a profile toggle. This lets a new
    # checkout pause old Alloy without silently upgrading Loki, PVCs, config,
    # or network policies. A normal `up` remains the explicit chart-upgrade path.
    with tempfile.TemporaryDirectory(prefix="space-logs-transition-") as directory:
        renderer = Path(directory) / "retain.py"
        (Path(directory) / "manifest.yaml").write_text(manifest_text(desired))
        renderer.write_text(
            "#!/usr/bin/env python3\nimport sys\nfrom pathlib import Path\n"
            "sys.stdin.read()\n"
            "sys.stdout.write(Path(__file__).with_name('manifest.yaml').read_text())\n"
        )
        renderer.chmod(0o700)
        upgrade = ["upgrade", release, chart, "--reuse-values", "--no-hooks", "--wait", "--timeout", "5m",
                   "--set", f"alloy.enabled={enabled}", "--post-renderer", str(renderer)]
        proposed = json.loads(helm(*upgrade, "--dry-run=client", "-o", "json"))
        previous_values = json.loads(helm("get", "values", release, "-o", "json"))
        if without_toggle(previous_values) != without_toggle(proposed["config"]):
            raise ValueError("Refusing logs transition that changes values other than alloy.enabled")
        if documents(proposed["manifest"]) != desired:
            raise ValueError("Refusing logs transition with a non-Alloy render delta")
        # Helm still owns pending-operation locks. Never steal one or delete
        # release history to recover an interrupted operation.
        if json.loads(helm("list", "--all", "--filter", f"^{release}$", "-o", "json")) != entries:
            raise ValueError("Logs release changed during profile reconciliation")
        helm(*upgrade)
except (ValueError, KeyError, TypeError, subprocess.CalledProcessError) as error:
    if isinstance(error, subprocess.CalledProcessError) and error.stderr:
        sys.exit(f"Logs profile guard: {error.stderr.strip()}")
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
    # Builder nodes' sandbox sidecars stay uncollected. The Kind cluster config
    # labels every node's pool when it creates the node, and a Kind node's
    # journal hostname is its node name. Set on every install and upgrade, so
    # a reused list never outlives the cluster's builder set.
    builders=$("${kube[@]}" get nodes -l space.pplx.net/node-pool=builder \
      -o jsonpath='{range .items[*]}{.metadata.name}{","}{end}')
    builders="collection.journal.sidecars.excludedHosts={${builders%,}}"
    if [[ $existing == present && ${#helm_options[@]} == 0 ]]; then
      # A managed chart upgrade is not a collector-only toggle. Retain the
      # caller's saved overrides while taking this checkout's chart updates.
      helm_options=(--reset-then-reuse-values)
      helm_upgrade 5m --set alloy.enabled=true --set "$builders"
      exit 0
    fi
    # Do not advance source cursors while a brand-new Loki is still starting.
    if [[ $existing == absent ]]; then
      # A concurrent same-name install must fail, not become an adoption.
      helm install "$release" "$chart_dir" --kube-context "$context" \
        --namespace "$namespace" --wait --timeout 5m \
        -f "$chart_dir/profiles/kind.yaml" "${helm_options[@]}" --set alloy.enabled=false \
        --set "$builders"
    fi
    helm_upgrade 5m --set "$builders"
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

"""The OSS observability helpers share one cluster and namespace guard.

That guard is the safety boundary for all four releases: it refuses anything
that is not the verified local Docker Kind bench, and refuses a namespace the
release does not own. These tests stub kubectl, docker, helm and kind so the
refusals can be exercised without a cluster.
"""

import os
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
KIND_DIR = REPO_ROOT / "infra/space/kind"
RELEASES = ("metrics", "logs", "traces", "grafana")

HELM_STUB = r'''
import json
import os
import subprocess
import sys

args = sys.argv[1:]
if os.getenv("STUB_LOG"):
    with open(os.environ["STUB_LOG"], "a") as stream:
        stream.write("helm " + " ".join(args) + "\n")
service = "apiVersion: v1\nkind: Service\nmetadata:\n  name: loki\n"
daemon = "apiVersion: apps/v1\nkind: DaemonSet\nmetadata:\n  name: alloy\nspec: {}\n"
paused = os.getenv("STUB_LOG_PAUSED") == "1"
values = {"profile": "kind", "alloy": {"enabled": not paused}}
def manifest(enabled):
    return service + ("---\n" + daemon if enabled else "")

if args[0] == "list":
    state = os.getenv("STUB_LOG_RELEASE", "absent")
    logs = "^space-logs$" in args
    print(json.dumps([] if state == "absent" or not logs else [{
        "name": "space-logs", "namespace": "space-logs",
        "status": os.getenv("STUB_LOG_STATUS", "deployed"),
        "chart": "foreign-1.0.0" if state == "foreign" else "space-logs-0.2.0",
        "revision": "2" if paused else "1",
    }]))
elif args[:2] == ["get", "manifest"]:
    stored = manifest(True if "--revision" in args else not paused)
    if os.getenv("STUB_LOG_DRIFT"):
        stored = stored.replace("name: loki", "name: loki\n  labels: {previous-chart: yes}")
    print(stored)
elif args[:2] == ["get", "values"]:
    print(json.dumps(values))
elif args[0] == "history":
    print(json.dumps([{"revision": 1, "status": "superseded", "chart": "space-logs-0.2.0"}]))
elif args[0] == "template":
    print(manifest(json.load(sys.stdin)["alloy"]["enabled"]))
elif args[0] == "upgrade" and "--dry-run=client" in args:
    enabled = "alloy.enabled=false" not in args
    values["alloy"]["enabled"] = enabled
    proposed = manifest(enabled)
    if "--post-renderer" in args:
        proposed = subprocess.run(
            [args[args.index("--post-renderer") + 1]], input=proposed,
            capture_output=True, text=True, check=True,
        ).stdout
    if os.getenv("STUB_NON_ALLOY_DELTA"):
        proposed = proposed.replace("name: loki", "name: replaced-storage")
    print(json.dumps({"manifest": proposed, "config": values}))
'''

STUBS = {
    "kubectl": """
        echo "kubectl $*" >> "$STUB_LOG"
        case "$*" in
          *"config view"*) echo "${STUB_SERVER:-https://127.0.0.1:6443}" ;;
          *"get namespace"*"space-local-owner"*)
            if [[ "${STUB_DYNAMIC_OWNER:-0}" == 1 ]]; then
              for signal in metrics traces logs grafana; do
                if [[ "$*" == *"get namespace space-$signal "* ]]; then
                  echo "${STUB_OWNER_OVERRIDE:-$signal}"
                fi
              done
            else echo "${STUB_OWNER}"; fi ;;
          *"get namespace"*)
            if [[ "${STUB_NS_EXISTS:-0}" == 0 ]]; then echo namespace/task;
            elif [[ "${STUB_NS_EXISTS}" != 1 ]]; then exit "$STUB_NS_EXISTS"; fi ;;
          *"get svc collector"*)
            [[ "${STUB_RECEIVER_ABSENT:-0}" == 0 ]] || exit 1
            if [[ "$*" == *"-n space-metrics "* ]]; then
              echo "${STUB_RECEIVER_ADDRESS:-10.96.0.20}"
            else echo "${STUB_RECEIVER_ADDRESS:-10.96.0.21}"; fi ;;
        esac
    """,
    "docker": """
        echo "docker $*" >> "$STUB_LOG"
        case "$*" in
          *inspect*) echo "${STUB_KIND_LABEL:-space-local}" ;;
          *"port space-local-control-plane"*) echo "${STUB_PORT:-127.0.0.1:6443}" ;;
        esac
    """,
    "helm": HELM_STUB,
    "kind": 'echo "kind $*" >> "$STUB_LOG"; echo space-local-control-plane',
}


class HelperGuardTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="kind-oss-helpers-")
        self.addCleanup(temporary.cleanup)
        self.tmp = Path(temporary.name)
        self.bin = self.tmp / "bin"
        self.bin.mkdir(parents=True, exist_ok=True)
        for name, body in STUBS.items():
            path = self.bin / name
            interpreter = "python3" if name == "helm" else "bash"
            path.write_text(f"#!/usr/bin/env {interpreter}\n" + textwrap.dedent(body))
            path.chmod(0o755)
        self.log = self.tmp / "commands.log"

    def run_helper(self, release, *args, **env):
        return self.run_script(KIND_DIR / release / "local.sh", release, *args, **env)

    def run_installer(self, **env):
        return self.run_script(
            KIND_DIR / "scripts/install-observability.sh", "logs",
            STUB_DYNAMIC_OWNER="1", **env,
        )

    def run_script(self, script, owner, *args, **env):
        self.log.write_text("")
        environment = {
            **{key: value for key, value in os.environ.items() if not key.startswith("SPACE_KIND_")},
            "PATH": f"{self.bin}:{os.environ['PATH']}",
            "STUB_LOG": str(self.log),
            # The directory is the short name; the namespace ownership label
            "STUB_OWNER": owner,
            "SPACE_KIND_DATADOG_MODE": "off",
            **env,
        }
        result = subprocess.run(
            ["bash", str(script), *args],
            capture_output=True, text=True, env=environment, check=False,
        )
        return result, self.log.read_text()

    def test_every_release_shares_the_library(self):
        for release in RELEASES:
            with self.subTest(release=release):
                source = (KIND_DIR / release / "local.sh").read_text()
                self.assertIn("lib/release.sh", source)

    def test_status_verifies_the_cluster_before_touching_it(self):
        for release in RELEASES:
            with self.subTest(release=release):
                result, log = self.run_helper(release, "status")
                self.assertEqual(result.returncode, 0, result.stderr)
                # The three independent identity checks, before any read of the
                # release's own resources.
                self.assertNotIn("apply -f", log)
                self.assertIn("config view", log)
                self.assertIn("docker inspect space-local-control-plane", log)
                self.assertIn("docker port space-local-control-plane", log)

    def test_refuses_a_non_loopback_api_server(self):
        for release in RELEASES:
            with self.subTest(release=release):
                result, _ = self.run_helper(
                    release, "status", STUB_SERVER="https://10.0.0.1:6443")
                self.assertEqual(result.returncode, 1)
                self.assertIn("non-loopback", result.stderr)

    def test_refuses_a_context_addressing_another_cluster(self):
        for release in RELEASES:
            with self.subTest(release=release):
                result, _ = self.run_helper(
                    release, "status", STUB_PORT="127.0.0.1:7777")
                self.assertEqual(result.returncode, 1)
                self.assertIn("does not address this Docker Kind cluster", result.stderr)

    def test_refuses_a_namespace_owned_by_another_release(self):
        for release in RELEASES:
            with self.subTest(release=release):
                result, log = self.run_helper(release, "up", STUB_OWNER="someone-else")
                self.assertNotIn("apply -f", log)
                self.assertNotIn("helm upgrade", log)
                self.assertEqual(result.returncode, 1)
                self.assertIn("Refusing unowned namespace", result.stderr)
        # A correctly labelled namespace is not proof of release ownership.
        # The second negative keeps that identity valid but changes a non-Alloy
        # object in the proposed upgrade. Neither may reach a real mutation.
        cases = [(action, {"STUB_LOG_RELEASE": "foreign"})
                 for action in ("preflight", "up", "check", "pause", "resume")]
        cases += [(action, {"STUB_LOG_RELEASE": "owned", "STUB_NON_ALLOY_DELTA": "1",
                            "STUB_LOG_PAUSED": "1" if action == "resume" else "0"})
                  for action in ("pause", "resume")]
        for action, extra in cases:
            result, log = self.run_helper("logs", action, **extra)
            self.assertNotEqual(result.returncode, 0, result.stdout)
            self.assertIn("Logs profile guard", result.stderr)
            self.assertFalse(any(line.startswith("helm upgrade") and "--dry-run" not in line
                                 for line in log.splitlines()), log)
            self.assertNotIn("helm install", log)
            self.assertNotIn("apply -f", log)

    def test_refuses_an_absent_namespace_unless_creating(self):
        for release in RELEASES:
            with self.subTest(release=release):
                result, _ = self.run_helper(release, "status", STUB_NS_EXISTS="1")
                self.assertEqual(result.returncode, 1)
                self.assertIn("Task namespace does not exist", result.stderr)

    def test_cleanup_requires_explicit_data_loss_consent(self):
        for release in RELEASES:
            with self.subTest(release=release):
                result, log = self.run_helper(release, "cleanup")
                self.assertEqual(result.returncode, 1)
                self.assertIn("--confirm-delete-local-data", result.stderr)
                self.assertNotIn("delete namespace", log)

    def test_up_applies_the_kind_overlay_and_rejects_stray_flags(self):
        for release in RELEASES:
            with self.subTest(release=release):
                result, log = self.run_helper(release, "up")
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn("apply -f", log)
                self.assertLess(log.index("space-local-owner"), log.index("apply -f"))
                self.assertIn("profiles/kind.yaml", log)
                self.assertIn("helm dependency build", log)
                result, _ = self.run_helper(release, "up", "--dangerous")
                self.assertEqual(result.returncode, 2)
                self.assertIn("Unsupported Helm option", result.stderr)

    def test_reuse_values_resets_stale_chart_defaults(self):
        # The plain --reuse-values keeps defaults from an older chart revision.
        _, log = self.run_helper("traces", "up", "--reuse-values")
        self.assertIn("--reset-then-reuse-values", log)
        self.assertNotIn(" --reuse-values", log)
        for action, paused, expected in (("pause", "0", 1), ("pause", "1", 0),
                                          ("resume", "0", 0), ("resume", "1", 1),
                                          ("check", "0", 0)):
            for drift in ("", "1"):
                result, log = self.run_helper(
                    "logs", action, STUB_LOG_RELEASE="owned",
                    STUB_LOG_PAUSED=paused, STUB_LOG_DRIFT=drift,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                actual = [line for line in log.splitlines()
                          if line.startswith("helm upgrade") and "--dry-run" not in line]
                self.assertEqual(len(actual), expected, log)
        # An owned failed release is recoverable with normal up, not a reason
        # to delete Loki data. A new checkout can also upgrade an old manifest.
        for state, drift, expected in (("deployed", "", 0), ("deployed", "1", 1),
                                        ("failed", "", 1)):
            result, log = self.run_helper(
                "logs", "up", STUB_LOG_RELEASE="owned",
                STUB_LOG_STATUS=state, STUB_LOG_DRIFT=drift,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            actual = [line for line in log.splitlines()
                      if line.startswith("helm upgrade") and "--dry-run" not in line]
            self.assertEqual(len(actual), expected, log)
            self.assertNotIn("delete namespace", log)

    def test_native_profiles_do_not_create_missing_backends(self):
        for mode in ("datadog", "off"):
            with self.subTest(mode=mode):
                result, log = self.run_installer(
                    SPACE_KIND_TELEMETRY_MODE=mode, STUB_NS_EXISTS="1",
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                for mutation in ("helm upgrade", "helm install", "apply -f", "docker pull"):
                    self.assertNotIn(mutation, log)

    def test_otel_installs_only_selected_local_receivers(self):
        for external in (False, True):
            with self.subTest(external=external):
                settings = {
                    "SPACE_KIND_OTLP_METRICS_ENDPOINT": "http://192.0.2.10:4317",
                    "SPACE_KIND_OTLP_TRACES_ENDPOINT": "http://192.0.2.11:4317",
                } if external else {}
                result, log = self.run_installer(SPACE_KIND_TELEMETRY_MODE="otel", **settings)
                self.assertEqual(result.returncode, 0, result.stderr)
                for signal in ("metrics", "traces"):
                    self.assertEqual(f"helm upgrade --install space-{signal}" in log, not external)
                self.assertIn("helm install space-logs", log)
                self.assertIn("helm upgrade --install space-grafana", log)
                self.assertLess(log.index("helm install space-logs"), log.index("helm upgrade --install space-grafana"))
                if external:
                    self.assertIn("datasources.prometheus.enabled=false", log)
                    self.assertIn("datasources.tempo.enabled=false", log)
                else:
                    self.assertLess(log.index("space-metrics "), log.index("helm install space-logs"))
                    self.assertIn("datasources.prometheus.enabled=true", log)
                    self.assertIn("datasources.tempo.enabled=true", log)
                self.assertNotIn("delete namespace", log)

    def test_reuse_only_checks_fixed_receivers_even_with_legacy_host_override(self):
        result, log = self.run_installer(
            SPACE_KIND_TELEMETRY_MODE="otel", SPACE_KIND_OBSERVABILITY="off",
            SPACE_KIND_SHIM_OTLP_ENDPOINT="http://127.0.0.1:4317",
            STUB_LOG_RELEASE="owned",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        for signal in ("metrics", "traces"):
            self.assertIn(f"-n space-{signal} get svc collector", log)
        actual = [
            line for line in log.splitlines()
            if line.startswith("helm upgrade") and "--dry-run" not in line
        ]
        self.assertEqual(actual, [])
        self.assertNotIn("apply -f", log)
        self.assertNotIn("docker pull", log)

    def test_reuse_only_refuses_absent_or_wrong_local_receiver(self):
        for failure in (
            {"STUB_RECEIVER_ABSENT": "1"},
            {"STUB_RECEIVER_ADDRESS": "192.0.2.99"},
            {"STUB_OWNER_OVERRIDE": "foreign"},
            {"STUB_NS_EXISTS": "1"},
        ):
            with self.subTest(failure=failure):
                result, log = self.run_installer(
                    SPACE_KIND_TELEMETRY_MODE="otel", SPACE_KIND_OBSERVABILITY="off",
                    STUB_LOG_RELEASE="owned", **failure,
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(any(
                    line.startswith(("helm install", "helm upgrade")) and "--dry-run" not in line
                    for line in log.splitlines()
                ), log)

    def test_external_reuse_requires_logs_but_no_local_metrics_or_traces(self):
        result, log = self.run_installer(
            SPACE_KIND_TELEMETRY_MODE="otel", SPACE_KIND_OBSERVABILITY="off",
            SPACE_KIND_OTLP_METRICS_ENDPOINT="http://192.0.2.10:4317",
            SPACE_KIND_OTLP_TRACES_ENDPOINT="http://192.0.2.11:4317",
            STUB_LOG_RELEASE="owned",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn("get svc collector", log)
        self.assertNotIn("--install", log)
        self.assertNotIn("helm upgrade", log)

    def test_foreign_logs_are_refused_before_image_loading_or_backend_installs(self):
        result, log = self.run_installer(
            SPACE_KIND_TELEMETRY_MODE="otel", STUB_LOG_RELEASE="foreign",
        )
        self.assertNotEqual(result.returncode, 0)
        for mutation in ("helm install", "helm upgrade", "docker pull", "apply -f"):
            self.assertNotIn(mutation, log)


if __name__ == "__main__":
    unittest.main()

"""Guarded authority restart and enrollment faults, never master key material."""

from collections.abc import Callable, Mapping
import json
import time
from typing import Protocol

from space_smoke.runtime import CommandResult

DEPLOYMENT = "deployment/space-credmgr-authority"
SELECTOR = "app.kubernetes.io/name=space-credmgr-authority"
REPLICAS_RECORD = "space.pplx.net/smoke-anchor-replicas"
PHASE_RECORD = "space.pplx.net/smoke-anchor-phase"
RECORDS = (REPLICAS_RECORD, PHASE_RECORD)


class Kubectl(Protocol):
    def __call__(self, *args: str, timeout: float | None = None) -> CommandResult: ...


Guard = Callable[[str, Mapping[str, str | None] | None], None]


class AnchorRecoveryDriver:
    def __init__(self, kubectl: Kubectl, guard: Guard) -> None:
        self.kubectl = kubectl
        self.guard = guard

    def checked(self, *args: str, timeout: float = 180) -> str:
        result = self.kubectl(*args, timeout=timeout)
        if result.returncode:
            raise RuntimeError(f"anchor authority operation failed: {result.detail()}")
        return result.stdout.strip()

    def write(self, *args: str) -> str:
        self.guard("sandbox anchor fixture mutation", None)
        return self.checked(*args)

    def stop_authority(self) -> None:
        self.write("scale", DEPLOYMENT, "--replicas=0")
        self.checked("rollout", "status", DEPLOYMENT, "--timeout=120s", timeout=130)
        pods = self.checked("get", "pods", "-l", SELECTOR, "-o", "name").splitlines()
        if pods:
            self.checked("wait", "--for=delete", *pods, "--timeout=120s", timeout=130)

    def restore(self, replicas: int) -> None:
        if not 1 <= replicas <= 5:
            raise RuntimeError("invalid recorded authority replica count")
        self.stop_authority()
        self.write("scale", DEPLOYMENT, f"--replicas={replicas}")
        self.checked("rollout", "status", DEPLOYMENT, "--timeout=150s", timeout=160)
        self.guard("anchor authority cleanup complete", {name: None for name in RECORDS})

    def repair(self, recorded: Mapping[str, object]) -> None:
        replicas = recorded.get(REPLICAS_RECORD)
        if not isinstance(replicas, str):
            return
        self.restore(int(replicas))

    def run(self) -> Mapping[str, str]:
        deployment = json.loads(self.checked("get", DEPLOYMENT, "-o", "json"))
        containers = deployment["spec"]["template"]["spec"]["containers"]
        enabled = any(
            item.get("name") == "SPACE_ANCHOR_KMS_KEY_ARN" and item.get("value")
            for container in containers for item in container.get("env", [])
        )
        replicas = int(deployment["spec"].get("replicas", 1))
        if not enabled or replicas != 2:
            raise RuntimeError("recovery proof requires two configured authority replicas")
        self.guard("record authority replicas before cold restart", {
            REPLICAS_RECORD: str(replicas), PHASE_RECORD: "armed",
        })
        self.restore(replicas)
        return {"restored_authority_replicas": str(replicas)}

    def startup_failed(self) -> bool:
        """Observe process failure/readiness through Kubernetes, never log text."""
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            pods = json.loads(self.checked("get", "pods", "-l", SELECTOR, "-o", "json"))
            statuses = [
                status for pod in pods["items"]
                for status in pod.get("status", {}).get("containerStatuses", [])
            ]
            failed = any(
                status.get("restartCount", 0) > 0
                and status.get("lastState", {}).get("terminated", {}).get("exitCode", 0) != 0
                for status in statuses
            )
            if any(status.get("ready", False) for status in statuses):
                return False
            if failed:
                return True
            time.sleep(2)
        return False


FLAG_RELEASES = ("space", "space-builder")
FLAG_RECORD = "space.pplx.net/smoke-anchor-enrollment"
ENROLLMENT_FLAG = "space-sandbox-scoped-trust"


class EnrollmentDriver:
    """Change the actual SDK flag file under a fenced, repairable fixture lease."""

    def __init__(self, kubectl: Kubectl, guard: Guard) -> None:
        self.control = AnchorRecoveryDriver(kubectl, guard)

    def current(self) -> str:
        captured = {}
        for release in FLAG_RELEASES:
            resource = json.loads(self.control.checked(
                "get", f"configmap/{release}-sandbox-feature-flags", "-o", "json",
            ))
            captured[release] = str(resource["data"]["flags.json"])
        return json.dumps(captured)

    def restore(self, value: str) -> None:
        captured = json.loads(value)
        if not isinstance(captured, dict) or set(captured) != set(FLAG_RELEASES):
            raise RuntimeError("invalid recorded enrollment flag releases")
        if any(not isinstance(flags, str) for flags in captured.values()):
            raise RuntimeError("invalid recorded enrollment flag payload")
        for release in FLAG_RELEASES:
            self.control.write(
                "patch", f"configmap/{release}-sandbox-feature-flags", "--type=merge",
                "-p", json.dumps({"data": {"flags.json": captured[release]}}),
            )
        # Production receives Eppo updates. The local file-provider fixture
        # restarts managers to fence ConfigMap projection; guest processes and
        # host-owned enrollment records remain intact.
        for release in FLAG_RELEASES:
            workload = f"daemonset/{release}-sandbox-manager"
            self.control.write("rollout", "restart", workload)
            self.control.checked("rollout", "status", workload, "--timeout=180s", timeout=190)

    def set(self, enabled: bool) -> str:
        previous = self.current()
        updated = {}
        for release, flags in json.loads(previous).items():
            value = json.loads(flags)
            value["flags"][ENROLLMENT_FLAG]["defaultVariant"] = "on" if enabled else "off"
            updated[release] = json.dumps(value)
        self.control.guard("record original enrollment flag", {FLAG_RECORD: previous})
        self.restore(json.dumps(updated))
        return previous

    def repair(self, annotations: Mapping[str, object]) -> None:
        previous = annotations.get(FLAG_RECORD)
        if isinstance(previous, str):
            self.restore(previous)
            self.control.guard("enrollment cleanup complete", {FLAG_RECORD: None})

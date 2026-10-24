"""Guarded loss/recovery of the public epoch catalog, never master key material."""

from collections.abc import Callable, Mapping
import hashlib
import json
import time
from typing import Protocol

from space_smoke.runtime import CommandResult

DEPLOYMENT = "deployment/space-credmgr-authority"
SELECTOR = "app.kubernetes.io/name=space-credmgr-authority"
CATALOG = "configmap/space-anchor-epochs"
CATALOG_RECORD = "space.pplx.net/smoke-anchor-catalog"
LABELS_RECORD = "space.pplx.net/smoke-anchor-catalog-labels"
REPLICAS_RECORD = "space.pplx.net/smoke-anchor-catalog-replicas"
PHASE_RECORD = "space.pplx.net/smoke-anchor-catalog-phase"
RECORDS = (CATALOG_RECORD, LABELS_RECORD, REPLICAS_RECORD, PHASE_RECORD)


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
            raise RuntimeError(f"anchor catalog recovery failed: {result.detail()}")
        return result.stdout.strip()

    def write(self, *args: str) -> str:
        self.guard("sandbox anchor catalog mutation", None)
        return self.checked(*args)

    def stop_authority(self) -> None:
        self.write("scale", DEPLOYMENT, "--replicas=0")
        self.checked("rollout", "status", DEPLOYMENT, "--timeout=120s", timeout=130)
        pods = self.checked("get", "pods", "-l", SELECTOR, "-o", "name").splitlines()
        if pods:
            self.checked("wait", "--for=delete", *pods, "--timeout=120s", timeout=130)

    def restore(self, catalog: str, labels: str, replicas: int) -> None:
        if not 1 <= replicas <= 5:
            raise RuntimeError("invalid recorded authority replica count")
        parsed = json.loads(catalog)
        if parsed.get("scheme") != 2 or not parsed.get("epochs"):
            raise RuntimeError("invalid recorded epoch catalog")
        # Refuse a truncated/backed-up secret masquerading as this public catalog.
        if set(parsed) != {"scheme", "environment", "epochs"}:
            raise RuntimeError("unexpected fields in public epoch catalog")
        self.stop_authority()
        self.write("delete", CATALOG, "--ignore-not-found=true")
        self.write("create", "configmap", "space-anchor-epochs", f"--from-literal=epochs.json={catalog}")
        restored_labels = json.loads(labels)
        if restored_labels:
            self.write(
                "label", CATALOG, "--overwrite",
                *(f"{key}={value}" for key, value in restored_labels.items()),
            )
        self.write("scale", DEPLOYMENT, f"--replicas={replicas}")
        self.checked("rollout", "status", DEPLOYMENT, "--timeout=150s", timeout=160)
        self.guard("anchor catalog cleanup complete", {name: None for name in RECORDS})

    def repair(self, recorded: Mapping[str, object]) -> None:
        catalog = recorded.get(CATALOG_RECORD)
        if not isinstance(catalog, str) or not catalog:
            return
        self.restore(
            catalog, str(recorded.get(LABELS_RECORD, "{}")),
            int(str(recorded.get(REPLICAS_RECORD, "0"))),
        )

    def run(self) -> Mapping[str, str]:
        deployment = json.loads(self.checked("get", DEPLOYMENT, "-o", "json"))
        containers = deployment["spec"]["template"]["spec"]["containers"]
        enabled = any(
            item.get("name") == "SPACE_SPLIT_TRUST_PROTOTYPE" and item.get("value") == "true"
            for container in containers for item in container.get("env", [])
        )
        replicas = int(deployment["spec"].get("replicas", 1))
        if not enabled or replicas != 2:
            raise RuntimeError("catalog proof requires two enabled authority replicas")
        resource = json.loads(self.checked("get", CATALOG, "-o", "json"))
        catalog = resource["data"]["epochs.json"]
        labels = json.dumps(resource["metadata"].get("labels", {}), sort_keys=True)
        self.guard("record public catalog before fault", {
            CATALOG_RECORD: catalog, LABELS_RECORD: labels,
            REPLICAS_RECORD: str(replicas), PHASE_RECORD: "armed",
        })
        fail_closed = False
        try:
            self.stop_authority()
            # Capture the current committed catalog after freezing replicas;
            # a later holder can recover it from the fenced Lease.
            resource = json.loads(self.checked("get", CATALOG, "-o", "json"))
            catalog = resource["data"]["epochs.json"]
            labels = json.dumps(resource["metadata"].get("labels", {}), sort_keys=True)
            self.guard("arm catalog deletion", {
                CATALOG_RECORD: catalog, LABELS_RECORD: labels,
                REPLICAS_RECORD: str(replicas), PHASE_RECORD: "deleting",
            })
            self.write("delete", CATALOG)
            self.write("scale", DEPLOYMENT, "--replicas=1")
            deadline = time.monotonic() + 90
            while time.monotonic() < deadline:
                pods = json.loads(self.checked("get", "pods", "-l", SELECTOR, "-o", "json"))
                statuses = [
                    status for pod in pods["items"]
                    for status in pod.get("status", {}).get("containerStatuses", [])
                    if status["name"] in {container["name"] for container in containers}
                ]
                failed = any(
                    status.get("restartCount", 0) > 0
                    and status.get("lastState", {}).get("terminated", {}).get("exitCode", 0) != 0
                    for status in statuses
                )
                missing = not self.checked("get", CATALOG, "--ignore-not-found=true", "-o", "name")
                if failed and missing and not any(status.get("ready", False) for status in statuses):
                    fail_closed = True
                    break
                time.sleep(2)
            if not fail_closed:
                raise RuntimeError("missing-catalog startup did not fail closed")
        finally:
            self.restore(catalog, labels, replicas)
        recovered = json.loads(self.checked("get", CATALOG, "-o", "json"))["data"]["epochs.json"]
        if recovered != catalog:
            raise RuntimeError("recovered public catalog differs from its committed copy")
        return {
            "recovered_catalog_sha256": hashlib.sha256(catalog.encode()).hexdigest(),
            "missing_catalog_failed_closed": str(fail_closed).lower(),
            "restored_authority_replicas": str(replicas),
        }

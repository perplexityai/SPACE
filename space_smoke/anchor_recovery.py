"""Guarded live-Secret-loss probe. Only metadata leaves the Kubernetes API.

The caller supplies an isolated-Kind guard and a resourceVersion-fenced mutation
Lease. Recovery coordinates, never keys, remain on the Lease until cleanup
succeeds so a later holder can repair an interrupted test.
"""

from collections.abc import Callable, Mapping
import json
import re
import time
from typing import Protocol

from space_smoke.runtime import CommandResult

DEPLOYMENT = "deployment/space-credmgr-authority"
SELECTOR = "app.kubernetes.io/name=space-credmgr-authority"
SECRET = "secret/space-anchor-keyring"
BACKUP_RECORD = "space.pplx.net/smoke-anchor-recovery-object"
REPLICAS_RECORD = "space.pplx.net/smoke-anchor-recovery-replicas"
BOOTSTRAP_RECORD = "space.pplx.net/smoke-anchor-recovery-bootstrap"
PHASE_RECORD = "space.pplx.net/smoke-anchor-recovery-phase"
RECORDS = (BACKUP_RECORD, REPLICAS_RECORD, BOOTSTRAP_RECORD, PHASE_RECORD)


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
            raise RuntimeError(f"anchor recovery operation failed: {result.detail()}")
        return result.stdout.strip()

    def write(self, *args: str) -> str:
        self.guard("sandbox anchor recovery mutation", None)
        return self.checked(*args)

    def annotation(self, suffix: str) -> str:
        # Deliberately never request Secret.data in harness output.
        return self.checked(
            "get",
            SECRET,
            "-o",
            "jsonpath={.metadata.annotations.space\\.pplx\\.net/" + suffix + "}",
        )

    def stop_authority(self) -> None:
        self.write("scale", DEPLOYMENT, "--replicas=0")
        self.checked("rollout", "status", DEPLOYMENT, "--timeout=120s", timeout=130)
        pods = self.checked("get", "pods", "-l", SELECTOR, "-o", "name").splitlines()
        if pods:
            self.checked("wait", "--for=delete", *pods, "--timeout=120s", timeout=130)

    def committed_backup(self) -> tuple[str, str]:
        value = self.checked(
            "get",
            SECRET,
            "-o",
            "jsonpath={.metadata.annotations.space\\.pplx\\.net/anchor-backup-object}"
            '{"\\n"}{.metadata.annotations.space\\.pplx\\.net/anchor-keyring-hash}',
        )
        parts = value.splitlines()
        if len(parts) != 2 or not parts[0].endswith(f"/{parts[1]}.kms"):
            raise RuntimeError("committed backup metadata does not match keyring hash")
        return parts[0], parts[1]

    def restore(self, backup: str, replicas: int, bootstrap: str) -> None:
        if not re.fullmatch(r"sandbox-anchor/v[1-9][0-9]*/[0-9a-f]{64}\.kms", backup):
            raise RuntimeError("refusing an invalid anchor backup object")
        if not 1 <= replicas <= 5 or bootstrap not in ("true", "false"):
            raise RuntimeError(
                "refusing invalid recorded authority deployment settings"
            )
        self.stop_authority()
        self.write("delete", SECRET, "--ignore-not-found=true")
        self.write(
            "set",
            "env",
            DEPLOYMENT,
            f"SPACE_ANCHOR_RECOVER_OBJECT={backup}",
            "SPACE_ANCHOR_BOOTSTRAP=false",
        )
        self.write("scale", DEPLOYMENT, f"--replicas={replicas}")
        self.checked("rollout", "status", DEPLOYMENT, "--timeout=150s", timeout=160)
        if self.annotation("anchor-recovered-from") != backup:
            raise RuntimeError(
                "authority did not report recovery from the selected encrypted backup"
            )
        self.restore_deployment(replicas, bootstrap)

    def restore_deployment(self, replicas: int, bootstrap: str) -> None:
        if not 1 <= replicas <= 5 or bootstrap not in ("true", "false"):
            raise RuntimeError(
                "refusing invalid recorded authority deployment settings"
            )
        self.write(
            "set",
            "env",
            DEPLOYMENT,
            "SPACE_ANCHOR_RECOVER_OBJECT-",
            f"SPACE_ANCHOR_BOOTSTRAP={bootstrap}",
        )
        self.write("scale", DEPLOYMENT, f"--replicas={replicas}")
        self.checked("rollout", "status", DEPLOYMENT, "--timeout=150s", timeout=160)
        self.guard("anchor recovery cleanup complete", {name: None for name in RECORDS})

    def repair(self, recorded: Mapping[str, object]) -> None:
        backup = recorded.get(BACKUP_RECORD)
        if not isinstance(backup, str) or not backup:
            return
        replicas = int(str(recorded.get(REPLICAS_RECORD, "0")))
        bootstrap = str(recorded.get(BOOTSTRAP_RECORD, ""))
        if recorded.get(PHASE_RECORD) == "armed":
            # The previous holder never armed Secret deletion. Do not roll a
            # still-valid live ring back to an earlier candidate.
            self.restore_deployment(replicas, bootstrap)
        elif recorded.get(PHASE_RECORD) == "deleting":
            self.restore(backup, replicas, bootstrap)
        else:
            raise RuntimeError("refusing recovery with an unknown recorded fault phase")

    def run(self) -> Mapping[str, str]:
        deployment = json.loads(self.checked("get", DEPLOYMENT, "-o", "json"))
        containers = deployment["spec"]["template"]["spec"]["containers"]
        environment = {
            item["name"]: item.get("value", "")
            for container in containers
            for item in container.get("env", [])
        }
        if environment.get("SPACE_SPLIT_TRUST_PROTOTYPE") != "true":
            raise RuntimeError(
                "refusing recovery probe: split-trust prototype is not enabled"
            )
        if environment.get("SPACE_ANCHOR_RECOVER_OBJECT"):
            raise RuntimeError("refusing recovery probe during an operator recovery")
        replicas = int(deployment["spec"].get("replicas", 1))
        if replicas != 2:
            raise RuntimeError(
                "the replica/recovery proof requires exactly two authority replicas"
            )
        bootstrap = environment.get("SPACE_ANCHOR_BOOTSTRAP", "false")
        backup, digest = self.committed_backup()
        self.guard(
            "record anchor recovery before deleting live Secret",
            {
                BACKUP_RECORD: backup,
                REPLICAS_RECORD: str(replicas),
                BOOTSTRAP_RECORD: bootstrap,
                PHASE_RECORD: "armed",
            },
        )
        fail_closed = False
        deletion_armed = False
        try:
            # Freeze writers and capture again before deletion. This is the
            # committed object, not a candidate backup from a CAS loser.
            self.stop_authority()
            backup, digest = self.committed_backup()
            self.guard(
                "record frozen anchor recovery coordinates",
                {
                    BACKUP_RECORD: backup,
                    REPLICAS_RECORD: str(replicas),
                    BOOTSTRAP_RECORD: bootstrap,
                    PHASE_RECORD: "deleting",
                },
            )
            deletion_armed = True
            self.write("delete", SECRET)
            self.write(
                "set",
                "env",
                DEPLOYMENT,
                "SPACE_ANCHOR_RECOVER_OBJECT-",
                "SPACE_ANCHOR_BOOTSTRAP=true",
            )
            self.write("scale", DEPLOYMENT, "--replicas=1")
            deadline = time.monotonic() + 90
            while time.monotonic() < deadline:
                pods = json.loads(
                    self.checked("get", "pods", "-l", SELECTOR, "-o", "json")
                )
                statuses = [
                    status
                    for pod in pods["items"]
                    for status in pod.get("status", {}).get("containerStatuses", [])
                    if status["name"] in {container["name"] for container in containers}
                ]
                failed_start = any(
                    status.get("restartCount", 0) > 0
                    and status.get("lastState", {})
                    .get("terminated", {})
                    .get("exitCode", 0)
                    != 0
                    for status in statuses
                )
                missing = not self.checked(
                    "get", SECRET, "--ignore-not-found=true", "-o", "name"
                )
                if failed_start and missing and not any(
                    status.get("ready", False) for status in statuses
                ):
                    fail_closed = True
                    break
                time.sleep(2)
            if not fail_closed:
                logs = self.kubectl(
                    "logs", DEPLOYMENT, "--all-containers=true", "--tail=40", timeout=15
                )
                raise RuntimeError(
                    "missing-Secret startup did not fail closed as expected: "
                    + logs.detail()
                )
        finally:
            if deletion_armed:
                self.restore(backup, replicas, bootstrap)
            else:
                self.restore_deployment(replicas, bootstrap)
        return {
            "encrypted_backup_object": backup,
            "recovered_keyring_sha256": digest,
            "missing_secret_failed_closed": str(fail_closed).lower(),
            "restored_authority_replicas": str(replicas),
        }

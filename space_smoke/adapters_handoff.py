"""Kind-only transport for the real volume-manager graceful-drain fixture."""

from __future__ import annotations

import json
import shlex
import shutil
import tempfile
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

from space_smoke.autosuspend_handoff import HandoffObservation
from space_smoke.capabilities import SandboxPodPlacement
from space_smoke.runtime import SmokeConfigurationError

if TYPE_CHECKING:
    from space_smoke.adapters import KindHarness


class KindAutosuspendHandoffControl:
    FINALIZER = "smoke.space.dev/volume-handoff"

    def __init__(self, harness: KindHarness) -> None:
        self.harness = harness
        self.directory: str | None = None
        self.object_store_pod: str | None = None
        self.object_store_node: str | None = None
        self.object_store_container: str | None = None
        self.old_volume_pod: str | None = None
        self.drain_job_id: str | None = None
        self.predecessor_logs = ""
        self.holds_successor = False
        self.successor_daemonset: str | None = None
        self.successor_strategy: dict[str, Any] | None = None
        self.successor_affinity: dict[str, Any] | None = None
        self.runtime_identity: tuple[int, str] | None = None
        self.carrier_identity: tuple[int, str] | None = None

    def _process_identity(
        self, node: str, pid: int, *, expected_name: str | None = None
    ) -> tuple[bool, str | None]:
        stat = self._shell(
            node,
            f"cat /proc/{pid}/stat 2>/dev/null || true",
            "observe a task-owned process identity",
        ).strip()
        if not stat:
            return False, None
        fields = stat.rsplit(")", 1)[1].split()
        name = stat.split("(", 1)[1].rsplit(")", 1)[0]
        return fields[0] != "Z" and (
            expected_name is None or name == expected_name
        ), fields[19]

    def _uploads(self, placement: SandboxPodPlacement) -> tuple[dict[str, object], ...]:
        self.harness._require_isolated_kind("read task-owned handoff evidence")
        # The privileged Kind operator shell shares the node's PID namespace.
        # Use its node-provided Python without installing tools in a service.
        # Select only this fixture's identity and recovery state, not secrets.
        script = """
import json, sqlite3, sys
db = sqlite3.connect("file:/space/run/sandbox-manager/sbx-manager.db?mode=ro", uri=True)
db.execute("PRAGMA query_only=ON")
rows = db.execute(
    "SELECT job_id, status, json_extract(payload, '$.snapshot_id'), "
    "json_extract(payload, '$.meta.local_artifact_id'), "
    "json_extract(payload, '$.job_ids'), json_extract(payload, '$.auto_suspend') "
    "FROM jobs WHERE subject_id = ? AND kind = 'snapshot_upload' ORDER BY created_at",
    (sys.argv[1],),
).fetchall()
print(json.dumps([dict(job_id=r[0], status=r[1], snapshot_id=r[2], local_artifact_id=r[3],
                      job_ids=json.loads(r[4] or "[]"), auto_suspend=json.loads(r[5] or "null")) for r in rows]))
"""
        result = self._shell(
            placement.node_name,
            "chroot /proc/1/root /usr/bin/python3 -c "
            f"{shlex.quote(script)} {shlex.quote(placement.sandbox_id)}",
            "read the task-owned durable upload",
        )
        return tuple(json.loads(result))

    def restart_handoff_owner(
        self, placement: SandboxPodPlacement
    ) -> dict[str, object]:
        from space_smoke.adapters import _require_ok

        self.harness._require_isolated_kind("restart the task-owned pending handoff")
        old = self.harness._node_service_pod(placement.node_name, "sandbox-manager")
        _require_ok(
            self.harness.kubectl("delete", "pod", old, "--wait=false"),
            "gracefully restart the disposable sandbox-manager",
        )
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            pods = self._kubectl_json(
                "get",
                "pods",
                "-l",
                "app.kubernetes.io/name=space-worker,app.kubernetes.io/component=sandbox-manager",
                "--field-selector",
                f"spec.nodeName={placement.node_name}",
            ).get("items", [])
            for pod in pods:
                if (
                    pod["metadata"]["name"] != old
                    and not pod["metadata"].get("deletionTimestamp")
                    and any(
                        c.get("type") == "Ready" and c.get("status") == "True"
                        for c in pod.get("status", {}).get("conditions", [])
                    )
                ):
                    return {
                        "predecessor": old,
                        "successor": pod["metadata"]["name"],
                        "uploads": self._uploads(placement),
                    }
            time.sleep(1)
        raise SmokeConfigurationError("sandbox-manager successor did not become ready")

    def locate_handoff(self, sandbox_id: str) -> SandboxPodPlacement:
        placement = self.harness.sandbox_pod_placement(sandbox_id)
        if placement is None:
            raise SmokeConfigurationError("the task-owned sandbox has no bound pod")
        return placement

    def _shell(self, node: str, script: str, action: str) -> str:
        from space_smoke.adapters import _require_ok

        result = self.harness._node_shell_exec(node, script, timeout=30)
        _require_ok(result, action)
        return result.stdout

    def _kubectl_json(self, *args: str) -> dict[str, Any]:
        from space_smoke.adapters import _require_ok

        result = self.harness.kubectl(*args, "-o", "json")
        _require_ok(result, "inspect task-owned Kubernetes resources")
        return json.loads(result.stdout)

    def prepare_handoff(self, placement: SandboxPodPlacement) -> None:
        from space_smoke.adapters import (
            KIND_NODE_SHELL_CONTAINER,
            REPO_ROOT,
            _require_ok,
        )

        h = self.harness
        h._require_isolated_kind("exercise a graceful volume-manager handoff")
        self.directory = f"/space/cache/space-smoke-handoff-{placement.sandbox_id}"
        self._shell(
            placement.node_name,
            f"mkdir -m 1777 {shlex.quote(self.directory)}",
            "create task-owned handoff probe directory",
        )
        grpcurl = shutil.which("grpcurl")
        if grpcurl is None:
            raise SmokeConfigurationError("the handoff probe requires grpcurl")
        pod = h._node_shell_pod(placement.node_name)
        with tempfile.TemporaryDirectory(prefix="space-smoke-handoff-") as temporary:
            descriptor = Path(temporary) / "services.pb"
            _require_ok(
                h.executor.run(
                    [
                        "protoc",
                        f"--proto_path={REPO_ROOT / 'proto'}",
                        "--proto_path=/usr/include",
                        "--include_imports",
                        f"--descriptor_set_out={descriptor}",
                        str(REPO_ROOT / "proto/sandbox/manager/v1/manager.proto"),
                        str(REPO_ROOT / "proto/volume/manager/v1/volume_manager.proto"),
                    ],
                    timeout=30,
                ),
                "compile the existing service descriptors for state-only observations",
            )
            for local, name in (
                (Path(grpcurl), "grpcurl"),
                (descriptor, "services.pb"),
            ):
                _require_ok(
                    h.kubectl(
                        "cp",
                        "-c",
                        KIND_NODE_SHELL_CONTAINER,
                        str(local),
                        f"{pod}:{self.directory}/{name}",
                        timeout=30,
                    ),
                    "install the task-owned service observation client",
                )

    def _rpc(
        self,
        placement: SandboxPodPlacement,
        service: str,
        method: str,
        request: dict[str, Any],
    ) -> dict[str, Any]:
        if self.directory is None:
            raise SmokeConfigurationError("handoff probe was not prepared")
        socket = (
            "/space/run/sandbox-manager/sandbox-manager.sock"
            if service == "space.sandbox.manager.v1.SandboxManager"
            else "/space/run/volume-manager/volume-manager.sock"
        )
        command = shlex.join(
            [
                f"{self.directory}/grpcurl",
                "-plaintext",
                "-max-time",
                "10",
                "-protoset",
                f"{self.directory}/services.pb",
                "-d",
                json.dumps(request),
                f"unix://{socket}",
                f"{service}/{method}",
            ]
        )
        return json.loads(
            self._shell(placement.node_name, command, f"observe {service}/{method}")
            or "{}"
        )

    def _hold_successor_placement(
        self, placement: SandboxPodPlacement, predecessor: dict[str, Any]
    ) -> None:
        from space_smoke.adapters import _require_ok

        owners = [
            owner["name"]
            for owner in predecessor["metadata"].get("ownerReferences", [])
            if owner.get("kind") == "DaemonSet" and owner.get("controller")
        ]
        if len(owners) != 1:
            raise SmokeConfigurationError("volume manager needs one DaemonSet owner")
        name = str(owners[0])
        current = self._kubectl_json("get", "daemonset", name)
        if current["spec"]["template"]["spec"].get("affinity") is not None:
            raise SmokeConfigurationError("refusing to replace existing node affinity")
        affinity = {
            "nodeAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": {
                    "nodeSelectorTerms": [
                        {
                            "matchFields": [
                                {
                                    "key": "metadata.name",
                                    "operator": "NotIn",
                                    "values": [placement.node_name],
                                }
                            ]
                        }
                    ]
                }
            }
        }
        strategy = current["spec"]["updateStrategy"]
        # Finalizers retain a terminating Pod but do not prevent its successor.
        # Exclude only this node; OnDelete keeps other workers from rolling.
        operations = [
            {
                "op": "test",
                "path": "/metadata/resourceVersion",
                "value": current["metadata"]["resourceVersion"],
            },
            {
                "op": "replace",
                "path": "/spec/updateStrategy",
                "value": {"type": "OnDelete"},
            },
            {"op": "add", "path": "/spec/template/spec/affinity", "value": affinity},
        ]
        _require_ok(
            self.harness.kubectl(
                "patch", "daemonset", name, "--type=json", "-p", json.dumps(operations)
            ),
            "hold only the task-owned volume-manager successor",
        )
        self.successor_daemonset = name
        self.successor_strategy = strategy
        self.successor_affinity = affinity

    def _restore_successor_placement(self) -> None:
        from space_smoke.adapters import _require_ok

        if self.successor_daemonset is None:
            return
        operations = [
            {
                "op": "test",
                "path": "/spec/template/spec/affinity",
                "value": self.successor_affinity,
            },
            {
                "op": "test",
                "path": "/spec/updateStrategy",
                "value": {"type": "OnDelete"},
            },
            {"op": "remove", "path": "/spec/template/spec/affinity"},
            {
                "op": "replace",
                "path": "/spec/updateStrategy",
                "value": self.successor_strategy,
            },
        ]
        _require_ok(
            self.harness.kubectl(
                "patch",
                "daemonset",
                self.successor_daemonset,
                "--type=json",
                "-p",
                json.dumps(operations),
            ),
            "restore the captured volume-manager placement",
        )
        self.successor_daemonset = None

    def begin_handoff(
        self, placement: SandboxPodPlacement, *, hold_successor: bool = False
    ) -> dict[str, object]:
        from space_smoke.adapters import _require_ok

        h = self.harness
        if self.directory is None:
            raise SmokeConfigurationError("handoff probe was not prepared")
        page_token = ""
        while True:
            page = self._rpc(
                placement,
                "space.sandbox.manager.v1.SandboxManager",
                "List",
                {"pageToken": page_token},
            )
            sandbox = next(
                (
                    row
                    for row in page.get("sandboxes", [])
                    if row.get("sandboxId") == placement.sandbox_id
                ),
                None,
            )
            if sandbox is not None:
                break
            page_token = page.get("nextPageToken", "")
            if not page_token:
                raise SmokeConfigurationError(
                    "the task sandbox is absent from inventory"
                )
        if sandbox.get("template") != "seed" or not sandbox.get("imageHash"):
            raise SmokeConfigurationError(
                "the drain fixture requires the published Kind seed"
            )
        # Read only the existing public template metadata. Volume-manager
        # obtains its normal read-only template grant; no credentials are
        # extracted, and no arbitrary upload prefix is authorized.
        source_uri = (
            f"s3://templates/templates/seed/{quote(sandbox['imageHash'], safe=':')}/metadata.json"
            "?region=us-east-1&endpoint=http://object-store:8333&path_style=true"
        )
        selector = self._kubectl_json("get", "deployment", "space-object-store")[
            "spec"
        ]["selector"]["matchLabels"]
        pods = self._kubectl_json(
            "get",
            "pods",
            "-l",
            ",".join(f"{key}={value}" for key, value in selector.items()),
        ).get("items", [])
        active = [p for p in pods if not p["metadata"].get("deletionTimestamp")]
        if len(active) != 1:
            raise SmokeConfigurationError(
                "the fixture requires exactly one object-store pod"
            )
        store = active[0]
        self.object_store_pod = str(store["metadata"]["name"])
        self.object_store_node = str(store["spec"]["nodeName"])
        self.object_store_container = next(
            row["containerID"].removeprefix("containerd://")
            for row in store["status"]["containerStatuses"]
            if row["name"] == "object-store"
        )
        # SIGSTOP retains existing real storage connections. No fake service
        # or injected gRPC status is involved. This window is shorter than the
        # object store's normal liveness failure threshold.
        self._signal_object_store("STOP")
        submitted = self._rpc(
            placement,
            "space.volume.manager.v1.VolumeManager",
            "SubmitPullFile",
            {
                "subjectId": "template:seed",
                "s3Uri": source_uri,
                "localPath": f"{self.directory}/seed-metadata.json",
            },
        )
        self.drain_job_id = str(submitted["jobId"])
        deadline = time.monotonic() + 15
        while True:
            state = self._rpc(
                placement,
                "space.volume.manager.v1.VolumeManager",
                "GetTransferJob",
                {"jobId": submitted["jobId"]},
            )["job"]
            if state.get("status") == "TRANSFER_JOB_STATUS_RUNNING":
                break
            if (
                state.get("status") == "TRANSFER_JOB_STATUS_FAILED"
                or time.monotonic() >= deadline
            ):
                raise SmokeConfigurationError(
                    f"owned template download did not become in-flight: {state}"
                )
            time.sleep(0.2)
        self.old_volume_pod = h._node_service_pod(placement.node_name, "volume-manager")
        old = self._kubectl_json("get", "pod", self.old_volume_pod)
        if old["metadata"].get("finalizers"):
            raise SmokeConfigurationError(
                "refusing a manager pod with another finalizer owner"
            )
        _require_ok(
            h.kubectl(
                "patch",
                "pod",
                self.old_volume_pod,
                "--type=json",
                "-p",
                json.dumps(
                    [
                        {
                            "op": "add",
                            "path": "/metadata/finalizers",
                            "value": [self.FINALIZER],
                        }
                    ]
                ),
            ),
            "retain only this predecessor until the controlled handoff release",
        )
        self.holds_successor = True
        if hold_successor:
            self._hold_successor_placement(placement, old)
        _require_ok(
            h.kubectl("delete", "pod", self.old_volume_pod, "--wait=false", timeout=15),
            "start the real volume-manager graceful shutdown",
        )
        return {"predecessor_pod": self.old_volume_pod, "inflight_transfer": state}

    def _signal_object_store(self, signal: str) -> None:
        from space_smoke.adapters import _require_ok

        if self.object_store_node is None or self.object_store_container is None:
            raise SmokeConfigurationError(
                "the object-store container has not been resolved"
            )
        pid = int(
            _require_ok(
                self.harness._docker_exec(
                    self.object_store_node,
                    "crictl",
                    "inspect",
                    "--output",
                    "go-template",
                    "--template",
                    "{{.info.pid}}",
                    self.object_store_container,
                ),
                "resolve the exact object-store process without reading credentials",
            )
        )
        if pid <= 1:
            raise SmokeConfigurationError(
                "the task object-store container is no longer running"
            )
        _require_ok(
            self.harness._docker_exec(
                self.object_store_node, "kill", f"-{signal}", str(pid)
            ),
            f"send {signal} to the isolated object-store process",
        )
        if signal == "STOP":
            state = _require_ok(
                self.harness._docker_exec(
                    self.object_store_node, "awk", "{print $3}", f"/proc/{pid}/stat"
                ),
                "verify the object-store process is stopped",
            ).strip()
            if state != "T":
                raise SmokeConfigurationError(
                    f"object-store process did not stop: {state}"
                )

    def release_handoff_storage(self) -> None:
        if self.object_store_pod is not None:
            self._signal_object_store("CONT")
            self.object_store_pod = None

    def release_handoff(self) -> None:
        from space_smoke.adapters import _require_ok

        if self.old_volume_pod:
            result = self.harness.kubectl("logs", self.old_volume_pod, timeout=15)
            if result.returncode == 0:
                self.predecessor_logs = result.stdout
        self.release_handoff_storage()
        self._restore_successor_placement()
        if self.holds_successor:
            _require_ok(
                self.harness.kubectl(
                    "patch",
                    "pod",
                    self.old_volume_pod or "",
                    "--type=json",
                    "-p",
                    json.dumps(
                        [
                            {
                                "op": "test",
                                "path": "/metadata/finalizers",
                                "value": [self.FINALIZER],
                            },
                            {"op": "remove", "path": "/metadata/finalizers"},
                        ]
                    ),
                ),
                "release only the task-owned handoff finalizer",
            )
            self.holds_successor = False

    def finish_handoff(self, placement: SandboxPodPlacement) -> None:
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            pods = self._kubectl_json(
                "get",
                "pods",
                "-l",
                "app.kubernetes.io/name=space-worker,app.kubernetes.io/component=volume-manager",
                "--field-selector",
                f"spec.nodeName={placement.node_name}",
            ).get("items", [])
            for pod in pods:
                if (
                    pod["metadata"]["name"] != self.old_volume_pod
                    and not pod["metadata"].get("deletionTimestamp")
                    and any(
                        c.get("type") == "Ready" and c.get("status") == "True"
                        for c in pod.get("status", {}).get("conditions", [])
                    )
                ):
                    return
            time.sleep(1)
        raise SmokeConfigurationError(
            "the real successor volume-manager did not become ready"
        )

    def observe_handoff(self, placement: SandboxPodPlacement) -> HandoffObservation:
        from space_smoke.adapters import KIND_SANDBOX_NAMESPACE

        state = self._rpc(
            placement,
            "space.sandbox.manager.v1.SandboxManager",
            "State",
            {"sandboxIds": [placement.sandbox_id]},
        )
        rows = state.get("sandboxes", [])
        if len(rows) != 1:
            raise SmokeConfigurationError(
                "the task-owned sandbox disappeared from node state"
            )
        row = rows[0]
        lifecycle = row.get("autoLifecycleStatus", {})
        pid = self.runtime_identity[0] if self.runtime_identity else row.get("pid")
        start_ticks = None
        alive = False
        if pid:
            alive, start_ticks = self._process_identity(
                placement.node_name,
                int(pid),
                expected_name="firecracker",
            )
            if alive and start_ticks and self.runtime_identity is None:
                self.runtime_identity = (int(pid), start_ticks)
            elif self.runtime_identity is not None:
                alive = alive and start_ticks == self.runtime_identity[1]
        pod = self.harness.kubectl(
            "get",
            "pod",
            placement.pod_name,
            "--ignore-not-found",
            "-o",
            "json",
            timeout=15,
            namespace=KIND_SANDBOX_NAMESPACE,
        )
        if pod.returncode != 0:
            raise SmokeConfigurationError("cannot observe the task-owned sandbox pod")
        pod_state = json.loads(pod.stdout) if pod.stdout.strip() else {}
        phase = pod_state.get("status", {}).get("phase")
        if self.carrier_identity is None:
            containers = pod_state.get("status", {}).get("containerStatuses", [])
            for container in containers:
                if container.get("name") != "sandbox" or not container.get(
                    "containerID"
                ):
                    continue
                container_id = str(container["containerID"]).removeprefix(
                    "containerd://"
                )
                sandbox_task = self._shell(
                    placement.node_name,
                    "chroot /proc/1/root /usr/local/bin/crictl inspect "
                    "--output go-template --template '{{.info.sandboxID}}' "
                    f"{shlex.quote(container_id)}",
                    "resolve the task-owned containerd sandbox",
                ).strip()
                # The task PID reported by CRI can be Firecracker itself.
                # Observe the independent shim that owns this exact task group.
                script = """
import json, os, pathlib, sys
matches = []
for executable in pathlib.Path("/proc").glob("[0-9]*/exe"):
    try:
        if pathlib.Path(os.readlink(executable)).name != "containerd-shim-space-v2":
            continue
        arguments = (executable.parent / "cmdline").read_bytes().split(b"\\0")
        if b"-id" in arguments and arguments[arguments.index(b"-id") + 1].decode() in sys.argv[1:]:
            matches.append(int(executable.parent.name))
    except (FileNotFoundError, ProcessLookupError):
        continue
print(json.dumps(matches))
"""
                carriers = json.loads(
                    self._shell(
                        placement.node_name,
                        "chroot /proc/1/root /usr/bin/python3 -c "
                        f"{shlex.quote(script)} {shlex.quote(sandbox_task)} "
                        f"{shlex.quote(container_id)}",
                        "resolve the task-owned shim process",
                    )
                )
                if len(carriers) != 1:
                    raise SmokeConfigurationError(
                        f"expected one task-owned shim process, found {carriers}"
                    )
                carrier_pid = int(carriers[0])
                carrier_alive, carrier_ticks = self._process_identity(
                    placement.node_name, carrier_pid
                )
                if carrier_alive and carrier_ticks:
                    self.carrier_identity = (carrier_pid, carrier_ticks)
        carrier_alive, carrier_ticks = (
            self._process_identity(placement.node_name, self.carrier_identity[0])
            if self.carrier_identity
            else (False, None)
        )
        carrier_alive = (
            carrier_alive
            and self.carrier_identity is not None
            and carrier_ticks == self.carrier_identity[1]
        )
        transfer_query_error = None
        try:
            transfers = self._rpc(
                placement,
                "space.volume.manager.v1.VolumeManager",
                "ListTransferJobs",
                {"subjectId": f"sandbox:{placement.sandbox_id}"},
            ).get("jobs", [])
        except RuntimeError as error:
            if not self.holds_successor:
                raise
            # The predecessor can finish draining before the fixture releases
            # its replacement. Record the unavailable query as evidence, not
            # as a false assertion that no durable transfer exists.
            transfer_query_error = str(error)
            transfers = []
        return HandoffObservation(
            lifecycle=str(row.get("lifecycle", "")),
            next_event=lifecycle.get("nextAutoEventAt"),
            last_error=lifecycle.get("lastAutoError"),
            runtime_pid=int(pid) if pid else None,
            runtime_alive=alive,
            runtime_start_ticks=start_ticks,
            pod_phase=phase,
            transfers=tuple(transfers),
            carrier_pid=self.carrier_identity[0] if self.carrier_identity else None,
            carrier_alive=carrier_alive,
            carrier_start_ticks=carrier_ticks,
            uploads=self._uploads(placement),
            transfer_query_error=transfer_query_error,
        )

    def collect_handoff_logs(self, placement: SandboxPodPlacement) -> str:
        pod = self.harness._node_service_pod(placement.node_name, "sandbox-manager")
        result = self.harness.kubectl("logs", pod, "--since=10m", timeout=30)
        return self.predecessor_logs + "\n" + result.stdout

    def cleanup_handoff(self, placement: SandboxPodPlacement) -> None:
        if self.drain_job_id is not None:
            self._rpc(
                placement,
                "space.volume.manager.v1.VolumeManager",
                "CancelTransfer",
                {"jobId": self.drain_job_id},
            )
            self.drain_job_id = None
        if self.directory is not None:
            self._shell(
                placement.node_name,
                f"rm -rf -- {shlex.quote(self.directory)}",
                "remove only this scenario's temporary probe and transfer source",
            )
            self.directory = None

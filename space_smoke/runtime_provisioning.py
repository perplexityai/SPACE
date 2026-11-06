"""Disposable real-binary runtime provisioning, without production credentials."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import secrets
import sys
import time
from typing import Mapping, Protocol
import uuid

from space_smoke.runtime import (
    InvariantViolation as SmokeFailure,
    ScenarioContext,
    SmokeConfigurationError,
    SubprocessExecutor,
)


class RuntimeProvisioningControl(Protocol):
    def verify_regular_reconciliation(self, context: ScenarioContext) -> None: ...


@dataclass(frozen=True)
class RuntimeProvisioningFixture:
    control: RuntimeProvisioningControl
    context: ScenarioContext
    fixture_name = "disposable-runtime-provisioning"
    timeout_minutes = 15


@dataclass(frozen=True)
class LocalRuntimeProvisioningConfig:
    binary_dir: Path
    source_dir: Path
    evidence_dir: Path
    worker_image: str
    postgres_image: str
    timeout_seconds: int = 600
    preflight_only: bool = False

    @classmethod
    def from_environment(cls, env: Mapping[str, str]) -> LocalRuntimeProvisioningConfig:
        names = (
            "SPACE_SMOKE_RUNTIME_BINARY_DIR",
            "SPACE_SMOKE_RUNTIME_SOURCE_DIR",
            "SPACE_SMOKE_RUNTIME_EVIDENCE_DIR",
            "SPACE_SMOKE_RUNTIME_WORKER_IMAGE",
            "SPACE_SMOKE_RUNTIME_POSTGRES_IMAGE",
        )
        missing = [name for name in names if not env.get(name)]
        if missing:
            raise SmokeConfigurationError("Missing local fixture settings: " + ", ".join(missing))
        return cls(
            Path(env[names[0]]).resolve(),
            Path(env[names[1]]).resolve(),
            Path(env[names[2]]).resolve(),
            env[names[3]],
            env[names[4]],
            preflight_only=env.get("SPACE_SMOKE_RUNTIME_PREFLIGHT_ONLY") == "1",
        )


class LocalDockerRuntimeProvisioning:
    """Owns only its labeled containers/network and newly created state tree."""

    def __init__(self, config: LocalRuntimeProvisioningConfig) -> None:
        self.config = config

    def verify_regular_reconciliation(self, context: ScenarioContext) -> None:
        cfg = self.config
        required = (
            "space-internal-api-svc", "space-nodectl-svc", "space-nodemgr-svc",
            "space-rtmgr-svc", "space-db-migrator", "space-nodectl",
            "space-nodemgr", "space-rtmgr", "space-runtime-provisioning-fixture",
        )
        for binary in required:
            if not (cfg.binary_dir / binary).is_file():
                raise SmokeConfigurationError(f"Missing real binary: {binary}")
        if not (cfg.source_dir / "proto/node/api/v1/cluster.proto").is_file():
            raise SmokeConfigurationError("Source directory must contain the matching protobufs")
        run_id = "space-runtime-fixture-" + uuid.uuid4().hex[:12]
        label = "space.smoke.runtime-fixture=" + run_id
        root = cfg.evidence_dir / run_id
        root.mkdir(parents=True, exist_ok=False)
        state = root / "space"
        state.mkdir(mode=0o755)
        network, worker, database = (run_id + suffix for suffix in ("-net", "-worker", "-pg"))
        receipt: dict[str, object] = {
            "run_id": run_id, "label": label, "root": str(root),
            "binary_sha256": {
                name: hashlib.file_digest((cfg.binary_dir / name).open("rb"), "sha256").hexdigest()
                for name in required
            },
            "docker_arguments": [], "resources": {},
        }
        limits = self._host_limits()
        receipt["host_limits_before"] = limits

        def persist() -> None:
            (root / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
            context.evidence["runtime_provisioning"] = {
                "receipt": str(root / "receipt.json"),
                "run_id": run_id,
                "phase": receipt.get("phase"),
            }

        def command(args: list[str], *, check: bool = True, timeout: int = 60,
                    env: Mapping[str, str] | None = None):
            result = SubprocessExecutor(base_env=os.environ).run(args, timeout=timeout, env=env)
            if check and result.returncode != 0:
                raise SmokeFailure(f"{args[0]} failed ({result.returncode}): {result.stderr[-3000:]}")
            return result

        def docker(args: list[str], *, check: bool = True, timeout: int = 60,
                   env: Mapping[str, str] | None = None):
            return command(["docker", *args], check=check, timeout=timeout, env=env)

        def cleanup_container(name: str) -> None:
            result = docker(["inspect", "--format", "{{json .Config.Labels}}", name], check=False)
            if result.returncode == 0:
                labels = json.loads(result.stdout)
                if labels.get("space.smoke.runtime-fixture") != run_id:
                    raise SmokeFailure(f"Refusing cleanup of non-owned container {name}")
                docker(["rm", "-f", name])
                if docker(["inspect", name], check=False).returncode == 0:
                    raise SmokeFailure(f"Owned container still exists: {name}")

        def cleanup_network() -> None:
            result = docker(["network", "inspect", network], check=False)
            if result.returncode == 0:
                metadata = json.loads(result.stdout)[0]
                if metadata["Labels"].get("space.smoke.runtime-fixture") != run_id:
                    raise SmokeFailure("Refusing cleanup of non-owned network")
                docker(["network", "rm", network])
            receipt["host_limits_after"] = self._host_limits()
            persist()
            if receipt["host_limits_after"] != limits:
                raise SmokeFailure("Host cgroup CPU/memory limits changed during fixture")

        docker(["network", "create", "--label", label, network])
        context.scope.defer("remove owned runtime fixture network", cleanup_network, critical=True)
        args = [
            "run", "-d", "--name", worker, "--label", label, "--network", network,
            "--cgroupns", "private", "--cpus", "3", "--memory", "3g",
            "--cap-add", "SYS_ADMIN", "--cap-add", "NET_ADMIN",
            "--cap-add", "SYS_RESOURCE",
            "--cap-drop", "MKNOD", "--cap-drop", "SYS_MODULE", "--cap-drop", "SYS_RAWIO",
            "--security-opt", "seccomp=unconfined", "--security-opt", "apparmor=unconfined",
            "--tmpfs", "/space:rw,exec,size=1g,mode=0755",
            "--env", "SPACE_INTERNAL_TOKEN", "--env", "SPACE_DB_USER_PASSWORD",
            "--entrypoint", "/bin/sleep", cfg.worker_image, "infinity",
        ]
        receipt["docker_arguments"].append(args)
        db_password = secrets.token_urlsafe(32)
        worker_id = docker(args, env={
            "SPACE_INTERNAL_TOKEN": secrets.token_urlsafe(32),
            "SPACE_DB_USER_PASSWORD": db_password,
        }).stdout.strip()
        receipt["resources"]["worker_id"] = worker_id
        context.scope.defer("remove owned runtime fixture worker", lambda: cleanup_container(worker), critical=True)
        persist()

        def execute(args: list[str], *, check: bool = True, timeout: int = 60):
            return docker(["exec", worker, *args], check=check, timeout=timeout)

        execute(["mkdir", "-p", "/fixture/bin", "/fixture/proto"])
        docker(["cp", str(cfg.binary_dir) + "/.", worker + ":/fixture/bin"], timeout=120)
        docker(["cp", str(cfg.source_dir / "proto") + "/.", worker + ":/fixture/proto"])
        docker(["cp", str(cfg.source_dir / "infra/space/identity/node-identities.sh"),
                worker + ":/fixture/node-identities.sh"])
        # Never start node-manager until namespace containment is independently visible.
        inspected = json.loads(docker(["inspect", worker]).stdout)[0]
        pid = inspected["State"]["Pid"]
        host_cgroup_namespace = os.readlink("/proc/self/ns/cgroup")
        container_cgroup_namespace = execute(["readlink", "/proc/self/ns/cgroup"]).stdout.strip()
        if host_cgroup_namespace == container_cgroup_namespace:
            raise SmokeFailure("Worker did not receive a private cgroup namespace")
        candidates = [
            path for path in Path("/sys/fs/cgroup").rglob("*")
            if path.name in (worker_id, "docker-" + worker_id + ".scope") and path.is_dir()
        ]
        if len(candidates) != 1:
            raise SmokeFailure("Cannot uniquely locate the full owned Docker ID cgroup")
        owned_cgroup = candidates[0]
        host_stat = owned_cgroup.stat()
        inside_stat = execute(["stat", "-c", "%d:%i", "/sys/fs/cgroup"]).stdout.strip()
        own_limits = {name: (owned_cgroup / name).read_text() for name in ("cpu.max", "memory.max")}
        if inside_stat != f"{host_stat.st_dev}:{host_stat.st_ino}":
            raise SmokeFailure("Namespaced cgroup mount is not the exact owned host subtree")
        if own_limits != {"cpu.max": "300000 100000\n", "memory.max": "3221225472\n"}:
            raise SmokeFailure("Owned Docker cgroup limits differ from the bounded fixture")
        host_pid_namespace = os.readlink("/proc/self/ns/pid")
        inside_pid_namespace = execute(["readlink", "/proc/self/ns/pid"]).stdout.strip()
        if host_pid_namespace == inside_pid_namespace:
            raise SmokeFailure("Worker shares the caller PID namespace")
        for path in ("/proc/1/cgroup", "/proc/self/cgroup"):
            if execute(["cat", path]).stdout.strip() != "0::/":
                raise SmokeFailure("Container processes are not at their private cgroup root")
        config = inspected["HostConfig"]
        if config["Privileged"] or config["CgroupnsMode"] != "private":
            raise SmokeFailure("Container isolation intent differs from the observed private fixture")
        parent_limits = {
            name: (owned_cgroup.parent / name).read_text()
            if (owned_cgroup.parent / name).exists() else None
            for name in ("cpu.max", "memory.max")
        }
        def verify_parent_limits() -> None:
            actual = {
                name: (owned_cgroup.parent / name).read_text()
                if (owned_cgroup.parent / name).exists() else None
                for name in ("cpu.max", "memory.max")
            }
            receipt["owned_parent_limits_after"] = actual
            persist()
            if actual != parent_limits:
                raise SmokeFailure("Owned container parent limits changed")
        context.scope.defer("verify owned container parent limits unchanged", verify_parent_limits, critical=True)
        receipt["cgroup_guard"] = {
            "host_namespace": host_cgroup_namespace,
            "worker_namespace": container_cgroup_namespace,
            "host_owned_cgroup": str(owned_cgroup),
            "host_owned_cgroup_stat": f"{host_stat.st_dev}:{host_stat.st_ino}",
            "inside_root_stat": inside_stat,
            "owned_limits": own_limits,
            "owned_parent_limits_before": parent_limits,
            "host_pid_namespace": host_pid_namespace,
            "inside_pid_namespace": inside_pid_namespace,
            "container_init_host_pid": pid,
            "container_id": worker_id,
            "privileged": config["Privileged"],
            "cgroupns_mode": config["CgroupnsMode"],
            "cgroup_parent": config["CgroupParent"],
            "worker_mountinfo": execute(["cat", "/proc/self/mountinfo"]).stdout,
        }
        if execute(["test", "-e", "/dev/loop-control"], check=False).returncode == 0:
            raise SmokeFailure("Fixture must not expose host loop-control")
        # A new cgroup2 mount resolves relative to this already-verified private namespace.
        execute(["mount", "-o", "remount,rw", "/sys/fs/cgroup"])
        receipt["cgroup_guard"]["writable_mountinfo"] = execute(["cat", "/proc/self/mountinfo"]).stdout
        persist()
        execute(["mkdir", "-p", "/fixture/python"])
        docker(["cp", str(Path(sys.base_prefix)) + "/.", worker + ":/fixture/python"], timeout=120)
        docker(["cp", str(Path(__file__).with_name("runtime_provisioning_inner.py")),
                worker + ":/fixture/inner.py"])
        grpcurl = command(["which", "grpcurl"]).stdout.strip()
        docker(["cp", grpcurl, worker + ":/fixture/grpcurl"])
        if cfg.preflight_only:
            result = execute(["/fixture/python/bin/python3", "/fixture/inner.py", "--preflight-only"],
                             check=False)
            copied = docker(["cp", worker + ":/fixture/inner-receipt.json",
                             str(root / "inner-receipt.json")], check=False)
            receipt["inner_exit_code"] = result.returncode
            receipt["inner_stderr"] = result.stderr
            if copied.returncode == 0:
                receipt["inner"] = json.loads((root / "inner-receipt.json").read_text())
            receipt["phase"] = "isolation_preflight_only_no_services_started"
            persist()
            if result.returncode:
                raise SmokeFailure("Inner cgroup delegation failed: " + result.stderr[-2000:])
            return

        pg_args = [
            "run", "-d", "--name", database, "--label", label,
            "--network", "container:" + worker, "--cpus", "1", "--memory", "512m",
            "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-e", "POSTGRES_DB=space_fixture",
            "--tmpfs", "/var/lib/postgresql/data:rw,size=512m",
            cfg.postgres_image,
        ]
        receipt["docker_arguments"].append(pg_args)
        receipt["resources"]["postgres_id"] = docker(pg_args).stdout.strip()
        context.scope.defer("remove owned runtime fixture database", lambda: cleanup_container(database), critical=True)
        deadline = time.monotonic() + 60
        while docker(["exec", database, "pg_isready", "-U", "postgres"], check=False).returncode != 0:
            if time.monotonic() >= deadline:
                raise SmokeFailure("Disposable PostgreSQL did not become ready")
            context.wait(0.25)
        url = "postgres://postgres@127.0.0.1:5432/space_fixture"
        execute(["/fixture/bin/space-db-migrator", "control-plane", "--database-url", url, "up"])
        docker([
            "exec", "-e", "SPACE_DB_BOOTSTRAP_USER_PASSWORD", worker,
            "/fixture/bin/space-db-migrator", "bootstrap-roles",
            "--database-url", url, "--allow-missing-passwords",
        ], timeout=60, env={"SPACE_DB_BOOTSTRAP_USER_PASSWORD": db_password})
        node_id = execute([
            "env", "SPACE_LOCAL_FIXTURE_DATABASE_URL=" + url,
            "/fixture/bin/space-runtime-provisioning-fixture", "--disposable-local-database",
        ]).stdout.strip()
        uuid.UUID(node_id)
        receipt["node_id"] = node_id
        # Configuration is generated data; service CLI commands remain typed argv.
        (state / "node.toml").write_text(
            "[network_pool]\ntarget = 0\nmin_ready = 0\nmax_slots = 1\n"
        )
        (state / "nodectl.toml").write_text(
            "[cluster]\nnode_api_endpoint = \"http://127.0.0.1:5443\"\n"
        )
        for config_name in ("node.toml", "nodectl.toml"):
            docker(["cp", str(state / config_name), worker + ":/fixture/" + config_name])
        execute(["mkdir", "-p", "/space/logs", "/space/run/node-manager", "/space/run/node-ctl",
                 "/space/run/runtime-manager", "/space/vendor/firecracker/releases",
                 "/space/vendor/os/linux/kernels", "/space/cache/kernels",
                 "/space/node/kernels", "/space/node/releases", "/space/node/runtime-manager/staging",
                 "/space/node/runtime-manager/state", "/space/node/runtime-manager/catalog",
                 "/space/cache/legacy-assets", "/space/cache/firecracker"])
        probe = execute([
            "/fixture/python/bin/python3", "-I", "-c",
            "import sys; print(sys.executable); print(sys.version)",
        ], check=False)
        receipt["python_probe"] = {
            "code": probe.returncode, "stdout": probe.stdout, "stderr": probe.stderr,
        }
        persist()
        result = execute([
            "/fixture/python/bin/python3", "-I", "/fixture/inner.py",
            "--node-id", node_id, "--timeout-seconds", str(cfg.timeout_seconds),
        ], check=False, timeout=cfg.timeout_seconds + 60)
        receipt["inner_exit_code"] = result.returncode
        receipt["inner_stderr"] = result.stderr
        receipt["inner_stdout"] = result.stdout
        receipt["worker_state_after_inner"] = json.loads(
            docker(["inspect", "--format", "{{json .State}}", worker]).stdout
        )
        receipt["owned_cgroup_after_inner"] = {
            name: (owned_cgroup / name).read_text() if (owned_cgroup / name).exists() else None
            for name in ("cgroup.procs", "memory.events", "memory.current")
        }
        receipt["delegated_leaf_exists"] = (owned_cgroup / "fixture-services").exists()
        copied = docker(["cp", worker + ":/fixture/inner-receipt.json",
                         str(root / "inner-receipt.json")], check=False)
        receipt["receipt_copy_error"] = copied.stderr
        docker(["cp", worker + ":/fixture/logs", str(root / "logs")], check=False)
        if copied.returncode == 0:
            receipt["inner"] = json.loads((root / "inner-receipt.json").read_text())
        persist()
        if result.returncode:
            raise SmokeFailure(
                f"Real runtime reconciliation failed: "
                f"{receipt.get('inner', {}).get('phase')}: "
                f"{receipt.get('inner', {}).get('error')}: "
                f"{result.stderr[-1000:]} {result.stdout[-1000:]}"
            )
        return

    @staticmethod
    def _host_limits() -> dict[str, str]:
        membership = Path("/proc/self/cgroup").read_text().strip()
        relative = next(line.split(":", 2)[2] for line in membership.splitlines() if line.startswith("0::"))
        base = Path("/sys/fs/cgroup") / relative.lstrip("/")
        return {
            "membership": membership,
            "loop_indices": json.dumps(sorted(path.name for path in Path("/sys/block").glob("loop[0-9]*"))),
            **{
                name: (base / name).read_text()
                for name in ("cpu.max", "memory.max")
            },
        }

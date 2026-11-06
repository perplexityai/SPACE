"""Standard-library-only observer inside one disposable worker namespace.

This is invoked once. No Docker exec may enter the cgroup root after delegation.
"""

from __future__ import annotations

import argparse
import base64
import grp
import hashlib
import json
import os
from pathlib import Path
import pwd
import signal
import subprocess
import time


ROOT = Path("/space")
CGROUP = Path("/sys/fs/cgroup")
RECEIPT = Path("/fixture/inner-receipt.json")
RPC_ERRORS: list[dict[str, object]] = []


def save(value: dict[str, object]) -> None:
    RECEIPT.write_text(json.dumps(value, indent=2) + "\n")


def run(args: list[str], *, timeout: int = 30) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, text=True, capture_output=True, timeout=timeout, check=False)


def delegate(receipt: dict[str, object]) -> None:
    initial = {
        name: (CGROUP / name).read_text()
        for name in ("cgroup.controllers", "cgroup.subtree_control", "cgroup.procs",
                     "cpu.max", "memory.max")
    }
    if initial["cpu.max"] != "300000 100000\n" or initial["memory.max"] != "3221225472\n":
        raise RuntimeError("Owned root resource limits changed before delegation")
    leaf = CGROUP / "fixture-services"
    leaf.mkdir()
    namespace = os.readlink("/proc/self/ns/pid")
    for raw in initial["cgroup.procs"].split():
        pid = int(raw)
        if os.readlink(f"/proc/{pid}/ns/pid") != namespace:
            raise RuntimeError("Refusing to move a process outside the owned PID namespace")
        (leaf / "cgroup.procs").write_text(str(pid))
    if (CGROUP / "cgroup.procs").read_text().strip():
        raise RuntimeError("Owned root still has processes before delegation")
    required = {"cpu", "memory", "pids"}
    if not required.issubset(initial["cgroup.controllers"].split()):
        raise RuntimeError("Required controllers unavailable")
    enabled = required | ({"io"} & set(initial["cgroup.controllers"].split()))
    (CGROUP / "cgroup.subtree_control").write_text(" ".join("+" + x for x in sorted(enabled)))
    sandbox_parent = CGROUP / "space"
    sandbox_parent.mkdir()
    (sandbox_parent / "cgroup.subtree_control").write_text(
        " ".join("+" + x for x in sorted(enabled))
    )
    receipt["delegation"] = {
        "before": initial,
        "after": {
            name: (CGROUP / name).read_text()
            for name in ("cgroup.controllers", "cgroup.subtree_control", "cgroup.procs",
                         "cpu.max", "memory.max")
        },
        "services_leaf": str(leaf),
        "services_leaf_processes": (leaf / "cgroup.procs").read_text(),
        "mountinfo": Path("/proc/self/mountinfo").read_text(),
    }
    save(receipt)


def rpc(proto: str, address: str, method: str, payload: dict[str, object],
        *, unix: bool = False) -> dict[str, object] | None:
    args = [
        "/fixture/grpcurl", "-plaintext", "-import-path", "/fixture/proto", "-proto", proto,
    ]
    if unix:
        address = "unix://" + address
    args += ["-expand-headers", "-H", "authorization: Bearer ${SPACE_INTERNAL_TOKEN}"]
    args += ["-d", json.dumps(payload), address, method]
    result = run(args)
    if result.returncode:
        token = os.environ.get("SPACE_INTERNAL_TOKEN", "")
        error = result.stderr.replace(token, "[redacted]") if token else result.stderr
        RPC_ERRORS[:] = [{"method": method, "code": result.returncode, "stderr": error}]
    return json.loads(result.stdout) if result.returncode == 0 else None


def inventory() -> dict[str, object] | None:
    response = rpc(
        "node/v1/node.proto", "/space/run/node-manager/node-manager.sock",
        "space.node.v1.NodeManager/GetExecutionInventory", {}, unix=True,
    )
    return response


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--preflight-only", action="store_true")
    parser.add_argument("--node-id")
    parser.add_argument("--timeout-seconds", type=int, default=600)
    args = parser.parse_args()
    receipt: dict[str, object] = {"phase": "inner-started", "cohort": "worker"}
    save(receipt)
    children: list[subprocess.Popen[str]] = []
    try:
        delegate(receipt)
        if args.preflight_only:
            receipt["phase"] = "delegation-preflight-passed-no-services"
            save(receipt)
            return 0
        if not args.node_id:
            raise RuntimeError("Full run requires the shared fixture's registered node")
        if Path("/dev/loop-control").exists():
            raise RuntimeError("No loop-control is permitted in this fixture")
        log_root = Path("/fixture/logs")
        log_root.mkdir(exist_ok=True)
        env = os.environ.copy()
        env.update({
            "DATABASE_URL": "postgres://postgres@127.0.0.1:5432/space_fixture",
            "SPACE_INTERNAL_API_RUNTIME_PROVISIONING_RECIPE_ENABLED": "true",
            "SPACE_INTERNAL_API_RUNTIME_PROVISIONING_PERCENT": "100",
            "SPACE_NODE_ID": args.node_id,
            "SPACE_NODEMGR_ACTIVATE_RUNTIME_SUPERVISOR_ON_START": "false",
            "SPACE_ALLOWED_EXECUTION_ENVIRONMENTS": "e0,fc-1.17.0-linux-6.1.155",
            "SPACE_INTERNAL_RPC_INSECURE_DEV": "true",
            "SPACE_CLUSTER_ID": "runtime-provisioning-fixture",
            "SPACE_API_ROUTE_ED25519_SIGNING_KEY_ID": "runtime-provisioning-fixture",
            "SPACE_API_ROUTE_ED25519_ISSUER": "cluster-api",
            # Same PKCS#8 Ed25519 seed format as the shared route test fixture.
            # The disposable local key exists only in process memory/environment.
            "SPACE_API_ROUTE_ED25519_SIGNING_KEY": base64.urlsafe_b64encode(
                bytes.fromhex("302e020100300506032b657004220420") + os.urandom(32)
            ).decode().rstrip("="),
            "SPACE_ECW_CREDENTIAL_KEY": base64.urlsafe_b64encode(os.urandom(32)).decode(),
        })
        receipt["cohort_settings"] = {
            key: value for key, value in env.items()
            if key.startswith("SPACE_INTERNAL_API_RUNTIME_") or key == "SPACE_NODE_ID"
        }
        ctl_user = pwd.getpwnam("space-nodectl")
        ctl_dir = ROOT / "node-ctl"
        ctl_dir.mkdir(exist_ok=True)
        os.chown(ctl_dir, ctl_user.pw_uid, ctl_user.pw_gid)
        ctl_socket_dir = ROOT / "run/node-ctl"
        ctl_socket_dir.mkdir(parents=True, exist_ok=True)
        os.chown(ctl_socket_dir, ctl_user.pw_uid, ctl_user.pw_gid)

        def service(name: str, argv: list[str], *, as_ctl: bool = False) -> None:
            def identity() -> None:
                os.initgroups(ctl_user.pw_name, ctl_user.pw_gid)
                identity_source = Path("/fixture/node-identities.sh").read_text()
                group_names = next(
                    line.split("=", 1)[1].strip('"').split()
                    for line in identity_source.splitlines()
                    if line.startswith("SPACE_NODECTL_GROUPS=")
                )
                gids = {grp.getgrnam(name).gr_gid for name in group_names}
                os.setgroups(sorted(set(os.getgroups()) | gids))
                os.setgid(ctl_user.pw_gid)
                os.setuid(ctl_user.pw_uid)
            with (log_root / (name + ".log")).open("w") as log:
                child = subprocess.Popen(
                    argv, env=env, stdout=log, stderr=subprocess.STDOUT,
                    text=True, start_new_session=True,
                    preexec_fn=identity if as_ctl else None,
                )
            children.append(child)
            receipt[name] = {
                "pid": child.pid, "uid": ctl_user.pw_uid if as_ctl else os.getuid(),
                "argv": argv,
                "observed_identity": [
                    line for line in Path(f"/proc/{child.pid}/status").read_text().splitlines()
                    if line.startswith(("Name:", "Pid:", "Uid:", "Gid:", "Groups:"))
                ],
            }
            save(receipt)

        service("runtime-manager", ["/fixture/bin/space-rtmgr-svc"])
        service("node-manager", ["/fixture/bin/space-nodemgr-svc", "--config", "/fixture/node.toml"])
        deadline = time.monotonic() + args.timeout_seconds
        initial_deadline = min(deadline, time.monotonic() + 30)
        while time.monotonic() < initial_deadline:
            initial = inventory()
            receipt["last_initial_inventory"] = initial
            receipt["inventory_rpc_errors"] = RPC_ERRORS
            save(receipt)
            if initial:
                observation = initial.get("environments", {})
                known = observation.get("known", [])
                usable = observation.get("usableEnvironments", [])
                if known and not usable:
                    receipt["initial_node_manager_inventory"] = initial
                    break
            if any(child.poll() is not None for child in children):
                raise RuntimeError("A real service exited before initial inventory")
            time.sleep(0.25)
        else:
            raise RuntimeError("No known-but-not-usable empty-root inventory")
        if any(path.is_file() for path in (ROOT / "vendor").rglob("firecracker")):
            raise RuntimeError("Initial runtime root unexpectedly contains a Firecracker binary")
        receipt["initial_runtime_files"] = [
            str(path) for path in (ROOT / "vendor").rglob("*") if path.is_file()
        ]
        initial_runtime = run([
            "/fixture/bin/space-rtmgr", "--socket",
            "/space/run/runtime-manager/runtime-manager.sock", "list", "--json",
        ])
        if initial_runtime.returncode:
            raise RuntimeError("Read-only initial runtime-manager listing failed")
        receipt["initial_runtime_manager_listing"] = json.loads(initial_runtime.stdout)
        save(receipt)
        service("cluster-api", ["/fixture/bin/space-internal-api-svc", "--listen", "0.0.0.0:5443"])
        service("node-ctl", [
            "/fixture/bin/space-nodectl-svc", "--config", "/fixture/nodectl.toml",
            "--socket", "/space/run/node-ctl/ctl.sock", "--db", "/space/node-ctl/state.sqlite",
            "--poll-interval-secs", "1", "--role", "worker",
        ], as_ctl=True)
        published = False
        publication_deadline = min(deadline, time.monotonic() + 30)
        while time.monotonic() < publication_deadline:
            status = run(["/fixture/bin/space-nodectl", "--socket", "/space/run/node-ctl/ctl.sock",
                          "status", "--json"])
            receipt["node_ctl_status_error"] = status.stderr
            if status.returncode == 0:
                state = json.loads(status.stdout)
                receipt["node_ctl_status"] = state
                for facet in state.get("facets", []):
                    if (
                        facet.get("facet") == "node-status"
                        and int(facet.get("outbound", {}).get("last_success_at_unix_secs", 0)) > 0
                        and not facet.get("outbound", {}).get("last_error")
                    ):
                        published = True
                        break
            save(receipt)
            if published:
                break
            if any(child.poll() is not None for child in children):
                raise RuntimeError("A real service exited during normal NodeCtl publication")
            time.sleep(0.5)
        if not published:
            raise RuntimeError("Normal NodeCtl node-status publication did not succeed")
        polls = []
        for _ in range(2):
            desired = rpc(
                "node/api/v1/cluster.proto", "127.0.0.1:5443",
                "space.node.api.v1.ClusterApi/GetNodeRuntimeDesiredState",
                {"nodeId": args.node_id, "role": "NODE_ROLE_WORKER"},
            )
            if desired is None:
                raise RuntimeError("Desired-state observer RPC failed after publication")
            polls.append(desired)
            save({**receipt, "desired_state_polls": polls})
            time.sleep(1.1)
        receipt["desired_state_polls"] = polls
        required = polls[-1].get("recipe", {}).get("requiredEnvironments", [])
        if not required:
            receipt["phase"] = "regression-empty-recipe-after-accepted-publication"
            receipt["final_node_manager_inventory"] = inventory()
            save(receipt)
            return 2
        receipt["phase"] = "recipe-accepted-awaiting-automatic-install"
        save(receipt)
        while time.monotonic() < deadline:
            final = inventory()
            if final:
                receipt["final_node_manager_inventory"] = final
                observation = final.get("environments", {})
                known = observation.get("known", [])
                selected = [
                    item["definition"]["reference"] for item in known
                    if item.get("definition", {}).get("descriptor", {}).get("runtime", {}).get("managedRelease", {}).get("runtime", {}).get("version") == "1.17.0"
                ]
                usable = set(observation.get("usableEnvironments", []))
                target_reference = "sha256:6e97c03b2523af6cc7b8e67bcde3fd89cf756c1958b10c24472aeadd68a32e0a"
                if target_reference in set(selected) & usable:
                    paths = [path for path in (ROOT / "vendor").rglob("firecracker") if path.is_file()]
                    pins = []
                    for path in paths:
                        version = run([str(path), "--version"])
                        if "1.17.0" in version.stdout:
                            pins.append({
                                "path": str(path), "version": version.stdout.strip(),
                                "sha256": hashlib.file_digest(path.open("rb"), "sha256").hexdigest(),
                            })
                    if not pins:
                        raise RuntimeError("Usable 1.17 environment has no matching actual binary")
                    expected_firecracker = "99ad0f5cd0514a88aad0e9ae8cfdb3cc3b4ab9d190e1194602406c786b5de7a5"
                    expected_kernel = "e20e46d0c36c55c0d1014eb20576171b3f3d922260d9f792017aeff53af3d4f2"
                    artifacts = [
                        {
                            "path": str(path), "bytes": path.stat().st_size,
                            "sha256": hashlib.file_digest(path.open("rb"), "sha256").hexdigest(),
                        }
                        for base in (ROOT / "vendor", ROOT / "cache")
                        for path in base.rglob("*")
                        if path.is_file() and path.stat().st_size > 1_000_000
                    ]
                    if expected_firecracker not in {pin["sha256"] for pin in pins}:
                        raise RuntimeError("Actual 1.17 Firecracker binary differs from reviewed catalog pin")
                    if expected_kernel not in {artifact["sha256"] for artifact in artifacts}:
                        raise RuntimeError("Actual 6.1.155 guest kernel differs from reviewed catalog pin")
                    receipt["firecracker_117_files"] = pins
                    receipt["installed_artifact_files"] = artifacts
                    receipt["usable_117_definitions"] = [
                        item["definition"] for item in known
                        if item["definition"]["reference"] in usable
                        and item["definition"]["reference"] in selected
                    ]
                    receipt["phase"] = "automatic-install-and-usable-117-verified"
                    save(receipt)
                    return 0
            if any(child.poll() is not None for child in children):
                raise RuntimeError("A real service exited during automatic reconciliation")
            time.sleep(0.5)
        raise RuntimeError("Regular recipe did not produce a usable Firecracker 1.17 environment")
    except BaseException as error:
        receipt["error"] = str(error)
        receipt["phase"] = "fixture-error"
        save(receipt)
        return 1
    finally:
        for child in reversed(children):
            if child.poll() is None:
                os.killpg(child.pid, signal.SIGTERM)
        for child in children:
            try:
                child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid, signal.SIGKILL)
                child.wait()
        receipt["owned_root_limits_after"] = {
            name: (CGROUP / name).read_text() for name in ("cpu.max", "memory.max")
        }
        save(receipt)


if __name__ == "__main__":
    raise SystemExit(main())

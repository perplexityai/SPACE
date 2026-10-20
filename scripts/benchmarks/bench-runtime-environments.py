#!/usr/bin/env python3
"""Alternate verified cold-boot environments through SPACE on one benchmark worker."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import subprocess
import time
from pathlib import Path

from sbx_bench_common import UUID_RE, SandboxCli, stats

E0 = "sha256:3168c96b5719ce62543ae53e6641a4eb80416a9f01e5585f6f76c3c70111fa0b"
C = "sha256:959f1181ac69f0dde7125fbc81be6fcd75b64e9e06e7a393f067b96eb6fb2ac4"


def digest(path: Path) -> str:
    with path.open("rb") as source:
        return "sha256:" + hashlib.file_digest(source, "sha256").hexdigest()


def command(argv: list[str]) -> str:
    result = subprocess.run(
        argv, text=True, capture_output=True, check=False, timeout=180
    )
    if result.returncode:
        raise RuntimeError(
            f"{argv[0]} failed ({result.returncode}): {result.stdout[-4096:]} {result.stderr[-4096:]}"
        )
    return result.stdout


def measure(argv: list[str]) -> tuple[float, str]:
    started = time.perf_counter()
    output = command(argv)
    return (time.perf_counter() - started) * 1000, output


def evidence(args, sandbox: str, arm: dict, preflight: dict) -> dict:
    metadata = json.loads((args.cache_dir / sandbox / "metadata.json").read_text())
    execution = metadata["execution"]
    if (
        execution != preflight["execution"]
        or execution["environment"] != arm["environment"]
    ):
        raise RuntimeError(
            "requested, preflight and actual execution identities disagree"
        )
    if metadata["content_hash"] != arm["content_hash"]:
        raise RuntimeError("sandbox did not use the pinned template digest")
    pid = int((args.run_dir / sandbox / "fc.pid").resolve(strict=True).name)
    executable = Path(f"/proc/{pid}/exe")
    # Query the running binary's API, not a separately installed command.
    sock = args.jail_dir / "firecracker" / sandbox / "root/firecracker.sock"
    info = json.loads(
        command(
            [
                "curl",
                "--fail",
                "--silent",
                "--show-error",
                "--max-time",
                "5",
                "--unix-socket",
                str(sock),
                "http://localhost/",
            ]
        )
    )
    if info["vmm_version"] != execution["runtime"]["version"]:
        raise RuntimeError("running VMM version differs from the resolved binding")
    binary_digest = digest(executable)
    if binary_digest != arm["firecracker_digest"]:
        raise RuntimeError("running VMM bytes differ from the compiled benchmark pin")
    kernel = args.jail_dir / "firecracker" / sandbox / "root/vmlinux.bin"
    kernel_digest = digest(kernel)
    boot = next(
        c
        for c in execution["components"]
        if c["kind"] == "kernel" and c["role"] == "boot"
    )
    if kernel_digest != boot["digest"]:
        raise RuntimeError("jailed boot kernel differs from the resolved binding")
    config = json.loads((args.run_dir / sandbox / "vm-config.json").read_text())[
        "machine-config"
    ]
    if config["vcpu_count"] != args.vcpu or config["mem_size_mib"] != args.memory_mib:
        raise RuntimeError(
            "actual CPU/memory settings differ from the comparison contract"
        )
    guest_kernel = command([args.sbxmgr, "exec", sandbox, "--", "uname", "-r"]).strip()
    if guest_kernel != boot["version"] and not guest_kernel.startswith(
        boot["version"] + "-"
    ):
        raise RuntimeError("guest-reported kernel differs from the boot kernel version")
    return {
        "execution": execution,
        "vmm": info,
        "binary_digest": binary_digest,
        "kernel_digest": kernel_digest,
        "guest_kernel": guest_kernel,
        "machine_config": config,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for arm in ("baseline", "candidate"):
        parser.add_argument(f"--{arm}-template", required=True)
        parser.add_argument(f"--{arm}-hash", required=True)
    parser.add_argument("--iterations", type=int, default=10)
    parser.add_argument(
        "--vcpu",
        type=int,
        required=True,
        help="Expected vCPU count; both templates must use this default",
    )
    parser.add_argument(
        "--memory-mib",
        type=int,
        required=True,
        help="Expected memory; both templates must use this default",
    )
    parser.add_argument("--space-revision", required=True)
    parser.add_argument(
        "--recipe",
        type=Path,
        required=True,
        help="Shared guest-userspace recipe provenance",
    )
    parser.add_argument(
        "--base-inputs",
        type=Path,
        required=True,
        help="JSON recording pinned inputs used for both builds",
    )
    parser.add_argument(
        "--cache-state",
        required=True,
        help="Declared host cache state; this harness never prunes caches",
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--sbxmgr", default="/space/bin/space-sbxmgr")
    parser.add_argument("--nodemgr", default="/space/bin/space-nodemgr")
    parser.add_argument(
        "--cache-dir", type=Path, default=Path("/space/cache/firecracker/sandboxes")
    )
    parser.add_argument(
        "--run-dir", type=Path, default=Path("/space/run/firecracker/sandboxes")
    )
    parser.add_argument(
        "--jail-dir", type=Path, default=Path("/space/cache/firecracker/jails")
    )
    args = parser.parse_args()
    if platform.system() != "Linux" or os.geteuid() != 0:
        parser.error("run locally as root on the designated Linux benchmark worker")
    if args.iterations < 1 or min(args.vcpu, args.memory_mib) < 1:
        parser.error("iterations and resource limits must be positive")
    for value in (args.baseline_hash, args.candidate_hash):
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", value):
            parser.error("each template hash must be a canonical SHA-256 digest")
    if args.baseline_template == args.candidate_template:
        parser.error(
            "use distinct template names to keep artifact and pool namespaces separate"
        )
    arms = [
        {
            "label": "A",
            "template": args.baseline_template,
            "content_hash": args.baseline_hash,
            "environment": E0,
            "firecracker_digest": "sha256:7e8b57e88c459396d4680d83dcdd8c7f72305447cb55b11f4ac98ad70a3f7825",
        },
        {
            "label": "C",
            "template": args.candidate_template,
            "content_hash": args.candidate_hash,
            "environment": C,
            "firecracker_digest": "sha256:99ad0f5cd0514a88aad0e9ae8cfdb3cc3b4ab9d190e1194602406c786b5de7a5",
        },
    ]
    report = {
        "valid": False,
        "space_revision": args.space_revision,
        "host": platform.uname()._asdict(),
        "cpu": command(["lscpu", "--json"]),
        "recipe_digest": digest(args.recipe),
        "base_inputs": json.loads(args.base_inputs.read_text()),
        "cache_state": args.cache_state,
        "pool_state": "disabled",
        "workload": "cold start to guest ready; stop/start to guest ready",
        "samples": [],
        "failures": [],
    }
    # Exclusive creation prevents accidentally replacing a previous benchmark.
    with args.output.open("x") as output:
        try:
            preflight = {
                arm["label"]: json.loads(
                    command(
                        [args.nodemgr, "environment", "--reference", arm["environment"]]
                    )
                )
                for arm in arms
            }
            report["preflight"] = preflight
            for iteration in range(args.iterations):
                for arm in arms if iteration % 2 == 0 else reversed(arms):
                    cli = SandboxCli(
                        prefix=[],
                        sbxmgr=args.sbxmgr,
                        tmplmgr="",
                        nodemgr=args.nodemgr,
                        template=arm["template"],
                        runtime="fc",
                        boot_mode="cold",
                        environment=arm["environment"],
                        content_hash=arm["content_hash"],
                    )
                    sandbox = None
                    try:
                        create_ms, created = measure(cli.create_cmd(autostart=False))
                        match = UUID_RE.search(created)
                        if not match:
                            raise RuntimeError(
                                "create did not return a sandbox identity"
                            )
                        sandbox = match.group(0)
                        report["active_sandbox"] = sandbox
                        output.seek(0)
                        json.dump(report, output, indent=2)
                        output.truncate()
                        output.flush()
                        start_ms, _ = measure(
                            [args.sbxmgr, "start", sandbox, "--blocking"]
                        )
                        observed = evidence(args, sandbox, arm, preflight[arm["label"]])
                        command([args.sbxmgr, "stop", sandbox, "--blocking"])
                        restart_ms, _ = measure(
                            [args.sbxmgr, "start", sandbox, "--blocking"]
                        )
                        restarted = evidence(
                            args, sandbox, arm, preflight[arm["label"]]
                        )
                        report["samples"].append(
                            {
                                **arm,
                                "iteration": iteration,
                                "sandbox_id": sandbox,
                                "create_ms": create_ms,
                                "start_ms": start_ms,
                                "restart_ms": restart_ms,
                                "observed": observed,
                                "restarted": restarted,
                            }
                        )
                    finally:
                        if sandbox:
                            command([args.sbxmgr, "delete", sandbox, "--blocking"])
                            report.pop("active_sandbox", None)
            report["summary"] = {
                arm["label"]: {
                    metric: stats(
                        sample[metric]
                        for sample in report["samples"]
                        if sample["label"] == arm["label"]
                    )
                    for metric in ("create_ms", "start_ms", "restart_ms")
                }
                for arm in arms
            }
            report["valid"] = True
        except Exception as error:
            report["failures"].append(str(error))
            raise
        finally:
            output.seek(0)
            json.dump(report, output, indent=2)
            output.truncate()
            output.write("\n")


if __name__ == "__main__":
    main()

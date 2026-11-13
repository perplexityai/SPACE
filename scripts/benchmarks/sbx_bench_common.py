#!/usr/bin/env python3
"""Shared helpers for SPACE sandbox benchmark scripts."""

from __future__ import annotations

import argparse
import base64
import concurrent.futures
import dataclasses
import ipaddress
import json
import math
import os
import platform
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path
from typing import Any, Iterable, Sequence


UUID_RE = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
)
BOOT_MODE_RE = re.compile(r"^Boot mode:\s+(\S+)", re.MULTILINE)
DEFAULT_SSH_OPTIONS = [
    "StrictHostKeyChecking=no",
    "ConnectTimeout=10",
    "ControlMaster=no",
    "ControlPath=none",
]


def eprint(*args: object) -> None:
    print(*args, file=sys.stderr, flush=True)


def now_ms() -> int:
    return time.monotonic_ns() // 1_000_000


def timestamp_for_path() -> str:
    return time.strftime("%Y%m%dT%H%M%S")


def parse_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def parse_int_csv(value: str, *, name: str) -> list[int]:
    out: list[int] = []
    for item in parse_csv(value):
        try:
            parsed = int(item)
        except ValueError as exc:
            raise SystemExit(f"ERROR: invalid {name} CSV item: {item}") from exc
        if parsed < 0:
            raise SystemExit(f"ERROR: {name} values must be >= 0: {item}")
        out.append(parsed)
    if not out:
        raise SystemExit(f"ERROR: {name} cannot be empty")
    return out


def percentile(sorted_values: Sequence[int], pct: int) -> int | None:
    if not sorted_values:
        return None
    idx = math.ceil(len(sorted_values) * pct / 100) - 1
    idx = max(0, min(idx, len(sorted_values) - 1))
    return sorted_values[idx]


def format_ms(value: int | float | None) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.1f}"
    return str(value)


def stats(values: Iterable[int]) -> dict[str, str | int]:
    ordered = sorted(values)
    if not ordered:
        return {
            "count": 0,
            "min": "-",
            "p50": "-",
            "p90": "-",
            "p95": "-",
            "p99": "-",
            "max": "-",
            "mean": "-",
        }
    return {
        "count": len(ordered),
        "min": ordered[0],
        "p50": percentile(ordered, 50),
        "p90": percentile(ordered, 90),
        "p95": percentile(ordered, 95),
        "p99": percentile(ordered, 99),
        "max": ordered[-1],
        "mean": f"{sum(ordered) / len(ordered):.1f}",
    }


def command_exists(path: str) -> bool:
    return os.access(path, os.X_OK) or shutil.which(path) is not None


def run_capture(cmd: Sequence[str], *, timeout: float | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(cmd),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=timeout,
    )


def extract_sandbox_id(text: str) -> str:
    match = UUID_RE.search(text)
    return match.group(0) if match else ""


def sanitize_error(text: str, limit: int = 180) -> str:
    one_line = " ".join(text.replace("\t", " ").split())
    return one_line[:limit] if one_line else "-"


def extract_boot_mode(text: str) -> str:
    match = BOOT_MODE_RE.search(text)
    return match.group(1) if match else ""


@dataclasses.dataclass(frozen=True)
class CommandResult:
    operation: str
    background: str | int
    burst: str | int
    repeat: str | int
    index: int
    ms: int
    rc: int
    sandbox_id: str
    error: str
    log_file: str


def choose_sudo_prefix(sbxmgr: str, use_sudo: str) -> list[str]:
    if use_sudo == "yes":
        return ["sudo"]
    if use_sudo == "no":
        return []
    if run_capture([sbxmgr, "list"]).returncode == 0:
        return []
    if run_capture(["sudo", "-n", sbxmgr, "list"]).returncode == 0:
        return ["sudo"]
    raise SystemExit(
        f"ERROR: cannot run {sbxmgr} directly or with passwordless sudo. "
        "Use --sudo or run as a user in the SPACE client groups."
    )


def checked_prereq(path: str, label: str) -> None:
    if not command_exists(path):
        raise SystemExit(f"ERROR: {label} not found or not executable: {path}")


def ensure_template(prefix: Sequence[str], tmplmgr: str, template: str) -> None:
    result = run_capture([*prefix, tmplmgr, "list"])
    if result.returncode != 0:
        raise SystemExit(f"ERROR: failed to list templates:\n{result.stdout}")
    if template not in result.stdout:
        raise SystemExit(f"ERROR: template '{template}' not found:\n{result.stdout}")


@dataclasses.dataclass
class SandboxCli:
    prefix: list[str]
    sbxmgr: str
    tmplmgr: str
    nodemgr: str
    template: str
    runtime: str
    boot_mode: str
    user: str = ""
    environment: str = ""
    content_hash: str = ""

    def create_cmd(self, *, autostart: bool = True, snapshots: bool = False) -> list[str]:
        cmd = [
            *self.prefix,
            self.sbxmgr,
            "create",
            "--template",
            self.template,
            "--boot-mode",
            self.boot_mode,
        ]
        if self.environment:
            cmd += ["--environment", self.environment]
        else:
            cmd += ["--runtime", self.runtime]
        if self.content_hash:
            cmd += ["--content-hash", self.content_hash]
        if self.user:
            cmd += ["--user", self.user]
        if autostart:
            cmd += ["--autostart", "--blocking"]
        if snapshots:
            cmd += ["--snapshots"]
        return cmd

    def id_cmd(self, operation: str, sandbox_id: str, ready_timeout_secs: str = "") -> list[str]:
        command = "start" if operation == "start_base_snap" else operation
        cmd = [*self.prefix, self.sbxmgr, command, sandbox_id]
        if operation == "start_base_snap":
            cmd += ["--from-warm", "--blocking"]
        elif operation in {"resume", "restore"}:
            cmd.append("--blocking")
            if ready_timeout_secs:
                cmd += ["--ready-timeout-secs", ready_timeout_secs]
        return cmd

    def delete(self, sandbox_id: str) -> None:
        result = run_capture([*self.prefix, self.sbxmgr, "delete", sandbox_id, "--blocking"])
        if result.returncode != 0:
            eprint(f"WARNING: cleanup failed for {sandbox_id}: {sanitize_error(result.stdout)}")


def timed_command(
    cmd: Sequence[str],
    *,
    operation: str,
    background: str | int = "-",
    burst: str | int = "-",
    repeat: str | int = "-",
    index: int = 1,
    log_file: Path,
    expected_boot_mode: str = "",
) -> CommandResult:
    started = now_ms()
    result = run_capture(cmd)
    elapsed = now_ms() - started
    log_file.parent.mkdir(parents=True, exist_ok=True)
    log_file.write_text(result.stdout)
    sandbox_id = extract_sandbox_id(result.stdout)
    rc = result.returncode
    error = "-" if rc == 0 else sanitize_error(result.stdout)
    if rc == 0 and operation.endswith("start") and not sandbox_id:
        rc = 97
        error = "command succeeded but did not print a sandbox ID"
    effective_boot_mode = extract_boot_mode(result.stdout)
    if rc == 0 and expected_boot_mode and effective_boot_mode and effective_boot_mode != expected_boot_mode:
        rc = 98
        error = f"effective boot mode {effective_boot_mode}, expected {expected_boot_mode}"
    if not sandbox_id:
        sandbox_id = "-"
    return CommandResult(
        operation=operation,
        background=background,
        burst=burst,
        repeat=repeat,
        index=index,
        ms=elapsed,
        rc=rc,
        sandbox_id=sandbox_id,
        error=error,
        log_file=str(log_file),
    )


def run_command_batch(
    commands: Sequence[tuple[list[str], dict[str, object]]],
    *,
    run_dir: Path,
    max_workers: int | None = None,
) -> tuple[int, list[CommandResult]]:
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "results").mkdir(exist_ok=True)
    (run_dir / "logs").mkdir(exist_ok=True)
    start_event = threading.Event()

    def one(item: tuple[list[str], dict[str, object]]) -> CommandResult:
        cmd, meta = item
        start_event.wait()
        return timed_command(cmd, **meta)  # type: ignore[arg-type]

    workers = max_workers or max(1, len(commands))
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(one, item) for item in commands]
        started = now_ms()
        start_event.set()
        results = [future.result() for future in futures]
        batch_ms = now_ms() - started

    for result in results:
        result_path = run_dir / "results" / f"{result.index}.tsv"
        result_path.write_text(command_result_tsv(result, include_error=True) + "\n")
    (run_dir / "batch_ms").write_text(f"{batch_ms}\n")
    return batch_ms, results


def command_result_tsv(result: CommandResult, *, include_error: bool) -> str:
    fields: list[object] = [
        result.operation,
        result.background,
        result.burst,
        result.repeat,
        result.index,
        result.ms,
        result.rc,
        result.sandbox_id,
    ]
    if include_error:
        fields.append(result.error)
    fields.append(result.log_file)
    return "\t".join(str(field) for field in fields)


def write_lines(path: Path, lines: Iterable[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as file:
        for line in lines:
            file.write(line)
            file.write("\n")


def valid_ids(results: Iterable[CommandResult]) -> list[str]:
    ids = []
    for result in results:
        if result.rc == 0 and UUID_RE.fullmatch(result.sandbox_id):
            ids.append(result.sandbox_id)
    return sorted(ids)


def cleanup_ids(cli: SandboxCli, ids: Iterable[str], *, enabled: bool = True) -> None:
    if not enabled:
        return
    for sandbox_id in sorted(set(id_ for id_ in ids if UUID_RE.fullmatch(id_))):
        cli.delete(sandbox_id)


def read_pool_counts(cli: SandboxCli) -> dict[str, str]:
    result = run_capture([*cli.prefix, cli.nodemgr, "status"])
    keys = ["status", "ready", "leased", "dirty", "resetting", "creating", "quarantined", "degraded"]
    values = {key: "-" for key in keys}
    if result.returncode != 0:
        return values
    for line in result.stdout.splitlines():
        if not line.startswith("Pool:"):
            continue
        for part in line.split():
            if "=" not in part:
                continue
            key, value = part.split("=", 1)
            if key in values:
                values[key] = value
        break
    return values


def read_ready_slots(cli: SandboxCli) -> int | None:
    ready = read_pool_counts(cli).get("ready", "-")
    return int(ready) if ready.isdigit() else None


def wait_for_ready_slots(cli: SandboxCli, required: int, timeout_secs: int, label: str) -> None:
    if required <= 0:
        return
    started = time.monotonic()
    while True:
        ready = read_ready_slots(cli)
        if ready is not None and ready >= required:
            print(f"  ready slots {ready}/{required}; {label}", flush=True)
            return
        if time.monotonic() - started >= timeout_secs:
            raise SystemExit(
                f"ERROR: timed out waiting for {required} ready network slots for {label}; "
                f"last ready={ready if ready is not None else 'unknown'}"
            )
        time.sleep(1)


def snapshot_resources(path: Path, cli: SandboxCli, phase: str, background: object, burst: object = "-", repeat: object = "-") -> None:
    load1 = "-"
    mem_total_kb = mem_avail_kb = None
    try:
        load1 = Path("/proc/loadavg").read_text().split()[0]
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemTotal:"):
                mem_total_kb = int(line.split()[1])
            elif line.startswith("MemAvailable:"):
                mem_avail_kb = int(line.split()[1])
    except (OSError, ValueError, IndexError):
        pass

    mem_used_gb = mem_total_gb = mem_pct = "-"
    if mem_total_kb and mem_avail_kb is not None:
        used = mem_total_kb - mem_avail_kb
        mem_used_gb = f"{used / 1048576:.2f}"
        mem_total_gb = f"{mem_total_kb / 1048576:.2f}"
        mem_pct = f"{(used * 100.0) / mem_total_kb:.2f}"

    fc_count = "0"
    pgrep = shutil.which("pgrep")
    if pgrep:
        result = run_capture([pgrep, "-c", "firecracker"])
        fc_count = result.stdout.strip() or "0"

    pool = read_pool_counts(cli)
    row = [
        time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        phase,
        background,
        burst,
        repeat,
        load1,
        mem_used_gb,
        mem_total_gb,
        mem_pct,
        fc_count,
        pool.get("status", "-"),
        pool.get("ready", "-"),
        pool.get("leased", "-"),
        pool.get("dirty", "-"),
        pool.get("resetting", "-"),
        pool.get("creating", "-"),
        pool.get("quarantined", "-"),
        pool.get("degraded", "-"),
    ]
    write_lines(path, ["\t".join(str(item) for item in row)])


def ssh_option_args(options: Sequence[str]) -> list[str]:
    args: list[str] = []
    for opt in options:
        args += ["-o", opt]
    return args


def normalize_target(target: str, ssh_user: str) -> str:
    return target if "@" in target else f"{ssh_user}@{target}"


def stage_and_run_remote_python(
    *,
    script_path: Path,
    target: str,
    ssh_options: Sequence[str],
    remote_args: Sequence[str],
) -> int:
    common_path = script_path.with_name("sbx_bench_common.py")
    suffix = f"{Path(script_path).stem}-{os.getpid()}"
    remote_dir = f"/tmp/space-bench-{suffix}"
    ssh_args = ssh_option_args(ssh_options)

    subprocess.run(["ssh", *ssh_args, target, "mkdir", "-p", remote_dir], check=True)
    subprocess.run(["scp", *ssh_args, str(script_path), str(common_path), f"{target}:{remote_dir}/"], check=True)
    remote_script = f"{remote_dir}/{script_path.name}"
    cmd = ["ssh", *ssh_args, target, "python3", remote_script, "--remote-run", *remote_args]
    return subprocess.run(cmd).returncode


def pull_results(
    *,
    target: str,
    ssh_options: Sequence[str],
    remote_output_dir: str,
    local_parent: str,
    files: Sequence[str],
    pull_raw: bool,
) -> None:
    local_dir = Path(local_parent) / Path(remote_output_dir).name
    local_dir.mkdir(parents=True, exist_ok=True)
    ssh_args = ssh_option_args(ssh_options)
    for file_name in files:
        subprocess.run(
            ["scp", "-q", *ssh_args, f"{target}:{remote_output_dir}/{file_name}", str(local_dir)],
            check=False,
        )
    if pull_raw:
        subprocess.run(
            ["scp", "-q", "-r", *ssh_args, f"{target}:{remote_output_dir}/runs", str(local_dir)],
            check=False,
        )
    print(f"Pulled compact results to: {local_dir}")


def add_common_ssh_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--ssh-user", default=os.environ.get("SSH_USER", "deploy"))
    parser.add_argument(
        "--ssh-option",
        action="append",
        default=[],
        help="Extra ssh option without -o. Repeatable.",
    )
    parser.add_argument("--pull-results", default="")
    parser.add_argument("--pull-raw", action="store_true")


def effective_ssh_options(extra: Sequence[str]) -> list[str]:
    opts = list(DEFAULT_SSH_OPTIONS)
    opts.extend(extra)
    return opts


def require_linux_remote_mode() -> None:
    if platform.system() != "Linux":
        raise SystemExit("ERROR: --remote-run benchmark mode must run on the worker node")


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args: Any, **kwargs: Any) -> None:
        return None


# Never send the management or attachment token through an inherited proxy or a redirect.
_HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect())
_TERMINAL_FAILED = {"failed", "canceled", "cancelled", "expired"}
_SANDBOX_GONE = {"failed", "deleted", "deleting", "stopped"}


class ApiError(RuntimeError):
    def __init__(self, message: str, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


def require_token_safe_url(url: str) -> str:
    """Return url if a bearer token may be sent to it, else raise ValueError.

    HTTPS is always allowed. Plain HTTP is allowed only to a loopback name or a
    private or loopback IP literal (for example a port-forward or a ClusterIP),
    so the token never crosses a public network in cleartext.
    """
    parts = urllib.parse.urlsplit(url)
    if parts.scheme == "https" and parts.hostname:
        return url
    if parts.scheme == "http" and parts.hostname:
        if parts.hostname == "localhost":
            return url
        try:
            address = ipaddress.ip_address(parts.hostname)
        except ValueError:
            address = None
        if address is not None and (address.is_loopback or address.is_private):
            return url
    raise ValueError(f"refusing to send the API token to {url!r}; use https, or http to a loopback or private IP")


class SpaceApi:
    """Minimal stdlib client for the public SPACE API used by the benchmarks.

    Commands inside a sandbox need a route token that only the SPACE API mints,
    so on clusters that enforce it (EKS) node-local ``space-sbxmgr exec`` is
    refused. API-mode benchmarks drive sandbox operations through this client.
    """

    def __init__(
        self,
        api_url: str,
        data_plane_url: str,
        token: str,
        *,
        poll_secs: float = 0.25,
        idempotency_prefix: str = "bench",
    ) -> None:
        self.api_url = require_token_safe_url(api_url).rstrip("/")
        self.data_plane_url = require_token_safe_url(data_plane_url).rstrip("/") if data_plane_url else ""
        self.token = token
        self.poll_secs = poll_secs
        self.idempotency_prefix = idempotency_prefix

    def request(
        self,
        method: str,
        path: str,
        body: Any = None,
        *,
        idempotent: bool = False,
        idempotency_key: str | None = None,
        timeout: float = 30,
    ) -> dict[str, Any]:
        headers = {"Authorization": f"Bearer {self.token}", "Accept": "application/json"}
        data = None
        if body is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps(body).encode()
        if idempotency_key or idempotent:
            headers["Idempotency-Key"] = idempotency_key or f"{self.idempotency_prefix}-{uuid.uuid4().hex}"
        req = urllib.request.Request(self.api_url + path, data=data, headers=headers, method=method)
        try:
            with _HTTP.open(req, timeout=timeout) as response:
                raw = response.read()
        except urllib.error.HTTPError as error:
            with error:
                detail = error.read(2048).decode("utf-8", errors="replace")
            raise ApiError(f"{method} {path}: HTTP {error.code}: {detail}", error.code) from error
        except (urllib.error.URLError, OSError) as error:
            raise ApiError(f"{method} {path}: {error}") from error
        if not raw.strip():
            return {}
        try:
            result = json.loads(raw)
        except ValueError as error:
            raise ApiError(f"{method} {path}: response is not valid JSON: {error}") from error
        if not isinstance(result, dict):
            raise ApiError(f"{method} {path}: expected a JSON object")
        return result

    def wait_operation(self, operation_id: str, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        while True:
            state = self.request("GET", f"/v1/operations/{operation_id}").get("state")
            if state == "succeeded":
                return
            if state in _TERMINAL_FAILED:
                raise ApiError(f"operation {operation_id} {state}")
            if time.monotonic() >= deadline:
                raise ApiError(f"operation {operation_id} still {state} after {timeout:.0f}s")
            time.sleep(self.poll_secs)

    def wait_state(self, sandbox_id: str, target: str, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        while True:
            state = self.request("GET", f"/v1/sandboxes/{sandbox_id}").get("state")
            if state == target:
                return
            if state in _SANDBOX_GONE:
                raise ApiError(f"sandbox {sandbox_id} is {state}, expected {target}")
            if time.monotonic() >= deadline:
                raise ApiError(f"sandbox {sandbox_id} still {state} after {timeout:.0f}s, expected {target}")
            time.sleep(self.poll_secs)

    def wait_ready(self, sandbox_id: str, timeout: float) -> None:
        self.wait_state(sandbox_id, "running", timeout)
        self.request("GET", f"/v1/sandboxes/{sandbox_id}/processes?limit=1", timeout=min(timeout, 180))

    def create(self, template: str, resources: dict[str, int]) -> tuple[str, str]:
        """POST the create and return (sandbox_id, operation_id) without waiting.

        A transport error can hide a create the server accepted, so the POST is
        retried once with the same idempotency key to recover its sandbox ID.
        """
        body = {"template": template, "runtime": "fc", "resources": resources}
        key = f"{self.idempotency_prefix}-{uuid.uuid4().hex}"
        try:
            result = self.request("POST", "/v1/sandboxes", body, idempotency_key=key)
        except ApiError as error:
            if error.status is not None:
                raise
            result = self.request("POST", "/v1/sandboxes", body, idempotency_key=key)
        sandbox_id, operation_id = result.get("sandbox_id"), result.get("operation_id")
        if not isinstance(sandbox_id, str) or not sandbox_id.startswith("sbx_") or not isinstance(operation_id, str):
            raise ApiError(f"create returned no sandbox_id or operation_id: {result}")
        return sandbox_id, operation_id

    def wait_created(self, sandbox_id: str, operation_id: str, timeout: float) -> None:
        self.wait_operation(operation_id, timeout)
        self.wait_ready(sandbox_id, timeout)

    def lifecycle(self, sandbox_id: str, action: str, timeout: float) -> None:
        result = self.request("POST", f"/v1/sandboxes/{sandbox_id}:{action}", {}, idempotent=True)
        operation_id = result.get("operation_id")
        if isinstance(operation_id, str):
            self.wait_operation(operation_id, timeout)

    def delete(self, sandbox_id: str, timeout: float = 300) -> None:
        result = self.request("DELETE", f"/v1/sandboxes/{sandbox_id}", idempotent=True)
        operation_id = result.get("operation_id")
        if isinstance(operation_id, str):
            self.wait_operation(operation_id, timeout)

    def sandbox_state(self, sandbox_id: str) -> str | None:
        """Return the sandbox state, or None when the API does not find it."""
        try:
            return self.request("GET", f"/v1/sandboxes/{sandbox_id}").get("state")
        except ApiError as error:
            if error.status == 404:
                return None
            raise

    def delete_after_create(self, sandbox_id: str, create_operation_id: str, timeout: float) -> None:
        """Delete a sandbox whose create may still be in flight, until the API no longer finds it.

        A delete issued while the create is queued may not take effect, so the
        sandbox can come up afterwards. The API also cannot find a sandbox before
        its create records it, so a miss only proves it gone once the create ends.
        """
        deadline = time.monotonic() + timeout
        last = ""
        try:
            # Issue one delete now so a create that has not started can be canceled;
            # don't wait on it, since its operation can queue behind the create.
            self.request("DELETE", f"/v1/sandboxes/{sandbox_id}", idempotent=True)
        except ApiError as error:
            last = str(error)
        while True:
            try:
                created = self.request("GET", f"/v1/operations/{create_operation_id}").get("state")
                if created == "succeeded" or created in _TERMINAL_FAILED:
                    state = self.sandbox_state(sandbox_id)
                    if state in (None, "deleted"):
                        return
                    if state != "deleting":
                        self.delete(sandbox_id, max(1.0, deadline - time.monotonic()))
            except ApiError as error:
                last = str(error)
            if time.monotonic() >= deadline:
                raise ApiError(f"sandbox {sandbox_id} still present after {timeout:.0f}s: {last}")
            time.sleep(self.poll_secs)

    def events_url(self, attach: dict[str, Any]) -> str:
        url = str(attach.get("events_url") or "")
        path = urllib.parse.urlsplit(url).path if "://" in url else url
        if not path.startswith("/v1/sessions/"):
            raise ApiError(f"unexpected events URL {url!r}")
        if self.data_plane_url:
            return self.data_plane_url + path
        if "://" in url:
            try:
                return require_token_safe_url(url)
            except ValueError as error:
                raise ApiError(f"events: {error}") from error
        raise ApiError("the API returned a relative events URL; pass --data-plane-url")

    def run(self, sandbox_id: str, argv: Sequence[str], timeout: float) -> tuple[int, str]:
        """Run argv in the guest; return (exit code, stdout+stderr). 125 means no clean exit."""
        body = {"command": list(argv), "timeout": f"{max(1, int(timeout))}s"}
        command = self.request("POST", f"/v1/sandboxes/{sandbox_id}/commands", body, idempotent=True)
        attach = command.get("attach") or {}
        token = attach.get("token")
        if not isinstance(token, str) or not token:
            raise ApiError("command response carried no attach token")
        req = urllib.request.Request(
            self.events_url(attach), headers={"Authorization": f"Bearer {token}", "Accept": "text/event-stream"}
        )
        chunks: list[bytes] = []
        exit_code: int | None = None
        # The socket timeout resets on every keep-alive, so also bound the whole stream.
        deadline = time.monotonic() + timeout + 30
        try:
            with _HTTP.open(req, timeout=timeout + 30) as response:
                data: list[bytes] = []
                for line in response:
                    if time.monotonic() > deadline:
                        raise ApiError(f"events: no process exit within {timeout + 30:.0f}s")
                    line = line.rstrip(b"\r\n")
                    if line:
                        field, _, value = line.partition(b":")
                        if field == b"data":
                            data.append(value[1:] if value.startswith(b" ") else value)
                        continue
                    if not data:
                        continue
                    try:
                        frame = json.loads(b"\n".join(data))
                    except ValueError as error:
                        raise ApiError(f"events: event frame is not valid JSON: {error}") from error
                    data = []
                    kind = frame.get("type") if isinstance(frame, dict) else None
                    if kind in ("stdout", "stderr"):
                        chunks.append(base64.b64decode(frame.get("data_base64") or ""))
                    elif kind == "process_exited":
                        code = frame.get("exit_code")
                        exit_code = code if type(code) is int else 125
                        break
                    elif kind in ("error", "resync_required"):
                        chunks.append(f"\n[event stream {kind}: {frame}]".encode())
                        break
        except urllib.error.HTTPError as error:
            with error:
                detail = error.read(2048).decode("utf-8", errors="replace")
            raise ApiError(f"events: HTTP {error.code}: {detail}") from error
        except (urllib.error.URLError, OSError) as error:
            raise ApiError(f"events: {error}") from error
        out = b"".join(chunks).decode("utf-8", errors="replace")
        return (125 if exit_code is None else exit_code), out

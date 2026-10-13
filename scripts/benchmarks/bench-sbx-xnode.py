#!/usr/bin/env python3
"""Cross-node reconnect benchmark for balloon template variants (Stage 2).

Stage 1 (``bench-sbx-balloon.py``) runs on one worker and cannot measure the
thing the balloon experiment is judged on: how long it takes to reconnect to a
suspended sandbox on a *different* worker, which downloads its snapshot from
object storage. This harness measures that on a small dedicated worker pool.

It runs from an operator machine with two credentials: an ECW API key (the
same E2B-compatible API Computer reconnects through, so the server-side
``sandbox.reconnect.duration_ms`` metric fires for every sample) and a kubectl
context for the pool's cluster.

How one block works (one variant, one template, ``--block-minutes`` long):

1. Every pool worker is schedulable. The harness creates load sandboxes, a
   correctness pattern in each (the Stage 1 guest scripts), and optional
   bystanders, then reads which worker each one landed on.
2. It seeds a reservoir: on every worker, ``--reservoir-per-node`` sandboxes
   run their workload and are suspended right after it ends.
3. The block clock starts. One worker at a time is the *source*: it is
   cordoned, and wake-ups are only issued to sandboxes that were suspended on
   it, so their restores have to land on another worker. The source rotates
   every ``--rotate-seconds``. Wake-ups arrive at random at ``--rate`` per
   minute (``--load loaded``) or one at a time every ``--quiet-interval-s``
   (``--load quiet``). Each wake-up is paired with the suspend of another
   sandbox on a target worker, so snapshot uploads compete with downloads and
   the reservoir refills for later rotations.
4. Each wake-up is timed on this machine's clock, from the ECW connect request
   to the first ``true`` command returning, then (untimed) the guest pattern
   is re-hashed to prove the restore is correct.
5. At the end of the block every worker is schedulable again. The harness
   reads the workers' sandbox-manager ``runtime_activated`` events to record
   where each restore landed and whether it downloaded its snapshot, then
   deletes the block's sandboxes.

Cordoning is limited to the nodes named in ``--nodes``. The harness refuses to
start if one of them is already cordoned, records the node it cordons in
``cordon.state`` before cordoning it, and uncordons on every exit path.
``--repair`` uncordons a node recorded by a run that died.

``bench-sbx-xnode-summary.py`` turns ``results.jsonl`` into the Stage 2
decision.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import dataclasses
import importlib.util
import json
import math
import os
import queue
import random
import re
import shlex
import signal
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence

SCHEMA_VERSION = 1
SANDBOX_ID_LABEL = "space.pplx.net/sandbox-id"
DEFAULT_SBXMGR_SELECTOR = "app.kubernetes.io/component=sandbox-manager"
DEFAULT_SBXMGR_CONTAINER = "sandbox-manager"
UUID_SEARCH = re.compile(r"[0-9a-fA-F]{8}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{12}")

# The API's own restore-selection predicate (see space_smoke/adapters.py
# RESTORE_PORTABLE_SQL). Until it holds, the planner pre-binds the restore to
# the source node and the cordon does nothing.
RESTORE_PORTABLE_SQL = """\
SELECT EXISTS (
  SELECT 1
    FROM sandboxes s
   WHERE s.id = '{sandbox_id}'::uuid
     AND s.lifecycle IN ('suspended', 'restoring')
     AND EXISTS (
       SELECT 1
         FROM snapshots sn
         JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id
        WHERE sn.sandbox_id = s.id
          AND sn.type = 'full'
          AND sn.state = 'ready'
          AND (s.placement_changed_at IS NULL OR sn.timestamp >= s.placement_changed_at)
     )
);
"""


def _load_stage1() -> Any:
    """Import the Stage 1 harness for its guest scripts and helpers."""
    here = Path(__file__).resolve().parent
    if str(here) not in sys.path:
        sys.path.insert(0, str(here))
    name = "bench_sbx_balloon"
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, here / "bench-sbx-balloon.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


stage1 = _load_stage1()


# --- Pure helpers (unit tested) -------------------------------------------


def now_ms() -> int:
    return int(time.time() * 1000)


def canonical_id(value: str) -> str:
    """Canonical UUID for an ECW sandbox ID; the pod label and events use it."""
    match = UUID_SEARCH.search(value or "")
    if not match:
        return value
    return str(uuid.UUID(match.group(0)))


def block_order(labels: Sequence[str], baseline: str, blocks_per_variant: int) -> list[str]:
    """Alternate forward and reversed passes so no variant always runs at the same point.

    With baseline B and variants A, C: B A C, C A B, B A C, ...
    """
    if baseline not in labels:
        raise ValueError(f"baseline {baseline!r} is not a variant label")
    if blocks_per_variant <= 0:
        raise ValueError("blocks per variant must be positive")
    forward = [baseline, *[label for label in labels if label != baseline]]
    order: list[str] = []
    for index in range(blocks_per_variant):
        order += forward if index % 2 == 0 else list(reversed(forward))
    return order


def parse_order(text: str, labels: Sequence[str]) -> list[str]:
    order = [part.strip() for part in text.split(",") if part.strip()]
    unknown = sorted(set(order) - set(labels))
    if unknown:
        raise ValueError(f"--order names unknown variants {unknown}; have {list(labels)}")
    if not order:
        raise ValueError("--order is empty")
    return order


def poisson_offsets(rate_per_min: float, duration_s: float, rng: random.Random) -> list[float]:
    """Arrival times (seconds from block start) of a Poisson process."""
    if rate_per_min <= 0:
        raise ValueError("rate must be positive")
    rate_per_s = rate_per_min / 60.0
    offsets: list[float] = []
    t = rng.expovariate(rate_per_s)
    while t < duration_s:
        offsets.append(t)
        t += rng.expovariate(rate_per_s)
    return offsets


def source_for(nodes: Sequence[str], offset_s: float, rotate_s: float) -> str:
    """The cordoned source worker at an offset into the block."""
    return nodes[int(offset_s // rotate_s) % len(nodes)]


def reservoir_target(rate_per_min: float, rotate_s: float, load: str, quiet_interval_s: float) -> int:
    """Suspended sandboxes one source window consumes, plus half again of headroom."""
    per_window = rotate_s / quiet_interval_s if load == "quiet" else rate_per_min * rotate_s / 60.0
    return max(1, math.ceil(per_window * 1.5))


def expected_samples(order: Sequence[str], label: str, *, rate_per_min: float, block_s: float,
                     warmup_s: float, load: str, quiet_interval_s: float) -> float:
    blocks = sum(1 for item in order if item == label)
    measured = max(0.0, block_s - warmup_s)
    per_block = measured / quiet_interval_s if load == "quiet" else rate_per_min * measured / 60.0
    return blocks * per_block


def parse_timestamp_ms(value: Any) -> int | None:
    """Epoch ms from an RFC 3339 string or a number of seconds/ms."""
    if isinstance(value, (int, float)):
        return int(value if value > 1e12 else value * 1000)
    if not isinstance(value, str) or not value:
        return None
    text = value.strip().replace("Z", "+00:00")
    # Python < 3.11 accepts at most 6 fractional digits.
    match = re.match(r"^(.*\.\d{6})\d*(.*)$", text)
    if match:
        text = match.group(1) + match.group(2)
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


@dataclasses.dataclass(frozen=True)
class Activation:
    sandbox_id: str
    node: str
    boot_type: str
    provenance: str
    storage_uri_present: bool
    generation: str
    ts_ms: int | None


def parse_activation(line: str, node: str) -> Activation | None:
    """A ``runtime_activated`` sandbox event from one sandbox-manager log line."""
    if "runtime_activated" not in line:
        return None
    try:
        event = json.loads(line)
    except ValueError:
        return None
    if not isinstance(event, dict):
        return None
    fields = event.get("fields") if isinstance(event.get("fields"), dict) else {}
    merged = {**fields, **{k: v for k, v in event.items() if k != "fields"}}
    if merged.get("event_type") != "runtime_activated":
        return None
    sandbox_id = merged.get("sandbox_id")
    if not isinstance(sandbox_id, str) or not sandbox_id:
        return None
    present = merged.get("restore_storage_uri_present", False)
    return Activation(
        sandbox_id=canonical_id(sandbox_id),
        node=node,
        boot_type=str(merged.get("boot_type", "")),
        provenance=str(merged.get("restore_provenance", "")),
        storage_uri_present=present is True or str(present).lower() == "true",
        generation=str(merged.get("execution_generation", "")),
        ts_ms=parse_timestamp_ms(merged.get("timestamp") or event.get("ts")),
    )


# Only `cross_node_download` samples count toward the primary metric: the
# restore landed off the source worker and had no local copy, so it downloaded
# its snapshot (sandbox.restore.placement with local_snapshot_present:false).
CLASSIFICATIONS = (
    "cross_node_download",
    "cross_node_local_copy",
    "cross_node_no_remote",
    "same_node",
    "outside_pool",
    "not_snapshot",
    "unclassified",
)


def classify(source_node: str | None, activation: Activation | None,
             pool: Sequence[str] | None = None) -> str:
    if activation is None or not source_node:
        return "unclassified"
    if activation.boot_type != "snapshot":
        return "not_snapshot"
    if activation.node == source_node:
        return "same_node"
    if pool is not None and activation.node not in pool:
        return "outside_pool"
    if activation.provenance == "remote_only":
        return "cross_node_download"
    if activation.provenance.startswith("remote_"):
        return "cross_node_local_copy"
    return "cross_node_no_remote"


def join_activations(
    wakes: Sequence[dict[str, Any]],
    activations: Iterable[Activation],
    *,
    skew_ms: int = 5000,
    window_ms: int = 300_000,
) -> dict[str, Activation | None]:
    """Match each wake (``wake_id``, ``sandbox_id``, ``wake_start_ms``) to its activation.

    A wake takes the earliest unused activation of its sandbox that is no
    earlier than ``skew_ms`` before the wake started and no later than
    ``window_ms`` after. Activations without a timestamp are matched in order.
    """
    by_sandbox: dict[str, list[Activation]] = {}
    for activation in activations:
        by_sandbox.setdefault(activation.sandbox_id, []).append(activation)
    for items in by_sandbox.values():
        items.sort(key=lambda a: (a.ts_ms is None, a.ts_ms or 0))
    used: set[int] = set()
    out: dict[str, Activation | None] = {}
    for wake in sorted(wakes, key=lambda w: w["wake_start_ms"]):
        candidates = by_sandbox.get(canonical_id(wake["sandbox_id"]), [])
        chosen = None
        start = wake["wake_start_ms"]
        for activation in candidates:
            if id(activation) in used:
                continue
            if activation.ts_ms is None or start - skew_ms <= activation.ts_ms <= start + window_ms:
                chosen = activation
                break
        if chosen is not None:
            used.add(id(chosen))
        out[wake["wake_id"]] = chosen
    return out


# --- Kubernetes ------------------------------------------------------------


class KubeError(RuntimeError):
    pass


@dataclasses.dataclass(frozen=True)
class Placement:
    node: str
    created_ms: int | None


class Kube:
    """The kubectl operations the harness needs, and nothing else."""

    def __init__(self, *, kubectl: str, context: str, sandbox_namespace: str, node_namespace: str,
                 sbxmgr_selector: str, sbxmgr_container: str) -> None:
        self.base = [kubectl] + (["--context", context] if context else [])
        self.sandbox_namespace = sandbox_namespace
        self.node_namespace = node_namespace
        self.sbxmgr_selector = sbxmgr_selector
        self.sbxmgr_container = sbxmgr_container

    def run(self, *args: str, timeout: float = 60, namespace: str | None = None) -> str:
        cmd = [*self.base] + (["-n", namespace] if namespace else []) + list(args)
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            raise KubeError(f"kubectl {' '.join(args)} timed out") from exc
        if proc.returncode != 0:
            raise KubeError(f"kubectl {' '.join(args)} failed: {proc.stderr.strip()[:500]}")
        return proc.stdout

    def node_unschedulable(self, node: str) -> bool:
        return self.run("get", "node", node, "-o", "jsonpath={.spec.unschedulable}", timeout=30).strip() == "true"

    def cordon(self, node: str) -> None:
        self.run("cordon", node, timeout=30)

    def uncordon(self, node: str) -> None:
        self.run("uncordon", node, timeout=30)

    def placements(self) -> dict[str, Placement]:
        """Canonical sandbox ID -> bound node for every live sandbox pod."""
        out = self.run("get", "pods", "-l", SANDBOX_ID_LABEL, "-o", "json", timeout=60,
                       namespace=self.sandbox_namespace)
        result: dict[str, Placement] = {}
        for pod in json.loads(out or "{}").get("items", []):
            metadata = pod.get("metadata", {})
            node = pod.get("spec", {}).get("nodeName")
            sandbox = (metadata.get("labels") or {}).get(SANDBOX_ID_LABEL)
            if not node or not sandbox or metadata.get("deletionTimestamp"):
                continue
            placement = Placement(node, parse_timestamp_ms(metadata.get("creationTimestamp")))
            key = canonical_id(sandbox)
            previous = result.get(key)
            if previous is None or (placement.created_ms or 0) >= (previous.created_ms or 0):
                result[key] = placement
        return result

    def sbxmgr_log_lines(self, nodes: Sequence[str], since_ms: int) -> list[tuple[str, str]]:
        """(node, line) for every sandbox-manager log line on ``nodes`` since ``since_ms``."""
        listing = json.loads(self.run("get", "pods", "-l", self.sbxmgr_selector, "-o", "json", timeout=60,
                                      namespace=self.node_namespace) or "{}")
        since = datetime.fromtimestamp(since_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        lines: list[tuple[str, str]] = []
        for pod in listing.get("items", []):
            node = pod.get("spec", {}).get("nodeName", "")
            name = pod.get("metadata", {}).get("name", "")
            if node not in nodes or not name:
                continue
            out = self.run("logs", name, "-c", self.sbxmgr_container, "--since-time", since, timeout=300,
                           namespace=self.node_namespace)
            lines += [(node, line) for line in out.splitlines()]
        return lines


class CordonGuard:
    """Cordons at most one pool worker at a time and always undoes its own cordon.

    The node is recorded in ``state_path`` before it is cordoned, so a run that
    dies leaves a record ``--repair`` can act on. Only nodes in the pool can be
    cordoned, and only nodes this guard cordoned are uncordoned.
    """

    def __init__(self, kube: Kube, pool: Sequence[str], state_path: Path) -> None:
        self.kube = kube
        self.pool = tuple(pool)
        self.state_path = state_path
        self.cordoned: set[str] = set()
        self.lock = threading.Lock()

    def _record(self) -> None:
        if self.cordoned:
            self.state_path.write_text(json.dumps({"cordoned": sorted(self.cordoned)}) + "\n")
        elif self.state_path.exists():
            self.state_path.unlink()

    def cordon(self, node: str) -> None:
        if node not in self.pool:
            raise ValueError(f"refusing to cordon {node!r}: not in --nodes {list(self.pool)}")
        with self.lock:
            if node in self.cordoned:
                return
            self.cordoned.add(node)
            self._record()
            self.kube.cordon(node)

    def uncordon(self, node: str) -> None:
        with self.lock:
            if node not in self.cordoned:
                return
            self.kube.uncordon(node)
            self.cordoned.discard(node)
            self._record()

    def release_all(self) -> list[str]:
        failed = []
        for node in sorted(self.cordoned):
            try:
                self.uncordon(node)
            except Exception as exc:  # keep trying the others
                failed.append(f"{node}: {exc}")
        return failed

    @staticmethod
    def repair(kube: Kube, state_path: Path) -> list[str]:
        if not state_path.exists():
            return []
        nodes = json.loads(state_path.read_text()).get("cordoned", [])
        for node in nodes:
            kube.uncordon(node)
        state_path.unlink()
        return list(nodes)


# --- ECW (E2B-compatible) API ---------------------------------------------


@dataclasses.dataclass
class CommandOutcome:
    exit_code: int
    stdout: str
    stderr: str


class Ecw:
    """Thin wrapper over the pinned e2b SDK (see pyproject.toml)."""

    def __init__(self, *, api_url: str, domain: str, api_key: str, sandbox_timeout_s: int) -> None:
        import e2b  # noqa: PLC0415 - only needed for live runs

        self.e2b = e2b
        self.options: dict[str, Any] = {"api_key": api_key, "domain": domain}
        if api_url:
            self.options["api_url"] = api_url.rstrip("/")
        self.sandbox_timeout_s = sandbox_timeout_s

    def create(self, template: str, metadata: dict[str, str]) -> Any:
        return self.e2b.Sandbox.create(template=template, timeout=self.sandbox_timeout_s, metadata=metadata,
                                       **self.options)

    def connect(self, sandbox_id: str, request_timeout: float) -> Any:
        return self.e2b.Sandbox.connect(sandbox_id, timeout=self.sandbox_timeout_s,
                                        request_timeout=request_timeout, **self.options)

    def pause(self, handle: Any) -> None:
        handle.pause()

    def kill(self, sandbox_id: str) -> None:
        self.e2b.Sandbox.kill(sandbox_id, **self.options)

    def run(self, handle: Any, command: str, timeout: float) -> CommandOutcome:
        try:
            result = handle.commands.run(command, timeout=timeout, request_timeout=timeout + 10)
        except self.e2b.CommandExitException as exc:
            return CommandOutcome(exc.exit_code, exc.stdout or "", exc.stderr or "")
        return CommandOutcome(result.exit_code, result.stdout or "", result.stderr or "")


def guest_command(script: str, *args: object) -> str:
    return " ".join(["sh", "-c", shlex.quote(script), "space-bench", *(shlex.quote(str(a)) for a in args)])


# --- Harness ---------------------------------------------------------------


@dataclasses.dataclass
class Sbx:
    sandbox_id: str
    role: str  # "load" or "bystander"
    profile: str
    handle: Any = None
    expected: dict[str, str] = dataclasses.field(default_factory=dict)
    state: str = "running"  # running | busy | suspended | waking | failed | orphaned
    node: str | None = None
    suspend_node: str | None = None
    suspended_at_ms: int | None = None
    portable: bool = False
    awake_since_ms: int | None = None
    last_workload_ms: int | None = None
    wakes: int = 0
    lock: threading.Lock = dataclasses.field(default_factory=threading.Lock)

    @property
    def canonical(self) -> str:
        return canonical_id(self.sandbox_id)


class Block:
    """State for one single-variant block."""

    def __init__(self, index: int, label: str, template: str) -> None:
        self.index = index
        self.label = label
        self.template = template
        self.sandboxes: dict[str, Sbx] = {}
        self.lock = threading.RLock()
        self.stop = threading.Event()
        self.suspend_requests: dict[str, int] = {}
        self.wakes: list[dict[str, Any]] = []
        self.inflight: dict[str, tuple[str, int]] = {}  # wake_id -> (sandbox_id, start_ms)
        self.start_ms = 0
        self.counters: dict[str, int] = {}

    def bump(self, key: str, by: int = 1) -> None:
        with self.lock:
            self.counters[key] = self.counters.get(key, 0) + by

    def request_suspend(self, node: str) -> None:
        with self.lock:
            self.suspend_requests[node] = self.suspend_requests.get(node, 0) + 1

    def take_suspend(self, node: str) -> bool:
        with self.lock:
            if self.suspend_requests.get(node, 0) > 0:
                self.suspend_requests[node] -= 1
                return True
            return False


class XnodeBenchmark:
    def __init__(self, args: argparse.Namespace, *, kube: Kube, ecw: Any, clock: Callable[[], float] = time.monotonic,
                 sleep: Callable[[float], None] = time.sleep) -> None:
        self.args = args
        self.kube = kube
        self.ecw = ecw
        self.clock = clock
        self.sleep = sleep
        self.nodes = list(args.nodes)
        self.variants = dict(stage1.parse_variants(args.variant, args.template))
        self.order = (parse_order(args.order, list(self.variants)) if args.order
                      else block_order(list(self.variants), args.baseline, args.blocks_per_variant))
        self.output_dir = Path(args.output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.results = self.output_dir / "results.jsonl"
        self.guard = CordonGuard(kube, self.nodes, self.output_dir / "cordon.state")
        self.emit_lock = threading.Lock()
        self.run_id = args.run_id or uuid.uuid4().hex[:12]
        self.rng = random.Random(args.seed)
        self.placements: dict[str, Placement] = {}
        self.placement_lock = threading.Lock()
        self.reservoir = args.reservoir_per_node or reservoir_target(
            args.rate, args.rotate_seconds, args.load, args.quiet_interval_s)
        self.pool = concurrent.futures.ThreadPoolExecutor(max_workers=args.max_threads)

    # -- Output

    def emit(self, row: dict[str, Any]) -> None:
        row = {"schema_version": SCHEMA_VERSION, "run_id": self.run_id, **row}
        line = json.dumps(row, sort_keys=True)
        with self.emit_lock:
            with self.results.open("a") as f:
                f.write(line + "\n")

    def log(self, message: str) -> None:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}", flush=True)

    # -- Placement

    def refresh_placements(self) -> None:
        placements = self.kube.placements()
        with self.placement_lock:
            self.placements = placements

    def node_of(self, sbx: Sbx) -> str | None:
        """The sandbox's current worker, or None until its post-wake pod has been seen."""
        with self.placement_lock:
            placement = self.placements.get(sbx.canonical)
        if placement is None:
            return None
        if sbx.awake_since_ms is not None and (placement.created_ms or 0) < sbx.awake_since_ms - 2000:
            return None
        return placement.node

    def bound_since(self, sbx: Sbx, since_ms: int) -> str | None:
        """The node a new pod for ``sbx`` was bound to after ``since_ms``, if any."""
        with self.placement_lock:
            placement = self.placements.get(sbx.canonical)
        if placement and placement.created_ms and placement.created_ms >= since_ms - 2000:
            return placement.node
        return None

    def placement_loop(self, block: Block) -> None:
        while not block.stop.is_set():
            try:
                self.refresh_placements()
            except KubeError as exc:
                self.log(f"WARNING: placement refresh failed: {exc}")
            block.stop.wait(self.args.placement_interval_s)

    # -- Guest helpers

    def workload_args(self, profile: str, rng: random.Random) -> list[object]:
        a = self.args
        if profile == "build":
            return [a.build_units, a.build_blob_mb]
        if profile == "alloc-free":
            return [rng.choice(range(a.alloc_min_mb, a.alloc_max_mb + 1, 512)) if a.alloc_max_mb > a.alloc_min_mb
                    else a.alloc_min_mb]
        if profile == "file-heavy":
            return [a.file_heavy_mb]
        return [a.idle_secs]

    def run_workload(self, block: Block, sbx: Sbx, rng: random.Random) -> tuple[int | None, bool]:
        started = self.clock()
        outcome = self.ecw.run(sbx.handle, guest_command(stage1.GUEST_WORKLOADS[sbx.profile],
                                                         *self.workload_args(sbx.profile, rng)),
                               timeout=self.args.guest_timeout_s)
        elapsed = int((self.clock() - started) * 1000)
        if outcome.exit_code != 0:
            block.bump("workload_failures")
            sbx.last_workload_ms = None
            return elapsed, False
        guest = stage1.guest_elapsed_ms(stage1.parse_kv(outcome.stdout))
        sbx.last_workload_ms = guest if guest is not None else elapsed
        return sbx.last_workload_ms, True

    def suspend(self, block: Block, sbx: Sbx, *, phase: str, rng: random.Random,
                run_workload_first: bool = True) -> None:
        """Suspend right after a workload ends (no settling wait).

        Seeds and quiet-mode suspends run the workload here; loaded-mode
        sandboxes have just finished one in ``load_loop``.
        """
        node = self.node_of(sbx)
        if run_workload_first:
            workload_ms, workload_ok = self.run_workload(block, sbx, rng)
        else:
            workload_ms, workload_ok = sbx.last_workload_ms, sbx.last_workload_ms is not None
        started = self.clock()
        error = None
        try:
            self.ecw.pause(sbx.handle)
            ok = True
        except Exception as exc:
            ok, error = False, str(exc)[:300]
        suspend_ms = int((self.clock() - started) * 1000)
        with sbx.lock:
            if ok and node is None:
                # Unknown source worker: never eligible, so it cannot pollute samples.
                sbx.state, sbx.handle = "orphaned", None
                block.bump("suspended_without_node")
            elif ok:
                sbx.state, sbx.suspend_node, sbx.suspended_at_ms, sbx.portable = "suspended", node, now_ms(), False
                sbx.handle, sbx.awake_since_ms = None, None
            else:
                sbx.state = "failed"
        self.emit({"kind": "suspend", "block": block.index, "variant": block.label, "template": block.template,
                   "phase": phase, "sandbox_id": sbx.sandbox_id, "profile": sbx.profile, "node": node,
                   "workload_ms": workload_ms, "workload_ok": workload_ok, "suspend_ms": suspend_ms,
                   "ok": ok, "error": error, "t_ms": now_ms()})

    def is_portable(self, sbx: Sbx) -> bool:
        if sbx.portable:
            return True
        if sbx.suspended_at_ms is None or now_ms() - sbx.suspended_at_ms < self.args.portable_wait_s * 1000:
            return False
        if self.args.database_url:
            try:
                out = subprocess.run(["psql", self.args.database_url, "-Atc",
                                      RESTORE_PORTABLE_SQL.format(sandbox_id=sbx.canonical)],
                                     capture_output=True, text=True, timeout=15)
            except subprocess.TimeoutExpired:
                return False
            if out.stdout.strip() != "t":
                return False
        sbx.portable = True
        return True

    # -- Block phases

    def create_sandboxes(self, block: Block, load_count: int, bystander_count: int, seed: int) -> list[Sbx]:
        a = self.args
        created: list[Sbx] = []
        profiles = stage1.assign_profiles(load_count, self.profiles, seed)
        metadata = {"space-bench": "xnode", "space-bench-run": self.run_id, "space-bench-block": str(block.index),
                    "space-bench-variant": block.label}

        def create(role: str, profile: str) -> None:
            try:
                handle = self.ecw.create(block.template, metadata)
            except Exception as exc:
                block.bump("create_failures")
                self.log(f"WARNING: create failed: {exc}")
                return
            sbx = Sbx(sandbox_id=handle.sandbox_id, role=role, profile=profile, handle=handle)
            with block.lock:
                block.sandboxes[sbx.sandbox_id] = sbx
                created.append(sbx)
            with self.emit_lock, (self.output_dir / f"sandboxes-block-{block.index}.txt").open("a") as ids:
                ids.write(sbx.sandbox_id + "\n")
            if role == "load":
                out = self.ecw.run(handle, guest_command(stage1.GUEST_SETUP, a.pattern_mem_mb, a.pattern_file_mb,
                                                         a.anon_check_mb, stage1.GUEST_ANON_HOLDER),
                                   timeout=a.guest_timeout_s)
                kv = stage1.parse_kv(out.stdout)
                if out.exit_code != 0 or "mem_sha" not in kv:
                    block.bump("setup_failures")
                    sbx.state = "failed"
                    return
                sbx.expected = {k: kv[k] for k in ("mem_sha", "file_sha", "anon_sha") if k in kv}

        jobs = [("load", p) for p in profiles] + [("bystander", "probe")] * bystander_count
        with concurrent.futures.ThreadPoolExecutor(max_workers=a.create_concurrency) as creators:
            list(creators.map(lambda job: create(*job), jobs))
        return created

    def seed_reservoir(self, block: Block, candidates: Sequence[Sbx]) -> dict[str, int]:
        """Run each reservoir sandbox's workload and suspend it on whichever worker it landed on."""
        self.refresh_placements()
        chosen = [s for s in candidates if s.state == "running"]
        seeded: dict[str, int] = {}
        for s in chosen:
            s.state = "busy"
            node = self.node_of(s) or "unknown"
            seeded[node] = seeded.get(node, 0) + 1
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.args.create_concurrency) as workers:
            list(workers.map(lambda s: self.suspend(block, s, phase="seed",
                                                    rng=random.Random(f"{self.args.seed}-{s.sandbox_id}")), chosen))
        return seeded

    def wait_portable(self, block: Block) -> None:
        deadline = self.clock() + self.args.portable_wait_s + self.args.portable_timeout_s
        while self.clock() < deadline:
            pending = [s for s in block.sandboxes.values() if s.state == "suspended" and not self.is_portable(s)]
            if not pending:
                return
            self.sleep(2)
        block.bump("seed_not_portable", sum(1 for s in block.sandboxes.values()
                                            if s.state == "suspended" and not s.portable))

    def pick_reservoir(self, block: Block, source: str) -> Sbx | None:
        with block.lock:
            candidates = sorted((s for s in block.sandboxes.values()
                                 if s.state == "suspended" and s.suspend_node == source),
                                key=lambda s: s.suspended_at_ms or 0)
        # Oldest first; the portability check may query the database, so it
        # runs outside the block lock and the claim is re-checked under it.
        for sbx in candidates:
            if not self.is_portable(sbx):
                continue
            with block.lock:
                if sbx.state == "suspended" and sbx.suspend_node == source:
                    sbx.state = "waking"
                    return sbx
        return None

    def wake(self, block: Block, sbx: Sbx, source: str, wake_id: str, paired_node: str | None,
             inflight: int) -> None:
        a = self.args
        start_ms = now_ms()
        started = self.clock()
        deadline = started + a.wake_timeout_s
        with block.lock:
            block.inflight[wake_id] = (sbx.sandbox_id, start_ms)
        outcome, error, connect_ms, handle = "ok", None, None, None
        try:
            handle = self.ecw.connect(sbx.sandbox_id, request_timeout=a.wake_timeout_s)
            connect_ms = int((self.clock() - started) * 1000)
            while True:
                remaining = deadline - self.clock()
                if remaining <= 0:
                    outcome = "timeout"
                    break
                try:
                    if self.ecw.run(handle, "true", timeout=max(1.0, remaining)).exit_code == 0:
                        break
                except Exception as exc:  # the data plane may not route yet
                    error = str(exc)
                self.sleep(0.2)
        except Exception as exc:
            outcome, error = ("timeout" if self.clock() >= deadline else "error"), str(exc)
        reconnect_ms = int((self.clock() - started) * 1000)
        if reconnect_ms > a.wake_timeout_s * 1000 and outcome == "ok":
            outcome = "timeout"
        row: dict[str, Any] = {
            "kind": "wake", "wake_id": wake_id, "block": block.index, "variant": block.label,
            "template": block.template, "load": a.load, "sandbox_id": sbx.sandbox_id, "profile": sbx.profile,
            "cycle": sbx.wakes + 1, "source_node": source, "paired_suspend_node": paired_node,
            "inflight_at_dispatch": inflight, "wake_start_ms": start_ms, "connect_ms": connect_ms,
            "reconnect_ms": reconnect_ms, "outcome": outcome, "error": error if outcome != "ok" else None,
            "correctness_ok": None, "correctness_failures": [], "verify_ms": None,
        }
        if outcome == "ok":
            verify_started = self.clock()
            try:
                out = self.ecw.run(handle, guest_command(stage1.GUEST_VERIFY, f"{wake_id}",
                                                         1 if "anon_sha" in sbx.expected else 0),
                                   timeout=a.guest_timeout_s)
                failures = stage1.compare_checksums(sbx.expected, stage1.parse_kv(out.stdout))
                row["correctness_ok"] = out.exit_code == 0 and not failures
                row["correctness_failures"] = failures if out.exit_code == 0 else ["verify_failed"]
            except Exception as exc:
                row["correctness_ok"] = False
                row["correctness_failures"] = [f"verify_error: {exc}"[:200]]
            row["verify_ms"] = int((self.clock() - verify_started) * 1000)
        self.emit(row)
        with block.lock:
            block.inflight.pop(wake_id, None)
            block.wakes.append({"wake_id": wake_id, "sandbox_id": sbx.sandbox_id, "wake_start_ms": start_ms,
                                "source_node": source})
        with sbx.lock:
            sbx.wakes += 1
            if outcome == "ok" and row["correctness_ok"]:
                sbx.handle, sbx.state, sbx.suspend_node, sbx.suspended_at_ms = handle, "running", None, None
                sbx.awake_since_ms = start_ms
            else:
                # A failed or corrupted sandbox never re-enters the experiment.
                sbx.state = "failed"
        if sbx.state == "running" and a.load == "loaded":
            self.pool.submit(self.load_loop, block, sbx)

    def load_loop(self, block: Block, sbx: Sbx) -> None:
        """Loaded mode: run the workload mix; between workloads, serve a pending paired suspend."""
        rng = random.Random(f"{self.args.seed}-{sbx.sandbox_id}-{sbx.wakes}")
        ran_once = False
        while not block.stop.is_set():
            with sbx.lock:
                if sbx.state != "running":
                    return
                sbx.state = "busy"
            node = self.node_of(sbx)
            if ran_once and node and block.take_suspend(node):
                self.suspend(block, sbx, phase="paired", rng=rng, run_workload_first=False)
                return
            try:
                self.run_workload(block, sbx, rng)
                ran_once = True
            except Exception:
                if block.stop.is_set():
                    return
                block.bump("workload_errors")
            node = self.node_of(sbx)
            if node and block.take_suspend(node):
                self.suspend(block, sbx, phase="paired", rng=rng, run_workload_first=False)
                return
            with sbx.lock:
                if sbx.state == "busy":
                    sbx.state = "running"
            block.stop.wait(self.args.load_rest_s)

    def quiet_suspend(self, block: Block, node: str) -> None:
        """Quiet mode: one idle sandbox on ``node`` runs its workload and is suspended."""
        with block.lock:
            idle = [s for s in block.sandboxes.values()
                    if s.role == "load" and s.state == "running" and self.node_of(s) == node]
            if not idle:
                block.bump("paired_suspend_skipped")
                return
            sbx = random.Random(f"{self.args.seed}-{len(block.wakes)}").choice(idle)
            sbx.state = "busy"
        self.suspend(block, sbx, phase="paired", rng=random.Random(f"{self.args.seed}-{sbx.sandbox_id}"))

    def probe_loop(self, block: Block, sbx: Sbx) -> None:
        """F1: a bystander runs ``true`` every ``--probe-interval-s`` and records the round trip."""
        while not block.stop.is_set():
            started = self.clock()
            ok = False
            try:
                ok = self.ecw.run(sbx.handle, "true", timeout=30).exit_code == 0
            except Exception:
                if block.stop.is_set():
                    return
            rtt_ms = int((self.clock() - started) * 1000)
            self.emit({"kind": "probe", "block": block.index, "variant": block.label, "node": self.node_of(sbx),
                       "sandbox_id": sbx.sandbox_id, "t_ms": now_ms(), "rtt_ms": rtt_ms, "ok": ok})
            block.stop.wait(max(0.0, self.args.probe_interval_s - (self.clock() - started)))

    def rotate(self, block: Block, old: str | None, new: str) -> None:
        """Cordon ``new``, let in-flight restores from ``old`` bind elsewhere, then uncordon ``old``."""
        self.guard.cordon(new)
        if old is None or old == new:
            return
        deadline = self.clock() + self.args.bind_timeout_s
        while self.clock() < deadline:
            with block.lock:
                pending = [(sid, start) for sid, start in block.inflight.values()]
            unbound = [sid for sid, start in pending
                       if self.bound_since(block.sandboxes[sid], start) is None]
            if not unbound:
                break
            self.refresh_placements()
            self.sleep(1)
        else:
            block.bump("rotation_bind_timeouts")
        self.guard.uncordon(old)

    def arrivals(self, block: Block) -> None:
        a = self.args
        block_s = a.block_minutes * 60
        rng = random.Random(f"{a.seed}-arrivals-{block.index}")
        futures: list[concurrent.futures.Future[None]] = []
        source: str | None = None
        targets_rr = 0
        start = self.clock()
        if a.load == "loaded":
            schedule: Iterable[float] = poisson_offsets(a.rate, block_s, rng)
        else:
            schedule = (i * a.quiet_interval_s for i in range(int(block_s // a.quiet_interval_s)))
        for offset in schedule:
            wait = start + offset - self.clock()
            if wait > 0:
                self.sleep(wait)
            if self.clock() - start >= block_s:
                break
            wanted = source_for(self.nodes, offset, a.rotate_seconds)
            if wanted != source:
                self.rotate(block, source, wanted)
                source = wanted
            sbx = self.pick_reservoir(block, source)
            if sbx is None:
                block.bump("arrivals_skipped_empty_reservoir")
                self.emit({"kind": "arrival_skipped", "block": block.index, "variant": block.label,
                           "source_node": source, "t_ms": now_ms(), "reason": "empty_reservoir"})
                continue
            targets = [n for n in self.nodes if n != source]
            paired = targets[targets_rr % len(targets)] if a.paired_suspend else None
            targets_rr += 1
            if paired:
                if a.load == "loaded":
                    block.request_suspend(paired)
                else:
                    futures.append(self.pool.submit(self.quiet_suspend, block, paired))
            with block.lock:
                inflight = len(block.inflight)
            wake_id = f"{block.index}-{len(block.wakes) + len(block.inflight)}-{uuid.uuid4().hex[:8]}"
            future = self.pool.submit(self.wake, block, sbx, source, wake_id, paired, inflight)
            futures.append(future)
            if a.load == "quiet":
                future.result()  # one restore at a time
        for future in futures:
            try:
                future.result(timeout=a.wake_timeout_s + a.guest_timeout_s)
            except Exception as exc:
                self.log(f"WARNING: wake task failed: {exc}")

    def classify_block(self, block: Block) -> dict[str, int]:
        lines = self.kube.sbxmgr_log_lines(self.nodes, block.start_ms - 60_000)
        activations = [act for node, line in lines if (act := parse_activation(line, node)) is not None]
        with (self.output_dir / f"activations-block-{block.index}.jsonl").open("w") as f:
            for act in activations:
                f.write(json.dumps(dataclasses.asdict(act), sort_keys=True) + "\n")
        joined = join_activations(block.wakes, activations)
        counts: dict[str, int] = {}
        for wake in block.wakes:
            act = joined.get(wake["wake_id"])
            label = classify(wake["source_node"], act, self.nodes)
            counts[label] = counts.get(label, 0) + 1
            self.emit({"kind": "wake_placement", "block": block.index, "variant": block.label,
                       "wake_id": wake["wake_id"], "sandbox_id": wake["sandbox_id"],
                       "source_node": wake["source_node"], "landed_node": act.node if act else None,
                       "boot_type": act.boot_type if act else None,
                       "restore_provenance": act.provenance if act else None, "classification": label})
        return counts

    def cleanup(self, block: Block) -> None:
        if self.args.no_cleanup:
            return
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.args.create_concurrency) as killers:
            def kill(sbx: Sbx) -> None:
                try:
                    self.ecw.kill(sbx.sandbox_id)
                except Exception as exc:
                    block.bump("delete_failures")
                    self.log(f"WARNING: delete {sbx.sandbox_id} failed: {exc}")
            list(killers.map(kill, list(block.sandboxes.values())))

    def run_block(self, index: int, label: str) -> None:
        a = self.args
        block = Block(index, label, self.variants[label])
        self.log(f"block {index} ({label}, template {block.template}): creating sandboxes")
        prep_started = now_ms()
        seeded: dict[str, int] = {}
        per_node: dict[str, int] = {}
        try:
            # Reservoir first, suspended before the running population exists,
            # so a worker never holds both sets in memory at once.
            n = len(self.nodes)
            reservoir = self.create_sandboxes(block, self.reservoir * n, 0, a.seed + block.index)
            seeded = self.seed_reservoir(block, reservoir)
            self.create_sandboxes(block, a.population_per_node * n, a.bystanders_per_node * n,
                                  a.seed + block.index + 10_000)
            self.log(f"block {index}: reservoir {seeded}; waiting for snapshots to reach object storage")
            self.wait_portable(block)
            for s in block.sandboxes.values():
                node = self.node_of(s)
                if node:
                    per_node[node] = per_node.get(node, 0) + 1
            outside = sum(1 for s in block.sandboxes.values() if self.node_of(s) not in (*self.nodes, None))
            if outside:
                block.bump("sandboxes_outside_pool", outside)
            block.start_ms = now_ms()
            threading.Thread(target=self.placement_loop, args=(block,), daemon=True).start()
            for s in list(block.sandboxes.values()):
                if s.role == "bystander" and s.state == "running":
                    self.pool.submit(self.probe_loop, block, s)
                elif a.load == "loaded" and s.role == "load" and s.state == "running":
                    self.pool.submit(self.load_loop, block, s)
            self.log(f"block {index}: measuring for {a.block_minutes} min")
            self.arrivals(block)
        finally:
            block.stop.set()
            end_ms = now_ms()
            failed = self.guard.release_all()
            if failed:
                self.log(f"ERROR: could not uncordon {failed}; run with --repair")
        try:
            counts = self.classify_block(block)
        except KubeError as exc:
            self.log(f"WARNING: could not read restore placement: {exc}")
            counts = {"unclassified": len(block.wakes)}
        self.emit({"kind": "block", "block": index, "variant": label, "template": block.template, "load": a.load,
                   "rate_per_min": a.rate if a.load == "loaded" else None,
                   "quiet_interval_s": a.quiet_interval_s if a.load == "quiet" else None,
                   "nodes": self.nodes, "prep_start_ms": prep_started, "start_ms": block.start_ms,
                   "end_ms": end_ms, "warmup_ms": int(a.warmup_minutes * 60_000), "rotate_s": a.rotate_seconds,
                   "reservoir_per_node": self.reservoir, "seeded": seeded,
                   "sandboxes_per_node": per_node,
                   "wakes": len(block.wakes), "classification": counts, "counters": block.counters})
        self.log(f"block {index}: {len(block.wakes)} wake-ups, placement {counts}, counters {block.counters}")
        self.cleanup(block)

    def preflight(self) -> None:
        cordoned = [node for node in self.nodes if self.kube.node_unschedulable(node)]
        if cordoned:
            raise SystemExit(f"ERROR: {cordoned} already cordoned; refusing to run (and to uncordon them later)")
        if self.guard.state_path.exists():
            raise SystemExit(f"ERROR: {self.guard.state_path} exists from an earlier run; use --repair first")

    @property
    def profiles(self) -> list[str]:
        return [p.strip() for p in self.args.profiles.split(",") if p.strip()]

    def run(self) -> None:
        self.preflight()
        self.emit({"kind": "run", "t_ms": now_ms(), "order": self.order, "variants": self.variants,
                   "baseline": self.args.baseline, "nodes": self.nodes, "reservoir_per_node": self.reservoir,
                   "args": {k: v for k, v in vars(self.args).items() if k not in {"database_url"}}})
        try:
            for index, label in enumerate(self.order):
                self.run_block(index, label)
        finally:
            self.guard.release_all()
            self.pool.shutdown(wait=False, cancel_futures=True)


def plan_text(args: argparse.Namespace, order: Sequence[str], reservoir: int) -> str:
    block_s = args.block_minutes * 60
    lines = [f"Order ({len(order)} blocks of {args.block_minutes:g} min, first {args.warmup_minutes:g} min dropped):",
             "  " + ", ".join(order),
             f"Load: {args.load}" + (f", {args.rate:g} wake-ups/min" if args.load == "loaded"
                                      else f", one wake-up every {args.quiet_interval_s:g} s"),
             f"Source rotates every {args.rotate_seconds:g} s across {', '.join(args.nodes)}",
             f"Per node per block: {args.population_per_node} load + {reservoir} reservoir + "
             f"{args.bystanders_per_node} bystander sandboxes"]
    minimum = 2000 if args.load == "loaded" else 400
    for label in dict.fromkeys(order):
        n = expected_samples(order, label, rate_per_min=args.rate, block_s=block_s,
                             warmup_s=args.warmup_minutes * 60, load=args.load,
                             quiet_interval_s=args.quiet_interval_s)
        flag = "" if n >= minimum else f"  (below the {minimum} minimum)"
        lines.append(f"  {label}: ~{n:.0f} measured wake-ups{flag}")
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0],
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--nodes", required=True, type=lambda s: [n.strip() for n in s.split(",") if n.strip()],
                   help="Comma-separated kube node names of the dedicated pool. Only these are ever cordoned.")
    p.add_argument("--template", default="seed", help="Template for a single unlabeled variant.")
    p.add_argument("--variant", action="append", default=[], metavar="LABEL=TEMPLATE")
    p.add_argument("--baseline", default="baseline", help="Variant label every other variant is compared against.")
    p.add_argument("--blocks-per-variant", type=int, default=4)
    p.add_argument("--order", default="", help="Explicit comma-separated block order (overrides --blocks-per-variant).")
    p.add_argument("--block-minutes", type=float, default=30)
    p.add_argument("--warmup-minutes", type=float, default=5, help="Recorded on each block; the summary drops it.")
    p.add_argument("--load", choices=["loaded", "quiet"], default="loaded")
    p.add_argument("--rate", type=float, default=6.0, help="Loaded: mean wake-ups per minute (Poisson).")
    p.add_argument("--quiet-interval-s", type=float, default=10.0, help="Quiet: seconds between wake-up starts.")
    p.add_argument("--rotate-seconds", type=float, default=180, help="How long each worker stays the cordoned source.")
    p.add_argument("--population-per-node", type=int, default=80, help="Running load sandboxes per worker.")
    p.add_argument("--reservoir-per-node", type=int, default=0, help="Seeded suspended sandboxes per worker; 0 sizes it from the rate.")
    p.add_argument("--bystanders-per-node", type=int, default=10)
    p.add_argument("--probe-interval-s", type=float, default=2.0)
    p.add_argument("--no-paired-suspend", dest="paired_suspend", action="store_false")
    p.add_argument("--load-rest-s", type=float, default=5.0, help="Loaded: pause between a sandbox's workloads.")
    p.add_argument("--wake-timeout-s", type=float, default=120)
    p.add_argument("--portable-wait-s", type=float, default=30,
                   help="Minimum time after suspend before a sandbox may be woken from another worker.")
    p.add_argument("--portable-timeout-s", type=float, default=600)
    p.add_argument("--database-url", default=os.environ.get("SPACE_BENCH_DATABASE_URL", ""),
                   help="Optional API database URL; when set, also require the API's restore-portability predicate.")
    p.add_argument("--bind-timeout-s", type=float, default=60)
    p.add_argument("--placement-interval-s", type=float, default=5)
    p.add_argument("--profiles", default=",".join(stage1.PROFILES))
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--pattern-mem-mb", type=int, default=256)
    p.add_argument("--pattern-file-mb", type=int, default=64)
    p.add_argument("--anon-check-mb", type=int, default=256)
    p.add_argument("--build-units", type=int, default=200)
    p.add_argument("--build-blob-mb", type=int, default=512)
    p.add_argument("--alloc-min-mb", type=int, default=2048)
    p.add_argument("--alloc-max-mb", type=int, default=4096)
    p.add_argument("--file-heavy-mb", type=int, default=1024)
    p.add_argument("--idle-secs", type=int, default=30)
    p.add_argument("--guest-timeout-s", type=float, default=900)
    p.add_argument("--create-concurrency", type=int, default=16)
    p.add_argument("--max-threads", type=int, default=512)
    p.add_argument("--sandbox-timeout-s", type=int, default=0, help="ECW sandbox lifetime; 0 = block length + 1 h.")
    p.add_argument("--api-url", default=os.environ.get("E2B_API_URL", ""))
    p.add_argument("--domain", default=os.environ.get("E2B_DOMAIN", ""))
    p.add_argument("--api-key-env", default="E2B_API_KEY", help="Environment variable holding the ECW API key.")
    p.add_argument("--kubectl", default="kubectl")
    p.add_argument("--context", default="", help="kubectl context of the pool's cluster.")
    p.add_argument("--sandbox-namespace", default="sandboxes")
    p.add_argument("--node-namespace", default="space", help="Namespace of the worker sandbox-manager pods.")
    p.add_argument("--sbxmgr-selector", default=DEFAULT_SBXMGR_SELECTOR)
    p.add_argument("--sbxmgr-container", default=DEFAULT_SBXMGR_CONTAINER)
    p.add_argument("--output-dir", default="")
    p.add_argument("--run-id", default="")
    p.add_argument("--no-cleanup", action="store_true")
    p.add_argument("--dry-run", action="store_true", help="Print the block plan and expected sample counts, then exit.")
    p.add_argument("--repair", action="store_true", help="Uncordon the node recorded in OUTPUT_DIR/cordon.state and exit.")
    return p


def validate(args: argparse.Namespace) -> None:
    if len(args.nodes) < 2 or len(set(args.nodes)) != len(args.nodes):
        raise SystemExit("ERROR: --nodes needs at least two distinct workers")
    if args.rotate_seconds <= 0 or args.block_minutes <= 0 or args.wake_timeout_s <= 0:
        raise SystemExit("ERROR: --rotate-seconds, --block-minutes and --wake-timeout-s must be positive")
    if args.warmup_minutes >= args.block_minutes:
        raise SystemExit("ERROR: --warmup-minutes must be shorter than --block-minutes")
    unknown = [p for p in args.profiles.split(",") if p.strip() and p.strip() not in stage1.PROFILES]
    if unknown:
        raise SystemExit(f"ERROR: unknown --profiles {unknown}; choose from {','.join(stage1.PROFILES)}")
    if not args.output_dir:
        args.output_dir = f"/tmp/space-sbx-xnode-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    if not args.sandbox_timeout_s:
        args.sandbox_timeout_s = min(86_400, int(args.block_minutes * 60 + 3600))


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    validate(args)
    labels = list(dict(stage1.parse_variants(args.variant, args.template)))
    if args.baseline not in labels:
        args.baseline = labels[0]
    order = parse_order(args.order, labels) if args.order else block_order(labels, args.baseline,
                                                                           args.blocks_per_variant)
    reservoir = args.reservoir_per_node or reservoir_target(args.rate, args.rotate_seconds, args.load,
                                                            args.quiet_interval_s)
    print(plan_text(args, order, reservoir))
    if args.dry_run:
        return 0
    kube = Kube(kubectl=args.kubectl, context=args.context, sandbox_namespace=args.sandbox_namespace,
                node_namespace=args.node_namespace, sbxmgr_selector=args.sbxmgr_selector,
                sbxmgr_container=args.sbxmgr_container)
    state = Path(args.output_dir) / "cordon.state"
    if args.repair:
        print(f"uncordoned: {CordonGuard.repair(kube, state) or 'nothing recorded'}")
        return 0
    api_key = os.environ.get(args.api_key_env, "")
    if not api_key or not args.domain:
        raise SystemExit(f"ERROR: set ${args.api_key_env} and --domain (or $E2B_DOMAIN)")
    ecw = Ecw(api_url=args.api_url, domain=args.domain, api_key=api_key, sandbox_timeout_s=args.sandbox_timeout_s)
    bench = XnodeBenchmark(args, kube=kube, ecw=ecw)

    def on_signal(signum: int, _frame: Any) -> None:
        failed = bench.guard.release_all()
        print(f"\nInterrupted (signal {signum}); uncordon failures: {failed or 'none'}", file=sys.stderr)
        raise SystemExit(130)

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)
    print(f"Results: {bench.results}")
    bench.run()
    print(f"Done. Summarize with:\n  python3 scripts/benchmarks/bench-sbx-xnode-summary.py {bench.results} "
          f"--baseline {args.baseline}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

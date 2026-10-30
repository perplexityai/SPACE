#!/usr/bin/env python3
"""Record and report where a Kind + Tilt bring-up spends its time.

Run on the host that runs the Kind stack. Standard library only; shells out to
`tilt`, `kubectl`, `docker` and `curl`, the tools the bring-up already needs.

    # background recorder: Tilt resource transitions, pod/job state, healthz
    # flips, host CPU/load, Docker data-root headroom, and every Tilt log line
    # stamped on arrival (the log file itself has no timestamps)
    python3 kind-bringup-timeline.py watch OUT &
    # stamp any command's output the same way
    cmd 2>&1 | python3 kind-bringup-timeline.py stamp > OUT/cmd.log
    # one-shot raw dump: uiresources, pods, jobs, events, tilt pid/cwd, disk
    python3 kind-bringup-timeline.py snapshot OUT
    # timeline relative to t0 (defaults to the watch start)
    python3 kind-bringup-timeline.py report OUT [--t0 2026-09-23T14:00:00Z]

The report reads the snapshot for exact Tilt build windows (buildHistory) and
Kubernetes transition times (pod conditions, init containers, jobs, image-pull
events), and the watch log for what those APIs do not keep: queueing before a
build starts, the first healthz success, when Tilt last had work, CPU usage,
and phase markers inside a build (cargo, artifact assembly, image build, kind
load) taken from the stamped Tilt log.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

CLUSTER = os.environ.get("SPACE_KIND_CLUSTER", "space-local")
CONTEXT = f"kind-{CLUSTER}"
NAMESPACE = "space"
STATE_DIR = Path(os.environ.get("SPACE_TILT_STATE_DIR", Path.home() / ".space-devbox/kind"))
TILT_LOG = Path(os.environ.get("SPACE_TILT_LOG", STATE_DIR / f"tilt-{CLUSTER}.log"))
HEALTHZ = {
    "api": os.environ.get("API_URL", "http://127.0.0.1:8080") + "/healthz",
    "data-plane": os.environ.get("DATA_PLANE_URL", "http://127.0.0.1:8090") + "/healthz",
}


def now() -> float:
    return time.time()


def iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-4] + "Z"


def parse_ts(value: str | None) -> float | None:
    if not value:
        return None
    value = value.strip()
    if re.fullmatch(r"\d+(\.\d+)?", value):
        return float(value)
    value = value.replace("Z", "+00:00")
    # Kubernetes and Tilt emit RFC 3339 with up to nanosecond fractions.
    value = re.sub(r"(\.\d{6})\d+", r"\1", value)
    try:
        return datetime.fromisoformat(value).timestamp()
    except ValueError:
        return None


def run(cmd: list[str], timeout: float = 20) -> str | None:
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except (OSError, subprocess.TimeoutExpired):
        return None
    return out.stdout if out.returncode == 0 else None


def run_json(cmd: list[str], timeout: float = 20):
    out = run(cmd, timeout)
    if not out:
        return None
    try:
        return json.loads(out)
    except json.JSONDecodeError:
        return None


def tilt_resources():
    return run_json(["tilt", "get", "uiresources", "--port", "10350", "-o", "json"], timeout=10)


def kube_json(*args: str, timeout: float = 20):
    return run_json(["kubectl", "--context", CONTEXT, "-n", NAMESPACE, *args, "-o", "json"], timeout)


def healthz_ok(url: str) -> bool:
    return run(["curl", "-sf", "-m", "2", "-o", "/dev/null", url], timeout=5) is not None


def tilt_pids() -> list[int]:
    pids = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            argv = (entry / "cmdline").read_bytes().split(b"\0")
        except OSError:
            continue
        if len(argv) >= 2 and argv[0].split(b"/")[-1] == b"tilt" and argv[1] == b"up":
            pids.append(int(entry.name))
    return sorted(pids)


def proc_start_epoch(pid: int) -> float | None:
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        start_ticks = int(fields[19])
        btime = next(int(line.split()[1]) for line in Path("/proc/stat").read_text().splitlines()
                     if line.startswith("btime"))
        return btime + start_ticks / os.sysconf("SC_CLK_TCK")
    except (OSError, ValueError, StopIteration, IndexError):
        return None


def docker_root_free_gib() -> float | None:
    images = run(["docker", "images", "--format", "{{.Repository}}:{{.Tag}}"], timeout=15) or ""
    image = next((i for i in images.splitlines()
                  if re.match(r"^(kindest/node|public\.ecr\.aws/docker/library/(debian|alpine)|debian|alpine):", i)), None)
    if not image:
        return None
    out = run(["docker", "run", "--rm", "--entrypoint", "df", image, "-Pk", "/"], timeout=60)
    try:
        return int(out.splitlines()[1].split()[3]) / 1048576
    except (AttributeError, IndexError, ValueError):
        return None


def cpu_sample():
    try:
        vals = [int(v) for v in Path("/proc/stat").read_text().splitlines()[0].split()[1:]]
        load1 = float(Path("/proc/loadavg").read_text().split()[0])
    except (OSError, ValueError, IndexError):
        return None
    idle = vals[3] + (vals[4] if len(vals) > 4 else 0)
    return sum(vals), idle, load1


# ── stamp ────────────────────────────────────────────────────────────────


def cmd_stamp(_args) -> int:
    for line in sys.stdin:
        sys.stdout.write(f"{iso(now())} {line}")
        sys.stdout.flush()
    return 0


# ── watch ────────────────────────────────────────────────────────────────


class Watch:
    def __init__(self, out: Path, interval: float):
        self.out = out
        self.interval = interval
        self.stop = threading.Event()
        self.events = open(out / "watch.jsonl", "a", buffering=1)
        self.lock = threading.Lock()

    def emit(self, kind: str, **fields):
        record = {"ts": round(now(), 3), "kind": kind, **fields}
        with self.lock:
            self.events.write(json.dumps(record, sort_keys=True) + "\n")

    def tail_tilt_log(self):
        # Tilt's log has no timestamps; stamp each line as it lands. The
        # bring-up truncates the file when it restarts Tilt, so reopen on shrink.
        # Binary mode, not text: a text handle's tell() is an opaque cookie, not
        # a byte offset, and on this log (box-drawing glyphs and emoji) it comes
        # back above 1e12 often enough that `st.st_size < pos` fired spuriously
        # and re-stamped the whole file from the top — every marker counted
        # again, with the re-read's arrival time.
        stamped = open(self.out / "tilt-log-stamped.txt", "a", buffering=1)
        handle, inode, pos = None, None, 0
        # Lines already in the file predate this watch; stamping them now would
        # date them wrong. Start at the end, and from the top after a reopen.
        try:
            st = TILT_LOG.stat()
            handle, inode, pos = open(TILT_LOG, "rb"), st.st_ino, st.st_size
            stamped.write(f"{iso(now())} ### watch attached at byte {pos} of an existing tilt log\n")
        except OSError:
            pass
        while not self.stop.is_set():
            try:
                st = TILT_LOG.stat()
            except OSError:
                time.sleep(0.5)
                continue
            if handle is None or st.st_ino != inode or st.st_size < pos:
                if handle:
                    handle.close()
                    stamped.write(f"{iso(now())} ### tilt log reopened\n")
                handle, inode, pos = open(TILT_LOG, "rb"), st.st_ino, 0
            handle.seek(pos)
            chunk = handle.readline()
            while chunk:
                if not chunk.endswith(b"\n"):
                    break  # partial line; wait for the rest
                stamped.write(f"{iso(now())} {chunk.decode('utf-8', 'replace')}")
                pos = handle.tell()
                chunk = handle.readline()
            time.sleep(0.2)

    def poll(self):
        last_res: dict[str, tuple] = {}
        last_pod: dict[str, tuple] = {}
        last_health: dict[str, bool] = {}
        last_tilt: tuple = ()
        prev_cpu = cpu_sample()
        next_disk = 0.0
        while not self.stop.is_set():
            started = now()
            pids = tilt_pids()
            tilt_state = tuple((p, os.path.realpath(f"/proc/{p}/cwd") if Path(f"/proc/{p}").exists() else "") for p in pids)
            if tilt_state != last_tilt:
                self.emit("tilt-process", pids=[{"pid": p, "cwd": c, "started": proc_start_epoch(p)} for p, c in tilt_state])
                last_tilt = tilt_state
            data = tilt_resources() if pids else None
            busy = []
            for item in (data or {}).get("items", []):
                name = item["metadata"]["name"]
                st = item.get("status", {})
                key = (st.get("updateStatus"), st.get("runtimeStatus"),
                       (st.get("currentBuild") or {}).get("startTime"), bool(st.get("queued")),
                       st.get("pendingBuildSince"))
                if st.get("updateStatus") in ("pending", "in_progress"):
                    busy.append(name)
                if last_res.get(name) != key:
                    self.emit("resource", name=name, update=key[0], runtime=key[1],
                              build_start=key[2], queued=key[3], pending_since=key[4])
                    last_res[name] = key
            self.emit("busy", resources=sorted(busy), tilt_up=bool(data))
            for label, url in HEALTHZ.items():
                ok = healthz_ok(url)
                if last_health.get(label) != ok:
                    self.emit("healthz", target=label, ok=ok)
                    last_health[label] = ok
            pods = kube_json("get", "pods", timeout=10)
            # A failed list is not an empty one: the API is legitimately down
            # during a bring-up, and reading its silence as "every pod gone"
            # would fabricate deletions and recreations. Reconcile only
            # against a response the API actually gave.
            if pods is not None:
                seen = set()
                for pod in pods.get("items", []):
                    name = pod["metadata"]["name"]
                    seen.add(name)
                    conds = {c["type"]: c["status"] for c in pod.get("status", {}).get("conditions", [])}
                    key = (pod.get("status", {}).get("phase"), conds.get("Ready"),
                           tuple(sorted((c["name"], c.get("ready", False)) for c in pod.get("status", {}).get("containerStatuses", []))))
                    if last_pod.get(name) != key:
                        self.emit("pod", name=name, phase=key[0], ready=key[1])
                        last_pod[name] = key
                for gone in set(last_pod) - seen:
                    self.emit("pod", name=gone, phase="Deleted", ready=None)
                    del last_pod[gone]
            cur = cpu_sample()
            if cur and prev_cpu:
                total, idle = cur[0] - prev_cpu[0], cur[1] - prev_cpu[1]
                util = 100.0 * (1 - idle / total) if total else 0.0
                self.emit("cpu", util=round(util, 1), load1=cur[2], ncpu=os.cpu_count())
            prev_cpu = cur
            if started >= next_disk:
                self.emit("disk", docker_free_gib=docker_root_free_gib())
                next_disk = started + 60
            self.stop.wait(max(0.0, self.interval - (now() - started)))

    def run(self):
        self.emit("watch-start", cluster=CLUSTER, tilt_log=str(TILT_LOG), host=os.uname().nodename)
        tailer = threading.Thread(target=self.tail_tilt_log, daemon=True)
        tailer.start()
        try:
            self.poll()
        finally:
            self.emit("watch-stop")


def cmd_watch(args) -> int:
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    watch = Watch(out, args.interval)
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: watch.stop.set())
    watch.run()
    return 0


# ── snapshot ─────────────────────────────────────────────────────────────


def cmd_snapshot(args) -> int:
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    snap = {"taken": now(), "cluster": CLUSTER}
    snap["uiresources"] = tilt_resources()
    for kind in ("pods", "jobs", "deployments", "daemonsets", "statefulsets", "replicasets", "events"):
        snap[kind] = kube_json("get", kind)
    snap["nodes"] = run_json(["kubectl", "--context", CONTEXT, "get", "nodes", "-o", "json"])
    created = run(["docker", "inspect", "-f", "{{.Created}}", f"{CLUSTER}-control-plane"], timeout=15)
    snap["control_plane_created"] = parse_ts(created)
    snap["tilt"] = [{"pid": p, "cwd": os.path.realpath(f"/proc/{p}/cwd"), "started": proc_start_epoch(p)}
                    for p in tilt_pids()]
    snap["docker_free_gib"] = docker_root_free_gib()
    snap["docker_df"] = run(["docker", "system", "df"], timeout=60)
    snap["images"] = run(["docker", "images", "--format", "{{.Repository}}:{{.Tag}} {{.ID}} {{.CreatedAt}} {{.Size}}"], timeout=30)
    (out / f"snapshot-{int(snap['taken'])}.json").write_text(json.dumps(snap, indent=1, default=str))
    (out / "snapshot-latest.json").write_text(json.dumps(snap, indent=1, default=str))
    print(out / f"snapshot-{int(snap['taken'])}.json")
    return 0


# ── report ───────────────────────────────────────────────────────────────

# Phase markers inside a custom_build, matched against the stamped Tilt log.
# First match per (resource, phase) wins; `end` markers take the last match.
MARKERS = [
    ("rust.lock-wait", r"\[rust-artifacts\] another artifact build holds"),
    ("rust.mode", r"\[rust-artifacts\] (incremental|docker) mode"),
    ("rust.cargo-first-compile", r"^\s*Compiling "),
    ("rust.cargo-finished", r"^\s*Finished .*profile"),
    ("rust.glibc-guard", r"\[rust-artifacts\] glibc guard"),
    ("rust.staging", r"\[rust-artifacts\] staging"),
    ("rust.assembling", r"\[rust-artifacts\] assembling"),
    ("rust.ready", r"\[rust-artifacts\] ready:"),
    ("rust.durations", r"\[rust-artifacts\] (builder image|cargo build|runtime-supervisor build|assembly|lock acquired)"),
    ("seed", r"\[build-seed\]"),
    ("docker-build", r"^#\d+ (\[|DONE|CACHED)"),
    ("kind-load", r"not yet present on node|kind load docker-image|Loading image|\[kind-load\]"),
    ("tilt.step", r"STEP \d+/\d+"),
]
# Image loads: Tilt's own push step ("Loading image to KIND"), timed by its
# build summary, and the load a custom_build runs itself
# (infra/space/kind/scripts/kind-load-image.py), timed by its last line. A
# build that loads itself still opens Tilt's push step, which skips at once.
# Tilt reprints a build's log, so a line seen twice is counted once: a load by
# the build reference its last line names (each build's is its own), Tilt's
# push step, which names none, by its text.
PUSH_START = re.compile(r"STEP (\d+)/\d+ — Pushing (\S+)")
PUSH_DONE = re.compile(r"Step (\d+) - ([\d.]+)s \(Pushing (\S+)\)")
OWN_LOAD = re.compile(
    r"\[kind-load\] (\S+): on (\d+) of (\d+) nodes, (\d+) loaded, in ([\d.]+) s(?: \(build (\S+)\))?")
# Tilt pads the resource column and truncates long names with an ellipsis.
LINE = re.compile(r"^(\S+) (?:\s*([A-Za-z0-9][\w.…-]*)\s+│ )?(.*)$")
ANSI = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]")


def load_watch(out: Path):
    path = out / "watch.jsonl"
    if not path.exists():
        return []
    events = []
    for line in path.read_text().splitlines():
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return events


def fmt(t: float | None, t0: float) -> str:
    return "      -" if t is None else f"{t - t0:7.1f}"


def dur(a: float | None, b: float | None) -> str:
    return "      -" if a is None or b is None else f"{b - a:7.1f}"


def cmd_report(args) -> int:
    out = Path(args.out)
    events = load_watch(out)
    snap_path = out / "snapshot-latest.json"
    snap = json.loads(snap_path.read_text()) if snap_path.exists() else {}
    t0 = parse_ts(args.t0) if args.t0 else None
    if t0 is None:
        t0 = next((e["ts"] for e in events if e["kind"] == "watch-start"), None) or snap.get("taken") or now()
    lines = []
    p = lines.append
    p(f"t0 = {iso(t0)}  (all times are seconds after t0)")
    if snap.get("control_plane_created"):
        p(f"kind control-plane container created: {fmt(snap['control_plane_created'], t0)}")
    for t in snap.get("tilt") or []:
        p(f"tilt pid {t['pid']} started {fmt(t.get('started'), t0)} cwd={t['cwd']}")

    # Tilt resources: exact build windows from buildHistory + queueing from watch.
    p("\n== Tilt resources (buildHistory; queued = pending before the build started)")
    p(f"{'resource':38} {'pending':>7} {'start':>7} {'finish':>7} {'build':>7} {'queued':>7}  result")
    # When each pending interval began, per resource: Tilt's own
    # pendingBuildSince when the sample carried it, else the sample that first
    # saw the resource pending.
    pending_starts: dict[str, list[float]] = {}
    last_update: dict[str, str | None] = {}
    for e in events:
        if e["kind"] != "resource":
            continue
        name, update = e["name"], e.get("update")
        if update == "pending" and last_update.get(name) != "pending":
            pending_starts.setdefault(name, []).append(parse_ts(e.get("pending_since")) or e["ts"])
        last_update[name] = update
    rows = []
    for item in ((snap.get("uiresources") or {}).get("items") or []):
        name = item["metadata"]["name"]
        st = item.get("status", {})
        builds = sorted(((parse_ts(b.get("startTime")), parse_ts(b.get("finishTime")), b)
                         for b in st.get("buildHistory") or []), key=lambda x: x[0] or 0)
        prev_finish = float("-inf")
        for s, f, b in builds:
            if s is None:
                continue
            # The pending interval this build ended: the last one to begin after
            # the previous build finished. None when no sample saw it — queue
            # time unknown, not measured from an earlier build's interval.
            q = max((t for t in pending_starts.get(name, []) if prev_finish < t <= s), default=None)
            prev_finish = f or s
            if s < t0 - 1:
                continue
            rows.append((f or 0, name, q, s, f, "ERROR " + b["error"][:60] if b.get("error") else "ok"))
    for f, name, q, s, fin, result in sorted(rows, key=lambda r: (r[4] or 0)):
        p(f"{name:38} {fmt(q, t0)} {fmt(s, t0)} {fmt(fin, t0)} {dur(s, fin)} {dur(q, s)}  {result}")

    # Kubernetes: pod lifecycle per workload.
    p("\n== Pods (created → scheduled → init done → ready); init containers listed separately")
    p(f"{'pod':52} {'created':>7} {'sched':>7} {'init':>7} {'ready':>7} {'c→r':>7}")
    for pod in sorted(((snap.get("pods") or {}).get("items") or []),
                      key=lambda x: parse_ts(x["metadata"].get("creationTimestamp")) or 0):
        md, st = pod["metadata"], pod.get("status", {})
        created = parse_ts(md.get("creationTimestamp"))
        conds = {c["type"]: parse_ts(c.get("lastTransitionTime")) for c in st.get("conditions", []) if c.get("status") == "True"}
        p(f"{md['name'][:52]:52} {fmt(created, t0)} {fmt(conds.get('PodScheduled'), t0)} "
          f"{fmt(conds.get('Initialized'), t0)} {fmt(conds.get('Ready'), t0)} {dur(created, conds.get('Ready'))}")
        for ic in st.get("initContainerStatuses", []) or []:
            term = (ic.get("state") or {}).get("terminated") or (ic.get("lastState") or {}).get("terminated") or {}
            a, b = parse_ts(term.get("startedAt")), parse_ts(term.get("finishedAt"))
            if a:
                p(f"    init {ic['name'][:44]:44} {fmt(a, t0)} → {fmt(b, t0)} ({dur(a, b).strip()} s, restarts {ic.get('restartCount', 0)})")
    p("\n== Jobs")
    for job in ((snap.get("jobs") or {}).get("items") or []):
        st = job.get("status", {})
        a, b = parse_ts(st.get("startTime")), parse_ts(st.get("completionTime"))
        p(f"{job['metadata']['name']:40} start {fmt(a, t0)} complete {fmt(b, t0)} ({dur(a, b).strip()} s)")
    pulls = []
    for ev in ((snap.get("events") or {}).get("items") or []):
        msg = ev.get("message", "")
        m = re.search(r'Successfully pulled image "([^"]+)" in ([\d.]+m?s|[\dm.]+s)', msg)
        if m:
            pulls.append((m.group(1), m.group(2), ev.get("involvedObject", {}).get("name", "")))
    if pulls:
        p("\n== Image pulls from registries (kubelet events)")
        for image, took, obj in pulls:
            p(f"{image[:70]:70} {took:>10}  {obj}")

    # Watch-derived milestones.
    p("\n== Milestones (watch)")
    for label in HEALTHZ:
        first = next((e["ts"] for e in events if e["kind"] == "healthz" and e["target"] == label and e["ok"]), None)
        p(f"first {label} /healthz 200: {fmt(first, t0)}")
    busy = [e for e in events if e["kind"] == "busy" and e.get("tilt_up")]
    last_busy = max((e["ts"] for e in busy if e["resources"]), default=None)
    p(f"last sample with a Tilt build pending/in progress: {fmt(last_busy, t0)}")
    ready_pods = [e["ts"] for e in events if e["kind"] == "pod"]
    p(f"last pod transition: {fmt(max(ready_pods) if ready_pods else None, t0)}")
    cpu = [e for e in events if e["kind"] == "cpu"]
    if cpu:
        p(f"host CPU util: mean {sum(e['util'] for e in cpu) / len(cpu):.0f}% over {len(cpu)} samples, ncpu {cpu[-1]['ncpu']}")
        # CPU by 30 s bucket, to see compile-bound vs waiting stretches.
        buckets: dict[int, list[float]] = {}
        for e in cpu:
            buckets.setdefault(int((e["ts"] - t0) // 30), []).append(e["util"])
        p("  util by 30 s bucket: " + " ".join(f"{k * 30}s:{sum(v) / len(v):.0f}%" for k, v in sorted(buckets.items()) if k >= 0))
    disk = [e for e in events if e["kind"] == "disk" and e.get("docker_free_gib") is not None]
    if disk:
        p(f"docker data root free: first {disk[0]['docker_free_gib']:.1f} GiB, min {min(e['docker_free_gib'] for e in disk):.1f} GiB, last {disk[-1]['docker_free_gib']:.1f} GiB")

    # Phase markers from the stamped Tilt log.
    stamped = out / "tilt-log-stamped.txt"
    if stamped.exists():
        p("\n== Build phase markers (stamped Tilt log; first occurrence per resource, 'finished' = last)")
        marks: dict[tuple[str, str], list[float]] = {}
        durations = []
        seen: set[tuple[str, str]] = set()
        load_lines: set = set()
        push_started: dict[tuple[str, str], float] = {}
        last_push: dict[str, tuple[str, str]] = {}
        loads: list[tuple[float, float, str, str, str]] = []
        for raw in stamped.read_text(errors="replace").splitlines():
            m = LINE.match(ANSI.sub("", raw))
            if not m:
                continue
            ts = parse_ts(m.group(1))
            res = m.group(2) or "-"
            text = m.group(3)
            for phase, pattern in MARKERS:
                if re.search(pattern, text):
                    marks.setdefault((res, phase), []).append(ts)
            # Tilt reprints a build's log when it re-renders the resource; keep
            # the first sighting of each line, which is its real arrival.
            key = (res, text.strip())
            if "[rust-artifacts]" in text and re.search(r"\d+s\)?$", text) and key not in seen:
                seen.add(key)
                durations.append(f"{fmt(ts, t0)} {res[:24]:24} {text.strip()[:110]}")
            if lm := OWN_LOAD.search(text):
                build = lm.group(6) or key
                if build not in load_lines:
                    load_lines.add(build)
                    seconds = float(lm.group(5))
                    loads.append((ts - seconds, seconds, lm.group(1), "own load",
                                  f"{lm.group(4)} loaded, on {lm.group(2)} of {lm.group(3)} nodes"))
                continue
            if key in load_lines:
                continue
            load_lines.add(key)
            if pm := PUSH_START.search(text):
                push_started[(pm.group(2), pm.group(1))] = ts
                last_push[res] = (pm.group(2), pm.group(1))
            elif "Skipping push" in text and res in last_push:
                push_started.pop(last_push.pop(res), None)
            elif pm := PUSH_DONE.search(text):
                start = push_started.pop((pm.group(3), pm.group(1)), None)
                if start is not None:
                    loads.append((start, float(pm.group(2)), pm.group(3), "tilt push", ""))
        for (res, phase), stamps in sorted(marks.items(), key=lambda kv: kv[1][0]):
            last = stamps[-1] if phase.endswith("finished") or phase == "kind-load" else stamps[0]
            extra = f" … {fmt(stamps[-1], t0).strip()} (x{len(stamps)})" if len(stamps) > 1 else ""
            p(f"{res[:30]:30} {phase:28} {fmt(stamps[0], t0)}{extra}" if phase != "rust.cargo-finished"
              else f"{res[:30]:30} {phase:28} {fmt(last, t0)}")
        if durations:
            p("\n== Rust artifact phase durations (as the build reported them)")
            lines.extend(durations)
        if loads:
            p("\n== Image loads (Tilt's push step, or the build's own load)")
            p(f"{'image':58} {'start':>7} {'end':>7} {'secs':>7}  how")
            for start, seconds, ref, how, detail in sorted(loads):
                p(f"{ref[:58]:58} {fmt(start, t0)} {fmt(start + seconds, t0)} {seconds:7.1f}  "
                  f"{how}{', ' + detail if detail else ''}")
            p(f"total load seconds: {sum(load[1] for load in loads):.1f} over {len(loads)} loads")

    # Stamped command output (`cmd | kind-bringup-timeline.py stamp > OUT/<name>.log`):
    # the bring-up's own progress lines, relative to t0.
    for log in sorted(out.glob("*.log")):
        picked = []
        for raw in log.read_text(errors="replace").splitlines():
            parts = ANSI.sub("", raw).split(" ", 1)
            ts = parse_ts(parts[0])
            if ts is None or len(parts) < 2:
                continue
            body = parts[1]
            # Tagged progress lines (`[bootstrap] …`, `[smoke] …`) and verdicts.
            if re.match(r"\s*(\[[A-Za-z][\w-]*[\] ]|ERROR|PASS|FAIL|==>)", body):
                picked.append(f"{fmt(ts, t0)} {body.strip()[:140]}")
        if picked:
            p(f"\n== {log.name} (stamped progress lines)")
            lines.extend(picked)
    text = "\n".join(lines)
    print(text)
    (out / "report.txt").write_text(text + "\n")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)
    w = sub.add_parser("watch")
    w.add_argument("out")
    w.add_argument("--interval", type=float, default=2.0)
    s = sub.add_parser("snapshot")
    s.add_argument("out")
    sub.add_parser("stamp")
    r = sub.add_parser("report")
    r.add_argument("out")
    r.add_argument("--t0", help="RFC 3339 or epoch seconds; default: watch start")
    args = parser.parse_args()
    return {"watch": cmd_watch, "snapshot": cmd_snapshot, "stamp": cmd_stamp, "report": cmd_report}[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())

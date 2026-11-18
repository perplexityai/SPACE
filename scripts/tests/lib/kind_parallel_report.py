#!/usr/bin/env python3
"""Inventory, coverage check, summary and timeline of a smoke-kind-parallel.sh run.

`inventory --benches N` prints, as JSON, each API surface's eligible scenarios
and each bench's share of them, computed with the typed runner's own registry
and partition (space_smoke.sharding.select_shard). It needs the project
environment (`uv run`), and reads the same variables the runner reads.

`summarize OUT` reads the run directory smoke-kind-parallel.sh wrote, writes
summary.md, summary.json and timeline.txt there, and exits 0 when every bench
passed, 1 when a scenario failed, and 2 when a bench did not come up, a
scenario was inconclusive, or the benches did not cover the inventory exactly
once. It needs only the standard library.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

SURFACES = ("ecw", "native")


def inventory(benches: int) -> int:
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
    from space_smoke.scenarios import REGISTRY
    from space_smoke.sharding import select_shard

    # The runner's own eligibility, in the order it applies it for a sharded
    # `--all` pass (space_smoke/__main__.py, _run_all): tiers and extra
    # scenarios, surface, backend, then browser withholding. Mode-dependent
    # skips happen after the partition and are read from the logs.
    tiers = tuple(
        part.strip() for part in os.environ.get("SPACE_SMOKE_TIERS", "routine").split(",")
    )
    included = tuple(
        part.strip()
        for part in os.environ.get("SPACE_SMOKE_INCLUDE_SCENARIOS", "").split(",")
        if part.strip()
    )
    withhold = os.environ.get("SPACE_SMOKE_BROWSER_SCENARIOS", "run") == "withhold"
    template = os.environ.get("E2B_TEMPLATE") or None
    inventories = {
        surface: tuple(
            scenario
            for scenario in REGISTRY.select(
                category=None, template=template, tiers=tiers, include_scenarios=included
            )
            if surface in scenario.api_surfaces
            and "kind" in scenario.backends
            and not (withhold and scenario.browser is not None)
        )
        for surface in SURFACES
    }
    shares = {
        str(index): {
            surface: [
                scenario.id
                for scenario in select_shard(
                    inventories[surface],
                    index=index,
                    count=benches,
                    api_surface=surface,
                    inventories=inventories,
                )
            ]
            for surface in SURFACES
        }
        for index in range(1, benches + 1)
    }
    json.dump(
        {
            "tiers": list(tiers),
            "surfaces": {
                surface: [scenario.id for scenario in inventories[surface]]
                for surface in SURFACES
            },
            "benches": shares,
        },
        sys.stdout,
        indent=2,
    )
    print()
    return 0


RUN = re.compile(r"^=== RUN (\S+) ===$")
END = re.compile(r"^=== END (\S+) \(exit (-?\d+)\) ===$")
SURFACE = re.compile(r"^--- typed smoke on the (\S+) API surface$")
SUITE = re.compile(r"^SUITE: (\d+) passed, (\d+) warned, (\d+) failed, (\d+) inconclusive")
SHARD = re.compile(r"^SHARD \d+/\d+: ")
SKIPPED = re.compile(r"^SKIPPED (\S+): (.*)$")
VERDICT = {0: "pass", 42: "warn", 1: "fail"}


def parse_typed(log: Path) -> dict:
    """Per surface: scenarios run with their exit codes, post-partition skips, SUITE."""

    surfaces: dict[str, dict] = {}
    current = None
    partitioned = False
    if not log.exists():
        return surfaces
    for raw in log.read_text(errors="replace").splitlines():
        line = raw.rstrip()
        if match := SURFACE.match(line):
            current = surfaces.setdefault(
                match[1], {"run": [], "exits": {}, "skipped": {}, "suite": None}
            )
            partitioned = False
            continue
        if current is None:
            continue
        if SHARD.match(line):
            partitioned = True
        elif match := RUN.match(line):
            current["run"].append(match[1])
        elif match := END.match(line):
            current["exits"][match[1]] = int(match[2])
        elif (match := SKIPPED.match(line)) and partitioned:
            current["skipped"][match[1]] = match[2]
        elif match := SUITE.match(line):
            current["suite"] = dict(
                zip(("passed", "warned", "failed", "inconclusive"), map(int, match.groups()))
            )
    return surfaces


def read_timings(out: Path) -> list[dict]:
    rows = []
    path = out / "timings.tsv"
    if path.exists():
        for line in path.read_text().splitlines():
            owner, name, start, end, rc = line.split("\t")
            rows.append(
                {"owner": owner, "phase": name, "start": int(start), "end": int(end), "rc": int(rc)}
            )
    return rows


def read_events(path: Path) -> list[tuple[int, str]]:
    events = []
    if path.exists():
        for line in path.read_text(errors="replace").splitlines():
            stamp, _, text = line.partition(" ")
            if stamp.isdigit():
                events.append((int(stamp), text))
    return events


def clock(seconds: int) -> str:
    return f"{seconds // 60}:{seconds % 60:02d}"


def resource_peaks(out: Path) -> dict:
    path = out / "resources.tsv"
    if not path.exists():
        return {}
    lines = path.read_text().splitlines()
    header = lines[0].split("\t")
    peaks: dict[str, float] = {}
    for line in lines[1:]:
        for key, value in zip(header[1:], line.split("\t")[1:]):
            try:
                number = float(value)
            except ValueError:
                continue
            peaks[key] = max(peaks.get(key, number), number)
    peaks["samples"] = len(lines) - 1
    # Throughput columns also as the run's total, sampled every 30 s.
    for key in header[1:]:
        if key.endswith("_mbps"):
            column = header.index(key)
            total = 0.0
            for line in lines[1:]:
                try:
                    total += float(line.split("\t")[column]) * 30 / 1000
                except (ValueError, IndexError):
                    pass
            peaks[key.replace("_mbps", "_total_gb")] = round(total, 1)
    return peaks


def timeline(out: Path, start: int, timings: list[dict], benches: list[str]) -> str:
    """Minute by minute, what each bench and the host were doing."""

    spans: dict[str, list[tuple[int, int, str]]] = defaultdict(list)
    for row in timings:
        owner = row["owner"]
        if owner.startswith("bench"):
            spans[owner[5:]].append((row["start"], row["end"], row["phase"]))
        elif owner == "host":
            spans["host"].append((row["start"], row["end"], row["phase"]))
    for bench in benches:
        events = read_events(out / "benches" / bench / "typed.log.events")
        running: dict[str, int] = {}
        surface = ""
        for stamp, text in events:
            if match := SURFACE.match(text):
                surface = match[1]
            elif match := RUN.match(text):
                running[match[1]] = stamp
            elif (match := END.match(text)) and match[1] in running:
                spans[bench].append((running.pop(match[1]), stamp, f"{surface}:{match[1]}"))
    end = max((span[1] for values in spans.values() for span in values), default=start)
    columns = ["host", *benches]

    def doing(column: str, moment: int) -> str:
        # A running scenario stands for the `typed` phase around it; phases
        # that overlap (the UI pre-build beside the cluster) are all named.
        covering = [s for s in spans.get(column, []) if s[0] <= moment < s[1]]
        scenarios = [s[2] for s in covering if ":" in s[2]]
        if scenarios:
            return ",".join(scenarios)
        return "+".join(sorted({s[2] for s in covering})) or "-"

    lines = ["minute | " + " | ".join(f"bench {c}" if c != "host" else "host" for c in columns)]
    for moment in range(start, end + 60, 60):
        lines.append(
            f"{clock(moment - start):>6} | " + " | ".join(doing(c, moment) for c in columns)
        )
    lines.append("")
    lines.append("Phases (start-end, relative to the run start):")
    for column in columns:
        for begin, finish, name in sorted(spans.get(column, [])):
            if ":" in name:
                continue
            lines.append(f"  {column:>5} {name:<16} {clock(begin - start)}-{clock(finish - start)} ({finish - begin}s)")
    return "\n".join(lines) + "\n"


def summarize(out: Path) -> int:
    start = int((out / "started").read_text())
    count = int((out / "bench-count").read_text())
    benches = [str(index) for index in range(1, count + 1)]
    timings = read_timings(out)
    inventory_path = out / "host" / "inventory.json"
    inventory_data = json.loads(inventory_path.read_text()) if inventory_path.exists() else None

    per_bench = {}
    seen: dict[str, Counter] = {surface: Counter() for surface in SURFACES}
    skipped: dict[str, dict[str, str]] = {surface: {} for surface in SURFACES}
    not_passed = []
    status = 0
    for bench in benches:
        directory = out / "benches" / bench
        result = (directory / "result").read_text().strip() if (directory / "result").exists() else "unknown"
        typed = parse_typed(directory / "typed.log")
        phases = {
            row["phase"]: row["end"] - row["start"]
            for row in timings
            if row["owner"] == f"bench{bench}"
        }
        finished = max((row["end"] for row in timings if row["owner"] == f"bench{bench}"), default=start)
        per_bench[bench] = {
            "result": result,
            "phases": phases,
            "finished_after": finished - start,
            "surfaces": {s: v["suite"] for s, v in typed.items()},
        }
        if not result.startswith("typed_exit_"):
            status = 2
        for surface, data in typed.items():
            seen.setdefault(surface, Counter()).update(data["run"])
            skipped.setdefault(surface, {}).update(data["skipped"])
            for scenario in data["run"]:
                code = data["exits"].get(scenario)
                if code not in (0, 42):
                    verdict = VERDICT.get(code, "inconclusive") if code is not None else "unfinished"
                    not_passed.append(
                        {
                            "bench": bench,
                            "surface": surface,
                            "scenario": scenario,
                            "verdict": verdict,
                            "exit": code,
                            "log": str(directory / "typed.log"),
                        }
                    )
                    status = max(status, 1 if verdict == "fail" else 2)
        if result.startswith("typed_exit_") and result != "typed_exit_0" and not not_passed:
            status = max(status, 2)

    coverage = {"checked": inventory_data is not None, "surfaces": {}}
    if inventory_data is None:
        status = 2
    else:
        for surface in SURFACES:
            expected = set(inventory_data["surfaces"][surface])
            ran = seen.get(surface, Counter())
            mode_skipped = set(skipped.get(surface, {}))
            covered = set(ran) | mode_skipped
            entry = {
                "expected": len(expected),
                "ran": sum(ran.values()),
                "skipped_by_mode": sorted(mode_skipped),
                "missing": sorted(expected - covered),
                "unexpected": sorted(covered - expected),
                "duplicated": sorted(s for s, n in ran.items() if n > 1),
            }
            entry["exactly_once"] = not (entry["missing"] or entry["unexpected"] or entry["duplicated"])
            coverage["surfaces"][surface] = entry
            if not entry["exactly_once"]:
                status = 2

    host_rows = [row for row in timings if row["owner"] in ("host", "run")]
    ended = max((row["end"] for row in timings), default=start)
    summary = {
        "run": out.name,
        "benches": count,
        "exit_status": status,
        "wall_clock_seconds": ended - start,
        "host_phases": {f"{r['owner']}:{r['phase']}": {"seconds": r["end"] - r["start"], "rc": r["rc"], "started_after": r["start"] - start} for r in host_rows},
        "per_bench": per_bench,
        "not_passed": not_passed,
        "coverage": coverage,
        "resource_peaks": resource_peaks(out),
    }
    (out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")

    phase_names = []
    for bench in benches:
        for name in per_bench[bench]["phases"]:
            if name not in phase_names:
                phase_names.append(name)
    md = [
        f"# Parallel Kind smoke: {out.name}",
        "",
        f"- benches: {count}; exit status {status}; wall-clock {clock(ended - start)} (start to last phase)",
        "",
        "## Host phases",
        "",
        "| phase | started at | seconds | rc |",
        "|---|---|---|---|",
    ]
    for key, value in summary["host_phases"].items():
        md.append(f"| {key} | {clock(value['started_after'])} | {value['seconds']} | {value['rc']} |")
    md += [
        "",
        "## Benches",
        "",
        "| bench | result | finished at | " + " | ".join(phase_names) + " | ecw | native |",
        "|---" * (len(phase_names) + 5) + "|",
    ]
    for bench in benches:
        data = per_bench[bench]

        def suite(surface: str) -> str:
            value = data["surfaces"].get(surface)
            if not value:
                return "-"
            return f"{value['passed']}p/{value['warned']}w/{value['failed']}f/{value['inconclusive']}i"

        md.append(
            f"| {bench} | {data['result']} | {clock(data['finished_after'])} | "
            + " | ".join(str(data["phases"].get(name, "-")) for name in phase_names)
            + f" | {suite('ecw')} | {suite('native')} |"
        )
    md += ["", "## Not passed", ""]
    if not_passed:
        md += ["| bench | surface | scenario | verdict | log |", "|---|---|---|---|---|"]
        md += [
            f"| {r['bench']} | {r['surface']} | {r['scenario']} | {r['verdict']} | {r['log']} |"
            for r in not_passed
        ]
    else:
        md.append("None.")
    md += ["", "## Coverage", ""]
    if not coverage["checked"]:
        md.append("Not checked: the inventory step failed.")
    for surface, entry in coverage["surfaces"].items():
        md.append(
            f"- {surface}: expected {entry['expected']}, ran {entry['ran']}, "
            f"skipped by mode {len(entry['skipped_by_mode'])}, "
            f"exactly once: {'yes' if entry['exactly_once'] else 'NO'}"
            + (f"; missing ({len(entry['missing'])}) {entry['missing'][:10]}" if entry["missing"] else "")
            + (f"; unexpected {entry['unexpected']}" if entry["unexpected"] else "")
            + (f"; duplicated {entry['duplicated']}" if entry["duplicated"] else "")
            + (f"; skipped {entry['skipped_by_mode']}" if entry["skipped_by_mode"] else "")
        )
    peaks = summary["resource_peaks"]
    if peaks:
        md += ["", "## Resource peaks (30 s samples)", ""]
        md += [f"- {key}: {value}" for key, value in peaks.items()]
    (out / "summary.md").write_text("\n".join(md) + "\n")
    (out / "timeline.txt").write_text(timeline(out, start, timings, benches))
    print((out / "summary.md").read_text())
    return status


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("inventory").add_argument("--benches", type=int, required=True)
    commands.add_parser("summarize").add_argument("out", type=Path)
    args = parser.parse_args()
    if args.command == "inventory":
        return inventory(args.benches)
    return summarize(args.out)


if __name__ == "__main__":
    raise SystemExit(main())

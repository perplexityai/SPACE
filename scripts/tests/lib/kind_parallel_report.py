#!/usr/bin/env python3
"""Inventory, coverage check, summary and timeline of a smoke-kind-parallel.sh run.

`inventory --benches N` prints, as JSON, each API surface's eligible scenarios
and each bench's share of them, computed with the typed runner's own
eligibility and partition (space_smoke.sharding.joint_inventories and
select_shard). It needs the project environment (`uv run`), and reads the same
variables the runner reads.

`source REPO FILE` records the checkout a run starts from (see `source`), and
`check-source REPO FILE TREE` prints `same` when TREE, the checkout or a bench's
copy of it, still matches that record.

`summarize OUT` reads the run directory smoke-kind-parallel.sh wrote, writes
summary.md, summary.json and timeline.txt there, and exits as the typed suite
ranks a suite (space_smoke/suite.py, suite_exit_code): 1 when any scenario
failed; else 2 when a bench did not come up, a scenario was inconclusive or did
not finish, a pass exited 2 outside any scenario, the benches did not cover
the inventory exactly once, the checkout changed during the run, or the run's
teardown left benches; else 42
when a scenario warned; else 0. A failure the known-failures list excused
(`KNOWN FAILURE <id>` in its pass) is listed and does not count as one, and a
scenario a bench's mode skipped after the partition counts as covered. It
needs only the standard library.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
from collections import Counter, defaultdict
from pathlib import Path

SURFACES = ("ecw", "native")


def inventory(benches: int) -> int:
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
    from space_smoke.scenarios import REGISTRY
    from space_smoke.sharding import joint_inventories, select_shard

    # The variables a bench's `--all` pass reads (space_smoke/__main__.py,
    # _run_all); smoke-kind-parallel.sh runs this with a bench's environment.
    tiers = tuple(
        part.strip() for part in os.environ.get("SPACE_SMOKE_TIERS", "routine").split(",")
    )
    inventories = joint_inventories(
        REGISTRY,
        template=os.environ.get("E2B_TEMPLATE") or None,
        tiers=tiers,
        include_scenarios=tuple(
            part.strip()
            for part in os.environ.get("SPACE_SMOKE_INCLUDE_SCENARIOS", "").split(",")
            if part.strip()
        ),
        backend=os.environ.get("SPACE_SMOKE_BACKEND", "kind"),
        browser_selection=os.environ.get("SPACE_SMOKE_BROWSER_SCENARIOS") or "run",
    )
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
# Searched, not matched: the runner prints a scenario's output as it comes, so
# output without a final newline puts the END marker mid-line.
END = re.compile(r"=== END (\S+) \(exit (-?\d+)\) ===$")
SURFACE = re.compile(r"^--- typed smoke on the (\S+) API surface$")
SUITE = re.compile(r"^SUITE: (\d+) passed, (\d+) warned, (\d+) failed, (\d+) inconclusive")
SHARD = re.compile(r"^SHARD \d+/\d+: ")
SKIPPED = re.compile(r"^SKIPPED (\S+): (.*)$")
# The runner's report of a failure SPACE_SMOKE_KNOWN_FAILURES excused.
KNOWN_FAILURE = re.compile(r"^  KNOWN FAILURE (\S+): ")
# smoke-kind-typed.sh `all` repeats both passes' SUITE lines after this; read
# as part of the last pass they would replace its counts with the first's.
SUMMARY = "=== typed smoke summary"
# The same line carries each pass's own exit status, which a mode check
# (telemetry, storage credentials, Rust build, browser state) can raise to 2
# while every scenario still runs.
PASS_STATUS = re.compile(r"^=== typed smoke summary: ecw exit (-?\d+), native exit (-?\d+) ===$")
VERDICT = {0: "pass", 42: "warn", 1: "fail"}


def parse_typed(log: Path) -> dict:
    """Per surface: each attempt as [scenario, exit or None], post-partition skips, SUITE."""

    surfaces: dict[str, dict] = {}
    current = None
    partitioned = False
    if not log.exists():
        return surfaces
    for raw in log.read_text(errors="replace").splitlines():
        line = raw.rstrip()
        if line.startswith(SUMMARY):
            if match := PASS_STATUS.match(line):
                for surface, status in zip(SURFACES, match.groups()):
                    surfaces.setdefault(
                        surface, {"run": [], "skipped": [], "known": set(), "suite": None}
                    )["status"] = int(status)
            break
        if match := SURFACE.match(line):
            current = surfaces.setdefault(
                match[1], {"run": [], "skipped": [], "known": set(), "suite": None}
            )
            partitioned = False
            continue
        if current is None:
            continue
        if SHARD.match(line):
            partitioned = True
        elif match := RUN.match(line):
            current["run"].append([match[1], None])
        elif match := END.search(line):
            # Each attempt keeps its own exit: a scenario run twice in one pass
            # is two attempts, and a pass after a failure does not undo it. The
            # runner runs one scenario at a time, so an END closes the latest
            # open attempt of its scenario.
            for attempt in reversed(current["run"]):
                if attempt[0] == match[1] and attempt[1] is None:
                    attempt[1] = int(match[2])
                    break
        elif match := KNOWN_FAILURE.match(line):
            current["known"].add(match[1])
        elif (match := SKIPPED.match(line)) and partitioned:
            # One entry per line, as runs are: a pair skipped twice is covered twice.
            current["skipped"].append(match[1])
        elif match := SUITE.match(line):
            current["suite"] = dict(
                zip(("passed", "warned", "failed", "inconclusive"), map(int, match.groups()))
            )
    return surfaces


def read_json(path: Path) -> dict | None:
    try:
        value = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    return value if isinstance(value, dict) else None


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
            elif (match := END.search(text)) and match[1] in running:
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


def rust_image_origin(image: dict) -> str:
    """Where the run's Rust image came from, for the summary."""

    if not image.get("image_id"):
        return "no image recorded"
    if not image["reused_from_main"]:
        return "built on the host"
    how = (
        "pulled by digest through a registry mirror"
        if image.get("reused_from") == "mirror"
        else "pulled"
    )
    return f"main's image for the same input key, {how}" + (
        f": {image['reused_image']}" if image.get("reused_image") else ""
    )


def summarize(out: Path) -> int:
    start = int((out / "started").read_text())
    count = int((out / "bench-count").read_text())
    benches = [str(index) for index in range(1, count + 1)]
    timings = read_timings(out)
    inventory_path = out / "host" / "inventory.json"
    try:
        inventory_data = json.loads(inventory_path.read_text())
    except (OSError, ValueError):
        inventory_data = None

    per_bench = {}
    seen: dict[str, Counter] = {surface: Counter() for surface in SURFACES}
    skipped: dict[str, Counter] = {surface: Counter() for surface in SURFACES}
    not_passed = []
    # The suite's ranking (suite_exit_code): a failure outranks an incomplete
    # result, which outranks a warning.
    failed = incomplete = warned = False
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
            incomplete = True
        for surface, data in typed.items():
            pass_failed = False
            seen.setdefault(surface, Counter()).update(scenario for scenario, _ in data["run"])
            skipped.setdefault(surface, Counter()).update(data["skipped"])
            for scenario, code in data["run"]:
                if code != 0:
                    if code is None:
                        verdict = "unfinished"
                    elif code == 1 and scenario in data["known"]:
                        verdict = "known failure"
                    else:
                        verdict = VERDICT.get(code, "inconclusive")
                    if verdict == "fail":
                        failed = pass_failed = True
                    elif verdict in ("inconclusive", "unfinished"):
                        incomplete = True
                    elif verdict == "warn":
                        warned = True
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
            # Each pass's own status: anything but 0, 1 and 42, or a 1 that no
            # failed scenario accounts for, is an incomplete pass.
            status = data.get("status")
            if status is not None and (status not in (0, 1, 42) or (status == 1 and not pass_failed)):
                incomplete = True
        # The shard's exit must be the one smoke-kind-typed.sh `all` gives for
        # those statuses: the ECW pass's when nonzero, else the native pass's.
        # Without the closing line, or with any other exit (the shard's log
        # pipeline failing after both passes ended, say), the bench's result
        # is unexplained.
        statuses = [typed.get(surface, {}).get("status") for surface in SURFACES]
        if None in statuses or result != f"typed_exit_{statuses[0] or statuses[1]}":
            incomplete = True

    coverage = {"checked": inventory_data is not None, "surfaces": {}}
    if inventory_data is None:
        incomplete = True
    else:
        for surface in SURFACES:
            expected = set(inventory_data["surfaces"][surface])
            ran = seen.get(surface, Counter())
            mode_skipped = skipped.get(surface, Counter())
            # Each pair once across the benches, run or skipped by a mode.
            occurrences = ran + mode_skipped
            entry = {
                "expected": len(expected),
                "ran": sum(ran.values()),
                "skipped_by_mode": sorted(mode_skipped),
                "missing": sorted(expected - set(occurrences)),
                "unexpected": sorted(set(occurrences) - expected),
                "duplicated": sorted(s for s, n in occurrences.items() if n > 1),
            }
            entry["exactly_once"] = not (entry["missing"] or entry["unexpected"] or entry["duplicated"])
            coverage["surfaces"][surface] = entry
            if not entry["exactly_once"]:
                incomplete = True
    # Each check that the checkout a bench copied or a host step read was the
    # recorded one (smoke-kind-parallel.sh check_source): a mismatch means the
    # run did not test the checkout the summary names.
    changes_path = out / "source-changes"
    source_changes = changes_path.read_text().splitlines() if changes_path.exists() else []
    if source_changes:
        incomplete = True
    # The run's own teardown (none under --keep): benches it could not remove
    # are still on the host, holding its disk and its next run.
    teardown_failed = any(
        row["owner"] == "run" and row["phase"] == "teardown" and row["rc"] != 0 for row in timings
    )
    if teardown_failed:
        incomplete = True
    status = 1 if failed else 2 if incomplete else 42 if warned else 0

    source = read_json(out / "source.json") or {}
    source["changed_during_run"] = source_changes
    manifest = read_json(out / "host" / "rust" / "manifest.json") or {}
    reuse = out / "host" / "rust-reuse.out"
    reuse_outputs = dict(
        line.partition("=")[::2]
        for line in (reuse.read_text().splitlines() if reuse.exists() else [])
    )
    reused = reuse_outputs.get("reused") == "true"
    source["rust_image"] = {
        key: manifest.get(key) for key in ("image_id", "built_from", "input_key")
    } | {
        "reused_from_main": reused,
        # mirror (by digest through SPACE_KIND_REGISTRY_MIRROR) or registry.
        "reused_from": reuse_outputs.get("reused_from") if reused else None,
        "reused_image": reuse_outputs.get("reused_image") if reused else None,
    }

    host_rows = [row for row in timings if row["owner"] in ("host", "run")]
    ended = max((row["end"] for row in timings), default=start)
    summary = {
        "run": out.name,
        "benches": count,
        "source": source,
        "exit_status": status,
        "wall_clock_seconds": ended - start,
        "host_phases": {f"{r['owner']}:{r['phase']}": {"seconds": r["end"] - r["start"], "rc": r["rc"], "started_after": r["start"] - start} for r in host_rows},
        "per_bench": per_bench,
        "not_passed": not_passed,
        "coverage": coverage,
        "teardown_failed": teardown_failed,
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
        f"- checkout: {source.get('head', 'unknown')}"
        + (
            f" with uncommitted changes (sha256 {source.get('changes_sha256')} over the paths"
            " that differ from HEAD)"
            if source.get("dirty")
            else " (clean)"
        ),
        f"- lane: substrate {source.get('substrate')}, Rust build {source.get('rust_build')},"
        f" tiers {source.get('tiers')}",
        f"- Rust image: {source['rust_image'].get('image_id')} built from"
        f" {source['rust_image'].get('built_from')} ({rust_image_origin(source['rust_image'])})",
    ]
    if source_changes:
        md.append(
            "- **the checkout changed during the run**, so it did not test the checkout above: "
            + "; ".join(source_changes)
        )
    if teardown_failed:
        md.append(
            f"- teardown: FAILED, benches are left on the host; see {out / 'teardown.log'},"
            " then run `smoke-kind-parallel.sh teardown`"
        )
    md += [
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


def git(repo: Path, *args: str) -> bytes:
    # No optional locks: the checks run while the caller may be using the
    # checkout, whose index a refresh would lock.
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
        env=os.environ | {"GIT_OPTIONAL_LOCKS": "0"},
    ).stdout


def fingerprint(repo: Path, tree: Path) -> dict:
    """The checkout as a bench's copy of <repo> reproduces it: <tree>'s HEAD,
    and a digest of every path the copy takes from <repo> instead of HEAD
    (kind_bench_checkout's lists), read from <tree>.

    <tree> is <repo> itself or a bench's copy. Without following links, a path
    counts as a symlink's target text, a regular file's content and executable
    bit, or otherwise (an embedded repository, say) the path alone; a path
    deleted from HEAD counts by whether <tree> has it.
    """

    def names(*args: str) -> list[bytes]:
        return sorted(set(filter(None, git(repo, *args).split(b"\0"))))

    changes = hashlib.sha256()
    for name in names("diff", "-z", "--no-renames", "--name-only", "--diff-filter=d", "HEAD") + names(
        "ls-files", "-z", "--others", "--exclude-standard"
    ):
        path = tree / os.fsdecode(name)
        changes.update(name + b"\0")
        if path.is_symlink():
            changes.update(b"link\0" + os.fsencode(os.readlink(path)))
        elif path.is_file():
            executable = b"x" if path.stat().st_mode & 0o100 else b"-"
            changes.update(b"file\0" + executable + path.read_bytes())
        changes.update(b"\0")
    for name in names("diff", "-z", "--no-renames", "--name-only", "--diff-filter=D", "HEAD"):
        present = os.path.lexists(tree / os.fsdecode(name))
        changes.update(b"deleted\0" + name + (b"\0present\0" if present else b"\0absent\0"))
    return {"head": git(tree, "rev-parse", "HEAD").decode().strip(), "changes_sha256": changes.hexdigest()}


def source(repo: Path, destination: Path) -> int:
    """Record the checkout a run starts from: its commit, whether the tree
    differs from it, its fingerprint, and the lane settings."""

    record = {
        **fingerprint(repo, repo),
        "dirty": bool(git(repo, "status", "--porcelain").strip()),
        "substrate": os.environ.get("SPACE_KIND_SUBSTRATE") or "kubernetes",
        "rust_build": os.environ.get("SPACE_KIND_RUST_BUILD") or "test-hooks",
        "tiers": os.environ.get("SPACE_SMOKE_TIERS") or "routine",
    }
    destination.write_text(json.dumps(record, indent=2) + "\n")
    return 0


def check_source(repo: Path, recorded: Path, tree: Path) -> int:
    """Print `same` when <tree> (<repo>, or a bench's copy of it) is still the
    checkout <recorded> describes, else what changed."""

    before = json.loads(recorded.read_text())
    now = fingerprint(repo, tree)
    if now["head"] != before["head"]:
        print(f"HEAD is {now['head']}, not {before['head']}")
    elif now["changes_sha256"] != before["changes_sha256"]:
        print("the files that differ from HEAD are not the recorded ones")
    else:
        print("same")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("inventory").add_argument("--benches", type=int, required=True)
    commands.add_parser("summarize").add_argument("out", type=Path)
    recorded = commands.add_parser("source")
    recorded.add_argument("repo", type=Path)
    recorded.add_argument("destination", type=Path)
    checked = commands.add_parser("check-source")
    checked.add_argument("repo", type=Path)
    checked.add_argument("recorded", type=Path)
    checked.add_argument("tree", type=Path)
    args = parser.parse_args()
    if args.command == "inventory":
        return inventory(args.benches)
    if args.command == "source":
        return source(args.repo, args.destination)
    if args.command == "check-source":
        return check_source(args.repo, args.recorded, args.tree)
    return summarize(args.out)


if __name__ == "__main__":
    raise SystemExit(main())

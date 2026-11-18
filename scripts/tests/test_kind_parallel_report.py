"""The one-host parallel Kind smoke's verdict: coverage and exit status.

Builds synthetic run directories in the layout scripts/tests/smoke-kind-parallel.sh
writes, so no bench, cluster or Docker daemon is involved.
"""

import importlib.util
import json
import pathlib

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts/tests/lib/kind_parallel_report.py"
spec = importlib.util.spec_from_file_location("kind_parallel_report", SCRIPT)
report = importlib.util.module_from_spec(spec)
spec.loader.exec_module(report)

START = 1_000_000
INVENTORY = {
    "tiers": ["routine"],
    "surfaces": {"ecw": ["ecw.a", "ecw.b", "local.c"], "native": ["ecw.a", "native.d"]},
    "benches": {},
}


def typed_log(passes: dict[str, list[tuple[str, int]]], skipped: dict[str, list[str]] | None = None) -> str:
    """A smoke-kind-typed.sh `all` log: per surface, its runs and exit codes."""

    lines = []
    for surface, runs in passes.items():
        lines += [f"--- typed smoke on the {surface} API surface", "TIERS: routine"]
        # A skip before the partition is the runner's, not the bench's.
        lines.append("SKIPPED local.console_x: withheld from this run")
        lines.append(f"SHARD 1/2: {len(runs)} scenarios ({surface})")
        lines += [f"SKIPPED {held}: needs telemetry mode otel" for held in (skipped or {}).get(surface, [])]
        for scenario, code in runs:
            lines += [f"=== RUN {scenario} ===", "output", f"=== END {scenario} (exit {code}) ==="]
        passed = sum(code == 0 for _, code in runs)
        failed = sum(code == 1 for _, code in runs)
        lines.append(f"SUITE: {passed} passed, 0 warned, {failed} failed, {len(runs) - passed - failed} inconclusive")
    return "\n".join(lines) + "\n"


def make_run(tmp_path: pathlib.Path, benches: dict[str, tuple[str, str]], inventory=INVENTORY) -> pathlib.Path:
    """benches: index -> (result, typed log text)."""

    out = tmp_path / "run"
    (out / "host").mkdir(parents=True)
    (out / "started").write_text(f"{START}\n")
    (out / "bench-count").write_text(f"{len(benches)}\n")
    if inventory is not None:
        (out / "host" / "inventory.json").write_text(json.dumps(inventory))
    timings = [f"host\trust\t{START}\t{START + 10}\t0"]
    for index, (result, log) in benches.items():
        directory = out / "benches" / index
        directory.mkdir(parents=True)
        (directory / "result").write_text(result + "\n")
        if log:
            (directory / "typed.log").write_text(log)
        timings.append(f"bench{index}\tbaseline\t{START + 10}\t{START + 200}\t0")
    (out / "timings.tsv").write_text("\n".join(timings) + "\n")
    return out


def test_every_pair_once_and_passing_exits_zero(tmp_path):
    out = make_run(
        tmp_path,
        {
            "1": ("typed_exit_0", typed_log({"ecw": [("ecw.a", 0), ("local.c", 0)], "native": [("native.d", 0)]})),
            "2": ("typed_exit_0", typed_log({"ecw": [("ecw.b", 0)], "native": [("ecw.a", 0)]})),
        },
    )
    assert report.summarize(out) == 0
    summary = json.loads((out / "summary.json").read_text())
    assert all(entry["exactly_once"] for entry in summary["coverage"]["surfaces"].values())
    assert summary["not_passed"] == []
    assert (out / "timeline.txt").exists()


def test_a_failed_scenario_exits_one_and_is_listed(tmp_path):
    out = make_run(
        tmp_path,
        {
            "1": ("typed_exit_1", typed_log({"ecw": [("ecw.a", 1), ("local.c", 0)], "native": [("native.d", 0)]})),
            "2": ("typed_exit_0", typed_log({"ecw": [("ecw.b", 0)], "native": [("ecw.a", 0)]})),
        },
    )
    assert report.summarize(out) == 1
    (row,) = json.loads((out / "summary.json").read_text())["not_passed"]
    assert (row["bench"], row["surface"], row["scenario"], row["verdict"]) == ("1", "ecw", "ecw.a", "fail")
    assert row["log"].endswith("benches/1/typed.log")


@pytest.mark.parametrize(
    "benches",
    [
        # A bench that never came up leaves its share uncovered.
        {
            "1": ("typed_exit_0", typed_log({"ecw": [("ecw.a", 0), ("local.c", 0)], "native": [("native.d", 0)]})),
            "2": ("baseline_failed", ""),
        },
        # A pair run twice is not exactly once, even when both pass.
        {
            "1": ("typed_exit_0", typed_log({"ecw": [("ecw.a", 0), ("local.c", 0)], "native": [("native.d", 0)]})),
            "2": ("typed_exit_0", typed_log({"ecw": [("ecw.b", 0), ("ecw.a", 0)], "native": [("ecw.a", 0)]})),
        },
        # An inconclusive scenario is not a failure, and not a pass.
        {
            "1": ("typed_exit_2", typed_log({"ecw": [("ecw.a", 2), ("local.c", 0)], "native": [("native.d", 0)]})),
            "2": ("typed_exit_0", typed_log({"ecw": [("ecw.b", 0)], "native": [("ecw.a", 0)]})),
        },
    ],
    ids=["bench-down", "duplicate", "inconclusive"],
)
def test_incomplete_evidence_exits_two(tmp_path, benches):
    assert report.summarize(make_run(tmp_path, benches)) == 2


def test_a_mode_skip_after_the_partition_counts_as_covered_not_run(tmp_path):
    out = make_run(
        tmp_path,
        {
            "1": (
                "typed_exit_0",
                typed_log({"ecw": [("ecw.a", 0)], "native": [("native.d", 0)]}, skipped={"ecw": ["local.c"]}),
            ),
            "2": ("typed_exit_0", typed_log({"ecw": [("ecw.b", 0)], "native": [("ecw.a", 0)]})),
        },
    )
    assert report.summarize(out) == 0
    ecw = json.loads((out / "summary.json").read_text())["coverage"]["surfaces"]["ecw"]
    assert ecw["skipped_by_mode"] == ["local.c"]
    assert ecw["ran"] == 2


def test_a_missing_inventory_is_inconclusive(tmp_path):
    out = make_run(
        tmp_path,
        {"1": ("typed_exit_0", typed_log({"ecw": [("ecw.a", 0)], "native": [("native.d", 0)]}))},
        inventory=None,
    )
    assert report.summarize(out) == 2

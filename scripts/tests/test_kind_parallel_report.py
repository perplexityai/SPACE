"""scripts/tests/lib/kind_parallel_report.py ranks a parallel Kind run and checks its coverage.

Each case writes a synthetic run directory, as smoke-kind-parallel.sh leaves
one, and reads the exit status and coverage `summarize` gives it.
"""

import importlib.util
import json
import pathlib

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "kind_parallel_report", ROOT / "scripts/tests/lib/kind_parallel_report.py"
)
report = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(report)

INVENTORY = {"ecw": ["ecw.a", "ecw.b"], "native": ["native.c", "native.d"]}


def suite_status(results, known=()):
    """A pass's exit as the typed suite ranks it."""

    codes = [code for scenario, code in results if not (code == 1 and scenario in known)]
    if 1 in codes:
        return 1
    if any(code not in (0, 1, 42) for code in codes):
        return 2
    return 42 if 42 in codes else 0


def typed_log(ecw=(), native=(), *, skipped=(), known=(), statuses=None, glued=False):
    """One shard's smoke-kind-typed.sh `all` output: (scenario, exit) per pass."""

    lines = []
    passes = {"ecw": list(ecw), "native": list(native)}
    for surface, results in passes.items():
        lines += [f"--- typed smoke on the {surface} API surface", "TIERS: routine"]
        lines.append(f"SHARD 1/2: {len(results)} scenarios ({surface})")
        lines += [f"SKIPPED {scenario}: needs telemetry mode otel" for scenario in skipped if scenario.startswith(surface)]
        for scenario, code in results:
            lines.append(f"=== RUN {scenario} ===")
            end = f"=== END {scenario} (exit {code}) ==="
            lines += [f"output without a final newline{end}"] if glued else ["output", end]
        counts = [sum(code == want for _, code in results) for want in (0, 42, 1)]
        lines.append(
            f"SUITE: {counts[0]} passed, {counts[1]} warned, {counts[2]} failed, "
            f"{len(results) - sum(counts)} inconclusive or infrastructure errors"
        )
        lines += [f"  KNOWN FAILURE {scenario}: listed" for scenario in known if scenario.startswith(surface)]
    ecw_status, native_status = statuses or (
        suite_status(passes["ecw"], known),
        suite_status(passes["native"], known),
    )
    lines.append(f"=== typed smoke summary: ecw exit {ecw_status}, native exit {native_status} ===")
    # The closing summary repeats each pass's SUITE line.
    lines += ["--- ecw API surface", "SUITE: 9 passed, 0 warned, 0 failed, 0 inconclusive"]
    lines += ["--- native API surface", "SUITE: 9 passed, 0 warned, 0 failed, 0 inconclusive"]
    return "\n".join(lines) + "\n", ecw_status or native_status


def run(tmp_path, benches, inventory=INVENTORY):
    """Write a run directory; each bench is (typed.log text, its result) or None for no log."""

    out = tmp_path / "run"
    (out / "host").mkdir(parents=True)
    (out / "started").write_text("1000\n")
    (out / "bench-count").write_text(f"{len(benches)}\n")
    (out / "timings.tsv").write_text("")
    if inventory is not None:
        (out / "host" / "inventory.json").write_text(json.dumps({"surfaces": inventory}))
    for index, (log, result) in enumerate(benches, start=1):
        directory = out / "benches" / str(index)
        directory.mkdir(parents=True)
        (directory / "result").write_text(result + "\n")
        if log is not None:
            (directory / "typed.log").write_text(log)
    status = report.summarize(out)
    return status, json.loads((out / "summary.json").read_text())


def shard(ecw=(), native=(), *, result=None, **kwargs):
    log, status = typed_log(ecw, native, **kwargs)
    return log, result or f"typed_exit_{status}"


PASSING = [shard([("ecw.a", 0)], [("native.c", 0)]), shard([("ecw.b", 0)], [("native.d", 0)])]


def test_a_complete_passing_run_exits_0(tmp_path):
    status, summary = run(tmp_path, PASSING)
    assert status == 0
    assert summary["not_passed"] == []
    assert all(entry["exactly_once"] for entry in summary["coverage"]["surfaces"].values())


@pytest.mark.parametrize(
    ("first", "second", "expected"),
    [
        # A failure outranks an incomplete result, which outranks a warning.
        ([("ecw.a", 1)], [("ecw.b", 2)], 1),
        ([("ecw.a", 42)], [("ecw.b", 2)], 2),
        ([("ecw.a", 42)], [("ecw.b", 0)], 42),
    ],
)
def test_the_suite_ranking(tmp_path, first, second, expected):
    status, _ = run(tmp_path, [shard(first, [("native.c", 0)]), shard(second, [("native.d", 0)])])
    assert status == expected


def test_a_bench_that_did_not_come_up_is_incomplete(tmp_path):
    status, summary = run(tmp_path, [PASSING[0], (None, "baseline_failed")])
    assert status == 2
    assert summary["coverage"]["surfaces"]["ecw"]["missing"] == ["ecw.b"]


def test_a_failure_beside_a_bench_that_did_not_come_up_still_fails(tmp_path):
    status, _ = run(tmp_path, [shard([("ecw.a", 1)], [("native.c", 0)]), (None, "up_failed")])
    assert status == 1


def test_an_excused_known_failure_is_listed_and_does_not_fail(tmp_path):
    status, summary = run(
        tmp_path, [shard([("ecw.a", 1)], [("native.c", 0)], known=["ecw.a"]), PASSING[1]]
    )
    assert status == 0
    assert [(row["scenario"], row["verdict"]) for row in summary["not_passed"]] == [("ecw.a", "known failure")]


def test_a_pass_that_exited_2_outside_any_scenario_is_incomplete(tmp_path):
    status, _ = run(tmp_path, [shard([("ecw.a", 0)], [("native.c", 0)], statuses=(2, 0)), PASSING[1]])
    assert status == 2


def test_a_pass_status_1_without_a_failed_scenario_is_incomplete(tmp_path):
    status, _ = run(tmp_path, [shard([("ecw.a", 0)], [("native.c", 0)], statuses=(0, 1)), PASSING[1]])
    assert status == 2


def test_a_shard_exit_the_pass_statuses_do_not_explain_is_incomplete(tmp_path):
    # Both passes reported 0, but the shard exited 1 (its log pipeline failed).
    status, _ = run(tmp_path, [shard([("ecw.a", 0)], [("native.c", 0)], result="typed_exit_1"), PASSING[1]])
    assert status == 2


def test_a_shard_without_its_closing_line_is_incomplete(tmp_path):
    log, _ = typed_log([("ecw.a", 0)], [("native.c", 0)])
    truncated = log.split("=== typed smoke summary")[0]
    status, _ = run(tmp_path, [(truncated, "typed_exit_0"), PASSING[1]])
    assert status == 2


def test_a_scenario_skipped_by_a_mode_after_the_partition_counts_as_covered(tmp_path):
    status, summary = run(
        tmp_path, [shard([("ecw.a", 0)], [("native.c", 0)]), shard([], [("native.d", 0)], skipped=["ecw.b"])]
    )
    assert status == 0
    assert summary["coverage"]["surfaces"]["ecw"]["skipped_by_mode"] == ["ecw.b"]


@pytest.mark.parametrize(
    "second",
    [
        shard([("ecw.a", 0), ("ecw.b", 0)], [("native.d", 0)]),  # run twice
        shard([("ecw.b", 0)], [("native.d", 0)], skipped=["ecw.a"]),  # run and skipped
    ],
)
def test_a_pair_covered_twice_is_duplicated(tmp_path, second):
    status, summary = run(tmp_path, [shard([("ecw.a", 0)], [("native.c", 0)]), second])
    assert status == 2
    assert summary["coverage"]["surfaces"]["ecw"]["duplicated"] == ["ecw.a"]


def test_a_pair_skipped_on_two_benches_is_duplicated(tmp_path):
    status, summary = run(
        tmp_path,
        [
            shard([("ecw.a", 0)], [("native.c", 0)], skipped=["ecw.b"]),
            shard([], [("native.d", 0)], skipped=["ecw.b"]),
        ],
    )
    assert status == 2
    assert summary["coverage"]["surfaces"]["ecw"]["duplicated"] == ["ecw.b"]


def test_missing_and_unexpected_pairs_fail_the_coverage_check(tmp_path):
    status, summary = run(
        tmp_path, [shard([("ecw.a", 0)], [("native.c", 0)]), shard([("ecw.z", 0)], [("native.d", 0)])]
    )
    assert status == 2
    entry = summary["coverage"]["surfaces"]["ecw"]
    assert (entry["missing"], entry["unexpected"], entry["exactly_once"]) == (["ecw.b"], ["ecw.z"], False)


def test_without_an_inventory_the_run_is_incomplete(tmp_path):
    status, summary = run(tmp_path, PASSING, inventory=None)
    assert status == 2
    assert summary["coverage"]["checked"] is False


def test_the_parser_reads_an_end_marker_glued_to_output(tmp_path):
    log, _ = typed_log([("ecw.a", 1)], [("native.c", 0)], glued=True)
    (tmp_path / "typed.log").write_text(log)
    parsed = report.parse_typed(tmp_path / "typed.log")
    assert parsed["ecw"]["exits"] == {"ecw.a": 1}
    assert parsed["native"]["exits"] == {"native.c": 0}


def test_the_parser_keeps_each_pass_s_counts_over_the_closing_summary(tmp_path):
    log, _ = typed_log([("ecw.a", 0), ("ecw.b", 42)], [("native.c", 1)])
    (tmp_path / "typed.log").write_text(log)
    parsed = report.parse_typed(tmp_path / "typed.log")
    assert parsed["ecw"]["suite"] == {"passed": 1, "warned": 1, "failed": 0, "inconclusive": 0}
    assert parsed["native"]["suite"] == {"passed": 0, "warned": 0, "failed": 1, "inconclusive": 0}
    assert (parsed["ecw"]["status"], parsed["native"]["status"]) == (42, 1)

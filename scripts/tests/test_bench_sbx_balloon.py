"""Unit tests for the balloon suspend/restore benchmark and its summary.

Run from the repo root:

    python -m pytest scripts/tests/test_bench_sbx_balloon.py -q

Both scripts use kebab-case CLI names, so they are imported via importlib.
"""

from __future__ import annotations

import argparse
import collections
import importlib.util
import json
import pathlib
import sys

import pytest

BENCH_DIR = pathlib.Path(__file__).resolve().parent.parent / "benchmarks"
sys.path.insert(0, str(BENCH_DIR))


def _load(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, BENCH_DIR / filename)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


summary = _load("bench_sbx_balloon_summary", "bench-sbx-balloon-summary.py")
bench = _load("bench_sbx_balloon", "bench-sbx-balloon.py")

MIB = 1024 * 1024
GIB = 1024 * MIB


def row(variant: str, mem_disk: int | None, *, cycle: int = 1, ok: bool | None = True, profile: str = "idle", **extra):
    out = {
        "variant": variant,
        "profile": profile,
        "cycle": cycle,
        "correctness_ok": ok,
        "snapshot_mem_disk_bytes": mem_disk,
        "restore_ms": 1000,
    }
    out.update(extra)
    return out


def rows_for(variant: str, sizes, **kwargs):
    return [row(variant, size, cycle=i + 1, **kwargs) for i, size in enumerate(sizes)]


def gate_by_variant(rows, **kwargs):
    return {g.variant: g for g in summary.evaluate_gate(rows, baseline="baseline", **kwargs)}


# --- Gate ------------------------------------------------------------------


def test_gate_passes_when_both_thresholds_met():
    rows = rows_for("baseline", [8 * GIB] * 3) + rows_for("reporting", [6 * GIB] * 3)
    result = gate_by_variant(rows)["reporting"]
    assert result.passed, result.reasons
    assert result.reduction_bytes == 2 * GIB
    assert result.reduction_pct == pytest.approx(25.0)


def test_gate_requires_percentage_threshold():
    # 100 MiB smaller clears 64 MiB but is only ~1.2% of 8 GiB.
    rows = rows_for("baseline", [8 * GIB] * 3) + rows_for("inflation", [8 * GIB - 100 * MIB] * 3)
    result = gate_by_variant(rows)["inflation"]
    assert not result.passed
    assert any("%" in reason for reason in result.reasons)


def test_gate_requires_absolute_threshold():
    # 50% smaller but only 50 MiB absolute.
    rows = rows_for("baseline", [100 * MIB] * 3) + rows_for("hinting", [50 * MIB] * 3)
    result = gate_by_variant(rows)["hinting"]
    assert not result.passed
    assert any("MiB <" in reason for reason in result.reasons)


def test_gate_boundary_is_inclusive():
    base = 640 * MIB
    rows = rows_for("baseline", [base] * 3) + rows_for("v", [base - 64 * MIB] * 3)
    result = gate_by_variant(rows)["v"]
    assert result.reduction_pct == pytest.approx(10.0)
    assert result.passed, result.reasons


def test_gate_uses_medians_not_means():
    base = rows_for("baseline", [8 * GIB, 8 * GIB, 8 * GIB])
    # One huge outlier would drag the mean above baseline; the median is 6 GiB.
    variant = rows_for("reporting", [6 * GIB, 6 * GIB, 40 * GIB])
    result = gate_by_variant(base + variant)["reporting"]
    assert result.variant_median == 6 * GIB
    assert result.passed


def test_gate_fails_on_any_correctness_failure():
    variant = rows_for("reporting", [4 * GIB] * 3)
    variant[1]["correctness_ok"] = False
    result = gate_by_variant(rows_for("baseline", [8 * GIB] * 3) + variant)["reporting"]
    assert not result.passed
    assert "1 correctness failures" in result.reasons


def test_gate_fails_on_unverified_cycles():
    variant = rows_for("reporting", [4 * GIB] * 3)
    variant.append(row("reporting", None, cycle=4, ok=None, error="restore failed"))
    result = gate_by_variant(rows_for("baseline", [8 * GIB] * 3) + variant)["reporting"]
    assert not result.passed
    assert "1 unverified cycles" in result.reasons


def test_gate_setup_errors_do_not_count_as_cycles():
    variant = rows_for("reporting", [4 * GIB] * 3)
    variant.append(row("reporting", None, cycle=0, ok=None, error="create failed"))
    result = gate_by_variant(rows_for("baseline", [8 * GIB] * 3) + variant)["reporting"]
    assert result.passed, result.reasons
    assert summary.correctness(variant).setup_errors == 1


def test_gate_baseline_failures_invalidate_all_variants():
    base = rows_for("baseline", [8 * GIB] * 3)
    base[0]["correctness_ok"] = False
    result = gate_by_variant(base + rows_for("reporting", [4 * GIB] * 3))["reporting"]
    assert not result.passed
    assert any(reason.startswith("baseline invalid") for reason in result.reasons)


def test_gate_missing_metric_fails_cleanly():
    rows = rows_for("baseline", [8 * GIB] * 3) + rows_for("reporting", [None] * 3)
    result = gate_by_variant(rows)["reporting"]
    assert not result.passed
    assert result.variant_median is None
    assert any("missing" in reason for reason in result.reasons)


def test_gate_selectable_metric_and_thresholds():
    rows = [
        row("baseline", 8 * GIB, snapshot_mem_compressed_bytes=2 * GIB),
        row("reporting", 8 * GIB, snapshot_mem_compressed_bytes=1 * GIB),
    ]
    assert not gate_by_variant(rows)["reporting"].passed
    assert gate_by_variant(rows, metric="snapshot_mem_compressed_bytes")["reporting"].passed
    assert not gate_by_variant(rows, metric="snapshot_mem_compressed_bytes", min_reduction_pct=60)["reporting"].passed


def test_gate_unknown_baseline_exits():
    with pytest.raises(SystemExit):
        summary.evaluate_gate(rows_for("reporting", [1]), baseline="baseline")


# --- Summary statistics ----------------------------------------------------


def test_percentile_nearest_rank():
    values = list(range(1, 101))
    assert summary.percentile(values, 95) == 95
    assert summary.percentile([5], 95) == 5
    assert summary.percentile([], 95) is None


def test_summarize_median_p95_by_profile():
    rows = [row("baseline", s * MIB, cycle=i + 1, profile="build") for i, s in enumerate([10, 20, 30, 40])]
    rows += [row("baseline", 100 * MIB, profile="idle")]
    rows += [row("baseline", 999 * MIB, cycle=0, ok=None)]  # setup row: ignored
    flat = {(r["variant"], r["metric"]): r for r in summary.summarize(rows)}
    mem = flat[("baseline", "snapshot_mem_disk_bytes")]
    assert mem["count"] == 5 and mem["median"] == 30 * MIB and mem["p95"] == 100 * MIB
    split = {(r["profile"], r["metric"]): r for r in summary.summarize(rows, by_profile=True)}
    assert split[("build", "snapshot_mem_disk_bytes")]["median"] == 25 * MIB


def test_metric_values_skip_null_and_bool():
    rows = [{"x": 1}, {"x": None}, {"x": True}, {}, {"x": 2.5}]
    assert summary.metric_values(rows, "x") == [1, 2.5]


def test_main_json_and_fail_on_gate(tmp_path, capsys):
    path = tmp_path / "results.jsonl"
    rows = rows_for("baseline", [8 * GIB] * 3) + rows_for("good", [6 * GIB] * 3) + rows_for("bad", [8 * GIB] * 3)
    path.write_text("".join(json.dumps(r) + "\n" for r in rows) + "\n")
    assert summary.main([str(path), "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert {g["variant"]: g["passed"] for g in payload["gate"]} == {"bad": False, "good": True}
    assert summary.main([str(path), "--fail-on-gate"]) == 1
    text = capsys.readouterr().out
    assert "good             PASS" in text and "bad              FAIL" in text


def test_load_rows_reports_bad_line(tmp_path):
    path = tmp_path / "results.jsonl"
    path.write_text('{"variant": "baseline"}\nnot-json\n')
    with pytest.raises(SystemExit, match=":2:"):
        summary.load_rows([path])


# --- Harness pure helpers --------------------------------------------------


@pytest.mark.parametrize("count", [4, 8, 40])
def test_assign_profiles_even_split(count):
    assigned = bench.assign_profiles(count, bench.PROFILES, seed=7)
    assert collections.Counter(assigned) == {p: count // 4 for p in bench.PROFILES}
    assert assigned == bench.assign_profiles(count, bench.PROFILES, seed=7)


def test_parse_variants():
    assert bench.parse_variants([], "seed") == [("seed", "seed")]
    assert bench.parse_variants(["baseline=t-a", "inflation = t-b"], "seed") == [("baseline", "t-a"), ("inflation", "t-b")]
    for bad in (["noequals"], ["=t"], ["a="], ["a=t", "a=u"]):
        with pytest.raises(SystemExit):
            bench.parse_variants(bad, "seed")


def test_parse_kv_ignores_noise():
    text = "WARNING: something\nmem_sha=abc\nfile_sha=def=ghi\n  anon_sha=123  \nBad-Key=x\n"
    assert bench.parse_kv(text) == {"mem_sha": "abc", "file_sha": "def=ghi", "anon_sha": "123"}


def test_compare_checksums():
    expected = {"mem_sha": "a", "file_sha": "b"}
    assert bench.compare_checksums(expected, {"mem_sha": "a", "file_sha": "b", "anon_sha": "zzz"}) == []
    assert bench.compare_checksums(expected, {"mem_sha": "x", "file_sha": "b"}) == ["mem_pattern"]
    assert bench.compare_checksums({**expected, "anon_sha": "c"}, {"mem_sha": "a"}) == ["file_pattern", "anon_buffer"]


def test_host_parsers():
    diskstats = (
        " 259       0 nvme0n1 10 0 0 0 20 0 1000 0 0 0 0\n"
        " 259       1 nvme0n1p1 10 0 0 0 20 0 999 0 0 0 0\n"
        "   7       0 loop0 1 0 0 0 1 0 555 0 0 0 0\n"
        " 259       2 nvme1n1 10 0 0 0 20 0 24 0 0 0 0\n"
    )
    assert bench.parse_diskstats(diskstats) == (1000 + 24) * 512
    assert bench.parse_vm_rss_kib("Name:\tfirecracker\nVmRSS:\t  123456 kB\n") == 123456
    assert bench.parse_vm_rss_kib("Name:\tx\n") is None
    assert bench.parse_meminfo_kib("Dirty:  12 kB\nWriteback: 3 kB\nMemFree: 9 kB\n", ("Dirty", "Writeback")) == {
        "Dirty": 12,
        "Writeback": 3,
    }
    du = "     Total   Exclusive  Set shared  Filename\n8589934592   104857600  8485076992  /x/rootfs.ext4\n"
    assert bench.parse_btrfs_du_exclusive(du) == 104857600
    assert bench.parse_btrfs_du_exclusive("ERROR: not btrfs\n") is None


def test_guest_elapsed_and_http_split():
    assert bench.guest_elapsed_ms({"start_ns": "1000000000", "end_ns": "3500000000"}) == 2500
    assert bench.guest_elapsed_ms({"start_ns": "", "end_ns": "5"}) is None
    assert bench.http_response_split('{"a":1}\n200') == (200, '{"a":1}')
    assert bench.http_response_split('{"fault_message":"no balloon"}\n400\n') == (400, '{"fault_message":"no balloon"}')
    assert bench.http_response_split("curl: (7) failed") == (None, "curl: (7) failed")


def test_remote_args_forward_only_non_defaults():
    parser = bench.build_parser()
    args = parser.parse_args(
        ["worker-1", "--ssh-option", "Port=2222", "--variant", "baseline=t-a", "--variant", "reporting=t-b",
         "--cycles", "5", "--no-compressed-size", "--sudo", "--output-dir", "/tmp/out"]
    )
    forwarded = bench.remote_args(args, parser)
    assert forwarded == [
        "--variant", "baseline=t-a", "--variant", "reporting=t-b", "--cycles", "5",
        "--no-compressed-size", "--output-dir", "/tmp/out", "--sudo",
    ]
    assert "worker-1" not in forwarded and "Port=2222" not in forwarded


def test_guest_scripts_are_valid_sh(tmp_path):
    import shutil
    import subprocess

    sh = shutil.which("sh")
    if not sh:
        pytest.skip("sh not available")
    scripts = {"setup": bench.GUEST_SETUP, "verify": bench.GUEST_VERIFY, **bench.GUEST_WORKLOADS}
    for name, script in scripts.items():
        result = subprocess.run([sh, "-n", "-c", script], capture_output=True, text=True)
        assert result.returncode == 0, f"{name}: {result.stderr}"
    compile(bench.GUEST_ANON_HOLDER, "anon-holder", "exec")


def test_args_namespace_is_json_serializable():
    args = bench.build_parser().parse_args(["--variant", "baseline=t"])
    assert isinstance(args, argparse.Namespace)
    json.dumps({k: v for k, v in vars(args).items() if k != "ssh_option"})


# --- End-to-end against a fake worker --------------------------------------

FAKE_SBXMGR = r'''#!/usr/bin/env python3
"""Minimal stand-in for space-sbxmgr/space-tmplmgr used by the harness tests."""
import os, pathlib, subprocess, sys, uuid

root = pathlib.Path(os.environ["FAKE_ROOT"])
cache, run, guest = root / "cache", root / "run", root / "guest"
cmd, rest = sys.argv[1], sys.argv[2:]
# Snapshot memory size per template, in MiB; "t-corrupt" also flips a guest byte.
MEM_MIB = {"t-base": 8, "t-small": 4, "t-corrupt": 4}

def tmpl(sid):
    return (run / sid / "template").read_text()

if cmd == "list" and os.path.basename(sys.argv[0]).startswith("tmpl"):
    print("\n".join(MEM_MIB))
elif cmd == "--version":
    print("space-sbxmgr 0.0.0-test")
elif cmd == "create":
    sid = str(uuid.uuid4())
    (run / sid).mkdir(parents=True)
    (run / sid / "template").write_text(rest[rest.index("--template") + 1])
    os.symlink(f"/proc/{os.getppid()}", run / sid / "fc.pid")
    print(f"Created sandbox {sid}")
elif cmd == "exec":
    sid = rest[0]
    argv = rest[rest.index("--") + 1:]
    env = dict(os.environ, SBX_BENCH_GUEST_ROOT=str(guest / sid))
    sys.exit(subprocess.run(argv, env=env).returncode)
elif cmd == "suspend":
    sid = rest[0]
    snap = cache / sid / "snapshot"
    snap.mkdir(parents=True, exist_ok=True)
    (snap / "memory").write_bytes(b"\x01" * (MEM_MIB[tmpl(sid)] << 20))
    (snap / "vmstate").write_bytes(b"v" * 4096)
    (snap / "rootfs.ext4").write_bytes(b"r" * 8192)
    count = run / sid / "suspends"
    count.write_text(str(int(count.read_text()) + 1 if count.exists() else 1))
elif cmd == "restore":
    sid = rest[0]
    if tmpl(sid) == "t-corrupt" and (run / sid / "suspends").read_text() == "2":
        pattern = guest / sid / "mem" / "pattern"
        data = bytearray(pattern.read_bytes())
        data[0] ^= 0xFF
        pattern.write_bytes(bytes(data))
elif cmd == "delete":
    subprocess.run(["pkill", "-f", str(guest / rest[0])], check=False)
else:
    sys.exit(f"unexpected command: {sys.argv}")
'''


def test_harness_end_to_end_with_fake_worker(tmp_path, monkeypatch, capsys):
    import shutil
    import subprocess

    for tool in ("sh", "dd", "sha256sum", "gzip", "pkill"):
        if not shutil.which(tool):
            pytest.skip(f"{tool} not available")
    fake = tmp_path / "bin" / "sbxmgr"
    fake.parent.mkdir()
    fake.write_text(FAKE_SBXMGR)
    fake.chmod(0o755)
    tmplmgr = tmp_path / "bin" / "tmplmgr"
    tmplmgr.symlink_to(fake)
    monkeypatch.setenv("FAKE_ROOT", str(tmp_path))
    out_dir = tmp_path / "out"
    parser = bench.build_parser()
    args = parser.parse_args(
        [
            "--variant", "baseline=t-base", "--variant", "reporting=t-small", "--variant", "broken=t-corrupt",
            "--sandboxes", "4", "--cycles", "3", "--concurrency", "2", "--no-sudo", "--no-compressed-size",
            "--settle-secs", "0", "--idle-secs", "0", "--pattern-mem-mb", "1", "--pattern-file-mb", "1",
            "--anon-check-mb", "1", "--build-units", "2", "--build-blob-mb", "1", "--alloc-min-mb", "8",
            "--alloc-max-mb", "8", "--file-heavy-mb", "8",
            "--sbxmgr", str(fake), "--tmplmgr", str(tmplmgr),
            "--sandbox-cache-dir", str(tmp_path / "cache"), "--sandbox-run-dir", str(tmp_path / "run"),
            "--jail-dir", str(tmp_path / "jail"), "--output-dir", str(out_dir),
        ]
    )
    try:
        bench.BalloonBenchmark(args).run()
    finally:
        subprocess.run(["pkill", "-f", str(tmp_path / "guest")], check=False)

    rows = summary.load_rows([out_dir / "results.jsonl"])
    assert len(rows) == 3 * 4 * 3
    assert {r["error"] for r in rows} == {None}
    by_variant = summary.group_by_variant(rows)
    for variant, profile_rows in by_variant.items():
        assert collections.Counter(r["profile"] for r in profile_rows) == {p: 3 for p in bench.PROFILES}, variant
    base = by_variant["baseline"][0]
    assert base["snapshot_mem_bytes"] == 8 * MIB
    assert base["anon_check"] is True
    assert base["balloon_config"] is None and base["balloon_stats_before_suspend"] is None
    assert isinstance(base["fc_rss_before_suspend_kib"], int)
    assert all(r["correctness_ok"] for r in by_variant["baseline"] + by_variant["reporting"])
    broken = by_variant["broken"]
    # Corruption injected at the second restore stays visible on every later verify.
    assert sorted(r["cycle"] for r in broken if r["correctness_ok"] is False) == [2, 2, 2, 2, 3, 3, 3, 3]
    assert all(r["correctness_ok"] for r in broken if r["cycle"] == 1)
    assert all(r["correctness_failures"] == ["mem_pattern"] for r in broken if r["correctness_ok"] is False)

    # 4 MiB vs 8 MiB clears 10% but not 64 MiB; lower the absolute bar to exercise both outcomes.
    capsys.readouterr()
    assert summary.main([str(out_dir / "results.jsonl"), "--min-reduction-mib", "1", "--fail-on-gate"]) == 1
    report = capsys.readouterr().out
    assert "reporting        PASS" in report
    assert "broken           FAIL" in report and "8 correctness failures" in report

"""Exercise the Rust compiler-cache refresh decision and snapshot pruning."""

from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

REPO_ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "sccache_snapshot_under_test", REPO_ROOT / "ci/scripts/sccache-snapshot.py"
)
assert SPEC is not None and SPEC.loader is not None
snapshot = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = snapshot
SPEC.loader.exec_module(snapshot)

HASH = "a" * 64
OTHER_HASH = "b" * 64
INPUTS = f"space-sccache-v1-Linux-X64-tests-{HASH}-"
RESTORED = f"{INPUTS}100-1"


def stats(hits: int, misses: int, size: int | None = None, maximum: int = 1000) -> str:
    report: dict = {
        "stats": {
            "cache_hits": {"counts": {"Rust": hits, "C/C++": 5}, "adv_counts": {}},
            "cache_misses": {"counts": {"Rust": misses}, "adv_counts": {}},
        }
    }
    if size is not None:
        report |= {"cache_size": size, "max_cache_size": maximum}
    return json.dumps(report)


class DecideTest(unittest.TestCase):
    def test_keeps_a_current_snapshot_below_the_threshold(self) -> None:
        decision = snapshot.decide(RESTORED, INPUTS, stats(830, 19), 20)
        self.assertFalse(decision.save)
        self.assertFalse(decision.trim)

    def test_refreshes_and_trims_a_snapshot_at_the_threshold(self) -> None:
        decision = snapshot.decide(RESTORED, INPUTS, stats(829, 20), 20)
        self.assertTrue(decision.save)
        self.assertTrue(decision.trim)

    def test_refreshes_a_snapshot_built_for_other_inputs(self) -> None:
        other = f"space-sccache-v1-Linux-X64-tests-{OTHER_HASH}-100-1"
        for restored in (other, other.removesuffix("-100-1")):
            decision = snapshot.decide(restored, INPUTS, stats(849, 0), 20)
            self.assertTrue(decision.save, restored)
            self.assertTrue(decision.trim, restored)

    def test_saves_when_nothing_was_restored(self) -> None:
        self.assertTrue(snapshot.decide("", INPUTS, stats(0, 854), 20).save)

    def test_saves_without_trimming_when_statistics_are_unusable(self) -> None:
        for stats_json in (None, "", "{", "{}", '{"stats": {"cache_hits": []}}'):
            decision = snapshot.decide(RESTORED, INPUTS, stats_json, 20)
            self.assertTrue(decision.save, stats_json)
            self.assertFalse(decision.trim, stats_json)

    def test_never_trims_when_sccache_saw_no_rust_compilations(self) -> None:
        decision = snapshot.decide("", INPUTS, stats(0, 0), 20)
        self.assertTrue(decision.save)
        self.assertFalse(decision.trim)

    def test_saves_when_the_threshold_is_missing_or_invalid(self) -> None:
        for value in ("", None, "many", "-1"):
            threshold = snapshot.parse_threshold(value)
            self.assertIsNone(threshold, value)
            self.assertTrue(snapshot.decide(RESTORED, INPUTS, stats(849, 0), threshold).save)
        self.assertEqual(snapshot.parse_threshold("20"), 20)


class CacheChecksTest(unittest.TestCase):
    """Which advisory checks a run trips; the warnings' wording is not a contract."""

    def test_a_cache_is_nearly_full_only_close_to_its_maximum(self) -> None:
        self.assertTrue(snapshot.nearly_full(stats(800, 50, size=1000)))
        for size in (0, 1, 500):
            self.assertFalse(snapshot.nearly_full(stats(800, 50, size=size)), size)
        for stats_json in (None, "", "{", stats(800, 50), stats(800, 50, size=0, maximum=0)):
            self.assertFalse(snapshot.nearly_full(stats_json), stats_json)

    def test_a_warm_restore_misses_only_when_most_compilations_miss(self) -> None:
        self.assertTrue(snapshot.missed_warm(RESTORED, INPUTS, stats(0, 900)))
        self.assertTrue(snapshot.missed_warm(RESTORED, INPUTS, stats(100, 900)))
        for hits, misses in ((900, 0), (900, 100), (0, 0)):
            self.assertFalse(snapshot.missed_warm(RESTORED, INPUTS, stats(hits, misses)), hits)

    def test_a_cold_restore_or_unusable_statistics_never_miss_warm(self) -> None:
        other = f"space-sccache-v1-Linux-X64-tests-{OTHER_HASH}-100-1"
        for restored in ("", other):
            self.assertFalse(snapshot.missed_warm(restored, INPUTS, stats(0, 900)), restored)
        for stats_json in (None, "", "{", "{}"):
            self.assertFalse(snapshot.missed_warm(RESTORED, INPUTS, stats_json), stats_json)


class DecideCommandTest(unittest.TestCase):
    """`decide` runs in every Rust-cache job: its warnings never fail it or
    change what it decides."""

    def run_decide(self, restored: str, stats_json: str | None) -> dict[str, str]:
        with tempfile.TemporaryDirectory() as directory:
            stats_path = Path(directory, "stats.json")
            if stats_json is not None:
                stats_path.write_text(stats_json, encoding="utf-8")
            output = Path(directory, "output")
            environment = {
                "GITHUB_OUTPUT": str(output),
                "GITHUB_EVENT_NAME": "push",
                "GITHUB_REF": "refs/heads/main",
            }
            with mock.patch.dict(os.environ, environment), contextlib.redirect_stdout(io.StringIO()):
                status = snapshot.main(
                    [
                        "decide",
                        "--key",
                        f"{INPUTS}200-1",
                        "--restored-key",
                        restored,
                        "--inputs-prefix",
                        INPUTS,
                        "--stats",
                        str(stats_path),
                        "--refresh-misses",
                        "20",
                    ]
                )
            self.assertEqual(status, 0)
            return dict(line.split("=", 1) for line in output.read_text(encoding="utf-8").splitlines())

    def test_a_nearly_full_cache_still_keeps_a_current_snapshot(self) -> None:
        stats_json = stats(830, 19, size=1000)
        self.assertTrue(snapshot.nearly_full(stats_json))
        self.assertEqual(self.run_decide(RESTORED, stats_json), {"save": "false", "trim": "false"})

    def test_a_warm_restore_that_missed_still_refreshes_and_trims(self) -> None:
        for size in (None, 100, 1000):
            stats_json = stats(100, 900, size=size)
            self.assertTrue(snapshot.missed_warm(RESTORED, INPUTS, stats_json), size)
            self.assertEqual(
                self.run_decide(RESTORED, stats_json), {"save": "true", "trim": "true"}, size
            )

    def test_unusable_or_missing_statistics_save_without_trimming(self) -> None:
        for stats_json in (None, "", "{", '{"cache_size": "full", "max_cache_size": 0}'):
            self.assertEqual(
                self.run_decide(RESTORED, stats_json), {"save": "true", "trim": "false"}, stats_json
            )


def cache(cache_id: int, key: str, created: str) -> dict:
    return {"id": cache_id, "key": key, "created_at": created}


class SupersededTest(unittest.TestCase):
    def test_keeps_only_the_newest_snapshot_of_each_family(self) -> None:
        tests = "space-sccache-v1-Linux-X64-tests"
        gateway = "space-sccache-v1-Linux-X64-gateway-traffic"
        caches = [
            cache(1, f"{tests}-{OTHER_HASH}", "2026-09-25T19:57:58.286928Z"),
            cache(2, f"{tests}-{HASH}-200-1", "2026-09-26T01:00:00.5Z"),
            cache(3, f"{tests}-{HASH}-300-2", "2026-09-26T01:00:00.45Z"),
            cache(4, f"{gateway}-{HASH}-200-1", "2026-09-26T00:00:00Z"),
            cache(5, f"{gateway}-{OTHER_HASH}-100-1", "2026-09-25T00:00:00Z"),
            cache(6, "space-buildkit-blob-sha256", "2026-09-20T00:00:00Z"),
        ]
        self.assertEqual(sorted(c["id"] for c in snapshot.superseded(caches)), [1, 3, 5])

    def test_keeps_the_newest_target_directory_of_each_build_inputs_hash(self) -> None:
        tests = "space-target-v2-Linux-X64-tests"
        caches = [
            cache(1, f"{tests}-0123456789abcdef-1111111111111111-200-1", "2026-09-26T01:00:00Z"),
            cache(2, f"{tests}-0123456789abcdef-2222222222222222-300-1", "2026-09-26T02:00:00Z"),
            cache(3, f"{tests}-fedcba9876543210-2222222222222222-400-1", "2026-09-26T03:00:00Z"),
            cache(4, f"space-sccache-v1-Linux-X64-tests-{HASH}-400-1", "2026-09-26T03:00:00Z"),
            cache(5, f"{tests}-0123456789abcdef-3333333333333333", "2026-09-26T04:00:00Z"),
        ]
        self.assertEqual(sorted(c["id"] for c in snapshot.superseded(caches)), [1])

    def test_keeps_target_directories_for_only_the_latest_build_inputs(self) -> None:
        tests = "space-target-v2-Linux-X64-tests"
        lock = "1111111111111111"
        newest = snapshot.TARGET_INPUTS_KEPT + 1
        caches = [
            cache(n, f"{tests}-{n:016x}-{lock}-{n}-1", f"2026-09-26T{n:02d}:00:00Z")
            for n in range(1, newest + 1)
        ]
        # An older snapshot of the newest inputs, and another family's oldest.
        caches.append(cache(100, f"{tests}-{newest:016x}-{lock}-99-1", "2026-09-25T00:00:00Z"))
        caches.append(
            cache(200, f"space-target-v2-Linux-ARM64-tests-{1:016x}-{lock}-1-1", "2026-09-25T00:00:00Z")
        )
        self.assertEqual(sorted(c["id"] for c in snapshot.superseded(caches)), [1, 100])

    def test_keeps_one_kind_state_for_each_of_the_two_newest_build_inputs(self) -> None:
        kind = "space-target-v2-Linux-X64-kind-test-hooks"
        tests = "space-target-v2-Linux-X64-tests"
        lock = "1111111111111111"
        caches = [
            cache(1, f"{kind}-{1:016x}-{lock}-1-1", "2026-09-26T01:00:00Z"),
            cache(2, f"{kind}-{2:016x}-{lock}-2-1", "2026-09-26T02:00:00Z"),
            cache(3, f"{kind}-{3:016x}-{lock}-3-1", "2026-09-26T03:00:00Z"),
            # An older commit's run of the second-newest inputs saved last.
            cache(4, f"{kind}-{2:016x}-{lock}-4-1", "2026-09-26T04:00:00Z"),
            # Another target family still keeps its last three inputs.
            cache(5, f"{tests}-{1:016x}-{lock}-1-1", "2026-09-26T01:00:00Z"),
            cache(6, f"{tests}-{2:016x}-{lock}-2-1", "2026-09-26T02:00:00Z"),
            cache(7, f"{tests}-{3:016x}-{lock}-3-1", "2026-09-26T03:00:00Z"),
        ]
        # The newest commit's state (3) survives the older commit's later save.
        self.assertEqual(sorted(c["id"] for c in snapshot.superseded(caches)), [1, 2])

    def test_leaves_a_lone_snapshot_alone(self) -> None:
        only = cache(1, f"space-sccache-v1-Linux-X64-clippy-{HASH}-1-1", "2026-09-26T00:00:00Z")
        self.assertEqual(snapshot.superseded([only]), [])


if __name__ == "__main__":
    unittest.main()

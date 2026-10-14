"""Exercise the Rust compiler-cache refresh decision and snapshot pruning."""

from __future__ import annotations

import importlib.util
import json
import sys
import unittest
from pathlib import Path

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


def stats(hits: int, misses: int) -> str:
    return json.dumps(
        {
            "stats": {
                "cache_hits": {"counts": {"Rust": hits, "C/C++": 5}, "adv_counts": {}},
                "cache_misses": {"counts": {"Rust": misses}, "adv_counts": {}},
            }
        }
    )


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

    def test_leaves_a_lone_snapshot_alone(self) -> None:
        only = cache(1, f"space-sccache-v1-Linux-X64-clippy-{HASH}-1-1", "2026-09-26T00:00:00Z")
        self.assertEqual(snapshot.superseded([only]), [])


if __name__ == "__main__":
    unittest.main()

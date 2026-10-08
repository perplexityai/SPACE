"""Contracts of the template-cache eviction scenario the bench cannot exercise.

The live run proves the loop reclaims a build; nothing here restates that. These
pin what a bench run would not catch: knob parsing that mirrors the binary
(including grace=0), shell-word validation on every parameter that reaches an
on-node `bash -c`, NotFound-vs-error mapping that must fail closed, the tick
parser across both tracing layouts, a bench with the loop off being a
configuration error rather than a pass, and — the one that bit on the bench —
the wait never resolving the build while it waits.
"""

from __future__ import annotations

import json
import unittest

from space_smoke import adapters
from space_smoke.adapters import (
    KindConfig,
    KindHarness,
    _template_cache_gc_config_from_env,
)
from space_smoke.runtime import CommandResult, SmokeConfigurationError


class GcConfigParsing(unittest.TestCase):
    def test_positive_budget_enables_and_others_default_like_the_binary(self) -> None:
        config = _template_cache_gc_config_from_env(
            {"SPACE_TEMPLATE_CACHE_BUDGET_BYTES": "805306368"}
        )
        self.assertTrue(config.enabled)
        self.assertEqual(config.budget_bytes, 805306368)
        self.assertEqual(config.interval_seconds, 300)
        self.assertEqual(config.grace_seconds, 1800)

    def test_unset_budget_uses_the_default_and_zero_or_malformed_means_off(self) -> None:
        unset = _template_cache_gc_config_from_env({})
        self.assertTrue(unset.enabled)
        self.assertEqual(unset.budget_bytes, 100 * 1024 * 1024 * 1024)
        invalid = _template_cache_gc_config_from_env(
            {"SPACE_TEMPLATE_CACHE_BUDGET_BYTES": "not-a-number"}
        )
        self.assertFalse(invalid.enabled)
        self.assertFalse(
            _template_cache_gc_config_from_env(
                {"SPACE_TEMPLATE_CACHE_BUDGET_BYTES": "0"}
            ).enabled
        )

    def test_zero_grace_is_honored_not_defaulted(self) -> None:
        # `GcConfig::from_env` filters interval for positivity but not grace.
        config = _template_cache_gc_config_from_env(
            {
                "SPACE_TEMPLATE_CACHE_BUDGET_BYTES": "1",
                "SPACE_TEMPLATE_GC_INTERVAL_SECS": "0",
                "SPACE_TEMPLATE_GC_GRACE_SECS": "0",
            }
        )
        self.assertEqual(config.grace_seconds, 0)
        self.assertEqual(config.interval_seconds, 300)

    def test_headroom_is_fixed_and_latest_n_defaults_to_five(self) -> None:
        # The binary's headroom fraction is a constant (0.9); prefetch's latest-N
        # default is 5, which is why the gate has to look at it at all.
        garbage = _template_cache_gc_config_from_env(
            {"SPACE_TEMPLATE_CACHE_BUDGET_BYTES": "1"}
        )
        self.assertEqual(garbage.headroom_fraction, 0.9)
        self.assertEqual(garbage.prefetch_latest_n, 5)
        self.assertEqual(
            _template_cache_gc_config_from_env(
                {"SPACE_TEMPLATE_CACHE_BUDGET_BYTES": "1", "SPACE_TEMPLATE_PREFETCH_LATEST_N": "1"}
            ).prefetch_latest_n,
            1,
        )


class _RecordingKubectl:
    """Answer kubectl by verb; record every argv."""

    def __init__(self, *, get_output: str = "", exec_results: list[CommandResult] | None = None) -> None:
        self.calls: list[tuple[str, ...]] = []
        self.get_output = get_output
        self.exec_results = list(exec_results or [])

    def run(self, command, **_options) -> CommandResult:
        argv = tuple(command)
        self.calls.append(argv)
        verb = argv[5]
        if verb == "exec":
            if self.exec_results:
                return self.exec_results.pop(0)
            return CommandResult(argv, 0, "", "")
        if verb == "get" and "jsonpath={.items[*].metadata.name}" in argv:
            # The pod lookup an on-node operation or log read starts with.
            return CommandResult(argv, 0, "space-pod-x", "")
        if verb in {"get", "logs"}:
            return CommandResult(argv, 0, self.get_output, "")
        return CommandResult(argv, 0, "", "")


class OnNodeOperations(unittest.TestCase):
    def test_only_the_scenarios_own_throwaway_names_reach_the_shell(self) -> None:
        harness = KindHarness(_RecordingKubectl(), KindConfig())
        with self.assertRaises(ValueError):
            harness.register_unpinned_template("worker-1", "seed", 1)
        with self.assertRaises(ValueError):
            harness.delete_template("worker-1", "smoke-lru-$(id)", )
        with self.assertRaises(ValueError):
            harness.register_unpinned_template("worker-1", "smoke-lru-0123abcd", 0)

    def test_build_state_reads_the_state_line_and_maps_not_found_to_none(self) -> None:
        argv = ("kubectl",)
        kubectl = _RecordingKubectl(
            exec_results=[
                CommandResult(argv, 0, "template_id:  seed\nstate:        ready\n", ""),
                # The CLI propagates the tonic Status; its Display is the gRPC
                # code's description followed by the daemon's message.
                CommandResult(
                    argv, 1, "",
                    "Error: code: 'Some requested entity was not found', "
                    "message: \"no builds found for 'smoke-lru-0123abcd'\"",
                ),
                CommandResult(argv, 1, "", "connection refused"),
            ]
        )
        harness = KindHarness(kubectl, KindConfig())
        digest = "sha256:" + "a" * 64
        self.assertEqual(harness.template_build_state("p", "seed", digest), "ready")
        # A bare-name `get` resolves through the cluster, which has no release
        # for a node-only plant; the probe must name the exact digest.
        self.assertIn(f"--content-hash {digest}", " ".join(kubectl.calls[-1]))
        self.assertIsNone(
            harness.template_build_state("p", "smoke-lru-0123abcd", digest)
        )
        with self.assertRaises(RuntimeError):
            harness.template_build_state("p", "seed", digest)


class DigestListingFailsClosed(unittest.TestCase):
    """An exec that could not run must not read as "the directory is gone"."""

    def test_exec_failure_raises_and_no_match_is_empty(self) -> None:
        argv = ("kubectl",)
        kubectl = _RecordingKubectl(
            exec_results=[
                CommandResult(argv, 1, "", "error: unable to upgrade connection: pod does not exist"),
                CommandResult(argv, 0, "\n", ""),
                CommandResult(argv, 0, "/space/cache/firecracker/templates/seed/sha256:abc\n", ""),
            ]
        )
        harness = KindHarness(kubectl, KindConfig())
        with self.assertRaises(RuntimeError):
            harness.template_digest_dirs("p", "seed")
        self.assertEqual(harness.template_digest_dirs("p", "seed"), ())
        self.assertEqual(harness.template_digest_dirs("p", "seed"), ("sha256:abc",))


class GcTickParsing(unittest.TestCase):
    def test_reads_nested_and_flat_tracing_layouts(self) -> None:
        nested = json.dumps(
            {
                "timestamp": "t",
                "level": "INFO",
                "fields": {"message": adapters.TEMPLATE_GC_TICK_MESSAGE, "deleted": 1, "candidates": 1, "bytes_before": 5, "bytes_freed": 4},
            }
        )
        flat = json.dumps({"message": adapters.TEMPLATE_GC_TICK_MESSAGE, "deleted": "0", "candidates": 0})
        unrelated = json.dumps({"fields": {"message": "template GC tick failed"}})
        kubectl = _RecordingKubectl(get_output="\n".join([nested, flat, unrelated, "not json"]))
        ticks = KindHarness(kubectl, KindConfig()).template_gc_ticks(
            "worker-1", since_time="2026-01-01T00:00:00Z"
        )
        self.assertEqual([t.deleted for t in ticks], [1, 0])
        self.assertEqual(ticks[0].bytes_freed, 4)


class BenchMisconfigurationIsAConfigurationError(unittest.TestCase):
    """Off, oversized, or over-pinned benches are refused, never patched or passed."""

    def _assertions(self, env):
        from space_smoke.fixtures import TemplateCacheAssertions

        class Control:
            def template_cache_gc_config(self, pod):
                return _template_cache_gc_config_from_env(env)

        return TemplateCacheAssertions(None, Control(), None)  # type: ignore[arg-type]

    def test_loop_off(self) -> None:
        with self.assertRaises(SmokeConfigurationError):
            self._assertions({}).require_smoke_sized_gc("p")

    def test_budget_too_large_to_plant(self) -> None:
        # 12 GiB would pass the interval/grace gate and then blow up as a bare
        # ValueError in the adapter's size bound; it is a bench problem.
        with self.assertRaises(SmokeConfigurationError):
            self._assertions(
                {"SPACE_TEMPLATE_CACHE_BUDGET_BYTES": str(12 * 1024**3), "SPACE_TEMPLATE_GC_INTERVAL_SECS": "15", "SPACE_TEMPLATE_GC_GRACE_SECS": "60"}
            ).require_smoke_sized_gc("p")

    def test_default_latest_n_would_overpin(self) -> None:
        with self.assertRaises(SmokeConfigurationError):
            self._assertions(
                {"SPACE_TEMPLATE_CACHE_BUDGET_BYTES": "1610612736", "SPACE_TEMPLATE_GC_INTERVAL_SECS": "15", "SPACE_TEMPLATE_GC_GRACE_SECS": "60"}
            ).require_smoke_sized_gc("p")


class WaitingForEvictionMustNotTouchTheBuild(unittest.TestCase):
    """`space-tmplmgr get` advances the LRU clock; polling it restarts grace forever."""

    def test_poll_reads_the_digest_dir_and_resolves_only_once_afterwards(self) -> None:
        from space_smoke.fixtures import TemplateCacheAssertions

        class Control:
            def __init__(self) -> None:
                self.present = 3
                self.gets = 0
                self.dir_polls = 0

            def template_cache_gc_config(self, pod):
                return _template_cache_gc_config_from_env(
                    {"SPACE_TEMPLATE_CACHE_BUDGET_BYTES": "1", "SPACE_TEMPLATE_GC_GRACE_SECS": "60", "SPACE_TEMPLATE_GC_INTERVAL_SECS": "15", "SPACE_TEMPLATE_PREFETCH_LATEST_N": "1"}
                )

            def template_digest_dirs(self, pod, name):
                self.dir_polls += 1
                self.present -= 1
                return ("sha256:x",) if self.present >= 0 else ()

            def template_build_state(self, pod, name, content_hash):
                self.gets += 1
                return None

            def template_gc_ticks(self, pod, *, since_time):
                return [adapters.TemplateGcTick("n", 1, 1, 2, 1)]

        class Fixture:
            def verify(self, ok, message, **evidence):
                if not ok:
                    raise AssertionError(message)

            def wait(self, seconds):
                pass

        control = Control()
        assertions = TemplateCacheAssertions(Fixture(), control, None)  # type: ignore[arg-type]
        assertions.require_smoke_sized_gc("p")
        assertions._planted_since["smoke-lru-0123abcd"] = "2026-01-01T00:00:00Z"
        assertions._planted_bytes["smoke-lru-0123abcd"] = 1
        assertions._planted_digests["smoke-lru-0123abcd"] = "sha256:x"
        assertions.wait_until_evicted("p", "smoke-lru-0123abcd")
        self.assertEqual(control.gets, 0)
        self.assertGreaterEqual(control.dir_polls, 3)
        assertions.expect_reclaimed("p", "smoke-lru-0123abcd")
        self.assertEqual(control.gets, 1)


if __name__ == "__main__":
    unittest.main()

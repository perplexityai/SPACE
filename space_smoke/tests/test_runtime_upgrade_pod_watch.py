"""The runtime-upgrade scenario's pod watch: cleanup stops it, and a watch
that failed is never taken as a complete pod history."""

from __future__ import annotations

import unittest
from typing import Any, cast

from space_smoke.runtime import InvariantViolation, ResourceScope
from space_smoke.runtime_upgrade import RuntimeUpgradeAssertions


class _Watch:
    """The `SandboxPodWatch` surface the assertions use."""

    def __init__(self, failure: str | None = None) -> None:
        self.stopped = False
        self._failure = failure

    def listed_uids(self) -> frozenset[str]:
        return frozenset({"listed"})

    def versions(self) -> list[tuple[object, dict[str, Any]]]:
        return [
            (None, {"metadata": {"uid": "listed"}}),
            (None, {"metadata": {"uid": "created"}}),
        ]

    def failure(self) -> str | None:
        return self._failure

    def stop(self) -> None:
        self.stopped = True


class _Control:
    def __init__(self, watch: _Watch) -> None:
        self._watch = watch

    def watch_sandbox_pods(self, _sandbox_id: str) -> _Watch:
        return self._watch


class _Fixture:
    def verify(self, condition: bool, message: str, **_evidence: object) -> None:
        if not condition:
            raise InvariantViolation(message)


def _assertions(watch: _Watch, scope: ResourceScope) -> RuntimeUpgradeAssertions:
    return RuntimeUpgradeAssertions(
        cast(Any, _Fixture()), cast(Any, _Control(watch)), scope
    )


class RuntimeUpgradePodWatchTests(unittest.TestCase):
    def test_cleanup_stops_the_watch(self) -> None:
        watch = _Watch()
        scope = ResourceScope()
        _assertions(watch, scope).watch("sandbox")

        self.assertEqual(scope.close(), ())
        self.assertTrue(watch.stopped)

    def test_only_pods_created_after_the_listing_are_reported(self) -> None:
        assertions = _assertions(_Watch(), ResourceScope())
        assertions.watch("sandbox")

        uids = [pod["metadata"]["uid"] for pod in assertions.created_pods()]

        self.assertEqual(uids, ["created"])

    def test_a_failed_watch_is_not_a_pod_history(self) -> None:
        assertions = _assertions(_Watch(failure="kubectl exited"), ResourceScope())
        assertions.watch("sandbox")

        with self.assertRaises(InvariantViolation):
            assertions.created_pods()


if __name__ == "__main__":
    unittest.main()

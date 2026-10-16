"""The data plane's scale-in ordering, observed on a real cluster.

The cluster controller probes each Ready data-plane pod's internal stream-load
listener, buckets its attached streams (0 / 10 / 100 / 1000), and
writes the bucket as `controller.kubernetes.io/pod-deletion-cost`,
so a ReplicaSet scale-down removes idle replicas first. Unit tests cover the
bucketing, the write decision, and the rate limit against a fake backend;
what only a cluster shows is the whole chain: the data plane's internal
listener answering on the pod IP, the controller's reflector seeing the pod,
its RBAC allowing the patch, and the annotation following a real stream.

The writer ticks every 10 s. A raise lands on the next tick; a lowering waits
until 30 s after the pod's last write, so it lands within about 40 s. The
scenario needs the bench's data plane otherwise idle: earlier scenarios clean
up their sandboxes, and a concurrent smoke run on the same bench would make it
fail. When a settle fails, the evidence includes each pod's own attached-stream
count, so a leftover stream reads as that rather than as a writer fault.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import TYPE_CHECKING

from space_smoke.capabilities import DataPlaneDeletionCostObserver

if TYPE_CHECKING:
    from space_smoke.fixtures import KindEcw

IDLE = "0"
ONE_STREAM = "10"
# One tick (10 s) plus one per-pod write interval (30 s), plus slack.
SETTLE_TIMEOUT = 60.0
POLL_SECONDS = 2.0


def is_idle(value: str | None) -> bool:
    """An absent annotation already ranks as cost 0 to the ReplicaSet controller."""
    return value is None or value == IDLE


class DataPlaneDeletionCostAssertions:
    """Read the annotation the cluster controller writes on data-plane pods."""

    def __init__(self, fixture: KindEcw, observer: DataPlaneDeletionCostObserver) -> None:
        self._fixture = fixture
        self._observer = observer

    def _ready_costs(self) -> dict[str, str | None]:
        costs = self._observer.data_plane_deletion_costs()
        self._fixture.verify(bool(costs), "no Ready data-plane pod to observe", data_plane_costs=costs)
        return costs

    def _await(
        self,
        settled: Callable[[dict[str, str | None]], bool],
        *,
        failure: str,
        timeout: float,
    ) -> tuple[dict[str, str | None], float]:
        """Poll until `settled` holds; returns the costs it held on and the wait."""
        started = time.monotonic()
        while True:
            costs = self._ready_costs()
            if settled(costs):
                return costs, round(time.monotonic() - started, 1)
            if time.monotonic() - started >= timeout:
                # Read only on failure: says whether the pod really holds
                # streams (left by an earlier scenario) or the writer lagged.
                self._fixture.verify(
                    False,
                    failure,
                    data_plane_costs=costs,
                    data_plane_active_streams=self._observer.data_plane_stream_loads(),
                    timeout_seconds=timeout,
                )
            self._fixture.wait(POLL_SECONDS, reason="await the deletion-cost writer")

    def expect_all_idle(self, *, label: str, timeout: float = SETTLE_TIMEOUT) -> None:
        """Every Ready data-plane pod reads cost 0 (or no annotation) within `timeout`."""
        costs, elapsed = self._await(
            lambda costs: all(is_idle(value) for value in costs.values()),
            failure="data-plane deletion cost did not settle to 0 with no live stream",
            timeout=timeout,
        )
        self._fixture.record(f"{label}_costs", costs)
        self._fixture.record(f"{label}_seconds", elapsed)

    def expect_one_stream(self, *, timeout: float = SETTLE_TIMEOUT) -> None:
        """The pod carrying the open stream reads cost 10; every other Ready pod stays idle."""

        def busy(costs: dict[str, str | None]) -> list[str]:
            return [name for name, value in costs.items() if value == ONE_STREAM]

        def one_busy_rest_idle(costs: dict[str, str | None]) -> bool:
            loaded = busy(costs)
            return len(loaded) == 1 and all(
                is_idle(value) for name, value in costs.items() if name not in loaded
            )

        costs, elapsed = self._await(
            one_busy_rest_idle,
            failure="the data-plane pod holding one live stream did not read deletion cost 10",
            timeout=timeout,
        )
        self._fixture.record("open_stream_pod", busy(costs)[0])
        self._fixture.record("open_stream_costs", costs)
        self._fixture.record("open_stream_seconds", elapsed)

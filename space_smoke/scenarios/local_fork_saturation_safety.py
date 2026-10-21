"""Bounded fork bursts preserve an existing sandbox's health and disk data."""

from concurrent.futures import ThreadPoolExecutor
from threading import Event
import time

import space_smoke as smoke
from space_smoke.capabilities import EcwForkResult

PATH = "/tmp/space-smoke-fork-pressure.bin"
CONTENT = bytes(range(256)) * 16
WAVES = (1, 2, 4, 8)
PROBE_BUDGET_SECONDS = 10


@smoke.scenario(
    "local.fork_saturation_safety",
    sandbox=smoke.SandboxConfig(timeout=900),
    ignored="Explicit bounded fork-pressure run on a dedicated Kind bench",
)
def fork_saturation_safety(test: smoke.KindEcw) -> None:
    """Fork waves have distinct children with intact independent disks while a
    colocated, existing sandbox keeps answering commands and file reads. The
    source remains suspended, and each wave is deleted before the next one.
    This is a bounded load check, not a maximum-capacity or latency benchmark.
    """

    with test.phase("prepare a durable source and an existing health sentinel"):
        test.sandbox.files.write(PATH, CONTENT)
        sentinel = test.sandboxes.create()
        sentinel.files.write(PATH, CONTENT)
        sentinel_node = test.cross_node_restore.current_placement(sentinel).node_name
        test.pause()
        test.cross_node_restore.wait_until_restore_portable()

    stop = Event()
    unhealthy = Event()
    probe_times: list[float] = []
    waves: list[dict[str, object]] = []
    colocated = False

    def probe() -> None:
        while not stop.is_set():
            started = time.monotonic()
            try:
                result = sentinel.run("printf sentinel-ready", timeout=5)
                if result.exit_code != 0 or result.stdout != "sentinel-ready":
                    raise AssertionError("existing sandbox command stopped working")
                if sentinel.files.read_bytes(PATH, request_timeout=5) != CONTENT:
                    raise AssertionError("existing sandbox disk data changed")
                elapsed = time.monotonic() - started
                if elapsed > PROBE_BUDGET_SECONDS:
                    raise AssertionError("existing sandbox health probe exceeded its budget")
                probe_times.append(elapsed)
            except Exception:
                unhealthy.set()
                raise
            stop.wait(0.25)

    def fork(_index: int) -> tuple[EcwForkResult, float]:
        if unhealthy.is_set():
            raise AssertionError("stopped fork submissions after a failed health probe")
        started = time.monotonic()
        # The collection pre-arms cleanup and recovers ambiguous replies by
        # replaying the original idempotency key, not by creating a new fork.
        result = test.sandboxes.fork(test.sandbox)
        return result, time.monotonic() - started

    with ThreadPoolExecutor(max_workers=1) as health:
        health_check = health.submit(probe)
        try:
            for count in WAVES:
                test.verify(not unhealthy.is_set(), "health sentinel failed before the next wave")
                with test.phase(f"fork and verify a bounded wave of {count}"):
                    started = time.monotonic()
                    with ThreadPoolExecutor(max_workers=count) as callers:
                        results = list(callers.map(fork, range(count)))
                    children = [result.sandbox for result, _elapsed in results]
                    test.verify(
                        len({child.sandbox_id for child in children}) == count,
                        "different-key forks returned duplicate child identities",
                    )
                    for index, child in enumerate(children):
                        test.verify(child.files.read_bytes(PATH) == CONTENT, "fork lost source bytes")
                        child.files.write(PATH, f"child-{index}")
                        node = test.cross_node_restore.current_placement(child).node_name
                        colocated |= node == sentinel_node
                    for index, child in enumerate(children):
                        test.verify(
                            child.files.read(PATH) == f"child-{index}",
                            "fork siblings shared writable disk state",
                        )
                    test.verify(
                        test.sandbox.get_info().state == smoke.SandboxState.PAUSED,
                        "fork pressure resumed the suspended source",
                    )
                    test.verify(not unhealthy.is_set(), "health sentinel failed during fork pressure")
                    waves.append({
                        "callers": count,
                        "elapsed_seconds": time.monotonic() - started,
                        "fork_seconds": [elapsed for _result, elapsed in results],
                    })
                with test.phase("delete this wave before submitting more work"):
                    for child in children:
                        test.delete_sandbox(child)
                        test.wait_until_deleted(child)
        finally:
            stop.set()
            try:
                health_check.result()
            finally:
                test.record("fork_pressure_waves", waves)
                test.record("sentinel_probe_seconds", probe_times)
                test.record("sentinel_node", sentinel_node)
                test.record("fork_colocated_with_sentinel", colocated)

    test.verify(bool(probe_times), "no existing-sandbox health probe completed")
    test.verify(colocated, "no fork shared the sentinel's worker; node safety was not exercised")
    with test.phase("restore the source and verify its disk was unchanged"):
        test.reconnect_by_id()
        test.verify(test.sandbox.files.read_bytes(PATH) == CONTENT, "fork pressure changed the source")

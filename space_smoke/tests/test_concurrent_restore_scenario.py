"""Contracts of the concurrent-restore scenario the bench cannot exercise.

The live run proves convergence under contention and would fail on any
broken parser. These pin only what a passing run cannot exercise: the raced
request's failure classification (an HTTP error keeps its status and body, a
transport failure is status 0, never a raise), the refusal of pod deletion
for anything that is not a sandbox execution pod, and the background
Running-pod watch seeing a pod that ran only while the callers were still
draining, or raising rather than reading a listing failure as "no pod ran".
"""

from __future__ import annotations

import json
import unittest
from unittest import mock

import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.request import Request

from space_smoke.adapters import KindConfig, KindHarness, _race_request
from space_smoke.capabilities import SandboxPodView
from space_smoke.concurrent_restore import RunningPodWatch
from space_smoke.runtime import CommandResult


class _ScriptedKubectl:  # replays scripted kubectl results in call order
    def __init__(self, results) -> None:
        self.results = list(results)
        self.calls: list[tuple[str, ...]] = []

    def run(self, command, **_options) -> CommandResult:
        self.calls.append(tuple(command))
        return self.results.pop(0)





class RacedRequestClassification(unittest.TestCase):
    def test_http_error_keeps_status_and_body_and_transport_failure_is_zero(self) -> None:
        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802 - http.server API
                code = int(self.path.rsplit("/", 1)[-1])
                body = b'{"code":%d,"message":"x"}' % code
                self.send_response(code)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *_args) -> None:
                return

        server = HTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            base = f"http://127.0.0.1:{server.server_port}"
            ok = _race_request(Request(f"{base}/s/200", method="POST"), timeout=5, gate=None)
            refused = _race_request(Request(f"{base}/s/500", method="POST"), timeout=5, gate=None)
            conflict = _race_request(Request(f"{base}/s/409", method="POST"), timeout=5, gate=None)
        finally:
            server.shutdown()
        self.assertEqual((ok.status, ok.detail), (200, ""))
        self.assertEqual(refused.status, 500)
        self.assertIn('"code":500', refused.detail)
        self.assertEqual(conflict.status, 409)
        self.assertGreater(ok.finished_at, 0)
        # Nothing listens here: a transport failure is status 0, not a raise.
        dead = _race_request(Request("http://127.0.0.1:9/never", method="POST"), timeout=2, gate=None)
        self.assertEqual(dead.status, 0)
        self.assertTrue(dead.detail)



class PodDeletionGuard(unittest.TestCase):
    def test_refuses_non_sandbox_pods(self) -> None:
        harness = KindHarness(_ScriptedKubectl([]), KindConfig())
        with self.assertRaises(ValueError):
            harness.delete_sandbox_pod("space-api-7c9d8-abcde")
        with self.assertRaises(ValueError):
            harness.delete_sandbox_pod("space-sandbox-manager-abcde")




def _ok(stdout: str) -> CommandResult:
    return CommandResult(command=("kubectl",), returncode=0, stdout=stdout, stderr="")


def _api_pods(*names: str, terminating: tuple[str, ...] = ()) -> CommandResult:
    items = [
        {"metadata": {"name": name, **({"deletionTimestamp": "t"} if name in terminating else {})}}
        for name in names
    ]
    return _ok(json.dumps({"items": items}))


class PublicApiScaleDownDrain(unittest.TestCase):
    """`rollout status` returns on a scale-down while surplus pods still exit.

    A localhost forward pinned to one of them drops when it does, so the
    scale-back compensation must not return until only the desired pods remain.
    """

    _DEPLOYMENT = json.dumps(
        {"spec": {"replicas": 1, "selector": {"matchLabels": {"app.kubernetes.io/name": "space-api"}}}}
    )

    def test_waits_until_terminating_pods_are_gone(self) -> None:
        kubectl = _ScriptedKubectl(
            [
                _ok("space-api"),
                _ok(self._DEPLOYMENT),
                _api_pods("a", "b", "c", terminating=("b", "c")),
                _api_pods("a", "b", terminating=("b",)),
                _api_pods("a"),
            ]
        )
        harness = KindHarness(kubectl, KindConfig())
        with mock.patch("space_smoke.adapters.time.sleep") as sleep:
            harness.wait_public_api_pods_drained(timeout=60)
        self.assertEqual(sleep.call_count, 2)
        self.assertEqual(kubectl.results, [])
        self.assertIn("app.kubernetes.io/name=space-api", kubectl.calls[-1])

    def test_raises_when_a_pod_is_still_terminating_at_the_deadline(self) -> None:
        kubectl = _ScriptedKubectl(
            [_ok("space-api"), _ok(self._DEPLOYMENT), _api_pods("a", "b", terminating=("b",))]
        )
        harness = KindHarness(kubectl, KindConfig())
        with self.assertRaisesRegex(RuntimeError, "did not drain"):
            harness.wait_public_api_pods_drained(timeout=0)


def _pod(name: str, phase: str) -> SandboxPodView:
    return SandboxPodView(
        pod_name=name,
        phase=phase,
        node_name="n",
        execution_generation="2",
        checkpoint="",
        restore_source_node="",
        restore_storage_uri="",
        deleting=False,
    )


class RunningPodWatchSpan(unittest.TestCase):
    def test_keeps_a_pod_that_ran_only_before_stop_was_requested(self) -> None:
        # The pod is Running on the first poll and gone afterwards — the shape
        # a post-join poll would miss. The watch must still report it.
        listings = iter([[_pod("a", "Running")], [_pod("a", "Terminating")], []])
        polls = threading.Event()

        def pods() -> list[SandboxPodView]:
            polls.set()
            return next(listings, [])

        watch = RunningPodWatch(pods, interval=0.01)
        watch.start()
        self.assertTrue(polls.wait(timeout=5))
        seen = watch.stop_after(0.05)
        self.assertEqual([p.pod_name for p in seen], ["a"])


    def test_listing_failure_is_raised_not_read_as_no_pod_ran(self) -> None:
        def pods() -> list[SandboxPodView]:
            raise RuntimeError("kubectl exploded")

        watch = RunningPodWatch(pods, interval=0.01)
        watch.start()
        with self.assertRaisesRegex(RuntimeError, "kubectl exploded"):
            watch.stop_after(0.05)



if __name__ == "__main__":
    unittest.main()

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

    A pooled connection to one of them is reset when it does, so the
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
                _ok(self._DEPLOYMENT),
                _api_pods("a", "b", terminating=("b",)),
                _ok(self._DEPLOYMENT),
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


class _AutoscalerKubectl:
    """A public API Deployment, optionally autoscaled, answering by subcommand."""

    def __init__(self, *, autoscaled: bool) -> None:
        self.calls: list[tuple[str, ...]] = []
        self.hpa = {
            "metadata": {"name": "space-api", "uid": "hpa-uid"},
            "spec": {
                "scaleTargetRef": {"kind": "Deployment", "name": "space-api"},
                "minReplicas": 2,
                "maxReplicas": 3,
            },
        }
        self.autoscaled = autoscaled

    def run(self, command, **_options) -> CommandResult:
        argv = tuple(command)
        self.calls.append(argv)
        verb, rest = argv[5], argv[6:]
        if verb == "get" and rest[:1] == ("namespace",):
            return _ok("uid")
        if verb == "get" and rest[:2] == ("deploy", "-l"):
            return _ok("space-api")
        if verb == "get" and rest[:1] == ("deploy",):
            return _ok("2")
        if verb == "get" and rest == ("hpa", "-o", "json"):
            return _ok(json.dumps({"items": [self.hpa] if self.autoscaled else []}))
        if verb == "get" and rest[:2] == ("hpa", "space-api"):
            return _ok(json.dumps(self.hpa))
        if verb == "patch" and rest[:2] == ("hpa", "space-api"):
            for op in json.loads(rest[-1]):
                if op["op"] == "replace":
                    self.hpa["spec"][op["path"].rsplit("/", 1)[-1]] = op["value"]
            return _ok("")
        return _ok("")


class PublicApiAutoscaler(unittest.TestCase):
    """An autoscaler reconciles a manual scale away, so its bounds carry the count."""

    def _harness(self, kubectl: _AutoscalerKubectl) -> KindHarness:
        harness = KindHarness(kubectl, KindConfig())
        harness._require_isolated_kind = lambda _action: None  # type: ignore[method-assign]
        return harness

    def test_an_autoscaled_api_is_pinned_and_its_bounds_restored(self) -> None:
        kubectl = _AutoscalerKubectl(autoscaled=True)
        harness = self._harness(kubectl)
        self.assertEqual(harness.scale_public_api(3), 2)
        self.assertEqual((kubectl.hpa["spec"]["minReplicas"], kubectl.hpa["spec"]["maxReplicas"]), (3, 3))
        # The scale-back pins the standing count; the original bounds survive it.
        harness.scale_public_api(2)
        self.assertEqual((kubectl.hpa["spec"]["minReplicas"], kubectl.hpa["spec"]["maxReplicas"]), (2, 2))
        harness.restore_public_api_autoscaler()
        self.assertEqual((kubectl.hpa["spec"]["minReplicas"], kubectl.hpa["spec"]["maxReplicas"]), (2, 3))
        harness.restore_public_api_autoscaler()  # idempotent
        self.assertTrue(any(call[5:7] == ("scale", "deploy/space-api") for call in kubectl.calls))

    def test_a_replaced_autoscaler_is_not_overwritten(self) -> None:
        kubectl = _AutoscalerKubectl(autoscaled=True)
        harness = self._harness(kubectl)
        harness.scale_public_api(3)
        kubectl.hpa["metadata"]["uid"] = "another-uid"
        from space_smoke.runtime import SmokeConfigurationError

        with self.assertRaisesRegex(SmokeConfigurationError, "replaced autoscaler"):
            harness.restore_public_api_autoscaler()

    def test_without_an_autoscaler_only_the_deployment_is_scaled(self) -> None:
        kubectl = _AutoscalerKubectl(autoscaled=False)
        harness = self._harness(kubectl)
        harness.scale_public_api(3)
        harness.restore_public_api_autoscaler()
        self.assertFalse(any(call[5] == "patch" for call in kubectl.calls))


class RolloutDrain(unittest.TestCase):
    """A rolling update's `rollout status` returns while the replaced pods still exit."""

    _DEPLOYMENT = json.dumps(
        {"spec": {"replicas": 2, "selector": {"matchLabels": {"app.kubernetes.io/name": "space-api"}}}}
    )

    def test_an_env_rollout_waits_for_the_replaced_pods_to_exit(self) -> None:
        kubectl = _ScriptedKubectl(
            [
                _ok("rolled out"),
                _ok(self._DEPLOYMENT),
                _api_pods("new-a", "new-b", "old-a", terminating=("old-a",)),
                _ok(self._DEPLOYMENT),
                _api_pods("new-a", "new-b"),
            ]
        )
        harness = KindHarness(kubectl, KindConfig())
        with mock.patch("space_smoke.adapters.time.sleep") as sleep:
            harness._await_deployment_rollout("space-api", timeout=60)
        self.assertEqual(sleep.call_count, 1)
        self.assertEqual(kubectl.results, [])

    def test_the_desired_count_is_reread_while_an_autoscaler_moves_it(self) -> None:
        three = json.dumps(
            {"spec": {"replicas": 3, "selector": {"matchLabels": {"app.kubernetes.io/name": "space-api"}}}}
        )
        kubectl = _ScriptedKubectl(
            [
                _ok(self._DEPLOYMENT),
                _api_pods("a", "b", "c"),
                _ok(three),
                _api_pods("a", "b", "c"),
            ]
        )
        harness = KindHarness(kubectl, KindConfig())
        with mock.patch("space_smoke.adapters.time.sleep"):
            harness._wait_deployment_pods_drained("space-api", timeout=60)
        self.assertEqual(kubectl.results, [])

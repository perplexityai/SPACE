"""Off-bench checks for the controls the data-plane drain scenario uses.

`begin_restart` deletes without waiting, `await_replacements` returns the
old pods still terminating once the replacements are ready, `await_exit`
bounds their exit, and `data_plane_shutdown_grace` reads the deadline the
Deployment runs with. `events` hands each event to `on_event` while the
stream is still open, which is what lets the scenario act mid-stream.
"""

from __future__ import annotations

import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

from space_smoke.native import NativeConfig
from space_smoke.native_hardening import NativeEvent, NativeHardeningFixture
from space_smoke.native_recovery import DeploymentCapture, NativeRecoveryControl
from space_smoke.runtime import CommandResult, ResourceScope, ScenarioContext, SmokeConfigurationError

SELECTOR = "app=space-data-plane"


def _pod(uid: str, *, ready: bool = True, terminating: bool = False) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "name": f"pod-{uid}",
        "uid": uid,
        "ownerReferences": [{"uid": "rs", "controller": True}],
    }
    if terminating:
        metadata["deletionTimestamp"] = "2026-10-17T00:00:00Z"
    return {
        "metadata": metadata,
        "status": {
            "phase": "Running",
            "conditions": [{"type": "Ready", "status": "True" if ready else "False"}],
        },
    }


class _Kubectl:
    """Pods, one ReplicaSet and the data-plane Deployment, advanced per poll."""

    def __init__(self, pods: list[dict[str, Any]], *, args: list[str] | None = None) -> None:
        self.pods = pods
        self.deleted: list[str] = []
        self.args = args if args is not None else ["--listen", "0.0.0.0:8090", "--shutdown-grace-secs", "150"]
        # Each pod read applies the next step, so a wait sees the cluster move.
        self.steps: list[list[dict[str, Any]]] = []

    def kubectl(self, *args: str, timeout: float | None = None, **_options: Any) -> CommandResult:
        if args[:2] == ("get", "replicasets"):
            return self._ok({"items": [{
                "metadata": {"uid": "rs", "ownerReferences": [{"uid": "deploy", "controller": True}]},
            }]})
        if args[:2] == ("get", "pods"):
            if self.steps:
                self.pods = self.steps.pop(0)
            return self._ok({"items": self.pods})
        if args[:2] == ("delete", "pod"):
            self.deleted.append(args[2])
            return CommandResult(args, 0, "", "")
        if args[:2] == ("get", "deployment"):
            return self._ok({
                "metadata": {"uid": "deploy"},
                "spec": {"template": {"spec": {"containers": [{"name": "data-plane", "args": self.args}]}}},
            })
        raise AssertionError(f"unexpected kubectl call {args}")

    @staticmethod
    def _ok(value: object) -> CommandResult:
        return CommandResult(("kubectl",), 0, json.dumps(value), "")


class _Context:
    def __init__(self) -> None:
        self.records: dict[str, object] = {}

    def record(self, name: str, value: object) -> None:
        self.records[name] = value


def _control(kubectl: _Kubectl, *, replicas: int = 1) -> NativeRecoveryControl:
    control = NativeRecoveryControl.__new__(NativeRecoveryControl)
    control.context = _Context()  # type: ignore[assignment]
    control.harness = kubectl  # type: ignore[assignment]
    control._captures = {"space-data-plane": DeploymentCapture("deploy", replicas, SELECTOR, "digest")}
    control._autoscalers = {}
    control._release = lambda: None
    control._fence = lambda: None  # type: ignore[method-assign]
    control.sleeps = []  # type: ignore[attr-defined]
    control._sleep = control.sleeps.append  # type: ignore[attr-defined, method-assign]
    return control


class RestartUnderAnOpenStreamTests(unittest.TestCase):
    def test_begin_restart_deletes_every_pod_without_waiting(self) -> None:
        kubectl = _Kubectl([_pod("a"), _pod("b")])
        control = _control(kubectl, replicas=2)

        self.assertEqual(control.begin_restart("space-data-plane"), {"a", "b"})

        self.assertEqual(kubectl.deleted, ["pod-a", "pod-b"])
        self.assertEqual(control.sleeps, [])  # type: ignore[attr-defined]

    def test_await_replacements_returns_the_old_pods_still_draining(self) -> None:
        kubectl = _Kubectl([_pod("old", terminating=True)])
        kubectl.steps = [
            # The replacement exists but isn't ready yet.
            [_pod("old", terminating=True), _pod("new", ready=False)],
            [_pod("old", terminating=True), _pod("new")],
        ]
        control = _control(kubectl)

        self.assertEqual(control.await_replacements("space-data-plane", {"old"}), {"old"})
        self.assertEqual(control.sleeps, [0.5])  # type: ignore[attr-defined]

    def test_await_replacements_reports_old_pods_already_gone(self) -> None:
        control = _control(_Kubectl([_pod("new")]))

        self.assertEqual(control.await_replacements("space-data-plane", {"old"}), set())

    def test_await_exit_waits_for_the_old_pods_and_bounds_the_wait(self) -> None:
        kubectl = _Kubectl([_pod("old", terminating=True), _pod("new")])
        kubectl.steps = [[_pod("old", terminating=True), _pod("new")], [_pod("new")]]
        control = _control(kubectl)

        control.await_exit("space-data-plane", {"old"}, timeout=60)
        self.assertEqual(control.remaining("space-data-plane", {"old"}), set())

        stuck = _control(_Kubectl([_pod("old", terminating=True)]))
        with self.assertRaises(TimeoutError):
            stuck.await_exit("space-data-plane", {"old"}, timeout=0)

    def test_the_drain_deadline_comes_from_the_deployment(self) -> None:
        self.assertEqual(_control(_Kubectl([])).data_plane_shutdown_grace(), 150.0)

        with self.assertRaises(SmokeConfigurationError):
            _control(_Kubectl([], args=["--listen", "0.0.0.0:8090"])).data_plane_shutdown_grace()


class EventCallbackTests(unittest.TestCase):
    def test_on_event_sees_each_event_while_the_stream_is_open(self) -> None:
        first_seen = threading.Event()

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802 - http.server naming
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.end_headers()
                self._frame("1", {"type": "stdout", "data_base64": "YmVndW4K"})
                # The rest of the stream waits on the reader having seen the
                # first event, which it can only report through `on_event`.
                if not first_seen.wait(5):
                    return
                self._frame("2", {"type": "process_exited", "exit_code": 0})

            def _frame(self, event_id: str, data: dict[str, object]) -> None:
                self.wfile.write(f"id: {event_id}\ndata: {json.dumps(data)}\n\n".encode())
                self.wfile.flush()

            def log_message(self, *_args: object) -> None:
                pass

        server = HTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        base = f"http://127.0.0.1:{server.server_address[1]}"
        fixture = NativeHardeningFixture(
            ScenarioContext(ResourceScope(), {}),
            NativeConfig(api_url=base, data_plane_url=base, token="token", template="seed"),
            "native_recovery.test",
        )
        seen: list[NativeEvent] = []

        def on_event(event: NativeEvent) -> None:
            seen.append(event)
            first_seen.set()

        events = fixture.events(
            {"attach": {"events_url": "/v1/sessions/s/events", "token": "attach"}},
            timeout=10,
            on_event=on_event,
        )

        self.assertEqual(events.status, 200)
        self.assertEqual(list(events.events), seen)
        self.assertEqual([event.data["type"] for event in seen], ["stdout", "process_exited"])


if __name__ == "__main__":
    unittest.main()

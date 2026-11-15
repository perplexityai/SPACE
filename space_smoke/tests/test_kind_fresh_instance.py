"""`start_kind_worker(..., fresh_node_state=True)` as the bench can't drive it.

A passing bench shows one fresh boot. It cannot show that the marker goes
only into a stopped container and only after the providerID's rewrite, that a
failed copy or readiness still leads to the boot's check, that a container
left stopped is reported as no boot checked, that a deadline in the check
neither skips its backstop nor drops what failed before it, or that the check
tells a node image without the unit from a unit that failed and removes a
marker the boot left.
"""

from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from space_smoke.adapters import (
    KIND_FRESH_INSTANCE_DONE,
    KIND_FRESH_INSTANCE_MARKER,
    KIND_FRESH_INSTANCE_UNIT,
    KIND_KUBELET_FLAGS,
    KindConfig,
    KindHarness,
    _leftover_marker_script,
)
from space_smoke.runtime import CommandResult, ScenarioTimedOut

CONTAINER = "space-local-worker2"
LABELS = {"space.pplx.net/role": "worker"}
PROVIDER_ID = "aws:///kind-a/i-0123456789abcdef0"
FLAGS = (
    'KUBELET_KUBEADM_ARGS="--container-runtime-endpoint=unix:///run/containerd/containerd.sock '
    '--provider-id=kind://docker/space-local/space-local-worker2"\n'
)
NOT_RUNNING = (
    f"Kind worker {CONTAINER} is not running, so no boot was checked; a marker copied into it "
    "stays and arms its next start"
)


class _Docker:
    """A Kind worker container as `docker` sees it; records every command."""

    def __init__(
        self,
        *,
        running: bool = False,
        copy_fails: bool = False,
        done: bool = True,
        unit: str = "LoadState=loaded\nActiveState=active\nResult=success\nConditionResult=yes\n",
        journal: str = "",
        marker_left: bool = False,
        flags: str = FLAGS,
        flags_copy_in_fails: bool = False,
        start_fails: bool = False,
        unit_read_raises: BaseException | None = None,
    ) -> None:
        self.running = running
        self.copy_fails = copy_fails
        self.done = done
        self.unit = unit
        self.journal = journal
        self.marker_left = marker_left
        self.flags = flags
        self.flags_copy_in_fails = flags_copy_in_fails
        self.start_fails = start_fails
        self.unit_read_raises = unit_read_raises
        self.calls: list[tuple[str, ...]] = []

    def run(self, command, **_options) -> CommandResult:
        command = tuple(command)
        self.calls.append(command)

        def answer(returncode: int = 0, stdout: str = "", stderr: str = "") -> CommandResult:
            return CommandResult(command, returncode, stdout, stderr)

        match command:
            case ("docker", "inspect", "-f", "{{.State.Running}}", _):
                return answer(stdout="true\n" if self.running else "false\n")
            case ("docker", "cp", source, local) if source == f"{CONTAINER}:{KIND_KUBELET_FLAGS}":
                Path(local).write_text(self.flags)
                return answer()
            case ("docker", "cp", local, target) if target == f"{CONTAINER}:{KIND_KUBELET_FLAGS}":
                if self.flags_copy_in_fails:
                    return answer(1, stderr="read-only file system")
                self.flags = Path(local).read_text()
                return answer()
            case ("docker", "cp", _, target) if target.endswith(KIND_FRESH_INSTANCE_MARKER):
                return answer(1, stderr="no space left on device") if self.copy_fails else answer()
            case ("docker", "start", _):
                if self.start_fails:
                    return answer(1, stderr="error during container init")
                self.running = True
                return answer(stdout=f"{CONTAINER}\n")
            case ("docker", "exec", _, *_) if not self.running:
                raise AssertionError(f"exec into a stopped container: {command}")
            case ("docker", "exec", _, "test", "-e", path) if path == KIND_FRESH_INSTANCE_DONE:
                return answer(0 if self.done else 1)
            case ("docker", "exec", _, "systemctl", "show", unit, *_) if unit == KIND_FRESH_INSTANCE_UNIT:
                if self.unit_read_raises is not None:
                    raise self.unit_read_raises
                return answer(stdout=self.unit)
            case ("docker", "exec", _, "journalctl", *_):
                return answer(stdout=self.journal)
            case ("docker", "exec", _, "sh", "-c", script) if KIND_FRESH_INSTANCE_MARKER in script:
                return answer(3 if self.marker_left else 0)
        raise AssertionError(f"unexpected command: {command}")

    def copied_marker(self) -> bool:
        return any(
            call[:2] == ("docker", "cp") and call[-1].endswith(KIND_FRESH_INSTANCE_MARKER)
            for call in self.calls
        )

    def ran(self, *prefix: str) -> bool:
        return any(call[: len(prefix)] == prefix for call in self.calls)


def _start(
    docker: _Docker, *, ready: object = None, provider_id: str | None = None
) -> mock.MagicMock:
    """Start the worker fresh through `docker`, under `provider_id` when
    given, readiness answering `ready` (raised, or called) or succeeding;
    returns the readiness stand-in."""

    harness = KindHarness(docker, KindConfig(), environment={})
    with mock.patch.object(harness, "_await_reregistered", side_effect=ready) as awaited:
        harness.start_kind_worker(
            CONTAINER, LABELS, timeout=5, provider_id=provider_id, fresh_node_state=True
        )
    return awaited


def _steps(docker: _Docker) -> list[str]:
    """The copies and the start `docker` saw, in order, by what they did."""

    steps = []
    for call in docker.calls:
        if call[:2] == ("docker", "cp"):
            if call[2] == f"{CONTAINER}:{KIND_KUBELET_FLAGS}":
                steps.append("flags out")
            elif call[3] == f"{CONTAINER}:{KIND_KUBELET_FLAGS}":
                steps.append("flags in")
            else:
                steps.append("marker")
        elif call[:2] == ("docker", "start"):
            steps.append("start")
    return steps


class FreshStart(unittest.TestCase):
    def test_a_stopped_container_gets_the_marker_then_starts_and_its_boot_is_checked(self) -> None:
        docker = _Docker()

        awaited = _start(docker)

        self.assertTrue(docker.copied_marker())
        order = [call[:2] for call in docker.calls if call[1] in ("cp", "start")]
        self.assertEqual(order, [("docker", "cp"), ("docker", "start")], docker.calls)
        awaited.assert_called_once_with(CONTAINER, LABELS, timeout=5)
        self.assertTrue(docker.ran("docker", "exec", CONTAINER, "test", "-e", KIND_FRESH_INSTANCE_DONE))
        self.assertTrue(docker.ran("docker", "exec", CONTAINER, "sh", "-c"))

    def test_a_retry_against_the_running_container_copies_nothing_and_checks_its_boot(self) -> None:
        docker = _Docker(running=True)

        _start(docker)

        self.assertFalse(docker.copied_marker(), docker.calls)
        self.assertTrue(docker.ran("docker", "exec", CONTAINER, "test", "-e", KIND_FRESH_INSTANCE_DONE))

    def test_a_marker_that_did_not_copy_still_starts_and_raises_with_the_boot_check(self) -> None:
        docker = _Docker(
            copy_fails=True,
            done=False,
            unit="LoadState=loaded\nActiveState=inactive\nResult=success\nConditionResult=no\n",
        )

        with self.assertRaises(RuntimeError) as raised:
            _start(docker)

        self.assertTrue(docker.ran("docker", "start", CONTAINER))
        message = str(raised.exception)
        self.assertIn("didn't copy: no space left on device", message)
        self.assertIn(f"left no {KIND_FRESH_INSTANCE_DONE}", message)
        self.assertIn("ConditionResult no", message)

    def test_a_readiness_failure_still_runs_the_boot_check_and_raises_both(self) -> None:
        docker = _Docker(
            done=False,
            unit="LoadState=loaded\nActiveState=failed\nResult=exit-code\nConditionResult=yes\n",
        )

        with self.assertRaises(RuntimeError) as raised:
            _start(docker, ready=RuntimeError("Kind worker did not re-register its Node"))

        self.assertTrue(docker.ran("docker", "exec", CONTAINER, "systemctl", "show"))
        message = str(raised.exception)
        self.assertIn("did not re-register its Node", message)
        self.assertIn("ActiveState failed, Result exit-code", message)

    def test_a_readiness_failure_alone_is_raised_as_it_was(self) -> None:
        docker = _Docker()
        failure = RuntimeError("Kind worker did not re-register its Node")

        with self.assertRaises(RuntimeError) as raised:
            _start(docker, ready=failure)

        self.assertIs(raised.exception, failure)

    def test_a_missing_done_file_names_a_node_image_without_the_unit(self) -> None:
        docker = _Docker(
            done=False,
            unit="LoadState=not-found\nActiveState=inactive\nResult=success\nConditionResult=no\n",
            journal="-- No entries --",
        )

        with self.assertRaises(RuntimeError) as raised:
            _start(docker)

        message = str(raised.exception)
        self.assertIn("LoadState not-found", message)
        self.assertIn("its node image predates the unit: recreate the cluster", message)

    def test_a_missing_done_file_names_a_unit_that_failed_with_its_journal(self) -> None:
        docker = _Docker(
            done=False,
            unit="LoadState=loaded\nActiveState=failed\nResult=exit-code\nConditionResult=yes\n",
            journal="find: cannot delete '/space/run/sandbox-manager': Device or resource busy",
        )

        with self.assertRaises(RuntimeError) as raised:
            _start(docker)

        message = str(raised.exception)
        self.assertIn("ActiveState failed, Result exit-code", message)
        self.assertNotIn("recreate the cluster", message)
        self.assertIn("its journal on this boot ends: find: cannot delete", message)

    def test_a_marker_left_behind_is_removed_and_fails_the_start(self) -> None:
        docker = _Docker(marker_left=True)

        with self.assertRaises(RuntimeError) as raised:
            _start(docker)

        self.assertIn(f"left {KIND_FRESH_INSTANCE_MARKER} behind; removed it", str(raised.exception))


class ProviderIdAndAStoppedContainer(unittest.TestCase):
    def test_the_provider_id_is_rewritten_before_the_marker_goes_in(self) -> None:
        docker = _Docker()

        _start(docker, provider_id=PROVIDER_ID)

        self.assertEqual(_steps(docker), ["flags out", "flags in", "marker", "start"])
        self.assertIn(f"--provider-id={PROVIDER_ID}", docker.flags)
        self.assertNotIn("kind://docker", docker.flags)

    def test_a_rewrite_that_fails_copies_no_marker_and_starts_nothing(self) -> None:
        for case, docker in [
            ("no --provider-id in the flags", _Docker(flags='KUBELET_KUBEADM_ARGS=""\n')),
            ("the flags don't copy back", _Docker(flags_copy_in_fails=True)),
        ]:
            with self.subTest(case), self.assertRaises(RuntimeError) as raised:
                _start(docker, provider_id=PROVIDER_ID)
            self.assertFalse(docker.copied_marker(), (case, docker.calls))
            self.assertFalse(docker.ran("docker", "start"), (case, docker.calls))
            self.assertIn(NOT_RUNNING, str(raised.exception), case)

    def test_a_start_that_fails_after_the_copy_reports_only_that_no_boot_was_checked(self) -> None:
        docker = _Docker(start_fails=True)

        with self.assertRaises(RuntimeError) as raised:
            _start(docker, provider_id=PROVIDER_ID)

        self.assertEqual(_steps(docker), ["flags out", "flags in", "marker", "start"])
        self.assertFalse(docker.ran("docker", "exec"), docker.calls)
        self.assertEqual(
            str(raised.exception),
            f"Kind worker {CONTAINER}: start Kind worker container {CONTAINER} failed (1): "
            f"error during container init; {NOT_RUNNING}",
        )

    def test_a_container_not_running_when_the_check_runs_is_reported_as_that_alone(self) -> None:
        docker = _Docker()

        def stops(*_args, **_kwargs) -> None:
            docker.running = False

        with self.assertRaises(RuntimeError) as raised:
            _start(docker, ready=stops)

        self.assertEqual(str(raised.exception), NOT_RUNNING)
        self.assertFalse(docker.ran("docker", "exec"), docker.calls)

    def test_a_check_that_cant_read_the_container_state_does_not_raise_it(self) -> None:
        docker = _Docker(running=True)
        inspected = 0
        run = docker.run

        def inspect_fails_in_the_check(command, **options) -> CommandResult:
            nonlocal inspected
            if tuple(command)[:2] == ("docker", "inspect"):
                inspected += 1
                if inspected > 1:
                    docker.calls.append(tuple(command))
                    return CommandResult(tuple(command), 1, "", "Cannot connect to the Docker daemon")
            return run(command, **options)

        docker.run = inspect_fails_in_the_check
        with self.assertRaises(RuntimeError) as raised:
            _start(docker)

        self.assertIn(
            "couldn't read whether it is running, so no boot was checked", str(raised.exception)
        )
        self.assertIn("Cannot connect to the Docker daemon", str(raised.exception))


class ADeadlineInTheCheck(unittest.TestCase):
    def test_it_still_runs_the_backstop_and_raises_the_timeout_with_every_failure(self) -> None:
        docker = _Docker(
            done=False,
            marker_left=True,
            unit_read_raises=ScenarioTimedOut("local.scenario timed out after 900s"),
        )

        with self.assertRaises(ScenarioTimedOut) as raised:
            _start(docker, ready=RuntimeError("Kind worker did not re-register its Node"))

        self.assertTrue(docker.ran("docker", "exec", CONTAINER, "sh", "-c"), docker.calls)
        message = str(raised.exception)
        self.assertIn("did not re-register its Node", message)
        self.assertIn("timed out after 900s", message)
        self.assertIn(f"its boot left no {KIND_FRESH_INSTANCE_DONE}", message)
        self.assertIn(f"left {KIND_FRESH_INSTANCE_MARKER} behind; removed it", message)
        self.assertIsInstance(raised.exception.__cause__, ScenarioTimedOut)

    def test_a_deadline_alone_is_raised_as_it_was(self) -> None:
        deadline = ScenarioTimedOut("local.scenario timed out after 900s")
        docker = _Docker()

        with self.assertRaises(ScenarioTimedOut) as raised:
            _start(docker, ready=deadline)

        self.assertIs(raised.exception, deadline)
        self.assertTrue(docker.ran("docker", "exec", CONTAINER, "sh", "-c"), docker.calls)


class LeftoverMarkerScript(unittest.TestCase):
    """The backstop's shell, run for real on a temporary marker."""

    def test_a_marker_there_is_removed_and_exits_3(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            marker = Path(scratch) / "kind-fresh-instance"
            marker.write_text("")

            ran = subprocess.run(["sh", "-c", _leftover_marker_script(marker)], check=False)

            self.assertEqual(ran.returncode, 3)
            self.assertFalse(marker.exists())

    def test_no_marker_exits_0(self) -> None:
        with tempfile.TemporaryDirectory() as scratch:
            marker = Path(scratch) / "kind-fresh-instance"

            ran = subprocess.run(["sh", "-c", _leftover_marker_script(marker)], check=False)

            self.assertEqual(ran.returncode, 0)
            self.assertFalse(marker.exists())


if __name__ == "__main__":
    unittest.main()

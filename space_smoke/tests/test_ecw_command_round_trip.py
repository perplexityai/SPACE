"""Characterization tests for the first framework smoke scenario."""

from __future__ import annotations

import importlib
import io
import json
import socket
import sys
import types
import unittest
from collections.abc import Mapping
from contextlib import nullcontext
from email.message import Message
from typing import BinaryIO, cast
from unittest import mock
from urllib.error import HTTPError

import httpx

import space_smoke as smoke
from space_smoke.adapters import (
    EcwConfig,
    EcwSdk,
    KindConfig,
    KindHarness,
    _SdkProcess,
    _SdkSandbox,
    validate_loopback_environment,
)
from space_smoke.capabilities import (
    ColdCreateSelection,
    EcwCommandResult,
    EcwCreateAttempt,
    EcwCreateNotDelivered,
    EcwDataPlaneNotFound,
    EcwFilesystem,
    EcwFilesystemEvent,
    EcwFilesystemEventType,
    EcwLifecycle,
    EcwPermissionDenied,
    EcwSandboxBusy,
    EcwSandboxDeleteRetryable,
    EcwSandboxes,
    EcwSandboxInfo,
    EcwSandboxState,
    EcwSandboxUnavailable,
    EcwTimeoutAction,
    NetworkConfig,
)
from space_smoke.fixtures import Ecw
from space_smoke.model import Verdict
from space_smoke.runtime import (
    CommandResult,
    ResourceScope,
    ScenarioContext,
    SmokeConfigurationError,
    execute_scenario,
)
from space_smoke.scenarios.ecw_authenticated_access_with_public_traffic_disabled import (
    MARKER as AUTHENTICATED_ACCESS_MARKER,
    authenticated_access_with_public_traffic_disabled,
)
from space_smoke.scenarios.ecw_cold_create_environment import cold_create_environment
from space_smoke.scenarios.ecw_command_round_trip import MARKER, command_round_trip
from space_smoke.scenarios.ecw_refresh_keepalive import (
    REFRESH_AFTER_SECONDS,
    TIMEOUT_SECONDS as REFRESH_TIMEOUT_SECONDS,
    VERIFY_AFTER_REFRESH_SECONDS,
    refresh_keepalive,
)
from space_smoke.scenarios.ecw_sandbox_delete import sandbox_delete
from space_smoke.scenarios.ecw_timeout_update import (
    EXTENDED_TIMEOUT_SECONDS,
    INITIAL_TIMEOUT_SECONDS,
    MARKER as TIMEOUT_MARKER,
    SUSPENDED_TIMEOUT_SECONDS,
    timeout_update,
)
from space_smoke.services import SmokeServices


class _Kind:
    def __init__(self, trace: list[str]) -> None:
        self.trace = trace

    def acquire_cold_create_setup(self):
        self.trace.append("kind.cold-lease")
        return lambda: self.trace.append("kind.cold-release")

    def prepare_cold_create(self, template, config):
        self.trace.append("kind.cold-setup:" + template)

        def verify():
            self.trace.append("kind.cold-verified")

        return ColdCreateSelection(template, "owned-config", "owned-receipt", verify)

    def cold_create_pod(self, sandbox_id: str):
        metadata = {
            "name": "fresh-pod",
            "uid": "pod-uid",
            "labels": {
                "space.pplx.net/sandbox-id": sandbox_id,
                "space.pplx.net/execution-generation": "1",
            },
            "annotations": {
                "space.pplx.net/sandbox-id": sandbox_id,
                "space.pplx.net/execution-generation": "1",
            },
        }
        pod = {"metadata": metadata, "spec": {"runtimeClassName": "space"}}
        executor = mock.Mock()
        executor.run.return_value = CommandResult(
            (), 0, json.dumps({"items": [pod]}), ""
        )
        return KindHarness(executor, KindConfig()).cold_create_pod(sandbox_id)

    def bootstrap(self) -> None:
        self.trace.append("kind.bootstrap")

    def seed_api_key(self, api_key: str) -> None:
        self.trace.append(f"kind.seed:{api_key}")

    def delete_api_key(self, api_key: str) -> None:
        self.trace.append(f"kind.delete-key:{api_key}")

    def reclaim_local_sandbox_everywhere(self, sandbox_id: str) -> None:
        self.trace.append(f"kind.reclaim:{sandbox_id}")


class _Sandbox:
    sandbox_id = "sandbox-1"

    def __init__(
        self,
        trace: list[str],
        result: EcwCommandResult,
        *,
        kill_conflicts: int = 0,
    ) -> None:
        self.trace = trace
        self.result = result
        self.kill_conflicts = kill_conflicts
        self.deleted = False
        self.state = EcwSandboxState.RUNNING
        self._files = _Filesystem()

    @property
    def files(self) -> EcwFilesystem:
        return cast(EcwFilesystem, self._files)

    def run(
        self,
        command: str,
        *,
        timeout: int = 60,
        user: str | None = None,
    ) -> EcwCommandResult:
        del user
        self.trace.append(f"sandbox.run:{command}:{timeout}")
        return self.result

    def kill(self) -> None:
        self.trace.append("sandbox.kill")
        if self.kill_conflicts:
            self.kill_conflicts -= 1
            raise EcwSandboxBusy("409: lifecycle transition in flight")
        self.deleted = True

    def get_info(self) -> EcwSandboxInfo:
        return EcwSandboxInfo(
            self.sandbox_id,
            "seed",
            self.state,
        )

    def try_get_info(self) -> EcwSandboxInfo | None:
        return None if self.deleted else self.get_info()

    def set_timeout(self, timeout: int) -> None:
        self.trace.append(f"sandbox.set-timeout:{timeout}")

    def refresh(self) -> None:
        self.trace.append("sandbox.refresh")

    def pause(self) -> None:
        self.state = EcwSandboxState.PAUSED

    def connect(self, *, timeout: int = 300) -> _Sandbox:
        del timeout
        self.state = EcwSandboxState.RUNNING
        return self


class _Filesystem:
    def write(
        self,
        path: str,
        data: str | bytes | BinaryIO,
        *,
        user: str | None = None,
        request_timeout: float = 60,
    ) -> None:
        del path, data, user, request_timeout

    def read(self, path: str, *, request_timeout: float = 60) -> str:
        del path, request_timeout
        return ""


class _Ecw:
    def __init__(
        self,
        trace: list[str],
        result: EcwCommandResult,
        *,
        fail_create: bool = False,
        create_not_delivered: int = 0,
        kill_conflicts: int = 0,
        create_attempt: EcwCreateAttempt | Exception | None = None,
    ) -> None:
        self.trace = trace
        self.sandbox = _Sandbox(trace, result, kill_conflicts=kill_conflicts)
        self.fail_create = fail_create
        self.create_not_delivered = create_not_delivered
        self.last_metadata: Mapping[str, str] | None = None
        self.metadata_history: list[Mapping[str, str]] = []
        self.create_attempt = create_attempt or EcwCreateAttempt(400, "rejected", None)

    def create(
        self,
        *,
        template: str,
        timeout: int,
        metadata: Mapping[str, str],
        envs: Mapping[str, str] | None = None,
        lifecycle: EcwLifecycle | None = None,
        allow_internet_access: bool | None = None,
        network: NetworkConfig | None = None,
    ) -> _Sandbox:
        self.last_metadata = metadata
        self.last_envs = envs
        self.metadata_history.append(metadata)
        self.trace.append(
            f"ecw.create:{template}:{timeout}:{metadata['space_smoke_run']}"
        )
        if lifecycle is not None:
            self.trace.append(
                f"ecw.lifecycle:{lifecycle.on_timeout.value}:{lifecycle.auto_resume}"
            )
        if allow_internet_access is not None:
            self.trace.append(f"ecw.allow-internet:{allow_internet_access}")
        if network is not None:
            self.trace.append(
                f"ecw.network:allow-public={network.allow_public_traffic}"
            )
        if self.create_not_delivered:
            self.create_not_delivered -= 1
            raise EcwCreateNotDelivered("Temporary failure in name resolution")
        if self.fail_create:
            raise TimeoutError("create response lost")
        return self.sandbox

    def kill_matching(
        self,
        metadata: Mapping[str, str],
        *,
        attempts: int = 10,
        interval_seconds: float = 1,
    ) -> None:
        del attempts, interval_seconds
        self.trace.append(f"ecw.kill-matching:{metadata['space_smoke_run']}")

    def attempt_create(
        self,
        *,
        template: str,
        timeout: int,
        metadata: Mapping[str, str],
        allow_internet_access: bool | None = None,
        network: NetworkConfig | None = None,
    ) -> EcwCreateAttempt:
        del template, timeout, allow_internet_access, network
        self.metadata_history.append(metadata)
        self.trace.append("ecw.attempt-create:configured")
        if isinstance(self.create_attempt, Exception):
            raise self.create_attempt
        return self.create_attempt


def _run(
    command_result: EcwCommandResult,
    *,
    owns_api_key: bool = True,
    fail_create: bool = False,
    create_not_delivered: int = 0,
    kill_conflicts: int = 0,
    registered=command_round_trip,
    template_override=None,
):
    trace: list[str] = []
    services = SmokeServices(
        kind=_Kind(trace),
        ecw=cast(
            EcwSandboxes,
            _Ecw(
                trace,
                command_result,
                fail_create=fail_create,
                create_not_delivered=create_not_delivered,
                kill_conflicts=kill_conflicts,
            ),
        ),
        api_key="run-key",
        owns_api_key=owns_api_key,
        template_override=template_override,
    )
    result = execute_scenario(
        title=registered.title,
        body=lambda context: registered.run(context, services),
    )
    return result, trace


class EcwCommandRoundTripTests(unittest.TestCase):
    def test_authenticated_access_scenario_requires_authenticated_ports(self) -> None:
        result, trace = _run(
            EcwCommandResult(0, AUTHENTICATED_ACCESS_MARKER, ""),
            registered=authenticated_access_with_public_traffic_disabled,
        )

        self.assertEqual(result.verdict, Verdict.PASS)
        self.assertIn("ecw.network:allow-public=False", trace)

    def test_scenario_reads_as_one_public_round_trip(self) -> None:
        with mock.patch("space_smoke.services.uuid.uuid4") as run_id:
            run_id.return_value.hex = "run-marker"
            result, trace = _run(EcwCommandResult(0, MARKER, ""))

        self.assertEqual(result.verdict, Verdict.PASS)
        self.assertEqual(result.evidence["sandbox_id"], "sandbox-1")
        self.assertEqual(result.evidence["command_stdout"], MARKER)
        self.assertEqual(
            trace,
            [
                "kind.bootstrap",
                "kind.seed:run-key",
                "ecw.create:seed:300:run-marker",
                "ecw.lifecycle:pause:False",
                f"sandbox.run:printf %s {MARKER}:60",
                "sandbox.kill",
                "kind.delete-key:run-key",
            ],
        )

    def test_wrong_output_fails_and_still_cleans_up(self) -> None:
        result, trace = _run(EcwCommandResult(0, "wrong", ""))

        self.assertEqual(result.verdict, Verdict.FAIL)
        self.assertEqual(result.exit_code, 1)
        self.assertEqual(result.summary, "sandbox command result did not match")
        self.assertEqual(result.evidence["expected"], (0, MARKER))
        self.assertEqual(result.evidence["actual"], (0, "wrong"))
        self.assertEqual(trace[-2:], ["sandbox.kill", "kind.delete-key:run-key"])
        # Plain `Ecw` scenarios get no worker-row reclaim; only `KindEcw` does.
        self.assertFalse(any(item.startswith("kind.reclaim:") for item in trace))

    def test_cleanup_retries_a_sandbox_lifecycle_conflict(self) -> None:
        with mock.patch("space_smoke.services.time.sleep") as sleep:
            result, trace = _run(
                EcwCommandResult(0, MARKER, ""),
                kill_conflicts=2,
            )

        self.assertEqual(result.verdict, Verdict.PASS)
        self.assertEqual(trace.count("sandbox.kill"), 3)
        self.assertEqual(trace[-1], "kind.delete-key:run-key")
        self.assertEqual(sleep.call_count, 2)

    def test_explicit_delete_retries_conflicts_and_requires_public_absence(
        self,
    ) -> None:
        with mock.patch("space_smoke.services.time.sleep") as sleep:
            result, trace = _run(
                EcwCommandResult(0, "", ""),
                kill_conflicts=2,
                registered=sandbox_delete,
            )

        self.assertEqual(result.verdict, Verdict.PASS)
        self.assertEqual(result.evidence["delete_attempts"], 3)
        self.assertEqual(result.evidence["observed_state"], "deleted")
        self.assertEqual(trace.count("sandbox.kill"), 4)
        self.assertEqual(sleep.call_count, 2)

    def test_scenario_timeout_configures_the_managed_sandbox_creation(self) -> None:
        with (
            mock.patch("space_smoke.services.uuid.uuid4") as run_id,
            mock.patch("space_smoke.runtime.time.sleep"),
        ):
            run_id.return_value.hex = "timeout-run"
            result, trace = _run(
                EcwCommandResult(0, TIMEOUT_MARKER, ""),
                registered=timeout_update,
            )

        self.assertEqual(result.verdict, Verdict.PASS)
        self.assertEqual(
            trace,
            [
                "kind.bootstrap",
                "kind.seed:run-key",
                f"ecw.create:seed:{INITIAL_TIMEOUT_SECONDS}:timeout-run",
                "ecw.lifecycle:pause:False",
                f"sandbox.set-timeout:{EXTENDED_TIMEOUT_SECONDS}",
                f"sandbox.run:printf %s {TIMEOUT_MARKER}:60",
                f"sandbox.set-timeout:{SUSPENDED_TIMEOUT_SECONDS}",
                f"sandbox.run:printf %s {TIMEOUT_MARKER}:60",
                "sandbox.kill",
                "kind.delete-key:run-key",
            ],
        )

    def test_lease_proof_uses_the_declared_sandbox_timeout(self) -> None:
        with (
            mock.patch("space_smoke.services.uuid.uuid4") as run_id,
            mock.patch("space_smoke.runtime.time.sleep") as sleep,
        ):
            run_id.return_value.hex = "refresh-run"
            result, trace = _run(
                EcwCommandResult(0, "space-smoke-idle-deadline-reset-ok", ""),
                registered=refresh_keepalive,
            )

        self.assertEqual(result.verdict, Verdict.PASS)
        self.assertEqual(result.evidence["timeout_seconds"], REFRESH_TIMEOUT_SECONDS)
        self.assertEqual(
            result.evidence["original_deadline_exceeded_by_seconds"],
            7,
        )
        self.assertEqual(
            result.evidence["renewed_deadline_remaining_seconds"],
            13,
        )
        self.assertEqual(
            [call.args[0] for call in sleep.call_args_list],
            [REFRESH_AFTER_SECONDS, VERIFY_AFTER_REFRESH_SECONDS],
        )
        self.assertEqual(
            trace,
            [
                "kind.bootstrap",
                "kind.seed:run-key",
                f"ecw.create:seed:{REFRESH_TIMEOUT_SECONDS}:refresh-run",
                "ecw.lifecycle:pause:False",
                "sandbox.refresh",
                "sandbox.run:printf %s space-smoke-idle-deadline-reset-ok:60",
                "sandbox.kill",
                "kind.delete-key:run-key",
            ],
        )

    def test_cold_environment_detects_unset_empty_and_changed_bytes_then_cleans_up(
        self,
    ) -> None:
        original = "x\0café 雪\0x\0first\nsecond\n\0x\0left=middle=right\0x\0\0"
        for output, verdict in [
            (original, Verdict.PASS),
            (original[:-3] + "\0\0", Verdict.FAIL),
            (original.replace("second\n", "second"), Verdict.FAIL),
            (original.replace("left=middle=right", "left"), Verdict.FAIL),
        ]:
            with self.subTest(verdict=verdict, output_hash=hash(output)):
                result, trace = _run(
                    EcwCommandResult(0, output, ""), registered=cold_create_environment
                )
                self.assertEqual(result.verdict, verdict)
                self.assertEqual(result.evidence["sandbox_id"], "sandbox-1")
                self.assertEqual(
                    sum(item.startswith("sandbox.run:") for item in trace), 1
                )
                self.assertEqual(trace.count("sandbox.kill"), 1)
                # A `KindEcw` teardown reclaims the sandbox's worker rows right
                # after the kill.
                self.assertEqual(
                    trace[trace.index("sandbox.kill") + 1], "kind.reclaim:sandbox-1"
                )
                self.assertNotIn("command_stdout", result.evidence)

    def test_cold_pod_guard_rejects_adoption_restore_and_missing_identity(self) -> None:
        prefix = "space.pplx.net/"
        original = "x\0café 雪\0x\0first\nsecond\n\0x\0left=middle=right\0x\0\0"
        variants = (
            [({}, True)]
            + [
                ({key: "present"}, False)
                for key in (
                    "pool-slot-id",
                    "pool-config-id",
                    "pool-role",
                    "checkpoint",
                    "suspend-restore-storage-uri",
                    "suspend-restore-source-node",
                    "fork-disk-restore",
                )
            ]
            + [({"sandbox-id": "other"}, False), ({"execution-generation": "2"}, False)]
            + [
                ({"case": case}, False)
                for case in (
                    "missing-uid",
                    "missing-identity",
                    "wrong-runtime",
                    "no-pods",
                    "two-pods",
                )
            ]
        )
        for delta, accepted in variants:
            with self.subTest(delta=delta):
                metadata = {
                    "name": "pod",
                    "uid": "uid",
                    "labels": {
                        prefix + "sandbox-id": "sandbox-1",
                        prefix + "execution-generation": "1",
                    },
                    "annotations": {
                        prefix + "sandbox-id": "sandbox-1",
                        prefix + "execution-generation": "1",
                    },
                }
                metadata["annotations"].update(
                    {prefix + k: v for k, v in delta.items()}
                )
                pod = {"metadata": metadata, "spec": {"runtimeClassName": "space"}}
                case = delta.get("case")
                if case == "missing-uid":
                    metadata.pop("uid")
                if case == "missing-identity":
                    metadata["labels"].pop(prefix + "sandbox-id")
                if case == "wrong-runtime":
                    pod["spec"]["runtimeClassName"] = "runc"
                pods = (
                    []
                    if case == "no-pods"
                    else [pod, pod]
                    if case == "two-pods"
                    else [pod]
                )
                executor = mock.Mock()
                executor.run.return_value = CommandResult(
                    (), 0, json.dumps({"items": pods}), ""
                )
                harness = KindHarness(executor, KindConfig())
                with mock.patch.object(
                    _Kind, "cold_create_pod", side_effect=harness.cold_create_pod
                ):
                    result, trace = _run(
                        EcwCommandResult(0, original, ""),
                        registered=cold_create_environment,
                    )
                self.assertEqual(
                    result.verdict, Verdict.PASS if accepted else Verdict.FAIL
                )
                self.assertEqual(trace.count("sandbox.kill"), 1)
                self.assertEqual(
                    sum(x.startswith("sandbox.run:") for x in trace), int(accepted)
                )

    def test_cold_setup_precedes_sdk_and_failure_prevents_create(self) -> None:
        pin = "custom@sha256:" + "b" * 64
        for failure in (None, "setup", "verification"):
            with self.subTest(failure=failure):

                def setup(kind, template, config, failure=failure):
                    self.assertEqual(template, "custom:latest")
                    if failure == "setup":
                        raise SmokeConfigurationError("unknown config ownership")

                    def verify():
                        kind.trace.append("kind.cold-verified")
                        if failure == "verification":
                            raise SmokeConfigurationError(
                                "config enabled before SDK create"
                            )

                    return ColdCreateSelection(
                        pin, "owned-config", "owned-receipt", verify
                    )

                with mock.patch.object(_Kind, "prepare_cold_create", setup):
                    result, trace = _run(
                        EcwCommandResult(
                            0,
                            "x\0café 雪\0x\0first\nsecond\n\0x\0left=middle=right\0x\0\0",
                            "",
                        ),
                        registered=cold_create_environment,
                        template_override="custom:latest",
                    )
                creates = [
                    i for i, item in enumerate(trace) if item.startswith("ecw.create:")
                ]
                if failure:
                    self.assertNotEqual(result.verdict, Verdict.PASS)
                    self.assertEqual(creates, [])
                else:
                    self.assertEqual(result.verdict, Verdict.PASS)
                    self.assertLess(trace.index("kind.cold-verified"), creates[0])
                    self.assertTrue(
                        trace[creates[0]].startswith("ecw.create:" + pin + ":")
                    )
                    self.assertLess(
                        trace.index("sandbox.kill"), trace.index("kind.cold-release")
                    )
                self.assertEqual(trace.count("kind.cold-release"), 1)

    def test_fixture_forwards_empty_and_multiline_environment_to_cold_create(
        self,
    ) -> None:
        @smoke.scenario(
            "ecw.configured_environment",
            sandbox=smoke.SandboxConfig(envs={"EMPTY": "", "TEXT": "café\nleft=right"}),
        )
        def configured_environment(_test: smoke.Ecw) -> None:
            """Cold creation receives empty values and exact UTF-8 without a reconnect."""

        with mock.patch.object(_Ecw, "create", wraps=None) as create:
            create.side_effect = lambda **options: _Sandbox(
                [], EcwCommandResult(0, "", "")
            )
            result, _trace = _run(
                EcwCommandResult(0, "", ""), registered=configured_environment
            )
        self.assertEqual(result.verdict, Verdict.PASS)
        self.assertEqual(
            create.call_args.kwargs["envs"], {"EMPTY": "", "TEXT": "café\nleft=right"}
        )

    def test_scenario_lifecycle_configures_the_fixture_created_sandbox(self) -> None:
        @smoke.scenario(
            "ecw.configured_lifecycle",
            sandbox=smoke.SandboxConfig(
                timeout=3,
                lifecycle=smoke.pause_on_timeout(auto_resume=True),
            ),
        )
        def configured_lifecycle(_test: smoke.Ecw) -> None:
            """Fixture construction receives the declared lifecycle."""

        with mock.patch("space_smoke.services.uuid.uuid4") as run_id:
            run_id.return_value.hex = "lifecycle-run"
            result, trace = _run(
                EcwCommandResult(0, "", ""),
                registered=configured_lifecycle,
            )

        self.assertEqual(result.verdict, Verdict.PASS)
        self.assertEqual(
            trace,
            [
                "kind.bootstrap",
                "kind.seed:run-key",
                "ecw.create:seed:3:lifecycle-run",
                "ecw.lifecycle:pause:True",
                "sandbox.kill",
                "kind.delete-key:run-key",
            ],
        )

    def test_pre_create_dns_failure_retries_without_duplicate_delivery(self) -> None:
        with (
            mock.patch("space_smoke.services.uuid.uuid4") as run_id,
            mock.patch("space_smoke.services.time.sleep") as sleep,
        ):
            run_id.return_value.hex = "dns-retry-run"
            result, trace = _run(
                EcwCommandResult(0, MARKER, ""),
                create_not_delivered=2,
            )

        self.assertEqual(result.verdict, Verdict.PASS)
        self.assertEqual(trace.count("ecw.create:seed:300:dns-retry-run"), 3)
        self.assertEqual(trace.count("sandbox.kill"), 1)
        self.assertNotIn("ecw.kill-matching:dns-retry-run", trace)
        self.assertEqual(sleep.call_count, 2)

    def test_repeated_pre_create_dns_failure_needs_no_visible_sandbox_cleanup(
        self,
    ) -> None:
        with (
            mock.patch("space_smoke.services.uuid.uuid4") as run_id,
            mock.patch("space_smoke.services.time.sleep") as sleep,
        ):
            run_id.return_value.hex = "dns-exhausted-run"
            result, trace = _run(
                EcwCommandResult(0, MARKER, ""),
                create_not_delivered=3,
            )

        self.assertEqual(result.verdict, Verdict.INFRA_ERROR)
        self.assertIn("EcwCreateNotDelivered", result.summary)
        self.assertEqual(result.cleanup_failures, ())
        self.assertEqual(trace.count("ecw.create:seed:300:dns-exhausted-run"), 3)
        self.assertFalse(any(item.startswith("ecw.kill-matching") for item in trace))
        self.assertFalse(any(item == "sandbox.kill" for item in trace))
        self.assertEqual(sleep.call_count, 2)

    def test_ambiguous_create_reconciles_the_prearmed_run_marker(self) -> None:
        with mock.patch("space_smoke.services.uuid.uuid4") as run_id:
            run_id.return_value.hex = "ambiguous-run"
            result, trace = _run(
                EcwCommandResult(0, MARKER, ""),
                fail_create=True,
            )

        self.assertEqual(result.verdict, Verdict.INFRA_ERROR)
        self.assertEqual(
            trace,
            [
                "kind.bootstrap",
                "kind.seed:run-key",
                "ecw.create:seed:300:ambiguous-run",
                "ecw.lifecycle:pause:False",
                "ecw.kill-matching:ambiguous-run",
                "kind.delete-key:run-key",
            ],
        )

    def test_supplied_api_key_is_not_seeded_or_deleted(self) -> None:
        result, trace = _run(
            EcwCommandResult(0, MARKER, ""),
            owns_api_key=False,
        )

        self.assertEqual(result.verdict, Verdict.PASS)
        self.assertFalse(any(item.startswith("kind.seed") for item in trace))
        self.assertFalse(any(item.startswith("kind.delete-key") for item in trace))

    def test_deployed_fixture_skips_kind_and_uses_the_matrix_template(self) -> None:
        trace: list[str] = []
        services = SmokeServices(
            kind=None,
            ecw=cast(
                EcwSandboxes,
                _Ecw(trace, EcwCommandResult(0, MARKER, "")),
            ),
            api_key="deployed-key",
            owns_api_key=False,
            template_override="code-runtime",
        )

        with mock.patch("space_smoke.services.uuid.uuid4") as run_id:
            run_id.return_value.hex = "deployed-run"
            result = execute_scenario(
                title=command_round_trip.title,
                body=lambda context: command_round_trip.run(context, services),
            )

        self.assertEqual(result.verdict, Verdict.PASS)
        self.assertEqual(
            trace,
            [
                "ecw.create:code-runtime:300:deployed-run",
                "ecw.lifecycle:pause:False",
                f"sandbox.run:printf %s {MARKER}:60",
                "sandbox.kill",
            ],
        )

    def test_fixture_receives_scenario_identity_explicitly(self) -> None:
        trace: list[str] = []
        ecw = _Ecw(trace, EcwCommandResult(0, MARKER, ""))
        services = SmokeServices(
            kind=_Kind(trace),
            ecw=cast(EcwSandboxes, ecw),
            api_key="run-key",
            owns_api_key=False,
        )
        context = ScenarioContext(ResourceScope(), {})

        services.fixture(
            Ecw,
            context,
            scenario_id="kind.explicit_identity",
            sandbox=smoke.SandboxConfig(),
        )

        self.assertIsNotNone(ecw.last_metadata)
        assert ecw.last_metadata is not None
        self.assertEqual(
            ecw.last_metadata["space_smoke_scenario"],
            "kind.explicit_identity",
        )
        context.scope.close()

    def test_additional_sandboxes_share_run_identity_and_get_own_cleanup(
        self,
    ) -> None:
        @smoke.scenario("ecw.additional_sandbox", sandboxes=2)
        def additional_sandbox(_test: smoke.Ecw) -> None:
            """Declared sandboxes remain scoped to one scenario run."""

        ids = [
            types.SimpleNamespace(hex="run-marker"),
            types.SimpleNamespace(hex="instance-primary"),
            types.SimpleNamespace(hex="instance-secondary"),
        ]
        trace: list[str] = []
        ecw = _Ecw(trace, EcwCommandResult(0, "", ""))
        services = SmokeServices(
            kind=None,
            ecw=cast(EcwSandboxes, ecw),
            api_key="deployed-key",
            owns_api_key=False,
        )

        with mock.patch("space_smoke.services.uuid.uuid4", side_effect=ids):
            result = execute_scenario(
                title=additional_sandbox.title,
                body=lambda context: additional_sandbox.run(context, services),
            )

        self.assertEqual(result.verdict, Verdict.PASS)
        self.assertEqual(len(ecw.metadata_history), 2)
        primary, secondary = ecw.metadata_history
        self.assertEqual(primary["space_smoke_run"], "run-marker")
        self.assertEqual(secondary["space_smoke_run"], "run-marker")
        self.assertEqual(primary["space_smoke_instance"], "instance-primary")
        self.assertEqual(secondary["space_smoke_instance"], "instance-secondary")
        self.assertEqual(trace.count("sandbox.kill"), 2)

    def test_declared_collection_cleans_up_when_a_later_create_is_ambiguous(
        self,
    ) -> None:
        @smoke.scenario("ecw.partial_collection", sandboxes=2)
        def partial_collection(_test: smoke.Ecw) -> None:
            """Every declared sandbox has cleanup armed before creation."""

        class FailSecondCreate(_Ecw):
            def create(
                self,
                *,
                template: str,
                timeout: int,
                metadata: Mapping[str, str],
                lifecycle: EcwLifecycle | None = None,
                allow_internet_access: bool | None = None,
                network: NetworkConfig | None = None,
            ) -> _Sandbox:
                self.fail_create = len(self.metadata_history) == 1
                return super().create(
                    template=template,
                    timeout=timeout,
                    metadata=metadata,
                    lifecycle=lifecycle,
                    allow_internet_access=allow_internet_access,
                    network=network,
                )

        ids = [
            types.SimpleNamespace(hex="run-marker"),
            types.SimpleNamespace(hex="instance-primary"),
            types.SimpleNamespace(hex="instance-ambiguous"),
        ]
        trace: list[str] = []
        services = SmokeServices(
            kind=None,
            ecw=cast(
                EcwSandboxes,
                FailSecondCreate(trace, EcwCommandResult(0, "", "")),
            ),
            api_key="deployed-key",
            owns_api_key=False,
        )

        with mock.patch("space_smoke.services.uuid.uuid4", side_effect=ids):
            result = execute_scenario(
                title=partial_collection.title,
                body=lambda context: partial_collection.run(context, services),
            )

        self.assertEqual(result.verdict, Verdict.INFRA_ERROR)
        self.assertEqual(trace.count("ecw.create:seed:300:run-marker"), 2)
        self.assertEqual(trace[-2:], ["ecw.kill-matching:run-marker", "sandbox.kill"])

    def test_scenario_metadata_cannot_override_framework_cleanup_identity(
        self,
    ) -> None:
        @smoke.scenario("ecw.reserved_metadata")
        def reserved_metadata(test: smoke.Ecw) -> None:
            """Framework-owned cleanup metadata remains private."""

            test.sandboxes.create(metadata={"space_smoke_instance": "shared"})

        trace: list[str] = []
        ecw = _Ecw(trace, EcwCommandResult(0, "", ""))
        services = SmokeServices(
            kind=None,
            ecw=cast(EcwSandboxes, ecw),
            api_key="deployed-key",
            owns_api_key=False,
        )

        result = execute_scenario(
            title=reserved_metadata.title,
            body=lambda context: reserved_metadata.run(context, services),
        )

        self.assertEqual(result.verdict, Verdict.INCONCLUSIVE)
        self.assertIn("space_smoke_instance", result.summary)
        self.assertEqual(len(ecw.metadata_history), 1)

    def test_rejected_create_disarms_its_prearmed_reconciliation(self) -> None:
        @smoke.scenario("ecw.rejected_create")
        def rejected_create(test: smoke.Ecw) -> None:
            """A definitive rejection does not leave ambiguous-create cleanup armed."""

            observed = test.sandboxes.attempt_create(allow_internet_access=False)
            test.network_policy.expect_create(observed, status=400)

        trace: list[str] = []
        ecw = _Ecw(trace, EcwCommandResult(0, "", ""))
        services = SmokeServices(
            kind=None,
            ecw=cast(EcwSandboxes, ecw),
            api_key="deployed-key",
            owns_api_key=False,
        )

        result = execute_scenario(
            title=rejected_create.title,
            body=lambda context: rejected_create.run(context, services),
        )

        self.assertEqual(result.verdict, Verdict.PASS)
        self.assertEqual(trace[-2:], ["ecw.attempt-create:configured", "sandbox.kill"])
        self.assertNotIn("ecw.kill-matching", " ".join(trace))

    def test_server_failed_create_keeps_prearmed_reconciliation(self) -> None:
        @smoke.scenario("ecw.server_failed_create")
        def server_failed_create(test: smoke.Ecw) -> None:
            """A server-failed create remains covered by run-marker cleanup."""

            observed = test.sandboxes.attempt_create(allow_internet_access=False)
            test.network_policy.expect_create(observed, status=503)

        trace: list[str] = []
        ecw = _Ecw(
            trace,
            EcwCommandResult(0, "", ""),
            create_attempt=EcwCreateAttempt(503, "service unavailable", None),
        )
        services = SmokeServices(
            kind=None,
            ecw=cast(EcwSandboxes, ecw),
            api_key="deployed-key",
            owns_api_key=False,
        )

        result = execute_scenario(
            title=server_failed_create.title,
            body=lambda context: server_failed_create.run(context, services),
        )

        self.assertEqual(result.verdict, Verdict.PASS)
        self.assertTrue(trace[-2].startswith("ecw.kill-matching:"))
        self.assertEqual(trace[-1], "sandbox.kill")

    def test_successful_raw_create_remains_covered_by_reconciliation(self) -> None:
        @smoke.scenario("ecw.accepted_create")
        def accepted_create(test: smoke.Ecw) -> None:
            """An accepted raw create retains its prearmed cleanup."""

            observed = test.sandboxes.attempt_create(
                allow_internet_access=False,
                network=smoke.NetworkConfig(
                    allow_public_traffic=False,
                ),
            )
            test.network_policy.expect_create(observed, status=201)

        trace: list[str] = []
        ecw = _Ecw(
            trace,
            EcwCommandResult(0, "", ""),
            create_attempt=EcwCreateAttempt(201, '{"sandboxID":"raw"}', "raw"),
        )
        services = SmokeServices(
            kind=None,
            ecw=cast(EcwSandboxes, ecw),
            api_key="deployed-key",
            owns_api_key=False,
        )

        result = execute_scenario(
            title=accepted_create.title,
            body=lambda context: accepted_create.run(context, services),
        )

        self.assertEqual(result.verdict, Verdict.PASS)
        self.assertTrue(trace[-2].startswith("ecw.kill-matching:"))
        self.assertEqual(trace[-1], "sandbox.kill")

    def test_ecw_adapter_uses_the_pinned_client_surface(self) -> None:
        seen: dict[str, object] = {}

        class Commands:
            def run(self, command: str, *, timeout: int):
                seen["command"] = (command, timeout)
                return types.SimpleNamespace(exit_code=0, stdout=MARKER, stderr="")

        class Sandbox:
            sandbox_id = "sandbox-sdk"
            commands = Commands()

            @classmethod
            def create(cls, **kwargs):
                seen["create"] = kwargs
                return cls()

            def kill(self) -> None:
                seen["killed"] = True

            def get_info(self):
                seen["get_info"] = True
                return types.SimpleNamespace(
                    sandbox_id=self.sandbox_id,
                    template_id="seed",
                )

            def set_timeout(self, timeout: int) -> None:
                seen["set_timeout"] = timeout

            def pause(self) -> None:
                seen["paused"] = True

            @classmethod
            def connect(cls, sandbox_id: str, *, timeout: int, **kwargs):
                seen["connected"] = (sandbox_id, timeout, kwargs)
                return cls()

        with mock.patch.dict(
            sys.modules, {"e2b": types.SimpleNamespace(Sandbox=Sandbox)}
        ):
            sandbox = EcwSdk(
                EcwConfig(
                    api_key="key",
                    domain="local.e2b.test",
                    api_url="http://127.0.0.1:8080",
                    api_host="api.local.e2b.test",
                )
            ).create(template="seed", timeout=300, metadata={"run": "marker"})
            command = sandbox.run("printf marker")
            info = sandbox.get_info()
            with mock.patch(
                "space_smoke.adapters.urlopen",
                return_value=nullcontext(types.SimpleNamespace(status=204)),
            ) as opened:
                sandbox.set_timeout(600)
            seen["set_timeout"] = json.loads(opened.call_args.args[0].data)["timeout"]
            sandbox.pause()
            connected = sandbox.connect(timeout=300)
            sandbox.kill()

        self.assertEqual(
            seen["create"],
            {
                "template": "seed",
                "timeout": 300,
                "metadata": {"run": "marker"},
                "api_key": "key",
                "domain": "local.e2b.test",
                "api_url": "http://127.0.0.1:8080",
                "headers": {"Host": "api.local.e2b.test"},
            },
        )
        self.assertEqual(seen["command"], ("printf marker", 60))
        self.assertEqual(command, EcwCommandResult(0, MARKER, ""))
        self.assertEqual(info.sandbox_id, "sandbox-sdk")
        self.assertEqual(info.template_id, "seed")
        self.assertEqual(seen["set_timeout"], 600)
        self.assertTrue(seen["paused"])
        self.assertEqual(
            seen["connected"],
            (
                "sandbox-sdk",
                300,
                {
                    "api_key": "key",
                    "domain": "local.e2b.test",
                    "api_url": "http://127.0.0.1:8080",
                    "headers": {"Host": "api.local.e2b.test"},
                },
            ),
        )
        self.assertIs(connected, sandbox)
        self.assertTrue(seen["killed"])

    def test_ecw_adapter_forwards_explicit_lifecycle_and_environment(self) -> None:
        seen: dict[str, object] = {}
        delegate = types.SimpleNamespace(sandbox_id="sandbox-sdk")

        class Sandbox:
            @staticmethod
            def create(**kwargs):
                seen.update(kwargs)
                return delegate

        with mock.patch.dict(
            sys.modules, {"e2b": types.SimpleNamespace(Sandbox=Sandbox)}
        ):
            EcwSdk(EcwConfig(api_key="key")).create(
                template="seed",
                timeout=10,
                metadata={"run": "marker"},
                lifecycle=EcwLifecycle(
                    EcwTimeoutAction.PAUSE,
                    auto_resume=True,
                ),
                envs={"SPACE_PREPARATION_UTF8": "café 雪"},
            )

        self.assertEqual(seen["envs"], {"SPACE_PREPARATION_UTF8": "café 雪"})
        self.assertEqual(
            seen["lifecycle"],
            {"on_timeout": "pause", "auto_resume": True},
        )

    def test_ecw_adapter_forwards_explicit_internet_and_network_config(self) -> None:
        seen: dict[str, object] = {}
        delegate = types.SimpleNamespace(sandbox_id="sandbox-sdk")

        class Sandbox:
            @staticmethod
            def create(**kwargs):
                seen.update(kwargs)
                return delegate

        with mock.patch.dict(
            sys.modules, {"e2b": types.SimpleNamespace(Sandbox=Sandbox)}
        ):
            EcwSdk(EcwConfig(api_key="key")).create(
                template="seed",
                timeout=120,
                metadata={"run": "marker"},
                allow_internet_access=False,
                network=NetworkConfig(
                    allow_public_traffic=False,
                    allow_out=("example.com",),
                    deny_out=("0.0.0.0/0", "::/0"),
                ),
            )

        self.assertIs(seen["allow_internet_access"], False)
        self.assertEqual(
            seen["network"],
            {
                "allow_public_traffic": False,
                "allow_out": ["example.com"],
                "deny_out": ["0.0.0.0/0", "::/0"],
            },
        )

    def test_ecw_adapter_observes_raw_create_rejection_contract(self) -> None:
        rejected = HTTPError(
            "https://api.example.test/sandboxes",
            400,
            "Bad Request",
            Message(),
            io.BytesIO(b"invalid network options"),
        )

        with mock.patch("space_smoke.adapters.urlopen", side_effect=rejected) as opened:
            observed = EcwSdk(
                EcwConfig(api_key="key", domain="example.test")
            ).attempt_create(
                template="seed",
                timeout=600,
                metadata={"run": "marker"},
                allow_internet_access=False,
                network=NetworkConfig(
                    allow_public_traffic=None,
                    allow_out=("example.com",),
                ),
            )

        self.assertEqual(
            observed,
            EcwCreateAttempt(
                status_code=400,
                body="invalid network options",
                sandbox_id=None,
            ),
        )
        request = opened.call_args.args[0]
        self.assertEqual(
            json.loads(request.data or b"{}"),
            {
                "templateID": "seed",
                "timeout": 600,
                "metadata": {"run": "marker"},
                "allowInternetAccess": False,
                "network": {"allow_out": ["example.com"]},
            },
        )

    def test_ecw_create_paths_share_network_wire_serialization(self) -> None:
        seen: dict[str, object] = {}
        delegate = types.SimpleNamespace(sandbox_id="sandbox-sdk")

        class Sandbox:
            @staticmethod
            def create(**kwargs):
                seen.update(kwargs)
                return delegate

        rejected = HTTPError(
            "https://api.example.test/sandboxes",
            400,
            "Bad Request",
            Message(),
            io.BytesIO(b"expected rejection"),
        )
        network = NetworkConfig(
            allow_public_traffic=False,
            allow_out=("example.com",),
            deny_out=("1.0.0.1/32",),
        )
        sdk = EcwSdk(EcwConfig(api_key="key", domain="example.test"))

        with (
            mock.patch.dict(
                sys.modules,
                {"e2b": types.SimpleNamespace(Sandbox=Sandbox)},
            ),
            mock.patch("space_smoke.adapters.urlopen", side_effect=rejected) as opened,
        ):
            sdk.create(
                template="seed",
                timeout=600,
                metadata={"run": "sdk"},
                network=network,
            )
            sdk.attempt_create(
                template="seed",
                timeout=600,
                metadata={"run": "raw"},
                network=network,
            )

        request = opened.call_args.args[0]
        raw_body = json.loads(request.data or b"{}")
        self.assertEqual(seen["network"], raw_body["network"])

    def test_ecw_adapter_uses_sdk_host_and_access_token_for_public_port(self) -> None:
        delegate = types.SimpleNamespace(
            sandbox_id="sandbox-sdk",
            traffic_access_token="traffic-token",
            get_host=lambda port: f"{port}-sandbox-sdk.e2b.test",
        )
        sandbox = _SdkSandbox(delegate, EcwConfig(api_key="key", domain="e2b.test"))

        route = sandbox.public_port(8123)

        self.assertEqual(route.base_url, "https://8123-sandbox-sdk.e2b.test")
        self.assertEqual(
            route.headers,
            {"e2b-traffic-access-token": "traffic-token"},
        )

    def test_ecw_adapter_classifies_only_temporary_dns_before_create(self) -> None:
        dns_error = socket.gaierror(
            socket.EAI_AGAIN, "Temporary failure in name resolution"
        )
        connect_error = httpx.ConnectError("DNS lookup failed")
        connect_error.__cause__ = dns_error
        create = mock.Mock(side_effect=connect_error)
        sandbox_type = types.SimpleNamespace(create=create)

        with (
            mock.patch.dict(
                sys.modules, {"e2b": types.SimpleNamespace(Sandbox=sandbox_type)}
            ),
            self.assertRaisesRegex(
                EcwCreateNotDelivered, "Temporary failure in name resolution"
            ),
        ):
            EcwSdk(EcwConfig(api_key="key")).create(
                template="seed", timeout=10, metadata={"run": "marker"}
            )

        create.assert_called_once()

    def test_ecw_adapter_preserves_ambiguous_create_connect_error(self) -> None:
        connect_error = httpx.ConnectError("connection reset")
        sandbox_type = types.SimpleNamespace(
            create=mock.Mock(side_effect=connect_error)
        )

        with (
            mock.patch.dict(
                sys.modules, {"e2b": types.SimpleNamespace(Sandbox=sandbox_type)}
            ),
            self.assertRaises(httpx.ConnectError) as caught,
        ):
            EcwSdk(EcwConfig(api_key="key")).create(
                template="seed", timeout=10, metadata={"run": "marker"}
            )

        self.assertIs(caught.exception, connect_error)
        sandbox_type.create.assert_called_once()

    def test_ecw_adapter_exposes_delete_conflicts_as_retryable(self) -> None:
        class SandboxException(Exception):
            pass

        class Sandbox:
            sandbox_id = "sandbox-sdk"

            @classmethod
            def create(cls, **_kwargs):
                return cls()

            def kill(self) -> None:
                raise SandboxException("409: lifecycle transition in flight")

        with (
            mock.patch.dict(
                sys.modules,
                {
                    "e2b": types.SimpleNamespace(
                        Sandbox=Sandbox,
                        SandboxException=SandboxException,
                    )
                },
            ),
            self.assertRaisesRegex(EcwSandboxBusy, "409: lifecycle transition"),
        ):
            EcwSdk(EcwConfig(api_key="key")).create(
                template="seed",
                timeout=10,
                metadata={"run": "marker"},
            ).kill()

    def test_ecw_adapter_exposes_transient_delete_failures_as_retryable(
        self,
    ) -> None:
        class SandboxException(Exception):
            pass

        class Sandbox:
            sandbox_id = "sandbox-sdk"

            @classmethod
            def create(cls, **_kwargs):
                return cls()

            def kill(self) -> None:
                raise SandboxException("503: upstream temporarily unavailable")

        with (
            mock.patch.dict(
                sys.modules,
                {
                    "e2b": types.SimpleNamespace(
                        Sandbox=Sandbox,
                        SandboxException=SandboxException,
                    )
                },
            ),
            self.assertRaisesRegex(EcwSandboxDeleteRetryable, "503: upstream"),
        ):
            EcwSdk(EcwConfig(api_key="key")).create(
                template="seed",
                timeout=10,
                metadata={"run": "marker"},
            ).kill()

    def test_ecw_adapter_treats_delete_not_found_as_already_absent(self) -> None:
        class SandboxException(Exception):
            pass

        class Sandbox:
            sandbox_id = "sandbox-sdk"

            @classmethod
            def create(cls, **_kwargs):
                return cls()

            def kill(self) -> bool:
                return False

        with mock.patch.dict(
            sys.modules,
            {
                "e2b": types.SimpleNamespace(
                    Sandbox=Sandbox,
                    SandboxException=SandboxException,
                )
            },
        ):
            sandbox = EcwSdk(EcwConfig(api_key="key")).create(
                template="seed",
                timeout=10,
                metadata={"run": "marker"},
            )
            self.assertIsNone(sandbox.kill())

    def test_ecw_adapter_refreshes_through_the_unwrapped_public_route(self) -> None:
        response = mock.MagicMock(status=204)
        opened = mock.MagicMock()
        opened.return_value.__enter__.return_value = response
        sandbox = _SdkSandbox(
            types.SimpleNamespace(sandbox_id="sandbox/with space"),
            EcwConfig(
                api_key="key",
                api_url="http://127.0.0.1:8080/",
                api_host="api.local.e2b.test",
            ),
        )

        with mock.patch("space_smoke.adapters.urlopen", opened):
            result = sandbox.refresh()

        self.assertIsNone(result)
        request = opened.call_args.args[0]
        headers = {name.casefold(): value for name, value in request.header_items()}
        self.assertEqual(
            request.full_url,
            "http://127.0.0.1:8080/sandboxes/sandbox%2Fwith%20space/refreshes",
        )
        self.assertEqual(request.method, "POST")
        self.assertEqual(request.data, b"{}")
        self.assertEqual(headers["x-api-key"], "key")
        self.assertEqual(headers["host"], "api.local.e2b.test")
        self.assertEqual(opened.call_args.kwargs["timeout"], 30)

    def test_ecw_adapter_refresh_error_preserves_bounded_response_detail(self) -> None:
        sandbox = _SdkSandbox(
            types.SimpleNamespace(sandbox_id="sandbox-id"),
            EcwConfig(api_key="key", api_url="https://api.example.test"),
        )
        response = HTTPError(
            "https://api.example.test/sandboxes/sandbox-id/refreshes",
            500,
            "Internal Server Error",
            Message(),
            io.BytesIO(b'{"message":"lease reset rejected"}'),
        )

        with (
            mock.patch("space_smoke.adapters.urlopen", side_effect=response),
            self.assertRaisesRegex(
                RuntimeError,
                'HTTP 500.*"message":"lease reset rejected"',
            ),
        ):
            sandbox.refresh()

    def test_nonzero_sdk_command_exit_is_returned_as_a_result(self) -> None:
        class CommandExitException(Exception):
            def __init__(self) -> None:
                self.exit_code = 17
                self.stdout = "partial output"
                self.stderr = "command failed"

        class Handle:
            def wait(self):
                raise CommandExitException()

        class Commands:
            def run(self, _command: str, *, timeout: int):
                del timeout
                raise CommandExitException()

        sandbox = _SdkSandbox(
            types.SimpleNamespace(sandbox_id="sandbox-sdk", commands=Commands()),
            EcwConfig(api_key="key"),
        )

        with mock.patch.dict(
            sys.modules,
            {
                "e2b": types.SimpleNamespace(
                    CommandExitException=CommandExitException,
                )
            },
        ):
            foreground = sandbox.run("false")
            background = _SdkProcess(Handle()).wait()

        expected = EcwCommandResult(17, "partial output", "command failed")
        self.assertEqual(foreground, expected)
        self.assertEqual(background, expected)

    def test_process_connect_normalizes_the_sdk_typed_transient_errors(self) -> None:
        e2b = importlib.import_module("e2b")

        class Commands:
            def __init__(self, error: Exception) -> None:
                self.error = error

            def connect(self, _pid: int, *, timeout: int):
                del timeout
                raise self.error

        for error, expected in (
            (e2b.NotFoundException("process not found"), EcwDataPlaneNotFound),
            (e2b.TimeoutException("sandbox unavailable"), EcwSandboxUnavailable),
            (e2b.RateLimitException("try again"), EcwSandboxUnavailable),
        ):
            with self.subTest(error=type(error).__name__):
                delegate = types.SimpleNamespace(
                    sandbox_id="sandbox-sdk",
                    commands=Commands(error),
                )
                sandbox = _SdkSandbox(delegate, EcwConfig(api_key="key"))

                with self.assertRaises(expected):
                    sandbox.connect_process(123, timeout=20)

    def test_kind_reuses_the_sdk_for_filesystem_and_process_rpcs(self) -> None:
        seen: dict[str, object] = {}
        reads: list[tuple[str, str, str | None, float]] = []
        connection_config_type = importlib.import_module(
            "e2b.connection_config"
        ).ConnectionConfig

        class Handle:
            pid = 123

            def disconnect(self) -> None:
                seen["disconnect"] = True

            def wait(self):
                seen["wait"] = True
                return types.SimpleNamespace(
                    exit_code=0,
                    stdout="reattached",
                    stderr="",
                )

            def kill(self) -> bool:
                seen["process_kill"] = True
                return True

        handle = Handle()

        class Files:
            def write(
                self,
                path: str,
                data: str | bytes | BinaryIO,
                *,
                user: str | None = None,
                request_timeout: float,
            ) -> None:
                seen["write"] = (path, data, user, request_timeout)

            def read(
                self,
                path: str,
                format: str = "text",
                *,
                user: str | None = None,
                request_timeout: float,
            ):
                reads.append((path, format, user, request_timeout))
                if format == "bytes":
                    return bytearray(b"\x00\xff")
                return "contents"

            def make_dir(
                self,
                path: str,
                *,
                user: str | None = None,
                request_timeout: float,
            ) -> None:
                seen["make_dir"] = (path, user, request_timeout)

            def exists(
                self,
                path: str,
                *,
                user: str | None = None,
                request_timeout: float,
            ) -> bool:
                seen["exists"] = (path, user, request_timeout)
                return True

            def list(
                self,
                path: str,
                *,
                user: str | None = None,
                request_timeout: float,
            ):
                seen["list"] = (path, user, request_timeout)
                return [types.SimpleNamespace(name="entry.txt")]

            def rename(
                self,
                old_path: str,
                new_path: str,
                *,
                user: str | None = None,
                request_timeout: float,
            ) -> None:
                seen["rename"] = (old_path, new_path, user, request_timeout)

            def remove(
                self,
                path: str,
                *,
                user: str | None = None,
                request_timeout: float,
            ) -> None:
                seen["remove"] = (path, user, request_timeout)

        class Commands:
            def run(
                self,
                command: str,
                *,
                background: bool = False,
                timeout: int,
                user: str | None = None,
            ):
                if not background:
                    seen["foreground"] = (command, user, timeout)
                    return types.SimpleNamespace(
                        exit_code=0,
                        stdout="foreground",
                        stderr="",
                    )
                seen["background"] = (command, background, timeout)
                return handle

            def connect(self, pid: int, *, timeout: int) -> Handle:
                seen["process_connect"] = (pid, timeout)
                return handle

        delegate = types.SimpleNamespace(
            sandbox_id="sandbox-local",
            sandbox_domain="local.e2b.test",
            connection_config=types.SimpleNamespace(
                sandbox_headers={
                    "Host": "api.local.e2b.test",
                    "X-Access-Token": "sandbox-token",
                    "E2b-Sandbox-Id": "sandbox-local",
                    "E2b-Sandbox-Port": "49983",
                }
            ),
            _envd_version=types.SimpleNamespace(),
            _envd_access_token="sandbox-token",
            traffic_access_token="traffic-token",
            get_host=lambda port: f"{port}-sandbox-local.local.e2b.test",
        )
        connected_delegate = types.SimpleNamespace(
            sandbox_id="sandbox-local",
            sandbox_domain="local.e2b.test",
            connection_config=types.SimpleNamespace(
                sandbox_headers={"X-Access-Token": "reconnected-token"}
            ),
            _envd_version=types.SimpleNamespace(),
            _envd_access_token="reconnected-token",
            traffic_access_token="traffic-token",
            get_host=lambda port: f"{port}-sandbox-local.local.e2b.test",
        )

        class Sandbox:
            @staticmethod
            def create(**_kwargs):
                return delegate

            @staticmethod
            def connect(sandbox_id: str, *, timeout: int, **_kwargs):
                seen["connect"] = (sandbox_id, timeout)
                return connected_delegate

            def __init__(self, **options):
                seen["local_options"] = options
                self.files = Files()
                self.commands = Commands()

        with mock.patch.dict(
            sys.modules,
            {
                "e2b": types.SimpleNamespace(Sandbox=Sandbox),
                "e2b.connection_config": types.SimpleNamespace(
                    ConnectionConfig=connection_config_type
                ),
            },
        ):
            sandbox = EcwSdk(
                EcwConfig(
                    api_key="key",
                    data_plane_url="http://127.0.0.1:8090",
                    data_plane_domain="local.e2b.test",
                )
            ).create(template="seed", timeout=300, metadata={"run": "marker"})
            sandbox.connect(timeout=23)
            streamed = io.BytesIO(b"contents")
            sandbox.files.write(
                "/tmp/root/file",
                streamed,
                user="user",
                request_timeout=8,
            )
            text = sandbox.files.read("/tmp/root/file", user="user", request_timeout=9)
            binary = sandbox.files.read_bytes(
                "/tmp/root/binary", user="root", request_timeout=10
            )
            sandbox.files.make_dir("/tmp/root", user="user", request_timeout=11)
            exists = sandbox.files.exists(
                "/tmp/root/file", user="user", request_timeout=11.5
            )
            entries = sandbox.files.list("/tmp/root", user="root", request_timeout=12)
            sandbox.files.rename(
                "/tmp/root/before",
                "/tmp/root/after",
                user="root",
                request_timeout=13,
            )
            sandbox.files.remove(
                "/tmp/root/after",
                user="root",
                request_timeout=13.5,
            )
            command = sandbox.run("printf foreground", timeout=14, user="user")
            process = sandbox.start_background("sleep 60", timeout=14)
            connected = sandbox.connect_process(process.pid, timeout=15)
            public_route = sandbox.public_port(8123)

        local_options = seen["local_options"]
        assert isinstance(local_options, dict)
        local_config = local_options["connection_config"]
        self.assertEqual(
            local_config.get_sandbox_url("sandbox-local", "local.e2b.test"),
            "http://127.0.0.1:8090",
        )
        self.assertEqual(
            local_config.sandbox_headers["Host"],
            "49983-sandbox-local.local.e2b.test",
        )
        self.assertEqual(
            local_config.sandbox_headers["E2b-Sandbox-Id"],
            "sandbox-local",
        )
        self.assertEqual(
            local_config.sandbox_headers["E2b-Sandbox-Port"],
            "49983",
        )
        self.assertEqual(public_route.base_url, "http://127.0.0.1:8090")
        self.assertEqual(
            public_route.headers,
            {
                "Host": "8123-sandbox-local.local.e2b.test",
                "e2b-sandbox-id": "sandbox-local",
                "e2b-sandbox-port": "8123",
                "e2b-traffic-access-token": "traffic-token",
            },
        )
        self.assertEqual(seen["connect"], ("sandbox-local", 23))
        self.assertEqual(
            seen["write"],
            ("/tmp/root/file", streamed, "user", 8),
        )
        self.assertEqual(
            reads,
            [
                ("/tmp/root/file", "text", "user", 9),
                ("/tmp/root/binary", "bytes", "root", 10),
            ],
        )
        self.assertEqual(text, "contents")
        self.assertEqual(binary, b"\x00\xff")
        self.assertTrue(exists)
        self.assertEqual(entries[0].name, "entry.txt")
        self.assertEqual(seen["make_dir"], ("/tmp/root", "user", 11))
        self.assertEqual(seen["exists"], ("/tmp/root/file", "user", 11.5))
        self.assertEqual(seen["list"], ("/tmp/root", "root", 12))
        self.assertEqual(
            seen["rename"],
            ("/tmp/root/before", "/tmp/root/after", "root", 13),
        )
        self.assertEqual(
            seen["remove"],
            ("/tmp/root/after", "root", 13.5),
        )
        self.assertEqual(
            seen["foreground"],
            ("printf foreground", "user", 14),
        )
        self.assertEqual(command, EcwCommandResult(0, "foreground", ""))
        self.assertEqual(seen["background"], ("sleep 60", True, 14))
        self.assertEqual(seen["process_connect"], (123, 15))
        self.assertEqual(connected.pid, 123)

    def test_sdk_data_plane_errors_are_normalized_for_fixture_polling(self) -> None:
        e2b = importlib.import_module("e2b")
        envd_api = importlib.import_module("e2b.envd.api")

        class Files:
            def __init__(self, error: Exception) -> None:
                self.error = error

            def read(self, _path: str, *, request_timeout: float):
                del request_timeout
                raise self.error

        for error, expected in (
            (e2b.NotFoundException("sandbox not found"), EcwDataPlaneNotFound),
            (e2b.TimeoutException("sandbox unavailable"), EcwSandboxUnavailable),
            (e2b.RateLimitException("try again"), EcwSandboxUnavailable),
            (e2b.SandboxException("408: request timed out"), EcwSandboxUnavailable),
            (
                e2b.SandboxException("409: Sandbox is not running"),
                EcwSandboxUnavailable,
            ),
            (e2b.SandboxException("503: unavailable"), EcwSandboxUnavailable),
            (
                e2b.SandboxException("Code.permission_denied: Permission denied"),
                EcwPermissionDenied,
            ),
            (
                e2b.SandboxException("403: permission denied"),
                EcwPermissionDenied,
            ),
            (
                envd_api.format_envd_api_exception(
                    502, "Permission denied (os error 13)"
                ),
                EcwPermissionDenied,
            ),
        ):
            with self.subTest(error=type(error).__name__, detail=str(error)):
                sandbox = _SdkSandbox(
                    types.SimpleNamespace(
                        sandbox_id="sandbox-sdk",
                        files=Files(error),
                    ),
                    EcwConfig(api_key="key"),
                )
                with self.assertRaises(expected):
                    sandbox.files.read("/tmp/marker")

    def test_sdk_data_plane_preserves_wrong_status_permission_errors(self) -> None:
        e2b = importlib.import_module("e2b")

        class Files:
            def __init__(self, error: Exception) -> None:
                self.error = error

            def read(self, _path: str, *, request_timeout: float):
                del request_timeout
                raise self.error

        for error in (
            e2b.SandboxException("500: permission denied while contacting worker"),
            e2b.SandboxException("permission denied while decoding response"),
        ):
            with self.subTest(detail=str(error)):
                sandbox = _SdkSandbox(
                    types.SimpleNamespace(
                        sandbox_id="sandbox-sdk",
                        files=Files(error),
                    ),
                    EcwConfig(api_key="key"),
                )
                with self.assertRaises(e2b.SandboxException) as caught:
                    sandbox.files.read("/tmp/marker")

                self.assertIs(caught.exception, error)

    def test_sdk_watcher_normalizes_events_and_uses_the_sdk_handle(self) -> None:
        seen: list[object] = []

        class WatchHandle:
            def get_new_events(self):
                seen.append("events")
                return [
                    types.SimpleNamespace(
                        name="nested/watched.txt",
                        type=types.SimpleNamespace(value="write"),
                    )
                ]

            def stop(self) -> None:
                seen.append("stop")

        class Files:
            def watch_dir(
                self,
                path: str,
                *,
                user: str,
                recursive: bool,
                request_timeout: float,
            ) -> WatchHandle:
                seen.append((path, user, recursive, request_timeout))
                return WatchHandle()

        sandbox = _SdkSandbox(
            types.SimpleNamespace(sandbox_id="sandbox-sdk", files=Files()),
            EcwConfig(api_key="key"),
        )

        watcher = sandbox.files.watch_dir(
            "/tmp/root",
            user="user",
            recursive=True,
            request_timeout=12,
        )
        events = watcher.get_new_events()
        watcher.stop()

        self.assertEqual(
            events,
            (
                EcwFilesystemEvent(
                    name="nested/watched.txt",
                    type=EcwFilesystemEventType.WRITE,
                ),
            ),
        )
        self.assertEqual(
            seen,
            [("/tmp/root", "user", True, 12), "events", "stop"],
        )

    def test_sdk_identity_observer_preserves_selector_headers_and_basic_user(
        self,
    ) -> None:
        reads: list[int | None] = []

        class Response:
            def __init__(self, body: bytes) -> None:
                self.body = body

            def __enter__(self):
                return self

            def __exit__(self, *_args) -> None:
                return None

            def read(self, limit: int | None = None) -> bytes:
                reads.append(limit)
                return self.body if limit is None else self.body[:limit]

        sandbox = _SdkSandbox(
            types.SimpleNamespace(
                sandbox_id="sandbox-sdk",
                envd_api_url="https://49983-sandbox-sdk.example.test",
                connection_config=types.SimpleNamespace(
                    sandbox_headers={
                        "X-Access-Token": "sandbox-token",
                        "E2b-Sandbox-Id": "sandbox-sdk",
                    }
                ),
                files=types.SimpleNamespace(),
            ),
            EcwConfig(api_key="key"),
        )

        with mock.patch(
            "space_smoke.adapters.urlopen",
            side_effect=[Response(b"secret"), Response(b"\x00\x00\x00\x00\x00")],
        ) as open_request:
            self.assertEqual(
                sandbox.compatibility.read_with_basic_auth(
                    "/root/secret.txt",
                    user="root",
                    request_timeout=7,
                ),
                "secret",
            )
            sandbox.compatibility.probe_watch_stream(
                "/root/private",
                user="root",
                recursive=True,
                request_timeout=8,
            )

        read_request = open_request.call_args_list[0].args[0]
        watch_request = open_request.call_args_list[1].args[0]
        self.assertEqual(
            read_request.full_url,
            "https://49983-sandbox-sdk.example.test/files?path=%2Froot%2Fsecret.txt",
        )
        self.assertEqual(read_request.get_header("Authorization"), "Basic cm9vdDo=")
        self.assertEqual(read_request.get_header("X-access-token"), "sandbox-token")
        self.assertEqual(watch_request.method, "POST")
        self.assertEqual(
            watch_request.full_url,
            "https://49983-sandbox-sdk.example.test/filesystem.Filesystem/WatchDir",
        )
        self.assertEqual(
            watch_request.data,
            b"\x0a\x0d/root/private\x10\x01",
        )
        self.assertEqual(reads, [None, 5])

    def test_sdk_identity_observer_normalizes_permission_rejection(self) -> None:
        sandbox = _SdkSandbox(
            types.SimpleNamespace(
                sandbox_id="sandbox-sdk",
                envd_api_url="https://49983-sandbox-sdk.example.test",
                connection_config=types.SimpleNamespace(sandbox_headers={}),
                files=types.SimpleNamespace(),
            ),
            EcwConfig(api_key="key"),
        )
        denied = HTTPError(
            "https://49983-sandbox-sdk.example.test/files",
            403,
            "Forbidden",
            Message(),
            io.BytesIO(b"permission denied"),
        )

        with (
            mock.patch("space_smoke.adapters.urlopen", side_effect=denied),
            self.assertRaises(EcwPermissionDenied),
        ):
            sandbox.compatibility.read_with_basic_auth("/root/secret.txt", user="root")

    def test_sdk_data_plane_preserves_unrelated_conflicts(self) -> None:
        e2b = importlib.import_module("e2b")
        conflict = e2b.SandboxException("409: rename conflict")

        class Files:
            def read(self, _path: str, *, request_timeout: float):
                del request_timeout
                raise conflict

        sandbox = _SdkSandbox(
            types.SimpleNamespace(sandbox_id="sandbox-sdk", files=Files()),
            EcwConfig(api_key="key"),
        )
        with self.assertRaises(e2b.SandboxException) as caught:
            sandbox.files.read("/tmp/marker")

        self.assertIs(caught.exception, conflict)

    def test_ecw_adapter_reconciles_sandboxes_by_run_metadata(self) -> None:
        seen: dict[str, object] = {}
        pages = iter(([], [types.SimpleNamespace(sandbox_id="sandbox-ambiguous")]))

        class Query:
            def __init__(self, *, metadata):
                seen["query"] = metadata

        class Paginator:
            has_next = True

            def next_items(self):
                self.has_next = False
                return next(pages)

        class Sandbox:
            @staticmethod
            def list(**kwargs):
                seen["list"] = kwargs
                return Paginator()

            @staticmethod
            def kill(sandbox_id, **kwargs):
                seen["kill"] = (sandbox_id, kwargs)

        with (
            mock.patch.dict(
                sys.modules,
                {"e2b": types.SimpleNamespace(Sandbox=Sandbox, SandboxQuery=Query)},
            ),
            mock.patch("space_smoke.adapters.time.sleep") as sleep,
        ):
            EcwSdk(EcwConfig(api_key="key")).kill_matching(
                {"run": "marker"}, attempts=2
            )

        self.assertEqual(seen["query"], {"run": "marker"})
        list_options = seen["list"]
        kill_call = seen["kill"]
        assert isinstance(list_options, dict)
        assert isinstance(kill_call, tuple)
        self.assertEqual(list_options["limit"], 100)
        self.assertEqual(kill_call[0], "sandbox-ambiguous")
        sleep.assert_called_once_with(1.0)

    def test_ecw_adapter_lists_filtered_pages_through_the_sdk(self) -> None:
        seen: dict[str, object] = {}

        class Query:
            def __init__(self, *, metadata):
                seen["query"] = metadata

        class Paginator:
            def __init__(self) -> None:
                self._pages = iter(
                    (
                        [
                            types.SimpleNamespace(
                                sandbox_id="sandbox-1",
                                template_id="seed",
                            ),
                            types.SimpleNamespace(
                                sandbox_id="sandbox-2",
                                template_id="seed",
                            ),
                        ],
                        [
                            types.SimpleNamespace(
                                sandbox_id="sandbox-3",
                                template_id="seed",
                            )
                        ],
                    )
                )
                self.has_next = True
                self._remaining = 2

            def next_items(self):
                page = next(self._pages)
                self._remaining -= 1
                self.has_next = self._remaining > 0
                return page

        class Sandbox:
            @staticmethod
            def list(**kwargs):
                seen["list"] = kwargs
                return Paginator()

        with mock.patch.dict(
            sys.modules,
            {"e2b": types.SimpleNamespace(Sandbox=Sandbox, SandboxQuery=Query)},
        ):
            result = EcwSdk(EcwConfig(api_key="key")).list_pages(
                {"space_smoke_run": "run-marker"},
                limit=2,
                max_pages=200,
            )

        self.assertEqual(seen["query"], {"space_smoke_run": "run-marker"})
        options = seen["list"]
        assert isinstance(options, dict)
        self.assertEqual(options["limit"], 2)
        self.assertEqual(options["api_key"], "key")
        self.assertEqual(
            [[item.sandbox_id for item in page] for page in result.pages],
            [["sandbox-1", "sandbox-2"], ["sandbox-3"]],
        )
        self.assertFalse(result.has_more)

    def test_ecw_adapter_reports_reconciliation_exhaustion(self) -> None:
        class Query:
            def __init__(self, *, metadata):
                self.metadata = metadata

        class Paginator:
            has_next = True

            def next_items(self):
                self.has_next = False
                return []

        class Sandbox:
            @staticmethod
            def list(**_kwargs):
                return Paginator()

        with (
            mock.patch.dict(
                sys.modules,
                {"e2b": types.SimpleNamespace(Sandbox=Sandbox, SandboxQuery=Query)},
            ),
            mock.patch("space_smoke.adapters.time.sleep") as sleep,
            self.assertRaisesRegex(RuntimeError, "no sandbox became visible"),
        ):
            EcwSdk(EcwConfig(api_key="key")).kill_matching(
                {"run": "marker"}, attempts=2
            )

        sleep.assert_called_once_with(1.0)

    def test_deployed_backend_uses_public_sdk_defaults_and_caller_credentials(
        self,
    ) -> None:
        with (
            mock.patch(
                "space_smoke.services.importlib.util.find_spec",
                return_value=object(),
            ),
            mock.patch(
                "space_smoke.services.shutil.which",
                side_effect=AssertionError("deployed backend must not require kubectl"),
            ),
        ):
            services = SmokeServices.from_environment(
                command_round_trip,
                {
                    "SPACE_SMOKE_BACKEND": "deployed",
                    "E2B_API_KEY": "deployed-key",
                    "E2B_DOMAIN": "e2b.testing.example",
                    "E2B_TEMPLATE": "code-runtime",
                },
            )

        self.assertIsNone(services.kind)
        self.assertFalse(services.owns_api_key)
        self.assertEqual(services.api_key, "deployed-key")
        self.assertEqual(services.template_override, "code-runtime")
        self.assertIsInstance(services.ecw, EcwSdk)
        assert isinstance(services.ecw, EcwSdk)
        self.assertEqual(
            services.ecw.config.sdk_options(),
            {
                "api_key": "deployed-key",
                "domain": "e2b.testing.example",
            },
        )

    def test_deployed_backend_routes_through_in_cluster_endpoints(self) -> None:
        with (
            mock.patch(
                "space_smoke.services.importlib.util.find_spec",
                return_value=object(),
            ),
            mock.patch(
                "space_smoke.services.shutil.which",
                side_effect=AssertionError("deployed backend must not require kubectl"),
            ),
        ):
            services = SmokeServices.from_environment(
                command_round_trip,
                {
                    "SPACE_SMOKE_BACKEND": "deployed",
                    "E2B_API_KEY": "deployed-key",
                    "E2B_DOMAIN": "e2b.prod-3.example",
                    "E2B_API_URL": "http://space-api:8080",
                    "ECW_API_HOST": "api.e2b.prod-3.example",
                    "ECW_DATA_PLANE_BASE_URL": "http://space-data-plane:8090",
                },
            )

        assert isinstance(services.ecw, EcwSdk)
        self.assertEqual(
            services.ecw.config.sdk_options(),
            {
                "api_key": "deployed-key",
                "domain": "e2b.prod-3.example",
                "api_url": "http://space-api:8080",
                "headers": {"Host": "api.e2b.prod-3.example"},
            },
        )
        self.assertEqual(
            services.ecw.config.data_plane_url,
            "http://space-data-plane:8090",
        )

    def test_deployed_api_url_without_a_host_header_is_rejected(self) -> None:
        with (
            mock.patch(
                "space_smoke.services.importlib.util.find_spec",
                return_value=object(),
            ),
            self.assertRaisesRegex(SmokeConfigurationError, "ECW_API_HOST"),
        ):
            SmokeServices.from_environment(
                command_round_trip,
                {
                    "SPACE_SMOKE_BACKEND": "deployed",
                    "E2B_API_KEY": "deployed-key",
                    "E2B_DOMAIN": "e2b.prod-3.example",
                    "E2B_API_URL": "http://space-api:8080",
                },
            )

    def test_deployed_backend_requires_explicit_domain_and_api_key(self) -> None:
        with mock.patch(
            "space_smoke.services.importlib.util.find_spec",
            return_value=object(),
        ):
            for environment, message in (
                (
                    {
                        "SPACE_SMOKE_BACKEND": "deployed",
                        "E2B_DOMAIN": "e2b.testing.example",
                    },
                    "E2B_API_KEY",
                ),
                (
                    {
                        "SPACE_SMOKE_BACKEND": "deployed",
                        "E2B_API_KEY": "deployed-key",
                    },
                    "E2B_DOMAIN",
                ),
            ):
                with (
                    self.subTest(environment=environment),
                    self.assertRaisesRegex(SmokeConfigurationError, message),
                ):
                    SmokeServices.from_environment(
                        command_round_trip,
                        environment,
                    )

    def test_unknown_backend_is_rejected_before_environment_mutation(self) -> None:
        with (
            mock.patch(
                "space_smoke.services.importlib.util.find_spec",
                return_value=object(),
            ),
            self.assertRaisesRegex(SmokeConfigurationError, "kind.*deployed"),
        ):
            SmokeServices.from_environment(
                command_round_trip,
                {"SPACE_SMOKE_BACKEND": "production"},
            )

    def test_remote_data_plane_requires_the_explicit_remote_override(self) -> None:
        with (
            mock.patch("space_smoke.services.shutil.which", return_value="kubectl"),
            mock.patch(
                "space_smoke.services.importlib.util.find_spec",
                return_value=object(),
            ),
            self.assertRaisesRegex(SmokeConfigurationError, "ECW_DATA_PLANE_BASE_URL"),
        ):
            SmokeServices.from_environment(
                command_round_trip,
                {
                    "API_URL": "http://127.0.0.1:8080",
                    "ECW_DATA_PLANE_BASE_URL": "https://data.example.com",
                },
            )

    def test_empty_api_key_is_treated_as_an_owned_generated_credential(self) -> None:
        with (
            mock.patch("space_smoke.services.shutil.which", return_value="kubectl"),
            mock.patch(
                "space_smoke.services.importlib.util.find_spec",
                return_value=object(),
            ),
        ):
            services = SmokeServices.from_environment(
                command_round_trip,
                {"E2B_API_KEY": ""},
            )

        self.assertTrue(services.owns_api_key)
        self.assertRegex(services.api_key, r"^[0-9a-f]{64}$")

    def test_loopback_guard_checks_the_parsed_hostname(self) -> None:
        with self.assertRaisesRegex(SmokeConfigurationError, "not loopback"):
            validate_loopback_environment(
                "http://localhost:8080@remote.example",
                False,
                scenario_id=command_round_trip.id,
            )


if __name__ == "__main__":
    unittest.main()

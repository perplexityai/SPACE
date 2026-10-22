"""The retention guard scenario's framework contract."""

from __future__ import annotations

import json
import unittest
from contextlib import nullcontext
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, ClassVar, cast
from unittest import mock

from space_smoke import adapters
from space_smoke.capabilities import RetentionSweeperConfig, SnapshotRecord
from space_smoke.fixtures import SMOKE_SWEEPER_ENV, Ecw, KindEcw, RetentionAssertions
from space_smoke.registry import scenario
from space_smoke.runtime import (
    CommandResult,
    InvariantViolation,
    ResourceScope,
    SmokeConfigurationError,
)
from space_smoke.scenarios.local_retention_claim_finish_forward import (
    claim_finish_forward,
)
from space_smoke.scenarios.local_retention_restore_source_guard import (
    restore_source_guard,
)
from space_smoke.services import MissingToolError, SmokeServices


class RetentionGuardRegistration(unittest.TestCase):
    def test_registered_with_kind_only_fixture(self) -> None:
        self.assertEqual(
            restore_source_guard.id, "local.retention_restore_source_guard"
        )
        self.assertEqual(restore_source_guard.category, "local")
        self.assertIs(restore_source_guard.fixture_type, KindEcw)
        self.assertEqual(restore_source_guard.backends, frozenset({"kind"}))
        self.assertEqual(restore_source_guard.fixture_name, "kind_ecw")


class RestoreSourceChoreography(unittest.TestCase):
    def test_history_is_pinned_before_the_sweeper_goes_live(self) -> None:
        # An inert standing sweeper already holds off; any other is disabled.
        for standing_inert in (False, True):
            with self.subTest(standing_inert=standing_inert):
                self._history_is_pinned_before_the_sweeper_goes_live(standing_inert)

    def _history_is_pinned_before_the_sweeper_goes_live(self, standing_inert: bool) -> None:
        pinned = SnapshotRecord("pinned", "artifact", "full", "s3://pinned", 1)
        control = SnapshotRecord("control", "artifact", "full", "s3://control", 1)
        retention = mock.Mock()
        retention.scoped_sweeper_mutation.return_value = nullcontext()
        retention.standing_sweeper_is_inert.return_value = standing_inert
        retention.stage_smoke_sweeper.side_effect = (
            [_config()] if standing_inert else [_config(enabled=False, dry_run=True), _config()]
        )
        retention.pin_to_oldest.return_value = pinned
        retention.oldest_unpinned.return_value = control

        fixture = mock.Mock(retention=retention)
        fixture.phase.side_effect = lambda _name: nullcontext()
        fixture.run_command.return_value = SimpleNamespace(
            exit_code=0, stdout="restore-source-ok", stderr=""
        )

        def verify(condition: bool, message: str, **_evidence: object) -> None:
            if not condition:
                raise InvariantViolation(message)

        fixture.verify.side_effect = verify

        restore_source_guard.verify(cast(Any, fixture))

        calls = retention.method_calls
        held_off = calls.index(
            mock.call.standing_sweeper_is_inert(for_seconds=1800)
            if standing_inert
            else mock.call.stage_smoke_sweeper(enabled=False, dry_run=True)
        )
        first_history_check = calls.index(mock.call.wait_for_ready_fulls(1))
        last_history_check = calls.index(mock.call.wait_for_ready_fulls(3))
        pin = calls.index(mock.call.pin_to_oldest())
        live = calls.index(mock.call.stage_smoke_sweeper(enabled=True, dry_run=False))
        prune = calls.index(mock.call.expect_pruned(control, timeout=74))
        self.assertLess(held_off, first_history_check)
        self.assertLess(last_history_check, pin)
        self.assertLess(pin, live)
        self.assertLess(live, prune)
        retention.scoped_sweeper_mutation.assert_called_once_with()


class ClaimFinishForwardChoreography(unittest.TestCase):
    def test_covers_crashes_before_and_after_backing_byte_deletion(self) -> None:
        after_claim = SnapshotRecord(
            "after-claim", "artifact-1", "full", "s3://snapshots/after-claim", 1
        )
        after_byte_delete = SnapshotRecord(
            "after-byte-delete",
            "artifact-2",
            "full",
            "s3://snapshots/after-byte-delete",
            1,
        )
        retention = mock.Mock()
        retention.scoped_sweeper_mutation.return_value = nullcontext()
        retention.stage_smoke_sweeper.side_effect = [
            _config(enabled=False, dry_run=True),
            _config(),
        ]
        retention.orphan_claim_on_oldest.side_effect = [
            after_claim,
            after_byte_delete,
        ]
        retention.ready_full_snapshots.return_value = [mock.sentinel.snapshot] * 3

        fixture = mock.Mock(retention=retention)
        fixture.phase.side_effect = lambda _name: nullcontext()
        fixture.run_command.return_value = SimpleNamespace(
            exit_code=0, stdout="finish-forward-ok", stderr=""
        )

        def verify(condition: bool, message: str, **_evidence: object) -> None:
            if not condition:
                raise InvariantViolation(message)

        fixture.verify.side_effect = verify

        claim_finish_forward.verify(cast(Any, fixture))

        self.assertEqual(retention.orphan_claim_on_oldest.call_count, 2)
        retention.delete_backing_objects_for_crash.assert_called_once_with(
            after_byte_delete
        )
        self.assertEqual(
            retention.expect_pruned.call_args_list,
            [
                mock.call(after_claim, timeout=64),
                mock.call(after_byte_delete, timeout=64),
            ],
        )
        retention.scoped_sweeper_mutation.assert_called_once_with()


class KindOnlyEnforcement(unittest.TestCase):
    def test_deployed_backend_preflights_scenario_tools(self) -> None:
        @scenario(
            "ecw.deployed_tool_preflight",
            required_tools=("missing-deployed-tool",),
        )
        def deployed_tool_preflight(_test: Ecw) -> None:
            """A deployed scenario validates its declared tools before execution."""

        with (
            mock.patch(
                "space_smoke.services.importlib.util.find_spec", return_value=object()
            ),
            mock.patch("space_smoke.services.shutil.which", return_value=None),
            self.assertRaisesRegex(MissingToolError, "missing-deployed-tool"),
        ):
            SmokeServices.from_environment(
                deployed_tool_preflight,
                {
                    "SPACE_SMOKE_BACKEND": "deployed",
                    "E2B_API_KEY": "key",
                    "E2B_DOMAIN": "example.test",
                },
            )

    def test_deployed_backend_refuses_privileged_fixture(self) -> None:
        services = SmokeServices(
            kind=None,
            ecw=cast(Any, None),  # never reached: the refusal comes first
            api_key="k",
            owns_api_key=False,
        )
        with self.assertRaisesRegex(SmokeConfigurationError, "kind backend"):
            services.fixture(
                KindEcw,
                cast(Any, None),
                scenario_id=restore_source_guard.id,
                sandbox=restore_source_guard.sandbox,
            )


class PrivilegedSqlInterpolation(unittest.TestCase):
    def test_interpolations_refuse_non_uuid_and_odd_kinds(self) -> None:
        with self.assertRaises(ValueError):
            adapters._sql_uuid("1; DROP TABLE snapshots")
        with self.assertRaises(ValueError):
            adapters._sql_identifier_ish("fu ll")


class _LeaseRecorder:
    def __init__(self) -> None:
        self.lease: dict[str, Any] | None = None
        self.resource_version = 0

    def run(
        self,
        command: Any,
        *,
        input_text: str | None = None,
        timeout: float | None = None,
        env: Any = None,
    ) -> CommandResult:
        invocation = tuple(command)
        if "get" in invocation and "lease" in invocation:
            if self.lease is None:
                return CommandResult(invocation, 1, "", "Error from server (NotFound)")
            return CommandResult(invocation, 0, json.dumps(self.lease), "")
        if "create" in invocation:
            assert input_text is not None
            if self.lease is not None:
                return CommandResult(
                    invocation, 1, "", "Error from server (AlreadyExists)"
                )
            self.lease = json.loads(input_text)
            self.resource_version += 1
            self.lease["metadata"].update(
                {"uid": "lease-uid", "resourceVersion": str(self.resource_version)}
            )
            return CommandResult(invocation, 0, json.dumps(self.lease), "")
        if "patch" in invocation and "lease" in invocation:
            assert self.lease is not None
            patch = json.loads(invocation[invocation.index("--patch") + 1])
            for operation in patch:
                if operation["op"] != "test":
                    continue
                section, key = operation["path"].lstrip("/").split("/")
                if self.lease[section].get(key) != operation["value"]:
                    return CommandResult(invocation, 1, "", "Conflict")
            replacement = next(
                operation["value"]
                for operation in patch
                if operation["op"] == "replace" and operation["path"] == "/spec"
            )
            self.lease["spec"] = replacement
            self.resource_version += 1
            self.lease["metadata"]["resourceVersion"] = str(self.resource_version)
            return CommandResult(invocation, 0, json.dumps(self.lease), "")
        return CommandResult(invocation, 0, "", "")


class SweeperEnvOps(unittest.TestCase):
    """The env override is the one deployment write scenarios may reach; pin
    its allowlist, value vocabulary, and kubectl shape without a cluster."""

    def _harness(
        self, controller_env: str = ""
    ) -> tuple[adapters.KindHarness, list[tuple[str, ...]]]:
        calls: list[tuple[str, ...]] = []

        class Recorder:
            def run(
                self,
                command: Any,
                *,
                input_text: str | None = None,
                timeout: float | None = None,
                env: Any = None,
            ) -> CommandResult:
                calls.append(tuple(command))
                stdout = ""
                if "jsonpath={.items[0].metadata.name}" in command:
                    stdout = "space-cluster-controller"
                elif (
                    "jsonpath={.items[0].spec.template.spec.containers[0].env}"
                    in command
                ):
                    stdout = controller_env
                return CommandResult(tuple(command), 0, stdout, "")

        harness = adapters.KindHarness(
            cast(Any, Recorder()), adapters.KindConfig(), environment={}
        )
        return harness, calls

    def test_refuses_keys_outside_the_allowlist_before_kubectl(self) -> None:
        harness, calls = self._harness()
        with self.assertRaises(ValueError):
            harness.set_retention_sweeper_env({"SPACE_INTERNAL_TOKEN": "x"})
        self.assertEqual(calls, [])

    def test_refuses_values_outside_the_closed_vocabulary(self) -> None:
        harness, calls = self._harness()
        with self.assertRaises(ValueError):
            harness.set_retention_sweeper_env(
                {"SPACE_SNAPSHOT_RETENTION_SWEEPER_TICK_SECS": "5; rm -rf /"}
            )
        self.assertEqual(calls, [])

    def test_sets_removes_and_waits_for_the_rollout(self) -> None:
        harness, calls = self._harness()
        harness.set_retention_sweeper_env(
            {
                "SPACE_SNAPSHOT_RETENTION_SWEEPER_MIN_AGE_SECS": "60",
                "SPACE_SNAPSHOT_RETENTION_SWEEPER_TICK_SECS": None,
            }
        )
        set_env = next(c for c in calls if "set" in c and "env" in c)
        self.assertIn("SPACE_SNAPSHOT_RETENTION_SWEEPER_MIN_AGE_SECS=60", set_env)
        self.assertIn("SPACE_SNAPSHOT_RETENTION_SWEEPER_TICK_SECS-", set_env)
        rollout_positions = [i for i, c in enumerate(calls) if "rollout" in c]
        self.assertTrue(rollout_positions)
        self.assertLess(calls.index(set_env), rollout_positions[-1])

    def test_refuses_nonliteral_or_empty_sweeper_env(self) -> None:
        invalid_entries = [
            (
                '[{"name":"SPACE_SNAPSHOT_RETENTION_SWEEPER_ENABLED",'
                '"valueFrom":{"secretKeyRef":{"name":"s","key":"k"}}}]'
            ),
            (
                '[{"name":"SPACE_SNAPSHOT_RETENTION_SWEEPER_KEEP_N",'
                '"valueFrom":{"secretKeyRef":{"name":"s","key":"k"}}}]'
            ),
            '[{"name":"SPACE_SNAPSHOT_RETENTION_SWEEPER_TICK_SECS","value":""}]',
        ]
        for controller_env in invalid_entries:
            with self.subTest(controller_env=controller_env):
                harness, calls = self._harness(controller_env)
                with self.assertRaises(SmokeConfigurationError):
                    harness.retention_sweeper_env()
                self.assertFalse(any("set" in call and "env" in call for call in calls))

    def test_config_boolean_parsing_matches_cluster_controller(self) -> None:
        for enabled, dry_run, expected in [
            ("1", "0", (True, False)),
            ("TRUE", "False", (True, False)),
            ("false", "True", (False, True)),
        ]:
            with self.subTest(enabled=enabled, dry_run=dry_run):
                controller_env = json.dumps(
                    [
                        {
                            "name": "SPACE_SNAPSHOT_RETENTION_SWEEPER_ENABLED",
                            "value": enabled,
                        },
                        {
                            "name": "SPACE_SNAPSHOT_RETENTION_SWEEPER_DRY_RUN",
                            "value": dry_run,
                        },
                    ]
                )
                harness, _ = self._harness(controller_env)
                config = harness.retention_sweeper_config()
                self.assertEqual((config.enabled, config.dry_run), expected)
                harness.set_retention_sweeper_env(harness.retention_sweeper_env())

    def test_config_numeric_parsing_matches_cluster_controller(self) -> None:
        keys = (
            "SPACE_SNAPSHOT_RETENTION_SWEEPER_KEEP_N",
            "SPACE_SNAPSHOT_RETENTION_SWEEPER_TICK_SECS",
            "SPACE_SNAPSHOT_RETENTION_SWEEPER_MIN_AGE_SECS",
        )
        for values, expected in [
            (("5", "7", "11"), (5, 7, 11)),
            (("0", "0", "0"), (3, 300, 0)),
            (("invalid", "-1", "invalid"), (3, 300, 3600)),
            (
                (str(1 << 32), str(1 << 64), str(1 << 64)),
                (3, 300, 3600),
            ),
        ]:
            with self.subTest(values=values):
                controller_env = json.dumps(
                    [
                        {"name": key, "value": value}
                        for key, value in zip(keys, values, strict=True)
                    ]
                )
                harness, _ = self._harness(controller_env)
                config = harness.retention_sweeper_config()
                self.assertEqual(
                    (config.keep_n, config.tick_seconds, config.min_age_seconds),
                    expected,
                )

    def test_restore_preserves_a_controller_defaulted_boolean(self) -> None:
        controller_env = json.dumps(
            [
                {
                    "name": "SPACE_SNAPSHOT_RETENTION_SWEEPER_ENABLED",
                    "value": "yes",
                }
            ]
        )
        harness, calls = self._harness(controller_env)

        harness.set_retention_sweeper_env(
            {"SPACE_SNAPSHOT_RETENTION_SWEEPER_ENABLED": "true"}
        )
        harness.restore_retention_sweeper_env(harness.retention_sweeper_env())

        set_env_calls = [c for c in calls if "set" in c and "env" in c]
        self.assertEqual(len(set_env_calls), 2)
        self.assertIn("SPACE_SNAPSHOT_RETENTION_SWEEPER_ENABLED=yes", set_env_calls[-1])

    def test_snapshot_listing_selects_one_storage_route(self) -> None:
        class Recorder:
            def run(self, command: Any, **_options: Any) -> CommandResult:
                rows = (
                    "00000000-0000-0000-0000-000000000001|"
                    "00000000-0000-0000-0000-000000000002|full|"
                    "s3://snapshots/one|1\n"
                    "00000000-0000-0000-0000-000000000004||||0"
                )
                return CommandResult(tuple(command), 0, rows, "")

        harness = adapters.KindHarness(
            cast(Any, Recorder()), adapters.KindConfig(), environment={}
        )
        records = harness.ready_full_snapshots("00000000-0000-0000-0000-000000000003")
        self.assertEqual(len(records), 2, "all ready/full rows count toward keep-N")
        self.assertEqual(records[0].storage_uri, "s3://snapshots/one")
        self.assertEqual(records[0].storage_location_count, 1)
        self.assertEqual(records[1].storage_location_count, 0)

    def test_storage_operations_use_one_scoped_prefix_and_isolated_credentials(
        self,
    ) -> None:
        calls: list[tuple[tuple[str, ...], dict[str, str | None], float]] = []

        class Recorder:
            def run(self, command: Any, **options: Any) -> CommandResult:
                calls.append((tuple(command), dict(options["env"]), options["timeout"]))
                return CommandResult(tuple(command), 0, '{"KeyCount": 1}', "")

        harness = adapters.KindHarness(
            cast(Any, Recorder()), adapters.KindConfig(), environment={}
        )
        record = SnapshotRecord(
            snapshot_id="00000000-0000-0000-0000-000000000001",
            artifact_id="00000000-0000-0000-0000-000000000002",
            artifact_kind="full",
            storage_uri="s3://snapshots/one",
            storage_location_count=1,
        )

        self.assertTrue(harness.snapshot_storage_exists(record, timeout=3))
        harness.delete_snapshot_storage(record)
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0][0][calls[0][0].index("--prefix") + 1], "one/")
        self.assertIn("s3://snapshots/one/", calls[1][0])
        self.assertIn("--recursive", calls[1][0])
        for _command, env, timeout in calls:
            self.assertIsNone(env["AWS_PROFILE"])
            self.assertEqual(env["AWS_ACCESS_KEY_ID"], "localadmin")
            self.assertGreater(timeout, 0)
        self.assertLessEqual(calls[0][2], 3)
        self.assertEqual(calls[1][2], 30)

    def test_recursive_storage_delete_refuses_a_remote_s3_endpoint(self) -> None:
        calls: list[tuple[str, ...]] = []

        class Recorder:
            def run(self, command: Any, **_options: Any) -> CommandResult:
                calls.append(tuple(command))
                return CommandResult(tuple(command), 0, "", "")

        harness = adapters.KindHarness(
            cast(Any, Recorder()),
            adapters.KindConfig(),
            environment={
                "S3_ENDPOINT": "https://objects.example.com",
                "SPACE_SMOKE_ALLOW_REMOTE_API": "1",
            },
        )
        record = SnapshotRecord(
            snapshot_id="00000000-0000-0000-0000-000000000001",
            artifact_id="00000000-0000-0000-0000-000000000002",
            artifact_kind="full",
            storage_uri="s3://snapshots/one",
            storage_location_count=1,
        )

        with self.assertRaisesRegex(
            SmokeConfigurationError, "SPACE_SMOKE_ALLOW_REMOTE_S3_DELETE"
        ):
            harness.delete_snapshot_storage(record)
        self.assertEqual(calls, [])

        opted_in = adapters.KindHarness(
            cast(Any, Recorder()),
            adapters.KindConfig(),
            environment={
                "S3_ENDPOINT": "https://objects.example.com",
                "SPACE_SMOKE_ALLOW_REMOTE_S3_DELETE": "1",
            },
        )
        opted_in.delete_snapshot_storage(record)
        self.assertEqual(len(calls), 1)

    def test_mutation_lease_uses_kubernetes_microtime_on_the_wire(self) -> None:
        # coordination.k8s.io/v1 Lease timestamps require six fractional digits,
        # including when the timestamp falls exactly on a whole second.
        for micros, expected in (
            (0, "2026-09-08T14:33:41.000000Z"),
            (123456, "2026-09-08T14:33:41.123456Z"),
        ):
            with self.subTest(microseconds=micros):
                recorder = _LeaseRecorder()
                harness = adapters.KindHarness(
                    cast(Any, recorder), adapters.KindConfig(), environment={}
                )
                with mock.patch(
                    "space_smoke.adapters.datetime", wraps=datetime
                ) as clock:
                    clock.now.return_value = datetime(
                        2026, 9, 8, 14, 33, 41, micros, tzinfo=UTC
                    )
                    release = harness.acquire_retention_sweeper_mutation()
                assert recorder.lease is not None
                for field in ("acquireTime", "renewTime"):
                    self.assertEqual(recorder.lease["spec"][field], expected)
                release()

    def test_retention_controller_mutation_contends_across_users(self) -> None:
        recorder = _LeaseRecorder()
        first = adapters.KindHarness(
            cast(Any, recorder), adapters.KindConfig(), environment={}
        )
        second = adapters.KindHarness(
            cast(Any, recorder), adapters.KindConfig(), environment={}
        )
        with mock.patch("space_smoke.adapters.os.getuid", return_value=1001):
            release_first = first.acquire_retention_sweeper_mutation()
        assert recorder.lease is not None
        # Kubernetes Lease MicroTime requires exactly six fractional digits.
        for field in ("acquireTime", "renewTime"):
            self.assertRegex(
                recorder.lease["spec"][field],
                r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$",
            )
        self.assertIn("uid=1001", recorder.lease["spec"]["holderIdentity"])
        with (
            mock.patch("space_smoke.adapters.os.getuid", return_value=1002),
            self.assertRaisesRegex(SmokeConfigurationError, "another smoke"),
        ):
            second.acquire_retention_sweeper_mutation()
        release_first()
        with mock.patch("space_smoke.adapters.os.getuid", return_value=1002):
            release_second = second.acquire_retention_sweeper_mutation()
        self.assertIn("uid=1002", recorder.lease["spec"]["holderIdentity"])
        release_second()

    def test_stale_lease_takeover_fences_the_previous_release(self) -> None:
        recorder = _LeaseRecorder()
        first = adapters.KindHarness(
            cast(Any, recorder), adapters.KindConfig(), environment={}
        )
        second = adapters.KindHarness(
            cast(Any, recorder), adapters.KindConfig(), environment={}
        )
        release_first = first.acquire_retention_sweeper_mutation()
        assert recorder.lease is not None
        recorder.lease["spec"]["renewTime"] = "2000-01-01T00:00:00Z"
        recorder.lease["spec"]["leaseDurationSeconds"] = 1
        release_second = second.acquire_retention_sweeper_mutation()
        second_holder = recorder.lease["spec"]["holderIdentity"]

        with self.assertRaisesRegex(SmokeConfigurationError, "live mutation Lease"):
            first.restore_retention_sweeper_env(
                {"SPACE_SNAPSHOT_RETENTION_SWEEPER_ENABLED": "true"}
            )
        with self.assertRaisesRegex(RuntimeError, "another caller"):
            release_first()
        self.assertEqual(recorder.lease["spec"]["holderIdentity"], second_holder)
        release_second()

    def test_claim_finish_forward_requires_aws_cli(self) -> None:
        def available(tool: str) -> str | None:
            return "/usr/bin/kubectl" if tool == "kubectl" else None

        with (
            mock.patch(
                "space_smoke.services.importlib.util.find_spec", return_value=object()
            ),
            mock.patch("space_smoke.services.shutil.which", side_effect=available),
            self.assertRaisesRegex(MissingToolError, "aws"),
        ):
            SmokeServices.from_environment(claim_finish_forward, {})


class ClaimRouteCardinality(unittest.TestCase):
    def test_pin_selects_the_oldest_row_with_an_artifact_identity(self) -> None:
        rows = [
            SnapshotRecord("oldest", "", "", "", 0),
            SnapshotRecord("pinnable", "artifact", "full", "", 0),
            SnapshotRecord("newest", "new-artifact", "full", "", 0),
        ]

        class Control:
            pinned: SnapshotRecord | None = None

            @staticmethod
            def ready_full_snapshots(_sandbox_id: str) -> list[SnapshotRecord]:
                return rows

            def pin_current_artifact(
                self, _sandbox_id: str, record: SnapshotRecord
            ) -> bool:
                self.pinned = record
                return True

        class Fixture:
            sandbox = SimpleNamespace(sandbox_id="sandbox")

            @staticmethod
            def verify(condition: bool, message: str, **_evidence: object) -> None:
                if not condition:
                    raise InvariantViolation(message)

        control = Control()
        retention = RetentionAssertions(
            cast(Any, Fixture()), cast(Any, control), ResourceScope()
        )

        pinned = retention.pin_to_oldest()
        self.assertEqual(pinned, rows[1])
        self.assertEqual(control.pinned, rows[1])
        self.assertEqual(
            retention.oldest_unpinned(pinned),
            rows[0],
            "the negative control must be selected relative to the returned pin",
        )

    def test_orphan_injection_refuses_the_oldest_row_without_exactly_one_route(
        self,
    ) -> None:
        rows = [
            SnapshotRecord("oldest", "artifact", "full", "", 2),
            SnapshotRecord("newer", "artifact", "full", "s3://snapshots/newer", 1),
        ]

        class Control:
            marked = False

            @staticmethod
            def ready_full_snapshots(_sandbox_id: str) -> list[SnapshotRecord]:
                return rows

            def mark_snapshot_deleting(self, _snapshot_id: str) -> bool:
                self.marked = True
                return True

        class Fixture:
            sandbox = SimpleNamespace(sandbox_id="sandbox")

            @staticmethod
            def verify(condition: bool, message: str, **_evidence: object) -> None:
                if not condition:
                    raise InvariantViolation(message)

        control = Control()
        retention = RetentionAssertions(
            cast(Any, Fixture()), cast(Any, control), ResourceScope()
        )

        with self.assertRaisesRegex(InvariantViolation, "exactly one"):
            retention.orphan_claim_on_oldest()
        self.assertFalse(control.marked)

    def test_backing_object_probe_requires_a_diagnostic_timeout_budget(self) -> None:
        record = SnapshotRecord("snapshot", "artifact", "full", "s3://snapshots/x", 1)

        class Control:
            probes = 0

            def snapshot_storage_exists(
                self, _record: SnapshotRecord, *, timeout: float
            ) -> bool:
                self.probes += 1
                return True

        class Fixture:
            sandbox = SimpleNamespace(sandbox_id="sandbox")

            @staticmethod
            def verify(condition: bool, message: str, **_evidence: object) -> None:
                if not condition:
                    raise InvariantViolation(message)

            @staticmethod
            def wait(_seconds: float) -> None:
                raise AssertionError("the probe should fail before waiting")

        control = Control()
        retention = RetentionAssertions(
            cast(Any, Fixture()), cast(Any, control), ResourceScope()
        )
        with (
            mock.patch(
                "space_smoke.fixtures.time.monotonic", side_effect=[100.0, 100.0]
            ),
            self.assertRaisesRegex(InvariantViolation, "prefix still contains data"),
        ):
            retention.expect_backing_objects_reaped(record, timeout=0.5)
        self.assertEqual(control.probes, 0)


def _config(**overrides: Any) -> RetentionSweeperConfig:
    values: dict[str, Any] = {
        "enabled": True,
        "dry_run": False,
        "keep_n": 1,
        "tick_seconds": 1,
        "min_age_seconds": 10,
    }
    values.update(overrides)
    return RetentionSweeperConfig(**values)


class _StubControl:
    """ClusterRetentionControl double for the ensure/restore choreography."""

    def __init__(
        self,
        configs: list[RetentionSweeperConfig],
        standing: dict[str, str | None],
    ) -> None:
        self._configs = list(configs)
        self._standing = standing
        self.applied: list[dict[str, str | None]] = []
        self.standing_reads = 0
        self.lock_acquisitions = 0
        self.lock_releases = 0
        self.failing_restores = 0
        self.events: list[str] = []

    def retention_sweeper_config(self) -> RetentionSweeperConfig:
        return self._configs.pop(0)

    def acquire_retention_sweeper_mutation(self) -> Any:
        self.lock_acquisitions += 1

        def release() -> None:
            self.lock_releases += 1
            self.events.append("unlock")

        return release

    def retention_sweeper_env(self) -> dict[str, str | None]:
        self.standing_reads += 1
        return dict(self._standing)

    def set_retention_sweeper_env(self, overrides: Any) -> None:
        self.applied.append(dict(overrides))

    def restore_retention_sweeper_env(self, standing: Any) -> None:
        if self.failing_restores:
            self.failing_restores -= 1
            self.events.append("restore failed")
            raise RuntimeError("rollout did not finish")
        self.events.append("restore")
        self.applied.append(dict(standing))


class EnsureLiveSweeper(unittest.TestCase):
    STANDING: ClassVar[dict[str, str | None]] = {
        "SPACE_SNAPSHOT_RETENTION_SWEEPER_ENABLED": "true",
        "SPACE_SNAPSHOT_RETENTION_SWEEPER_DRY_RUN": None,
        "SPACE_SNAPSHOT_RETENTION_SWEEPER_TICK_SECS": None,
        "SPACE_SNAPSHOT_RETENTION_SWEEPER_MIN_AGE_SECS": "3600",
        "SPACE_SNAPSHOT_RETENTION_SWEEPER_KEEP_N": None,
    }

    def _retention(
        self, control: _StubControl
    ) -> tuple[RetentionAssertions, ResourceScope]:
        scope = ResourceScope()
        return RetentionAssertions(cast(Any, None), cast(Any, control), scope), scope

    def test_smoke_overrides_satisfy_the_fixture_bounds(self) -> None:
        env = SMOKE_SWEEPER_ENV
        self.assertEqual(set(env), set(adapters.RETENTION_SWEEPER_ENV_KEYS))
        self.assertEqual(env["SPACE_SNAPSHOT_RETENTION_SWEEPER_ENABLED"], "true")
        self.assertEqual(env["SPACE_SNAPSHOT_RETENTION_SWEEPER_DRY_RUN"], "false")
        tick = env["SPACE_SNAPSHOT_RETENTION_SWEEPER_TICK_SECS"]
        grace = env["SPACE_SNAPSHOT_RETENTION_SWEEPER_MIN_AGE_SECS"]
        assert tick is not None and grace is not None
        self.assertLessEqual(int(tick), 30)
        self.assertTrue(1 <= int(grace) <= 300)

    def test_scopes_the_override_and_restores_the_standing_env(self) -> None:
        control = _StubControl(
            configs=[
                _config(dry_run=True, tick_seconds=300, min_age_seconds=3600),
                _config(),
            ],
            standing=dict(self.STANDING),
        )
        retention, scope = self._retention(control)
        config = retention.ensure_live_sweeper()
        self.assertEqual(config.min_age_seconds, 10)
        self.assertEqual(control.applied, [SMOKE_SWEEPER_ENV])
        # The body restores eagerly; teardown must not restore a second time.
        retention.restore_standing_sweeper()
        self.assertEqual(control.applied, [SMOKE_SWEEPER_ENV, self.STANDING])
        self.assertEqual(scope.close(), ())
        self.assertEqual(control.applied, [SMOKE_SWEEPER_ENV, self.STANDING])
        self.assertEqual((control.lock_acquisitions, control.lock_releases), (1, 1))

    def test_exception_restores_before_resource_scope_cleanup(self) -> None:
        control = _StubControl(
            configs=[
                _config(dry_run=True, tick_seconds=300, min_age_seconds=3600),
                _config(),
            ],
            standing=dict(self.STANDING),
        )
        retention, scope = self._retention(control)

        with (
            self.assertRaisesRegex(RuntimeError, "scenario failed"),
            retention.scoped_sweeper_mutation(),
        ):
            retention.ensure_live_sweeper()
            raise RuntimeError("scenario failed")

        self.assertEqual(control.applied, [SMOKE_SWEEPER_ENV, self.STANDING])
        self.assertEqual((control.lock_acquisitions, control.lock_releases), (1, 1))
        self.assertEqual(scope.close(), ())

    def test_disabled_to_live_staging_preserves_the_original_restore(self) -> None:
        control = _StubControl(
            configs=[
                _config(dry_run=True, tick_seconds=300, min_age_seconds=3600),
                _config(enabled=False, dry_run=True),
                _config(enabled=False, dry_run=True),
                _config(),
            ],
            standing=dict(self.STANDING),
        )
        retention, scope = self._retention(control)

        disabled = retention.stage_smoke_sweeper(enabled=False, dry_run=True)
        live = retention.stage_smoke_sweeper(enabled=True, dry_run=False)
        self.assertFalse(disabled.enabled)
        self.assertTrue(live.enabled)
        self.assertEqual(control.standing_reads, 1)
        self.assertEqual(
            [
                (
                    env["SPACE_SNAPSHOT_RETENTION_SWEEPER_ENABLED"],
                    env["SPACE_SNAPSHOT_RETENTION_SWEEPER_DRY_RUN"],
                )
                for env in control.applied
            ],
            [("false", "true"), ("true", "false")],
        )

        self.assertEqual(scope.close(), ())
        self.assertEqual(control.applied[-1], self.STANDING)
        self.assertEqual((control.lock_acquisitions, control.lock_releases), (1, 1))

    def test_eager_restore_allows_a_later_scope_to_record_a_fresh_restore(self) -> None:
        standing = _config(dry_run=True, tick_seconds=300, min_age_seconds=3600)
        control = _StubControl(
            configs=[standing, _config(), standing, _config()],
            standing=dict(self.STANDING),
        )
        retention, scope = self._retention(control)

        retention.ensure_live_sweeper()
        retention.restore_standing_sweeper()
        retention.ensure_live_sweeper()
        retention.restore_standing_sweeper()

        self.assertEqual(control.standing_reads, 2)
        self.assertEqual(
            control.applied,
            [SMOKE_SWEEPER_ENV, self.STANDING, SMOKE_SWEEPER_ENV, self.STANDING],
        )
        self.assertEqual(scope.close(), ())
        self.assertEqual((control.lock_acquisitions, control.lock_releases), (2, 2))

    def test_leaves_an_already_smoke_sized_sweeper_untouched(self) -> None:
        control = _StubControl(configs=[_config()], standing=dict(self.STANDING))
        retention, scope = self._retention(control)
        retention.ensure_live_sweeper()
        retention.restore_standing_sweeper()
        self.assertEqual(scope.close(), ())
        self.assertEqual(control.applied, [])

    def test_a_failed_restore_keeps_the_lease_until_a_retry_lands(self) -> None:
        control = _StubControl(
            configs=[_config(dry_run=True, tick_seconds=300, min_age_seconds=3600), _config()],
            standing=dict(self.STANDING),
        )
        control.failing_restores = 2
        retention, scope = self._retention(control)

        with self.assertRaisesRegex(RuntimeError, "rollout"), retention.scoped_sweeper_mutation():
            retention.ensure_live_sweeper()
            retention.restore_standing_sweeper()

        # The body's restore and the unwind's retry both failed; neither let go
        # of the Lease, so the scope's retry still runs under it.
        self.assertEqual(control.events, ["restore failed", "restore failed"])
        self.assertEqual(scope.close(), ())
        self.assertEqual(control.events, ["restore failed", "restore failed", "restore", "unlock"])
        self.assertEqual(control.applied[-1], self.STANDING)

    def test_still_restores_when_the_override_does_not_take(self) -> None:
        control = _StubControl(
            configs=[_config(dry_run=True), _config(dry_run=True)],
            standing=dict(self.STANDING),
        )
        retention, scope = self._retention(control)
        with self.assertRaises(SmokeConfigurationError):
            retention.ensure_live_sweeper()
        self.assertEqual(scope.close(), ())
        self.assertEqual(control.applied[-1], self.STANDING)


if __name__ == "__main__":
    unittest.main()

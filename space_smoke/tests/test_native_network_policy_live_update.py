from __future__ import annotations

import unittest
from collections.abc import Mapping
from contextlib import nullcontext
from typing import cast
from unittest import mock

from space_smoke.capabilities import CommandResult
from space_smoke.native import NativeConfig, NativeSandboxFactory
from space_smoke.scenarios import REGISTRY
from space_smoke.scenarios.native_network_policy_live_update import (
    BLOCKED,
    FIRST,
    REACHABLE,
    SECOND,
    network_policy_live_update,
)


class _Violation(AssertionError):
    pass


class _FakeGuest:
    """A running sandbox whose egress follows the last applied allow list."""

    def __init__(self, *, enforce: bool = True, reboot_on_update: bool = False):
        self.enforce = enforce
        self.reboot_on_update = reboot_on_update
        self.boot_id = "boot-1"
        self.desired: dict[str, object] | None = None
        self.enforced_allow: set[str] = {FIRST, SECOND}
        self.updates: list[Mapping[str, object]] = []

    # Sandbox surface used by the scenario.
    def set_network_policy(self, policy: Mapping[str, object]) -> None:
        self.updates.append(policy)
        self.desired = dict(policy)
        if self.enforce:
            egress = cast(Mapping[str, object], policy["egress"])
            rules = cast(list[Mapping[str, str]], egress["allow"])
            self.enforced_allow = {rule["ip"] for rule in rules}
        if self.reboot_on_update:
            self.boot_id = f"boot-{len(self.updates) + 1}"

    def get_network_policy(self) -> dict[str, object]:
        assert self.desired is not None
        return self.desired

    def run(self, command: str) -> CommandResult:
        if "boot_id" in command:
            return CommandResult(stdout=f"{self.boot_id}\n", stderr="", exit_code=0)
        for ip in (FIRST, SECOND):
            if f"https://{ip}/" in command:
                state = REACHABLE if ip in self.enforced_allow else BLOCKED
                return CommandResult(stdout=state, stderr="", exit_code=0)
        raise AssertionError(f"unexpected command {command!r}")


class _FakeFixture:
    def __init__(self, guest: _FakeGuest) -> None:
        self.sandbox = guest
        self.records: dict[str, object] = {}
        self.phases: list[str] = []

    def phase(self, name: str) -> nullcontext[None]:
        self.phases.append(name)
        return nullcontext()

    def record(self, name: str, value: object) -> None:
        self.records[name] = value

    def verify(self, condition: bool, message: str, **_: object) -> None:
        if not condition:
            raise _Violation(message)

    def run_command(self, command: str, **_: object) -> CommandResult:
        return self.sandbox.run(command)

    run_command_when_ready = run_command

    def wait_until_command_matches(
        self, command: str, *, stdout: str, failure_message: str, **_: object
    ) -> CommandResult:
        result = self.sandbox.run(command)
        self.verify(result.stdout == stdout, failure_message)
        return result


class NativeNetworkPolicyLiveUpdateTest(unittest.TestCase):
    def test_registered_for_native_implicit_selection(self) -> None:
        self.assertIs(
            REGISTRY.get("native.network_policy_live_update"),
            network_policy_live_update,
        )
        self.assertEqual(network_policy_live_update.api_surfaces, frozenset({"native"}))
        self.assertIsNone(network_policy_live_update.templates)
        self.assertIsNone(network_policy_live_update.quarantined)
        self.assertIsNone(network_policy_live_update.ignored)

    def test_two_live_updates_swap_reachability_without_reboot(self) -> None:
        guest = _FakeGuest()
        network_policy_live_update.verify(cast(object, _FakeFixture(guest)))
        self.assertEqual(
            [
                cast(Mapping[str, list[object]], update["egress"])["allow"]
                for update in guest.updates
            ],
            [[{"ip": FIRST}], [{"ip": SECOND}]],
        )

    def test_stale_enforcement_fails(self) -> None:
        with self.assertRaisesRegex(_Violation, "stayed reachable"):
            network_policy_live_update.verify(
                cast(object, _FakeFixture(_FakeGuest(enforce=False)))
            )

    def test_restart_instead_of_live_apply_fails(self) -> None:
        with self.assertRaisesRegex(_Violation, "restarted the guest"):
            network_policy_live_update.verify(
                cast(object, _FakeFixture(_FakeGuest(reboot_on_update=True)))
            )

    def test_adapter_reads_network_policy_document(self) -> None:
        factory = NativeSandboxFactory(
            NativeConfig(
                api_url="http://127.0.0.1:8080",
                data_plane_url="http://127.0.0.1:8081",
                token="token",
                template="seed:latest",
            )
        )
        with mock.patch.object(
            factory, "_json_request", return_value={"version": "v1"}
        ) as request:
            self.assertEqual(factory.get_network_policy("sbx/id"), {"version": "v1"})
        request.assert_called_once_with(
            "GET",
            "/v1/sandboxes/sbx%2Fid/network-policy",
            expected_status=200,
        )


if __name__ == "__main__":
    unittest.main()

"""Sharding preserves the discovered inventory and scenario failure semantics."""

from contextlib import redirect_stdout
from io import StringIO
from unittest import mock

import pytest

from space_smoke.__main__ import _run_all, main
from space_smoke.scenarios import REGISTRY
from space_smoke.suite import ScenarioProcessResult


@pytest.mark.parametrize("surface", ["ecw", "native"])
def test_shards_run_every_eligible_scenario_once_and_propagate_failures(surface):
    inventory = {
        scenario.id
        for scenario in REGISTRY
        if surface in scenario.api_surfaces
        and "kind" in scenario.backends
        and scenario.quarantined is None
        and scenario.ignored is None
        and scenario.applies_to(None)
    }
    seen = []

    def execute(scenarios, **kwargs):
        seen.extend(scenario.id for scenario in scenarios)
        assert kwargs["environment"]["SKIP_BOOTSTRAP"] == "1"
        return tuple(
            ScenarioProcessResult(s.id, 1 if i == 0 else 0, "", "")
            for i, s in enumerate(scenarios)
        )

    with (
        mock.patch.dict(
            "os.environ",
            {"SPACE_SMOKE_BACKEND": "kind", "SPACE_SMOKE_API_SURFACE": surface},
            clear=True,
        ),
        mock.patch(
            "space_smoke.__main__.SmokeServices.from_environment",
            **{
                "return_value.telemetry_mode.return_value": "otel",
                "return_value.storage_credentials.return_value": "exchange",
                "return_value.substrate.return_value": "kubernetes",
            },
        ),
        mock.patch("space_smoke.__main__.run_scenarios", side_effect=execute),
        redirect_stdout(StringIO()),
    ):
        for index in range(1, 5):
            assert _run_all(shard=(index, 4)) == 1
    assert set(seen) == inventory
    assert len(seen) == len(inventory)


@pytest.mark.parametrize("shard", ["0/2", "3/2", "1/0", "1", "a/2", "1/2/3"])
def test_invalid_shard_fails_before_bootstrap(shard):
    with (
        mock.patch("sys.argv", ["space-smoke", "run", "--all", "--shard", shard]),
        mock.patch("space_smoke.__main__.SmokeServices.from_environment") as bootstrap,
        pytest.raises(SystemExit) as raised,
    ):
        main()
    assert raised.value.code == 2
    bootstrap.assert_not_called()


def test_shard_cli_routes_to_the_selected_inventory():
    with (
        mock.patch("sys.argv", ["space-smoke", "run", "--all", "--shard", "2/3"]),
        mock.patch("space_smoke.__main__._run_all", return_value=42) as run,
    ):
        assert main() == 42
    run.assert_called_once_with(shard=(2, 3))


def test_shard_cannot_silently_ignore_a_named_scenario():
    with (
        mock.patch(
            "sys.argv",
            ["space-smoke", "run", "ecw.command_round_trip", "--shard", "1/2"],
        ),
        pytest.raises(SystemExit) as raised,
    ):
        main()
    assert raised.value.code == 2


def test_empty_shard_does_not_bootstrap_or_report_green():
    count = len(tuple(REGISTRY)) + 1
    with (
        mock.patch.dict("os.environ", {}, clear=True),
        mock.patch("space_smoke.__main__.SmokeServices.from_environment") as bootstrap,
    ):
        assert _run_all(shard=(count, count)) == 2
    bootstrap.assert_not_called()

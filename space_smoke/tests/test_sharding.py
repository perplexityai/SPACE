"""Sharding preserves the discovered inventory and scenario failure semantics."""

from contextlib import redirect_stdout
from io import StringIO
from types import SimpleNamespace
from unittest import mock

import pytest

from space_smoke.__main__ import _run_all, main
from space_smoke.scenarios import REGISTRY
from space_smoke.suite import ScenarioProcessResult


def _inventory(surface):
    return {
        scenario.id
        for scenario in REGISTRY
        if surface in scenario.api_surfaces
        and "kind" in scenario.backends
        and scenario.quarantined is None
        and scenario.tier == "routine"
        and scenario.applies_to(None)
    }


# One more shard than the smaller surface has scenarios, so that surface always
# leaves a shard with nothing to run, whatever the inventory grows to.
_EMPTY_PASS_COUNT = min(len(_inventory(surface)) for surface in ("ecw", "native")) + 1


@pytest.mark.parametrize("count", [4, 8, 12, 14, _EMPTY_PASS_COUNT])
def test_shards_run_every_eligible_scenario_once_and_propagate_failures(count):
    """Each (surface, scenario) pair of a sharded run runs on exactly one shard."""

    seen = []
    empty_passes = []

    def execute(scenarios, **kwargs):
        surface = kwargs["environment"].get("SPACE_SMOKE_API_SURFACE")
        seen.extend((surface, scenario.id) for scenario in scenarios)
        assert kwargs["environment"]["SKIP_BOOTSTRAP"] == "1"
        return tuple(
            ScenarioProcessResult(s.id, 1 if i == 0 else 0, "", "")
            for i, s in enumerate(scenarios)
        )

    for surface in ("ecw", "native"):
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
                    "return_value.browser_unavailable.return_value": None,
                    "return_value.storage_credentials.return_value": "exchange",
                    "return_value.substrate.return_value": "kubernetes",
                },
            ) as bootstrap,
            mock.patch("space_smoke.__main__.run_scenarios", side_effect=execute),
            redirect_stdout(StringIO()),
        ):
            for index in range(1, count + 1):
                before = len(seen)
                bootstraps = bootstrap.call_count
                status = _run_all(shard=(index, count))
                # A pass whose share is empty skips its bootstrap and passes;
                # one that ran scenarios reports the first one's failure.
                if len(seen) == before:
                    assert status == 0
                    assert bootstrap.call_count == bootstraps
                    empty_passes.append((surface, index))
                else:
                    assert status == 1
    expected = {(surface, sid) for surface in ("ecw", "native") for sid in _inventory(surface)}
    assert len(seen) == len(expected)
    assert set(seen) == expected
    if count == _EMPTY_PASS_COUNT:
        assert empty_passes, "a shard with nothing to run was never exercised"


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


def test_the_joint_partition_is_lighter_than_balancing_each_surface_alone(monkeypatch):
    from space_smoke.sharding import select_shard

    # Each surface has one heavy and one light case. Independent balancing
    # stacks the heavy cases on shard one; joint balancing separates them.
    weights = {surface: {f"{surface}.heavy": 100, f"{surface}.light": 10}
               for surface in ("ecw", "native")}
    monkeypatch.setattr("space_smoke.sharding._DURATION_SECONDS", weights)
    pools = {surface: tuple(SimpleNamespace(id=name) for name in cases)
             for surface, cases in weights.items()}

    def loads(joint):
        return [sum(
            weights[surface][scenario.id]
            for surface, scenarios in pools.items()
            for scenario in select_shard(
                scenarios, index=index, count=2, api_surface=surface,
                inventories=pools if joint else None,
            )
        ) for index in (1, 2)]

    assert loads(joint=False) == [200, 20]
    assert loads(joint=True) == [110, 110]

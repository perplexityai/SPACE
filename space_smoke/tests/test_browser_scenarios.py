"""The runner's selection of the scenarios that drive a browser."""

from __future__ import annotations

from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from unittest import mock

import pytest

from space_smoke.__main__ import _run_all
from space_smoke.registry import ScenarioRegistry
from space_smoke.runtime import ResourceScope, ScenarioContext, SmokeConfigurationError
from space_smoke.scenarios import REGISTRY
from space_smoke.suite import ScenarioProcessResult

BROWSER_IDS = {scenario.id for scenario in REGISTRY if scenario.browser is not None}
MISSING = "Playwright for Python is not installed (uv sync --frozen --python 3.11 --group console)"


def _services(browser_unavailable: str | None) -> mock.Mock:
    services = mock.Mock()
    services.telemetry_mode.return_value = "otel"
    services.storage_credentials.return_value = "exchange"
    services.substrate.return_value = "kubernetes"
    services.browser_unavailable.return_value = browser_unavailable
    return services


def _run(
    environment: dict[str, str],
    services: mock.Mock,
    *,
    registry: ScenarioRegistry = REGISTRY,
    shard: tuple[int, int] | None = None,
) -> tuple[int, tuple[str, ...], str, str]:
    """`_run_all` with every scenario passing: its status, what it ran, stdout and stderr."""

    ran: list[str] = []

    def execute(scenarios, **kwargs):
        ran.extend(scenario.id for scenario in scenarios)
        return tuple(ScenarioProcessResult(scenario.id, 0, "", "") for scenario in scenarios)

    stdout, stderr = StringIO(), StringIO()
    with (
        mock.patch.dict(
            "os.environ", {"SPACE_SMOKE_BACKEND": "kind", "SPACE_SMOKE_API_SURFACE": "ecw", **environment}, clear=True
        ),
        mock.patch("space_smoke.__main__.REGISTRY", registry),
        mock.patch("space_smoke.__main__.SmokeServices.from_environment", return_value=services),
        mock.patch("space_smoke.__main__.run_scenarios", side_effect=execute),
        redirect_stdout(stdout),
        redirect_stderr(stderr),
    ):
        status = _run_all(shard=shard)
    return status, tuple(ran), stdout.getvalue(), stderr.getvalue()


def test_the_registry_has_browser_scenarios_on_the_ecw_surface():
    assert BROWSER_IDS
    assert all(REGISTRY.get(scenario_id).api_surfaces == frozenset({"ecw"}) for scenario_id in BROWSER_IDS)


def test_withhold_leaves_them_out_visibly_without_probing_the_browser():
    services = _services(None)
    status, ran, stdout, _ = _run({"SPACE_SMOKE_BROWSER_SCENARIOS": "withhold"}, services)
    assert status == 0
    assert ran and not BROWSER_IDS & set(ran)
    for scenario_id in BROWSER_IDS:
        assert f"SKIPPED {scenario_id}: withheld from this run" in stdout
    services.browser_unavailable.assert_not_called()


@pytest.mark.parametrize("selection", [None, "run"])
def test_run_skips_them_visibly_where_the_browser_is_missing(selection):
    environment = {} if selection is None else {"SPACE_SMOKE_BROWSER_SCENARIOS": selection}
    status, ran, stdout, stderr = _run(environment, _services(MISSING))
    assert status == 0
    assert ran and not BROWSER_IDS & set(ran)
    for scenario_id in BROWSER_IDS:
        assert f"SKIPPED {scenario_id}: needs a chromium browser; {MISSING}" in stdout
    assert "ERROR" not in stderr


def test_require_fails_the_suite_where_the_browser_is_missing():
    status, ran, stdout, stderr = _run({"SPACE_SMOKE_BROWSER_SCENARIOS": "require"}, _services(MISSING))
    assert status == 2
    # The rest of the selection still runs and reports; the missing browser
    # is not a skip.
    assert ran and not BROWSER_IDS & set(ran)
    assert "SKIPPED local.console_" not in stdout
    assert "SPACE_SMOKE_BROWSER_SCENARIOS=require, but" in stderr
    assert MISSING in stderr
    for scenario_id in BROWSER_IDS:
        assert scenario_id in stderr


def test_require_fails_a_selection_of_only_browser_scenarios():
    registry = ScenarioRegistry(tuple(scenario for scenario in REGISTRY if scenario.browser is not None))
    status, ran, _, stderr = _run({"SPACE_SMOKE_BROWSER_SCENARIOS": "require"}, _services(MISSING), registry=registry)
    assert status == 2
    assert ran == ()
    assert "SPACE_SMOKE_BROWSER_SCENARIOS=require, but" in stderr


def test_require_runs_them_where_the_browser_launches():
    status, ran, _, _ = _run({"SPACE_SMOKE_BROWSER_SCENARIOS": "require"}, _services(None))
    assert status == 0
    assert BROWSER_IDS <= set(ran)


def test_an_unknown_selection_fails_before_the_bootstrap():
    services = _services(None)
    status, ran, _, stderr = _run({"SPACE_SMOKE_BROWSER_SCENARIOS": "skip"}, services)
    assert status == 2
    assert ran == ()
    assert "must be one of run, require, withhold" in stderr
    services.bootstrap.assert_not_called()


@pytest.mark.parametrize("count", [4, 12])
def test_withheld_shards_are_those_of_a_registry_without_them(count):
    """With the switch off, every shard runs what it runs on `main`.

    `main`'s registry is this one without the browser scenarios, which are
    all the change adds to it; their duration hints must not move any other
    scenario between shards on either surface.
    """

    without = ScenarioRegistry(tuple(scenario for scenario in REGISTRY if scenario.browser is None))
    withheld: dict[tuple[str, int], tuple[str, ...]] = {}
    absent: dict[tuple[str, int], tuple[str, ...]] = {}
    for surface in ("ecw", "native"):
        for index in range(1, count + 1):
            environment = {"SPACE_SMOKE_API_SURFACE": surface, "SPACE_SMOKE_BROWSER_SCENARIOS": "withhold"}
            _, withheld[surface, index], _, _ = _run(environment, _services(None), shard=(index, count))
            environment = {"SPACE_SMOKE_API_SURFACE": surface}
            _, absent[surface, index], _, _ = _run(
                environment, _services(None), registry=without, shard=(index, count)
            )
    assert withheld == absent
    assert all(withheld[surface, index] for surface in ("ecw",) for index in range(1, count + 1))


def test_a_scenario_run_by_name_without_its_browser_creates_no_fixture():
    scenario = REGISTRY.get(sorted(BROWSER_IDS)[0])
    services = _services(MISSING)
    with pytest.raises(SmokeConfigurationError, match="needs a chromium browser"):
        scenario.run(ScenarioContext(ResourceScope(), {}), services)
    services.fixture.assert_not_called()

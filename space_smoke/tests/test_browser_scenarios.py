"""The runner's browser selection and the console adapter's own judgement rules."""

from __future__ import annotations

import sys
import types
from contextlib import AbstractContextManager, redirect_stderr, redirect_stdout
from io import StringIO
from unittest import mock

import pytest

from space_smoke import console as console_module
from space_smoke.__main__ import _run_all
from space_smoke.console import (
    BrowserEvent,
    ConsoleAssertions,
    ConsoleSession,
    RecordedOperation,
    operations_match,
)
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


class _PlaywrightError(Exception):
    pass


def _playwright(*, start: Exception | None = None, launch: Exception | None = None) -> AbstractContextManager[object]:
    """A stand-in for `playwright.sync_api` whose driver start or browser launch fails."""

    module = types.ModuleType("playwright.sync_api")
    driver = mock.MagicMock()
    driver.chromium.launch.side_effect = launch
    manager = mock.MagicMock()
    manager.__enter__.side_effect = start
    manager.__enter__.return_value = driver
    module.Error = _PlaywrightError  # type: ignore[attr-defined]
    module.sync_playwright = mock.Mock(return_value=manager)  # type: ignore[attr-defined]
    package = types.ModuleType("playwright")
    package.sync_api = module  # type: ignore[attr-defined]
    return mock.patch.dict(sys.modules, {"playwright": package, "playwright.sync_api": module})


DRIVER_FAILURES = {
    "the driver cannot start": {"start": OSError("[Errno 2] No such file or directory: 'node'")},
    "the launch raises OSError": {"launch": OSError("[Errno 24] Too many open files")},
    "the launch raises Playwright's error": {"launch": _PlaywrightError("Executable doesn't exist at /x")},
}


@pytest.mark.parametrize("failure", DRIVER_FAILURES.values(), ids=DRIVER_FAILURES.keys())
def test_a_browser_that_fails_to_launch_is_unavailable_with_a_reason(failure):
    with _playwright(**failure):
        reason = console_module.browser_unavailable("chromium")
    assert reason is not None
    assert reason.startswith("Playwright's Chromium ")


def test_a_browser_that_launches_is_available():
    with _playwright():
        assert console_module.browser_unavailable("chromium") is None


@pytest.mark.parametrize("failure", DRIVER_FAILURES.values(), ids=DRIVER_FAILURES.keys())
@pytest.mark.parametrize("selection", [None, "run"])
def test_run_skips_them_when_the_browser_fails_to_launch(failure, selection):
    services = _services(None)
    services.browser_unavailable.side_effect = console_module.browser_unavailable
    environment = {} if selection is None else {"SPACE_SMOKE_BROWSER_SCENARIOS": selection}
    with _playwright(**failure):
        status, ran, stdout, stderr = _run(environment, services)
    assert status == 0
    assert ran and not BROWSER_IDS & set(ran)
    for scenario_id in BROWSER_IDS:
        assert f"SKIPPED {scenario_id}: needs a chromium browser; Playwright's Chromium " in stdout
    assert "ERROR" not in stderr


@pytest.mark.parametrize("failure", DRIVER_FAILURES.values(), ids=DRIVER_FAILURES.keys())
def test_require_fails_the_suite_when_the_browser_fails_to_launch(failure):
    services = _services(None)
    services.browser_unavailable.side_effect = console_module.browser_unavailable
    with _playwright(**failure):
        status, ran, stdout, stderr = _run({"SPACE_SMOKE_BROWSER_SCENARIOS": "require"}, services)
    assert status == 2
    assert ran and not BROWSER_IDS & set(ran)
    assert "SKIPPED local.console_" not in stdout
    assert "SPACE_SMOKE_BROWSER_SCENARIOS=require, but" in stderr
    for scenario_id in BROWSER_IDS:
        assert scenario_id in stderr


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


# --- console.py ---------------------------------------------------------------


class _Page:
    def __init__(self) -> None:
        self.url = "about:blank"
        self.waits: list[float] = []
        self.visits: list[str] = []
        self.handlers: dict[str, object] = {}

    def on(self, event: str, handler: object) -> None:
        self.handlers[event] = handler

    def is_closed(self) -> bool:
        return False

    def wait_for_timeout(self, milliseconds: float) -> None:
        self.waits.append(milliseconds)

    def goto(self, url: str, **_: object) -> None:
        self.visits.append(url)


def _console() -> tuple[ConsoleAssertions, ConsoleSession, _Page]:
    control = mock.Mock()
    control.console_url.return_value = "http://127.0.0.1:3000"
    console = ConsoleAssertions(mock.Mock(), control, ResourceScope())
    page = _Page()
    session = ConsoleSession(console, page, "operator")  # type: ignore[arg-type]
    console._sessions.append(session)
    return console, session, page


def _not_found(path: str) -> BrowserEvent:
    return BrowserEvent("http", "404", path, path)


def test_an_expected_404_is_excused_only_inside_its_probe():
    console, session, page = _console()
    path = "/admin/api/v1/sandboxes/sbx_00000000-0000-4000-8000-000000000001"
    before, during, after = _not_found(path), _not_found(path), _not_found(path)
    console.events.append(before)
    with console._expecting_missing(session, path, "the probe reads a sandbox that is gone"):
        console.events.append(during)
    # Leaving the page ends the probe, so its polls cannot outlive it.
    assert page.visits == ["about:blank"]
    console.events.append(after)
    assert console.unexpected_events() == [before, after]
    assert console._allowed_counts() == {"the probe reads a sandbox that is gone": 1}


def test_the_baseline_is_excused_in_every_phase():
    console, _, _ = _console()
    snapshots = _not_found("/api/v1/sandboxes/sbx_00000000-0000-4000-8000-000000000001/snapshots")
    console.events.append(snapshots)
    assert console.unexpected_events() == []


def test_waits_with_a_page_open_deliver_the_browsers_events():
    console, _, page = _console()
    with mock.patch.object(console_module.time, "sleep") as sleep:
        console._wait(console_module.CANCEL_SETTLE_SECONDS)
    sleep.assert_not_called()
    assert page.waits == [console_module.CANCEL_SETTLE_SECONDS * 1000]


def test_waits_without_an_open_page_sleep():
    console, _, _ = _console()
    console._sessions.clear()
    with mock.patch.object(console_module.time, "sleep") as sleep:
        console._wait(0.5)
    sleep.assert_called_once_with(0.5)


def _operation(identifier: str, kind: str, state: str = "succeeded") -> RecordedOperation:
    return RecordedOperation(identifier, kind, state)


def test_operations_match_each_record_by_id_and_rendered_fields():
    recorded = [
        _operation("op-1", "create_sandbox"),
        _operation("op-2", "suspend_sandbox"),
        _operation("op-3", "restore_sandbox"),
        _operation("op-4", "suspend_sandbox"),
    ]
    assert operations_match(recorded, list(reversed(recorded)))
    # One of two records of a kind dropped: every kind is still shown.
    assert not operations_match(recorded, recorded[:-1])
    # One record shown twice in place of another of its kind.
    assert not operations_match(recorded, [*recorded[:-1], recorded[1]])
    # A record shown in a state the API no longer reports.
    assert not operations_match(recorded, [*recorded[:-1], _operation("op-4", "suspend_sandbox", "running")])
    assert operations_match([], [])


class _Row:
    def __init__(self, href: str) -> None:
        self.href = href

    @property
    def first(self) -> _Row:
        return self

    def locator(self, _selector: str) -> _Row:
        return self

    def get_attribute(self, _name: str) -> str:
        return self.href


class _Search:
    def __init__(self, listing: _Listing) -> None:
        self.listing = listing

    def fill(self, value: str) -> None:
        self.listing.typed = value


class _Rows:
    def __init__(self, listing: _Listing) -> None:
        self.listing = listing

    def all(self) -> list[_Row]:
        return [_Row(href) for href in self.listing.shown()]


class _Listing(_Page):
    """The sandbox list: the sandboxes it holds, narrowed by the typed text unless it ignores it."""

    def __init__(self, held: dict[str, str], *, filters: bool) -> None:
        super().__init__()
        self.held = held
        self.filters = filters
        self.typed = ""

    def shown(self) -> list[str]:
        return [
            f"/dashboard/sandboxes/{public_id}"
            for public_id, name in self.held.items()
            if not self.filters or not self.typed or self.typed in (public_id, name)
        ]

    def get_by_placeholder(self, _text: str) -> _Search:
        return _Search(self)

    def locator(self, _selector: str) -> _Rows:
        return _Rows(self)


def _search_console(listing: _Listing) -> tuple[ConsoleAssertions, ConsoleSession]:
    control = mock.Mock()
    control.console_url.return_value = "http://127.0.0.1:3000"
    fixture = mock.Mock()

    def verify(condition: bool, message: str, **_: object) -> None:
        if not condition:
            raise AssertionError(message)

    fixture.verify.side_effect = verify
    console = ConsoleAssertions(fixture, control, ResourceScope())
    listing.url = "http://127.0.0.1:3000/dashboard/sandboxes"
    session = ConsoleSession(console, listing, "operator")  # type: ignore[arg-type]
    console._sessions.append(session)
    return console, session


TARGET, OTHER = "sbx_00000000-0000-4000-8000-000000000001", "sbx_00000000-0000-4000-8000-000000000002"


def test_a_search_that_narrows_the_list_to_the_target_passes():
    listing = _Listing({TARGET: "target", OTHER: "other"}, filters=True)
    console, session = _search_console(listing)
    with mock.patch.object(console_module, "PAGE_TIMEOUT_MS", 50):
        console.expect_search_finds(session, "public id", TARGET, TARGET, OTHER)
        console.expect_search_finds(session, "name", "target", TARGET, OTHER)
    assert listing.typed == ""


def test_a_list_that_ignores_the_typed_id_fails_the_search():
    listing = _Listing({TARGET: "target", OTHER: "other"}, filters=False)
    console, session = _search_console(listing)
    with mock.patch.object(console_module, "PAGE_TIMEOUT_MS", 50):
        with pytest.raises(AssertionError, match="did not narrow both sandboxes"):
            console.expect_search_finds(session, "public id", TARGET, TARGET, OTHER)


def test_a_search_cannot_start_from_a_list_that_lacks_the_other_sandbox():
    listing = _Listing({TARGET: "target"}, filters=False)
    console, session = _search_console(listing)
    with mock.patch.object(console_module, "PAGE_TIMEOUT_MS", 50):
        with pytest.raises(AssertionError, match="did not show both sandboxes"):
            console.expect_search_finds(session, "public id", TARGET, TARGET, OTHER)

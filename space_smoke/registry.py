"""Scenario declaration, discovery, and inventory."""

from __future__ import annotations

import importlib
import inspect
import pkgutil
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from typing import Any, Generic, TypeVar, cast, get_type_hints

from space_smoke.config import SandboxConfig
from space_smoke.direct_node import DirectNodeRecoveryFixture
from space_smoke.fixtures import CommandSandboxFixture, Ecw, KindEcw
from space_smoke.native_hardening import NativeHardeningFixture
from space_smoke.native_recovery import KindNativeHardeningFixture
from space_smoke.runtime import ScenarioContext
from space_smoke.services import SmokeServices

# The fixture type is the scenario's capability declaration; requesting a
# privileged fixture narrows the backends the scenario may run on.
FIXTURE_BACKENDS: dict[type[Any], frozenset[str]] = {
    DirectNodeRecoveryFixture: frozenset({"deployed"}),
    NativeHardeningFixture: frozenset({"kind", "deployed"}),
    KindNativeHardeningFixture: frozenset({"kind"}),
    CommandSandboxFixture: frozenset({"kind", "deployed"}),
    Ecw: frozenset({"kind", "deployed"}),
    KindEcw: frozenset({"kind"}),
}

# The control-plane substrates a fixture can drive: the Kubernetes substrate
# (pod per sandbox) and the Direct substrate (node-local VMs on registered
# nodes, infra/space/kind/lib/substrate.sh). A fixture that needs no Kind-only
# Kubernetes wiring (Ecw, CommandSandboxFixture, NativeHardeningFixture) serves
# both; KindEcw and KindNativeHardeningFixture serve Kubernetes alone. A
# scenario narrows its fixture's set (`substrates=`) where a substrate does not
# support what it asserts by contract: one that asserts sandbox pods, or forks,
# narrows to kubernetes.
FIXTURE_SUBSTRATES: dict[type[Any], frozenset[str]] = {
    # A standalone node, outside any cluster control plane.
    DirectNodeRecoveryFixture: frozenset({"direct"}),
    NativeHardeningFixture: frozenset({"kubernetes", "direct"}),
    KindNativeHardeningFixture: frozenset({"kubernetes"}),
    CommandSandboxFixture: frozenset({"kubernetes", "direct"}),
    Ecw: frozenset({"kubernetes", "direct"}),
    KindEcw: frozenset({"kubernetes"}),
}
SUBSTRATES = frozenset({"kubernetes", "direct"})

Fixture = TypeVar("Fixture")


@dataclass(frozen=True)
class Scenario(Generic[Fixture]):
    id: str
    category: str
    title: str
    invariant: str
    fixture_name: str
    fixture_type: type[Fixture]
    sandbox: SandboxConfig
    sandbox_count: int
    templates: frozenset[str] | None
    rerun_environment: tuple[str, ...]
    required_tools: tuple[str, ...]
    api_surfaces: frozenset[str]
    timeout_minutes: int
    backends: frozenset[str]
    substrates: frozenset[str]
    quarantined: str | None
    ignored: str | None
    telemetry_mode: str | None
    storage_credentials: str | None
    test_hooks: str | None
    verify: Callable[[Fixture], None]
    source_module: str

    def run(self, context: ScenarioContext, services: SmokeServices) -> None:
        context.record("scenario_id", self.id)
        self.verify(
            services.fixture(
                self.fixture_type,
                context,
                scenario_id=self.id,
                sandbox=self.sandbox,
                sandbox_count=self.sandbox_count,
            )
        )

    def applies_to(self, template: str | None) -> bool:
        return self.templates != frozenset() and (
            template is None
            or self.templates is None
            or template.split(":", 1)[0] in self.templates
        )


def scenario(
    id: str,
    *,
    sandbox: SandboxConfig | None = None,
    sandboxes: int = 1,
    templates: Iterable[str] | None = None,
    rerun_environment: Iterable[str] = (),
    required_tools: Iterable[str] = (),
    api_surfaces: Iterable[str] = ("ecw",),
    quarantined: str | None = None,
    ignored: str | None = None,
    telemetry_mode: str | None = None,
    timeout_minutes: int | None = None,
    storage_credentials: str | None = None,
    test_hooks: str | None = None,
    substrates: Iterable[str] | None = None,
) -> Callable[[Callable[[Fixture], None]], Scenario[Fixture]]:
    """Declare a smoke; its typed parameter selects the prepared fixture.

    `quarantined` names the reason (a PR or issue) a scenario is withheld from
    implicit selection (`--all`, `--category`). A quarantined scenario still
    runs when named explicitly, so it can be fixed and re-proven in place.
    `ignored` withholds a proposed contract that is not yet agreed, or an
    opt-in scenario too slow or too invasive for the per-merge set. It stays
    explicitly runnable and visible in suite logs, outside the defect backlog.
    `telemetry_mode` names the Kind telemetry mode whose backends the scenario
    reads; implicit selection skips it, visibly, on a bench running another
    known mode that the runner did not ask for. `storage_credentials` does the
    same for the Kind storage credentials mode the scenario exercises
    (SPACE_KIND_STORAGE_CREDENTIALS).
    `test_hooks` names the test hooks (`pplx_space_common_core::env::test_hook`)
    the scenario cannot pass without; implicit selection skips it, visibly, on
    a bench of release binaries (SPACE_KIND_RUST_BUILD=release), which read
    none.
    `timeout_minutes` raises the fixture's wall-clock budget for a scenario
    whose own waits outlast it; it never lowers it.
    `substrates` narrows the control-plane substrates the fixture serves
    (`FIXTURE_SUBSTRATES`) for what a substrate does not support by contract,
    such as sandbox pods on Direct. A Direct-only defect is not a narrowing:
    it is tracked by its issue and fails on the Direct lane. Do not quarantine
    it either: a quarantine withholds the scenario on every substrate, the
    required Kubernetes lane included. A suite run skips, visibly, the
    scenarios its bench's substrate does not serve: on Kind it reads the
    bench's, and on the deployed backend SPACE_SMOKE_SUBSTRATE names it.
    """

    if not id or any(part == "" for part in id.split(".")):
        raise ValueError("smoke id must be a non-empty dotted identifier")
    if sandboxes <= 0:
        raise ValueError("smoke sandbox count must be positive")
    sandbox_config = sandbox or SandboxConfig()
    supported_templates = (
        frozenset(template.strip() for template in templates)
        if templates is not None
        else None
    )
    # An empty list disables optional workload scenarios until templates are configured.
    if supported_templates is not None and any(
        not template for template in supported_templates
    ):
        raise ValueError("smoke templates must contain non-empty template ids")
    rerun_names = tuple(rerun_environment)
    if len(set(rerun_names)) != len(rerun_names) or any(
        not _is_environment_name(name) for name in rerun_names
    ):
        raise ValueError(
            "smoke rerun environment must contain unique shell variable names"
        )
    tool_names = tuple(required_tools)
    if len(set(tool_names)) != len(tool_names) or any(
        not tool or any(character.isspace() for character in tool)
        for tool in tool_names
    ):
        raise ValueError(
            "smoke required tools must contain unique non-empty command names"
        )
    if quarantined is not None and not quarantined.strip():
        raise ValueError("smoke quarantine must state its reason")
    if ignored is not None and not ignored.strip():
        raise ValueError("ignored smoke must state its reason")
    if ignored is not None and quarantined is not None:
        raise ValueError("smoke cannot be both ignored and quarantined")
    # Only otel installs backends a scenario can read.
    if telemetry_mode not in (None, "otel"):
        raise ValueError("smoke telemetry mode must be 'otel'")
    if timeout_minutes is not None and timeout_minutes <= 0:
        raise ValueError("smoke timeout must be a positive number of minutes")
    if storage_credentials not in (None, "exchange", "static"):
        raise ValueError(
            "smoke storage credentials mode must be 'exchange' or 'static'"
        )
    if test_hooks is not None and not test_hooks.strip():
        raise ValueError("smoke test hooks must name the hooks the scenario needs")
    supported_surfaces = frozenset(api_surfaces)
    if not supported_surfaces or not supported_surfaces <= {"ecw", "native", "node"}:
        raise ValueError("smoke API surfaces must contain ecw, native or node")
    narrowed_substrates = frozenset(substrates) if substrates is not None else None
    if narrowed_substrates is not None and (
        not narrowed_substrates or not narrowed_substrates <= SUBSTRATES
    ):
        raise ValueError("smoke substrates must contain kubernetes or direct")

    def define(verify: Callable[[Fixture], None]) -> Scenario[Fixture]:
        docstring = inspect.getdoc(verify)
        if not docstring:
            raise ValueError(f"smoke {id} must describe its invariant in a docstring")
        parameters = list(inspect.signature(verify).parameters.values())
        if len(parameters) != 1:
            raise TypeError(f"smoke {id} must accept exactly one typed fixture")
        parameter = parameters[0]
        fixture_type = get_type_hints(verify).get(parameter.name)
        if fixture_type not in FIXTURE_BACKENDS:
            supported = ", ".join(sorted(known.__name__ for known in FIXTURE_BACKENDS))
            raise TypeError(
                f"smoke {id} fixture must be one of {supported}; got "
                f"{getattr(fixture_type, '__name__', fixture_type)!r}"
            )
        if (fixture_type is DirectNodeRecoveryFixture) != (
            "node" in supported_surfaces
        ):
            raise TypeError("node API scenarios require DirectNodeRecoveryFixture")
        if fixture_type is DirectNodeRecoveryFixture and supported_surfaces != {"node"}:
            raise TypeError(
                "DirectNodeRecoveryFixture requires only the node API surface"
            )
        if "native" in supported_surfaces and fixture_type not in (
            CommandSandboxFixture,
            NativeHardeningFixture,
            KindNativeHardeningFixture,
        ):
            raise TypeError(
                "Native API scenarios require CommandSandboxFixture or NativeHardeningFixture"
            )
        if fixture_type in (
            NativeHardeningFixture,
            KindNativeHardeningFixture,
        ) and supported_surfaces != {"native"}:
            raise TypeError(
                "NativeHardeningFixture requires only the native API surface"
            )
        if telemetry_mode is not None and fixture_type is not KindEcw:
            raise TypeError("a telemetry mode requires the KindEcw fixture")
        if (
            timeout_minutes is not None
            and timeout_minutes < fixture_type.timeout_minutes
        ):
            raise ValueError(
                f"smoke {id} may only raise its fixture's {fixture_type.timeout_minutes}-minute timeout"
            )
        if storage_credentials is not None and fixture_type is not KindEcw:
            raise TypeError("a storage credentials mode requires the KindEcw fixture")
        fixture_substrates = FIXTURE_SUBSTRATES[fixture_type]
        if narrowed_substrates is not None and not narrowed_substrates < fixture_substrates:
            raise ValueError(
                f"smoke {id} substrates may only narrow its fixture's "
                f"{', '.join(sorted(fixture_substrates))}"
            )
        return Scenario(
            id=id,
            category=id.split(".", 1)[0],
            title=_title_from_id(id),
            invariant=" ".join(docstring.split()),
            fixture_name=fixture_type.fixture_name,
            fixture_type=cast(type[Fixture], fixture_type),
            sandbox=sandbox_config,
            sandbox_count=sandboxes,
            templates=supported_templates,
            rerun_environment=rerun_names,
            required_tools=tool_names,
            api_surfaces=supported_surfaces,
            timeout_minutes=timeout_minutes or fixture_type.timeout_minutes,
            backends=FIXTURE_BACKENDS[fixture_type],
            substrates=narrowed_substrates or fixture_substrates,
            quarantined=quarantined,
            ignored=ignored,
            telemetry_mode=telemetry_mode,
            storage_credentials=storage_credentials,
            test_hooks=test_hooks,
            verify=verify,
            source_module=verify.__module__,
        )

    return define


def _is_environment_name(name: str) -> bool:
    return (
        bool(name)
        and name.isascii()
        and (name[0].isalpha() or name[0] == "_")
        and all(character.isalnum() or character == "_" for character in name)
    )


def _title_from_id(scenario_id: str) -> str:
    namespace, _, leaf = scenario_id.rpartition(".")
    words = leaf.split("_")
    protocol_names = {"ecw": "ECW", "v1": "V1"}
    title = " ".join(protocol_names.get(word, word) for word in words)
    if namespace == "ecw":
        title = f"ECW {title}"
    elif namespace == "runtime":
        title = f"Runtime {title}"
    elif namespace == "network_policy":
        title = f"Network policy {title}"
    return title[:1].upper() + title[1:]


class ScenarioRegistry:
    def __init__(self, scenarios: Iterable[Scenario[Any]] = ()) -> None:
        self._scenarios: dict[str, Scenario[Any]] = {}
        for registered in scenarios:
            self.register(registered)

    def register(self, registered: Scenario[Any]) -> None:
        if registered.id in self._scenarios:
            raise ValueError(f"duplicate smoke id: {registered.id}")
        self._scenarios[registered.id] = registered

    def get(self, scenario_id: str) -> Scenario[Any]:
        try:
            return self._scenarios[scenario_id]
        except KeyError as error:
            choices = ", ".join(sorted(self._scenarios))
            raise KeyError(
                f"unknown smoke {scenario_id!r}; available: {choices}"
            ) from error

    def __iter__(self) -> Iterator[Scenario[Any]]:
        for scenario_id in sorted(self._scenarios):
            yield self._scenarios[scenario_id]

    def __len__(self) -> int:
        return len(self._scenarios)

    @property
    def categories(self) -> tuple[str, ...]:
        return tuple(
            sorted({scenario.category for scenario in self._scenarios.values()})
        )

    def select(
        self,
        *,
        category: str | None = None,
        template: str | None = None,
        backend: str | None = None,
        substrate: str | None = None,
        include_quarantined: bool = False,
        include_ignored: bool = False,
    ) -> tuple[Scenario[Any], ...]:
        return tuple(
            scenario
            for scenario in self
            if (category is None or scenario.category == category)
            and scenario.applies_to(template)
            and (backend is None or backend in scenario.backends)
            and (substrate is None or substrate in scenario.substrates)
            and (include_quarantined or scenario.quarantined is None)
            and (include_ignored or scenario.ignored is None)
        )


def discover_scenarios(
    package_name: str,
    package_paths: Iterable[str],
) -> ScenarioRegistry:
    """Import scenario modules and collect their decorated declaration."""

    registry = ScenarioRegistry()
    modules = sorted(
        pkgutil.iter_modules(package_paths),
        key=lambda module: module.name,
    )
    for module_info in modules:
        if module_info.ispkg or module_info.name.startswith("_"):
            continue
        module = importlib.import_module(f"{package_name}.{module_info.name}")
        declared = {
            id(value): value
            for value in vars(module).values()
            if isinstance(value, Scenario) and value.source_module == module.__name__
        }
        if len(declared) != 1:
            raise ValueError(
                f"scenario module {module.__name__} must declare exactly one "
                "@smoke.scenario"
            )
        registry.register(next(iter(declared.values())))
    return registry


def discover_scenario_packages(
    package_names: Iterable[str],
) -> ScenarioRegistry:
    """Discover the built-in inventory plus optional scenario packages."""

    registry = ScenarioRegistry()
    for package_name in dict.fromkeys(package_names):
        package = importlib.import_module(package_name)
        package_paths = getattr(package, "__path__", None)
        if package_paths is None:
            raise ValueError(
                f"smoke scenario package {package_name!r} is not a package"
            )
        for registered in discover_scenarios(package_name, package_paths):
            registry.register(registered)
    return registry

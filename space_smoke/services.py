"""Environment composition and fixture construction shared by smoke scenarios."""

from __future__ import annotations

import importlib.util
import os
import secrets
import shutil
import time
import uuid
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from functools import partial
from threading import Lock
from typing import Any, Protocol, TypeVar, cast

from space_smoke.adapters import (
    EcwConfig,
    EcwSdk,
    KindConfig,
    KindHarness,
    validate_loopback_environment,
)
from space_smoke.capabilities import (
    ColdEvictionControl,
    CommandSandbox,
    CommandSandboxFactory,
    DurableLifecycleSandbox,
    EcwCreateAttempt,
    EcwCreateNotDelivered,
    EcwForkResult,
    EcwLifecycle,
    EcwSandbox,
    EcwSandboxDeleteRetryable,
    EcwSandboxes,
    EcwSandboxPages,
    EncryptedResuspendControl,
    ForkableSandbox,
    KindEnvironment,
    NetworkConfig,
    ProcessSandbox,
    ResidentLifecycleSandbox,
    SandboxInfo,
    SandboxPages,
    StoppableLifecycleSandbox,
    WarmPoolCandidate,
)
from space_smoke.config import SandboxConfig
from space_smoke.direct_node import DirectNodeConfig, DirectNodeRecoveryFixture
from space_smoke.encrypted_resuspend import ByokIdentity, prepare_byok_identity
from space_smoke.fixtures import (
    SANDBOX_CLEANUP_ATTEMPTS,
    SANDBOX_CLEANUP_RETRY_SECONDS,
    CommandSandboxFixture,
    Ecw,
    KindEcw,
    retry_sandbox_delete,
)
from space_smoke.native import NativeConfig, NativeSandboxFactory
from space_smoke.native_hardening import NativeHardeningFixture
from space_smoke.native_recovery import KindNativeHardeningFixture
from space_smoke.runtime import (
    Compensation,
    ScenarioContext,
    SmokeConfigurationError,
    SubprocessExecutor,
)
from space_smoke.template_build import build_dedicated_template


class MissingToolError(RuntimeError):
    """A command required by the selected environment is unavailable."""


class ScenarioDefinition(Protocol):
    @property
    def id(self) -> str: ...

    @property
    def fixture_type(self) -> type[Any]: ...

    @property
    def sandbox(self) -> SandboxConfig: ...

    @property
    def required_tools(self) -> tuple[str, ...]: ...

    @property
    def api_surfaces(self) -> frozenset[str]: ...


Fixture = TypeVar("Fixture")
SANDBOX_CREATE_ATTEMPTS = 3
SANDBOX_CREATE_RETRY_SECONDS = 1.0
KIND_BACKEND = "kind"
DEPLOYED_BACKEND = "deployed"
REQUIRE_EXIT_RETENTION_ENV = "SPACE_SMOKE_REQUIRE_EXIT_RETENTION"
ECW_API_SURFACE = "ecw"
NATIVE_API_SURFACE = "native"
# Set by the suite runner on the scenarios it launches once its own bootstrap
# has passed; a scenario run on its own does every bootstrap check itself.
SUITE_BOOTSTRAPPED_ENV = "SPACE_SMOKE_SUITE_BOOTSTRAPPED"


@dataclass(frozen=True)
class SmokeServices:
    """Concrete environment dependencies used to construct typed fixtures."""

    kind: KindEnvironment | None
    ecw: EcwSandboxes | None
    api_key: str
    owns_api_key: bool
    template_override: str | None = None
    native: CommandSandboxFactory | None = None
    api_surface: str = ECW_API_SURFACE
    direct_node: DirectNodeConfig | None = None

    def bootstrap(self) -> bool:
        """Prepare the selected environment once before suite dispatch.

        Returns whether the bootstrap checked that every SPACE pod runs the
        latest build of its image.
        """

        return self.kind.bootstrap() if self.kind is not None else False

    def telemetry_mode(self) -> str:
        """The telemetry mode of the Kind bench, read from the cluster."""

        if self.kind is None:
            raise SmokeConfigurationError(
                "the telemetry mode is only readable on the kind backend"
            )
        return self.kind.telemetry_mode()

    def storage_credentials(self) -> str:
        """The storage credentials mode of the Kind bench, read from the cluster."""

        if self.kind is None:
            raise SmokeConfigurationError(
                "the storage credentials mode is only readable on the kind backend"
            )
        return self.kind.storage_credentials_mode()

    def rust_build(self) -> str:
        """The Rust build (`test-hooks` or `release`) the Kind bench runs."""

        if self.kind is None:
            raise SmokeConfigurationError(
                "the Rust build is only readable on the kind backend"
            )
        return self.kind.rust_build()

    def fixture(
        self,
        fixture_type: type[Fixture],
        context: ScenarioContext,
        *,
        scenario_id: str,
        sandbox: SandboxConfig,
        sandbox_count: int = 1,
    ) -> Fixture:
        if fixture_type is DirectNodeRecoveryFixture:
            if self.direct_node is None:
                raise SmokeConfigurationError("node recovery configuration is missing")
            return cast(Fixture, DirectNodeRecoveryFixture(context, self.direct_node))
        if fixture_type not in (
            CommandSandboxFixture,
            Ecw,
            KindEcw,
            NativeHardeningFixture,
            KindNativeHardeningFixture,
        ):
            raise SmokeConfigurationError(
                f"unsupported smoke fixture: {fixture_type.__name__}"
            )
        if fixture_type in (NativeHardeningFixture, KindNativeHardeningFixture):
            if self.api_surface != NATIVE_API_SURFACE or not isinstance(
                self.native, NativeSandboxFactory
            ):
                raise SmokeConfigurationError(
                    "Native hardening requires a native client"
                )
            return cast(Fixture, fixture_type(context, self.native.config, scenario_id))
        if fixture_type is CommandSandboxFixture:
            if self.api_surface == NATIVE_API_SURFACE:
                return cast(
                    Fixture,
                    self._native(
                        context,
                        scenario_id=scenario_id,
                        sandbox_config=sandbox,
                        sandbox_count=sandbox_count,
                    ),
                )
            fixture_type = cast(type[Fixture], Ecw)
        if fixture_type is KindEcw and self.kind is None:
            raise SmokeConfigurationError(
                "this scenario's fixture carries privileged control-plane "
                "operations and runs only on the kind backend "
                "(SPACE_SMOKE_BACKEND=kind)"
            )
        return cast(
            Fixture,
            self._ecw(
                context,
                scenario_id=scenario_id,
                sandbox_config=sandbox,
                sandbox_count=sandbox_count,
                fixture_type=cast(type[Ecw], fixture_type),
            ),
        )

    def _ecw(
        self,
        context: ScenarioContext,
        *,
        scenario_id: str,
        sandbox_config: SandboxConfig,
        sandbox_count: int,
        fixture_type: type[Ecw] = Ecw,
    ) -> Ecw:
        if sandbox_count <= 0:
            raise SmokeConfigurationError("smoke sandbox count must be positive")
        kind = self.kind
        if kind is not None:
            with context.phase("bootstrap kind stack"):
                kind.bootstrap()
        byok_identity: ByokIdentity | None = None
        if sandbox_config.kind_byok:
            if kind is None or fixture_type is not KindEcw or not self.owns_api_key:
                raise SmokeConfigurationError(
                    "BYOK setup requires KindEcw and a run-owned ECW credential"
                )
            with context.phase("enroll run-owned BYOK organization"):
                byok_identity = prepare_byok_identity(context, kind)
        if self.owns_api_key:
            if kind is None:
                raise SmokeConfigurationError(
                    "deployed ECW smoke credentials must be supplied by the caller"
                )
            # The insert may commit even if kubectl loses its response. Arm the
            # idempotent delete before dispatch so an ambiguous seed cannot leak.
            context.scope.defer(
                "delete run-scoped ECW credential",
                lambda: kind.delete_api_key(self.api_key),
                critical=True,
            )
            with context.phase("seed run-scoped ECW credential"):
                if byok_identity is None:
                    kind.seed_api_key(self.api_key)
                else:
                    kind.seed_api_key(
                        self.api_key,
                        org_id=byok_identity.org_id,
                        user_id=byok_identity.user_id,
                    )
        template = self.template_override or sandbox_config.template
        if sandbox_config.kind_dedicated_template:
            if kind is None or fixture_type is not KindEcw or self.ecw is None:
                raise SmokeConfigurationError("a dedicated template requires KindEcw")
            with context.phase("build the scenario-owned minimal template"):
                built = build_dedicated_template(
                    context.scope, cast(Any, kind), cast(Any, self.ecw)
                )
                template = built.alias
                context.record(
                    "dedicated_template",
                    {"alias": built.alias, "build_seconds": built.elapsed_seconds},
                )
        before_create = None
        warm_pool_candidate: WarmPoolCandidate | None = None
        if sandbox_config.kind_cold_create is not None:
            if kind is None or fixture_type is not KindEcw:
                raise SmokeConfigurationError("configured cold create requires KindEcw")
            with context.phase("establish owned disabled cold-create config"):
                release = kind.acquire_cold_create_setup()
                context.scope.defer(
                    "release Kind cold-create setup Lease", release, critical=True
                )
                selection = kind.prepare_cold_create(
                    template, sandbox_config.kind_cold_create
                )
                template = selection.template
                before_create = selection.verify
                context.record("cold_config_id", selection.config_id)
                context.record(
                    "cold_config_ownership_receipt", selection.ownership_receipt
                )
                context.record("cold_template_pin", template)
        if sandbox_config.kind_warm_pool is not None:
            if kind is None or fixture_type is not KindEcw:
                raise SmokeConfigurationError("configured warm pool requires KindEcw")
            with context.phase("establish owned ready warm-pool slot"):
                release = kind.acquire_cold_create_setup()
                context.scope.defer(
                    "release Kind warm-pool setup Lease", release, critical=True
                )
                selection = kind.prepare_warm_pool(
                    template, sandbox_config.kind_warm_pool
                )
                context.scope.defer(
                    "restore owned warm-pool config",
                    selection.restore,
                    critical=True,
                )
                template = selection.template
                before_create = selection.verify
                warm_pool_candidate = kind.warm_pool_candidate(selection.config_id)
                context.record("warm_pool_config_id", selection.config_id)
                context.record(
                    "warm_pool_config_ownership_receipt",
                    selection.ownership_receipt,
                )
                context.record("warm_pool_template_pin", template)
                context.record(
                    "warm_pool_candidate_sandbox_id", warm_pool_candidate.sandbox_id
                )
                context.record("warm_pool_candidate_state", warm_pool_candidate.state)
        if self.ecw is None:
            raise SmokeConfigurationError("ECW client is unavailable")
        sandboxes = _ScenarioSandboxes(
            context=context,
            ecw=self.ecw,
            scenario_id=scenario_id,
            template=template,
            timeout=sandbox_config.timeout,
            lifecycle=sandbox_config.lifecycle,
            allow_internet_access=sandbox_config.allow_internet_access,
            network=sandbox_config.network,
            envs=sandbox_config.envs,
            before_create=before_create,
            # Privileged scenarios drive sandboxes across workers (cross-node
            # restores, forced failures), and the local rows those leave are
            # reclaimed by SPACE after the configured quiescence. Each
            # scenario removes its own so none outlives it into another
            # scenario's view of a worker.
            after_teardown_delete=(
                kind.reclaim_local_sandbox_everywhere
                if kind is not None and fixture_type is KindEcw
                else None
            ),
        )
        phase = "create sandbox" if sandbox_count == 1 else "create sandbox collection"
        with context.phase(phase):
            for _ in range(sandbox_count):
                sandboxes.create()
        sandbox = sandboxes.items[0]
        fixture = fixture_type(
            context,
            sandbox,
            sandboxes,
            _sandbox_timeout=sandbox_config.timeout,
            _encrypted_resuspend_control=(
                cast(EncryptedResuspendControl, kind)
                if kind is not None and fixture_type is KindEcw
                else None
            ),
        )
        if isinstance(fixture, KindEcw):
            # `fixture` requires kind (enforced above); the harness provides
            # the named privileged operations.
            fixture._retention_control = cast(Any, kind)
            fixture._cold_create_observer = cast(Any, kind)
            fixture._cross_node_control = cast(Any, kind)
            fixture._concurrent_restore_control = cast(Any, kind)
            fixture._rate_limiter_control = cast(Any, kind)
            fixture._node_group_control = cast(Any, kind)
            fixture._node_selector_extras_control = cast(Any, kind)
            fixture._node_fence_control = cast(Any, kind)
            fixture._pending_backup_control = cast(Any, kind)
            fixture._teardown_restart_control = cast(Any, kind)
            fixture._orphaned_restore_pod_gc_control = cast(Any, kind)
            fixture._reconcile_queue_gc_control = cast(Any, kind)
            fixture._unanswered_template_control = cast(Any, kind)
            fixture._template_build_control = cast(Any, kind)
            fixture._template_builder = cast(Any, self.ecw)
            fixture._rate_limit_probe = cast(Any, self.ecw)
            fixture._node_drain_control = cast(Any, kind)
            fixture._template_cache_control = cast(Any, kind)
            fixture._cold_eviction_control = cast(Any, kind)
            fixture._snapshot_reaper_control = cast(Any, kind)
            fixture._startup_recovery_control = cast(Any, kind)
            fixture._drain_gate_control = cast(Any, kind)
            fixture._suspend_codec_control = cast(Any, kind)
            fixture._telemetry_control = cast(Any, kind)
            fixture._leader_election_control = cast(Any, kind)
            fixture._sandbox_network_isolation_control = cast(Any, kind)
            fixture._storage_exchange_control = cast(Any, kind)
            fixture._template_selector = template
            fixture._warm_pool_observer = cast(Any, kind)
            fixture._warm_pool_candidate = warm_pool_candidate
            if isinstance(kind, KindHarness) and isinstance(self.ecw, EcwSdk):
                from space_smoke.private_template_build import (
                    PrivateTemplateBuildAssertions,
                )

                fixture._private_template_build_setup = partial(
                    PrivateTemplateBuildAssertions, context, kind, self.ecw.config
                )
        return fixture

    def _native(
        self,
        context: ScenarioContext,
        *,
        scenario_id: str,
        sandbox_config: SandboxConfig,
        sandbox_count: int,
    ) -> CommandSandboxFixture:
        if sandbox_count <= 0:
            raise SmokeConfigurationError("smoke sandbox count must be positive")
        if self.native is None:
            raise SmokeConfigurationError("Native client is unavailable")
        native = self.native
        kind = self.kind
        if sandbox_config.kind_byok:
            if kind is None or not isinstance(native, NativeSandboxFactory):
                raise SmokeConfigurationError(
                    "native BYOK setup requires the kind backend and a native client"
                )
            with context.phase("enroll run-owned BYOK organization"):
                byok_identity = prepare_byok_identity(context, kind)
                # The sandbox delete below is deferred after the identity
                # cleanup, so it runs first and with this member's session.
                native = native.as_member(byok_identity.email, byok_identity.password)
        run_metadata = {
            "space_smoke_scenario": scenario_id,
            "space_smoke_run": uuid.uuid4().hex,
        }
        sandboxes: list[DurableLifecycleSandbox] = []
        phase = "create sandbox" if sandbox_count == 1 else "create sandbox collection"
        with context.phase(phase):
            for _ in range(sandbox_count):
                sandbox = cast(
                    DurableLifecycleSandbox,
                    native.create(
                        {
                            **run_metadata,
                            "space_smoke_instance": uuid.uuid4().hex,
                        }
                    ),
                )
                context.scope.defer(
                    f"delete Native sandbox {sandbox.sandbox_id}",
                    partial(native.delete, sandbox),
                    critical=True,
                )
                sandboxes.append(sandbox)
        sandbox_ids = [sandbox.sandbox_id for sandbox in sandboxes]
        context.record("sandbox_id", sandbox_ids[0])
        context.record("sandbox_ids", sandbox_ids)
        collection = _NativeScenarioSandboxes(native, tuple(sandboxes))
        return CommandSandboxFixture(
            context,
            sandboxes[0],
            collection,
            _durable_lifecycle=sandboxes[0],
            _resident_lifecycle=cast(ResidentLifecycleSandbox, sandboxes[0]),
            _stoppable_lifecycle=cast(StoppableLifecycleSandbox, sandboxes[0]),
            _local_cleanup_control=(
                cast(ColdEvictionControl, kind) if kind is not None else None
            ),
            _encrypted_resuspend_control=(
                cast(EncryptedResuspendControl, kind) if kind is not None else None
            ),
            _processes=cast(ProcessSandbox, sandboxes[0]),
        )

    @classmethod
    def from_environment(
        cls,
        scenario: ScenarioDefinition,
        environment: Mapping[str, str] | None = None,
        *,
        required_tools: Iterable[str] | None = None,
    ) -> SmokeServices:
        if scenario.fixture_type is DirectNodeRecoveryFixture:
            values = dict(os.environ if environment is None else environment)
            if (
                values.get("SPACE_SMOKE_BACKEND") != DEPLOYED_BACKEND
                or values.get("SPACE_SMOKE_API_SURFACE") != "node"
            ):
                raise SmokeConfigurationError(
                    "node recovery requires deployed backend and node API surface"
                )
            if shutil.which("ssh") is None:
                raise MissingToolError("node recovery requires ssh")
            return cls(
                kind=None,
                ecw=None,
                api_key="",
                owns_api_key=False,
                direct_node=DirectNodeConfig.from_environment(values),
                api_surface="node",
            )
        if scenario.fixture_type not in (
            CommandSandboxFixture,
            Ecw,
            KindEcw,
            NativeHardeningFixture,
            KindNativeHardeningFixture,
        ):
            raise SmokeConfigurationError(
                f"unsupported smoke fixture: {scenario.fixture_type.__name__}"
            )
        values = dict(os.environ if environment is None else environment)
        backend = values.get("SPACE_SMOKE_BACKEND", KIND_BACKEND)
        if backend not in {KIND_BACKEND, DEPLOYED_BACKEND}:
            raise SmokeConfigurationError(
                "SPACE_SMOKE_BACKEND must be 'kind' or 'deployed'"
            )
        if (
            scenario.fixture_type is KindNativeHardeningFixture
            and backend != KIND_BACKEND
        ):
            raise SmokeConfigurationError(
                "native recovery requires the kind backend; deployed clusters are unsupported"
            )
        api_surface = values.get("SPACE_SMOKE_API_SURFACE", ECW_API_SURFACE)
        if api_surface not in {ECW_API_SURFACE, NATIVE_API_SURFACE}:
            raise SmokeConfigurationError(
                "SPACE_SMOKE_API_SURFACE must be 'ecw' or 'native'"
            )
        if api_surface not in scenario.api_surfaces:
            raise SmokeConfigurationError(
                f"{scenario.id} does not support the {api_surface!r} API surface"
            )

        selected_tools = (
            scenario.required_tools if required_tools is None else required_tools
        )
        for tool in selected_tools:
            if shutil.which(tool) is None:
                raise MissingToolError(f"required command not found: {tool}")

        if api_surface == NATIVE_API_SURFACE:
            token = values.get("SPACE_API_TOKEN") or None
            if token is None:
                raise SmokeConfigurationError(
                    "Native smokes require a non-empty SPACE_API_TOKEN"
                )
            api_url = values.get("SPACE_API_URL", "http://127.0.0.1:8080")
            data_plane_url = values.get("SPACE_DATA_PLANE_URL", api_url)
            template = values.get("SPACE_TEMPLATE") or scenario.sandbox.template
            kind = None
            if (
                backend == KIND_BACKEND
                and scenario.fixture_type is CommandSandboxFixture
            ):
                validate_loopback_environment(api_url, False, scenario_id=scenario.id)
                validate_loopback_environment(
                    data_plane_url,
                    False,
                    scenario_id=scenario.id,
                    endpoint_name="SPACE_DATA_PLANE_URL",
                )
                kind = KindHarness(
                    SubprocessExecutor(base_env=values),
                    KindConfig(
                        cluster=values.get("KIND_CLUSTER", "space-local"),
                        api_url=api_url,
                        data_plane_url=data_plane_url,
                        skip_bootstrap=values.get("SKIP_BOOTSTRAP", "0") == "1",
                        bootstrap_timeout=int(values.get("BOOTSTRAP_TIMEOUT", "1800")),
                    ),
                    environment=values,
                )
            return cls(
                kind=kind,
                ecw=None,
                api_key="",
                owns_api_key=False,
                native=NativeSandboxFactory(
                    NativeConfig(
                        api_url=api_url,
                        data_plane_url=data_plane_url,
                        token=token,
                        template=template,
                        ready_timeout_seconds=float(
                            values.get("SPACE_READY_TIMEOUT_SECS", "180")
                        ),
                        reports_exited_processes=_require_exit_retention(
                            values, backend
                        ),
                    )
                ),
                api_surface=api_surface,
            )

        if importlib.util.find_spec("e2b") is None:
            raise SmokeConfigurationError(
                "smoke client dependencies are unavailable; "
                "run with `uv run -m space_smoke ...`"
            )

        supplied_key = values.get("E2B_API_KEY") or None
        template_override = values.get("E2B_TEMPLATE") or None
        if backend == DEPLOYED_BACKEND:
            domain = values.get("E2B_DOMAIN") or None
            if supplied_key is None:
                raise SmokeConfigurationError(
                    "deployed ECW smokes require a non-empty E2B_API_KEY"
                )
            if domain is None:
                raise SmokeConfigurationError(
                    "deployed ECW smokes require a non-empty E2B_DOMAIN"
                )
            # A caller inside the target cluster reaches the ECW surface through
            # the space-api and space-data-plane Services, because the ECW
            # hostnames resolve only in the consuming VPCs that hold the
            # PrivateLink records. Endpoint selection then has to be explicit:
            # the SDK derives every URL from the domain otherwise.
            api_url = values.get("E2B_API_URL") or None
            api_host = values.get("ECW_API_HOST") or None
            if api_url is not None and api_host is None:
                raise SmokeConfigurationError(
                    "E2B_API_URL requires ECW_API_HOST: the API selects its ECW "
                    "handler by Host header, and a request carrying the "
                    "endpoint's own host answers 404, which reads as a bad key"
                )
            return cls(
                kind=None,
                ecw=EcwSdk(
                    EcwConfig(
                        api_key=supplied_key,
                        domain=domain,
                        api_url=api_url,
                        api_host=api_host,
                        data_plane_url=values.get("ECW_DATA_PLANE_BASE_URL") or None,
                        data_plane_domain=values.get("ECW_DATA_PLANE_DOMAIN") or None,
                    )
                ),
                api_key=supplied_key,
                owns_api_key=False,
                template_override=template_override,
            )

        if shutil.which("kubectl") is None:
            raise MissingToolError("required command not found: kubectl")
        api_url = values.get("API_URL", "http://127.0.0.1:8080")
        data_plane_url = values.get("ECW_DATA_PLANE_BASE_URL", "http://127.0.0.1:8090")
        allow_remote = values.get("SPACE_SMOKE_ALLOW_REMOTE_API", "0") == "1"
        validate_loopback_environment(
            api_url,
            allow_remote,
            scenario_id=scenario.id,
        )
        validate_loopback_environment(
            data_plane_url,
            allow_remote,
            scenario_id=scenario.id,
            endpoint_name="ECW_DATA_PLANE_BASE_URL",
        )

        # Random, not pid+clock: the fallback is seeded into the shared Local
        # org for the run, and another local user could otherwise predict it.
        api_key = supplied_key or secrets.token_hex(32)
        kind_config = KindConfig(
            cluster=values.get("KIND_CLUSTER", "space-local"),
            api_url=api_url,
            data_plane_url=data_plane_url,
            skip_bootstrap=values.get("SKIP_BOOTSTRAP", "0") == "1",
            bootstrap_timeout=int(values.get("BOOTSTRAP_TIMEOUT", "1800")),
            suite_bootstrapped=values.get(SUITE_BOOTSTRAPPED_ENV) == "1",
        )
        executor = SubprocessExecutor(base_env=values)
        kind = KindHarness(executor, kind_config, environment=values)
        domain = values.get("E2B_DOMAIN", "local.e2b.test")
        ecw = EcwSdk(
            EcwConfig(
                api_key=api_key,
                domain=domain,
                api_url=api_url,
                api_host=values.get("ECW_API_HOST", "api.local.e2b.test"),
                data_plane_url=data_plane_url,
                data_plane_domain=values.get("ECW_DATA_PLANE_DOMAIN", domain),
            )
        )
        return cls(
            kind=kind,
            ecw=ecw,
            api_key=api_key,
            owns_api_key=supplied_key is None,
            template_override=template_override,
        )


def _require_exit_retention(values: Mapping[str, str], backend: str) -> bool:
    """Kind builds its templates from this tree; deployed templates may predate it."""

    override = values.get(REQUIRE_EXIT_RETENTION_ENV)
    if not override:
        return backend == KIND_BACKEND
    if override not in {"0", "1"}:
        raise SmokeConfigurationError(f"{REQUIRE_EXIT_RETENTION_ENV} must be '1' or '0'")
    return override == "1"


def repair_orphaned_kind_faults(environment: Mapping[str, str]) -> bool:
    """On the kind backend, undo a limiter fault or a fast rollout pace an earlier run left, or refuse.

    Runs before every scenario, whatever its fixture: a scenario that takes no
    Lease would otherwise run against shrunk caps, a store scaled to zero, or
    a control-plane Deployment that rolls without the charts' pace.
    """

    values = dict(environment)
    if values.get("SPACE_SMOKE_BACKEND", KIND_BACKEND) != KIND_BACKEND:
        return False
    harness = KindHarness(
        SubprocessExecutor(base_env=values),
        KindConfig(
            cluster=values.get("KIND_CLUSTER", "space-local"),
            api_url=values.get("API_URL", "http://127.0.0.1:8080"),
        ),
        environment=values,
    )
    limiter = harness.repair_orphaned_limiter_faults()
    return harness.repair_orphaned_rollout_pace() or limiter


@dataclass(frozen=True)
class _NativeScenarioSandboxes:
    factory: CommandSandboxFactory
    items: tuple[CommandSandbox, ...]

    def list_pages(self, *, limit: int, max_pages: int) -> SandboxPages[SandboxInfo]:
        return self.factory.list_pages(
            limit=limit,
            max_pages=max_pages,
            stop_after_sandbox_ids={sandbox.sandbox_id for sandbox in self.items},
        )

    def delete(self, sandbox: CommandSandbox) -> int:
        self.factory.delete(sandbox)
        return 1


class _ScenarioSandboxes:
    """Run-scoped sandbox collection with cleanup armed before every create."""

    def __init__(
        self,
        *,
        context: ScenarioContext,
        ecw: EcwSandboxes,
        scenario_id: str,
        template: str,
        timeout: int,
        lifecycle: EcwLifecycle | None,
        allow_internet_access: bool | None,
        network: NetworkConfig | None,
        envs: Mapping[str, str] | None = None,
        before_create: Callable[[], None] | None = None,
        after_teardown_delete: Callable[[str], None] | None = None,
    ) -> None:
        self._before_create = before_create
        self._after_teardown_delete = after_teardown_delete
        self._context = context
        self._ecw = ecw
        self._template = template
        self._timeout = timeout
        self._lifecycle = lifecycle
        self._allow_internet_access = allow_internet_access
        self._network = network
        self._envs = dict(envs) if envs is not None else None
        self._run_metadata = {
            "space_smoke_scenario": scenario_id,
            "space_smoke_run": uuid.uuid4().hex,
        }
        self._created: list[EcwSandbox] = []
        self._fork_children: set[str] = set()
        self._fork_lock = Lock()

    @property
    def items(self) -> tuple[EcwSandbox, ...]:
        return tuple(self._created)

    def create(self, metadata: Mapping[str, str] | None = None) -> EcwSandbox:
        if self._before_create is not None:
            self._before_create()
        create_metadata = self._create_metadata(metadata)
        created: list[EcwSandbox] = []
        create_not_delivered = False

        self._arm_cleanup(
            create_metadata,
            created,
            reconcile_uncreated=lambda: not create_not_delivered,
        )
        for attempt in range(SANDBOX_CREATE_ATTEMPTS):
            try:
                if attempt and self._before_create is not None:
                    self._before_create()
                sandbox = self._ecw.create(
                    template=self._template,
                    timeout=self._timeout,
                    metadata=create_metadata,
                    lifecycle=self._lifecycle,
                    allow_internet_access=self._allow_internet_access,
                    network=self._network,
                    **({"envs": self._envs} if self._envs is not None else {}),
                )
                break
            except EcwCreateNotDelivered:
                if attempt + 1 == SANDBOX_CREATE_ATTEMPTS:
                    create_not_delivered = True
                    raise
                time.sleep(SANDBOX_CREATE_RETRY_SECONDS)
        created.append(sandbox)
        self._record_created_sandbox(sandbox)
        return sandbox

    def attempt_create(
        self,
        *,
        allow_internet_access: bool | None = None,
        network: NetworkConfig | None = None,
    ) -> EcwCreateAttempt:
        """Issue a tracked raw create when the HTTP response is the invariant."""

        create_metadata = self._create_metadata()
        observed_ids: list[str] = []
        cleanup = self._arm_cleanup(create_metadata, [], observed_ids=observed_ids)
        observed = self._ecw.attempt_create(
            template=self._template,
            timeout=self._timeout,
            metadata=create_metadata,
            allow_internet_access=allow_internet_access,
            network=network,
        )
        # A client rejection is definitive. Success and server failures remain
        # commit-ambiguous, so run-marker reconciliation stays armed for them.
        if 400 <= observed.status_code < 500:
            cleanup.disarm()
        if observed.sandbox_id is not None:
            observed_ids.append(observed.sandbox_id)
            self._context.record("sandbox_id", observed.sandbox_id)
            sandbox_ids = self._context.evidence.setdefault("sandbox_ids", [])
            assert isinstance(sandbox_ids, list)
            sandbox_ids.append(observed.sandbox_id)
        return observed

    def fork(
        self,
        source: EcwSandbox,
        *,
        idempotency_key: str | None = None,
        timeout: int | None = None,
    ) -> EcwForkResult:
        """Track a fork before attaching its data plane, including replayed children."""

        if source not in self._created:
            raise SmokeConfigurationError("fork source must belong to this scenario")
        forkable = cast(ForkableSandbox, source)
        key = idempotency_key or f"space-smoke-fork-{secrets.token_hex(16)}"
        lifetime = self._timeout if timeout is None else timeout
        child_ids: list[str] = []

        def record_child(response: Mapping[str, Any]) -> None:
            child_id = str(response["sandboxID"])
            self._context.verify(
                child_id != source.sandbox_id,
                "fork returned the source sandbox instead of a new identity",
                fork_source_id=source.sandbox_id,
                fork_child_id=child_id,
            )
            with self._fork_lock:
                if child_id in self._fork_children:
                    compensation.disarm()
                else:
                    self._fork_children.add(child_id)
                    child_ids.append(child_id)
                    sandbox_ids = self._context.evidence.setdefault("sandbox_ids", [])
                    assert isinstance(sandbox_ids, list)
                    sandbox_ids.append(child_id)

        def cleanup() -> None:
            if not child_ids:
                # Replay the original operation, never create a replacement.
                # Child expiry inherits the source's lifecycle and may pause.
                record_child(
                    forkable.fork_response(timeout=lifetime, idempotency_key=key)
                )
            for child_id in child_ids:
                retry_sandbox_delete(partial(forkable.delete_forked, child_id))
                if self._after_teardown_delete is not None:
                    self._after_teardown_delete(child_id)

        # A lost response still leaves an operation recoverable by this key.
        # Record its child before SDK construction, which can also fail.
        compensation = self._context.scope.defer(
            "delete fork child", cleanup, critical=True
        )
        response = forkable.fork_response(timeout=lifetime, idempotency_key=key)
        record_child(response)
        child = self._ecw.attach_fork(response)
        self._created.append(child)
        return EcwForkResult(child, response)

    def _create_metadata(
        self,
        metadata: Mapping[str, str] | None = None,
    ) -> dict[str, str]:
        supplied = dict(metadata or {})
        reserved = set(supplied) & {
            *self._run_metadata,
            "space_smoke_instance",
        }
        if reserved:
            raise SmokeConfigurationError(
                "scenario metadata cannot override run identity: "
                + ", ".join(sorted(reserved))
            )
        return {
            **self._run_metadata,
            "space_smoke_instance": uuid.uuid4().hex,
            **supplied,
        }

    def _arm_cleanup(
        self,
        create_metadata: Mapping[str, str],
        created: list[EcwSandbox],
        *,
        reconcile_uncreated: Callable[[], bool] | None = None,
        observed_ids: list[str] | None = None,
    ) -> Compensation:
        def delete() -> None:
            if created:
                created[0].kill()
            elif reconcile_uncreated is None or reconcile_uncreated():
                self._ecw.kill_matching(create_metadata)

        def cleanup() -> None:
            retry_sandbox_delete(delete)
            # Only at teardown: a scenario that deletes mid-body may be
            # asserting on the local rows the delete leaves behind. A raw
            # create reports its id in `observed_ids`; a create whose id was
            # never seen is killed by metadata above and left to the eviction.
            if self._after_teardown_delete is None:
                return
            ids = [created[0].sandbox_id] if created else list(observed_ids or ())
            for sandbox_id in ids:
                self._after_teardown_delete(sandbox_id)

        return self._context.scope.defer(
            f"kill run-scoped sandbox {create_metadata['space_smoke_instance']}",
            cleanup,
            critical=True,
        )

    def _record_created_sandbox(self, sandbox: EcwSandbox) -> None:
        self._created.append(sandbox)
        self._context.evidence.setdefault("sandbox_id", sandbox.sandbox_id)
        sandbox_ids = self._context.evidence.setdefault("sandbox_ids", [])
        assert isinstance(sandbox_ids, list)
        sandbox_ids.append(sandbox.sandbox_id)

    def delete(self, sandbox: CommandSandbox) -> int:
        target = cast(EcwSandbox, sandbox)
        for attempt in range(1, SANDBOX_CLEANUP_ATTEMPTS + 1):
            self._context.record("delete_attempts", attempt)
            try:
                target.kill()
                return attempt
            except EcwSandboxDeleteRetryable as error:
                self._context.record("delete_error_type", type(error).__name__)
                self._context.record("delete_error", str(error))
                if attempt == SANDBOX_CLEANUP_ATTEMPTS:
                    raise
                time.sleep(SANDBOX_CLEANUP_RETRY_SECONDS)
        raise AssertionError("unreachable sandbox deletion retry state")

    def list_pages(self, *, limit: int, max_pages: int) -> EcwSandboxPages:
        return self._ecw.list_pages(
            self._run_metadata,
            limit=limit,
            max_pages=max_pages,
        )

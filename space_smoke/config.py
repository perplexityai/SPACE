"""Typed configuration for scenario-owned smoke resources."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from space_smoke.capabilities import (
    EcwLifecycle,
    EcwTimeoutAction,
    KindColdCreateConfig,
    KindWarmPoolConfig,
    NetworkConfig,
)

DEFAULT_SANDBOX_TIMEOUT_SECONDS = 300
DEFAULT_SANDBOX_LIFECYCLE = EcwLifecycle(EcwTimeoutAction.PAUSE)


@dataclass(frozen=True)
class SandboxConfig:
    """How the fixture should create its scenario-owned ECW sandbox.

    Scenario-owned sandboxes pause at timeout by default so an idle timeout
    does not destroy state while a smoke is still diagnosing a failure.
    """

    timeout: int = DEFAULT_SANDBOX_TIMEOUT_SECONDS
    template: str = "seed"
    lifecycle: EcwLifecycle | None = DEFAULT_SANDBOX_LIFECYCLE
    allow_internet_access: bool | None = None
    network: NetworkConfig | None = None
    envs: Mapping[str, str] | None = None
    kind_byok: bool = False
    kind_cold_create: KindColdCreateConfig | None = None
    kind_warm_pool: KindWarmPoolConfig | None = None
    kind_dedicated_template: bool = False
    """Create the sandbox from a minimal run-owned template no worker prefetches.

    For scenarios that need a worker to stay without the sandbox's template
    once it is removed there; `template` is then not used.
    """

    def __post_init__(self) -> None:
        if self.timeout <= 0:
            raise ValueError("sandbox timeout must be positive")
        if not self.template:
            raise ValueError("sandbox template must not be empty")
        if self.kind_cold_create is not None and self.kind_warm_pool is not None:
            raise ValueError("cold-create and warm-pool setup are mutually exclusive")
        if self.kind_dedicated_template and (
            self.template != "seed"
            or self.kind_cold_create is not None
            or self.kind_warm_pool is not None
        ):
            raise ValueError(
                "a dedicated template replaces the configured template and its "
                "cold-create or warm-pool setup"
            )


def sandbox_with_authenticated_public_ports() -> SandboxConfig:
    """Create a sandbox whose public port routes require its access token."""

    return SandboxConfig(network=NetworkConfig(allow_public_traffic=False))


def network_policy_sandbox(
    *,
    internet: bool | None,
    allow: tuple[str, ...] = (),
    deny: tuple[str, ...] = (),
) -> SandboxConfig:
    """Reproduce the legacy network-policy suite's ECW create shape."""

    return SandboxConfig(
        timeout=600,
        lifecycle=None,
        allow_internet_access=internet,
        network=NetworkConfig(
            allow_public_traffic=False,
            allow_out=allow,
            deny_out=deny,
        ),
    )


def kill_on_timeout() -> EcwLifecycle:
    """Delete the sandbox when its access-idle timeout expires."""

    return EcwLifecycle(EcwTimeoutAction.KILL)


def pause_on_timeout(*, auto_resume: bool = False) -> EcwLifecycle:
    """Pause the sandbox at timeout and optionally wake it on public access."""

    return EcwLifecycle(EcwTimeoutAction.PAUSE, auto_resume=auto_resume)

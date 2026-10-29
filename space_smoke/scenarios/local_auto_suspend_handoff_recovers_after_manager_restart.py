"""Durable autosuspend admission survives a long handoff and owner restart."""

import space_smoke as smoke


@smoke.scenario(
    "local.auto_suspend_handoff_recovers_after_manager_restart",
    sandbox=smoke.SandboxConfig(timeout=30),
    api_surfaces=("ecw",),
    required_tools=("kubectl", "grpcurl", "protoc"),
    ignored="requires isolated Kind, a long dependency outage and a manager restart",
)
def auto_suspend_handoff_recovers_after_manager_restart(f: smoke.KindEcw) -> None:
    """The original capture survives foreground exhaustion and owner restart."""
    f.autosuspend_handoff.run(outage_seconds=190, restart_pending=True)

"""An access-idle suspend survives a real volume-manager graceful handoff."""

import space_smoke as smoke


@smoke.scenario(
    "local.auto_suspend_recovers_volume_handoff",
    sandbox=smoke.SandboxConfig(timeout=30),
    api_surfaces=("ecw",),
    required_tools=("kubectl", "grpcurl", "protoc"),
    ignored="requires an isolated Kind stack and deliberately holds real object storage",
)
def auto_suspend_recovers_volume_handoff(f: smoke.KindEcw) -> None:
    """No guest access, timer replacement or unrelated reaper assists recovery."""
    f.autosuspend_handoff.run()

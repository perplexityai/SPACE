"""Native spaced and Wasmtime in the production launcher's namespaces."""

import os
from dataclasses import replace
from pathlib import Path

from space_smoke.registry import scenario
from space_smoke.runtime import ResourceScope, ScenarioContext
from space_smoke.scenarios.wasm_spaced import wasm_spaced
from space_smoke.spaced_runtime import SpacedRuntimeFixture


@scenario(
    "wasm.contained-spaced",
    api_surfaces=("node",),
    tier="manual",
    manual_reason="Requires root, a disposable private cgroup namespace and task-owned launch inputs",
    timeout_minutes=5,
    required_tools=("grpcurl",),
    rerun_environment=(
        "SPACE_SMOKE_SPACED_SOCKET",
        "SPACE_SMOKE_SPACED_ENDPOINT",
        "SPACE_SMOKE_WASI_LAUNCHER",
        "SPACE_SMOKE_WASI_LAUNCH_SPEC",
    ),
)
def wasm_contained_spaced(test: SpacedRuntimeFixture) -> None:
    """Own a fresh namespace child, then exercise the normal spaced command API."""
    context = test.context
    program = Path(os.environ["SPACE_SMOKE_WASI_LAUNCHER"])
    specification = Path(os.environ["SPACE_SMOKE_WASI_LAUNCH_SPEC"])
    with test.contained_agent(program, specification) as pid:
        with context.phase("verify the native guest isolation boundary"):
            status = (Path("/proc") / str(pid) / "status").read_text()
            fields = dict(line.split(":", 1) for line in status.splitlines() if ":" in line)
            context.verify(
                int(fields["Uid"].split()[1]) >= 10000,
                "native guest retained a privileged identity",
            )
            context.verify(
                fields["NSpid"].split()[-1] == "1",
                "native guest is not the contained PID-namespace init",
            )
            context.verify(fields["NoNewPrivs"].strip() == "1", "guest may acquire new privileges")
            root = Path("/proc") / str(pid) / "root"
            context.verify(not (root / "dev/kvm").exists(), "WASI guest received KVM")
            context.verify(not (root / "dev/net/tun").exists(), "WASI guest received TUN")
            context.verify(not (root / "control.sock").exists(), "WASI guest received a VMM listener")
        # The existing scenario owns command/file/replay/cancellation contracts.
        # This scenario's distinct delta is creation and real containment.
        commands = ResourceScope()
        try:
            wasm_spaced.verify(replace(
                test, context=ScenarioContext(commands, context.evidence),
            ))
        finally:
            # File/process cleanup must happen before the agent is terminated.
            failures = commands.close()
            context.verify(not failures, "contained command cleanup failed", cleanup=repr(failures))

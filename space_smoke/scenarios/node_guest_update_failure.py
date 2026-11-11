"""Failure recovery for an operator-provisioned unready guest candidate."""

import time

import space_smoke as smoke
from space_smoke.direct_node import DirectNodeRecoveryFixture


@smoke.scenario(
    "node.guest_update_failure",
    sandbox=smoke.SandboxConfig(lifecycle=None),
    api_surfaces=("node",),
    required_tools=("ssh",),
    tier="manual",
    manual_reason="requires a dedicated idle node and an authorized candidate that hangs before READY until systemd terminates it",
    rerun_environment=(
        "SPACE_SMOKE_NODE_RECOVERY", "SPACE_SMOKE_NODE_HOST", "SPACE_SMOKE_NODE_MACHINE_ID",
        "SPACE_SMOKE_NODE_CLIENT_ENV", "SPACE_SMOKE_NODE_TEMPLATE",
        "SPACE_SMOKE_NODE_CONTENT_HASH", "SPACE_SMOKE_NODE_ENVIRONMENT",
    ),
)
def guest_update_failure(test: DirectNodeRecoveryFixture) -> None:
    """A failed trial cannot starve policy changes or poison later checkpoints."""
    with test.context.phase("start a bounded failing guest trial"):
        test.prepare(rootfs_size_mib=4096)
        execution = test.start(memory_mib=1024)
        original = test.guest_maintenance_status()
        operation = test.guest_maintenance(update=True)
        deadline = time.monotonic() + 30
        while True:
            receipt = test.guest_maintenance_status(operation)
            if receipt.phase == "GUEST_UPDATE_OPERATION_PHASE_ACTIVATING":
                break
            test.context.verify(
                receipt.phase != "GUEST_UPDATE_OPERATION_PHASE_COMPLETE" and time.monotonic() < deadline,
                f"fixture did not enter an observable trial: {receipt}",
            )
            test.context.wait(0.2)
    with test.context.phase("persist timeout intent while the candidate is unready"):
        test.command(["space-sbxmgr", "update", test.owned_selector(),
                      "--idle-policy", "suspend@30m", "--wake-on", "access"], timeout=5)
    with test.context.phase("stop revokes an unfinished trial without waiting for its deadline"):
        test.command(["space-sbxmgr", "stop", test.owned_selector(), "--blocking"], timeout=30)
        retired = test.await_retired_guest_maintenance(operation)
        test.context.verify(retired.outcome == "superseded", f"stop retained update authority: {retired}")
        test.context.verify(retired.incarnation is None, "receipt lookup contacted a stopped guest")
        replacement = test.start(memory_mib=1024)
        test.context.verify(replacement != execution, "cold start reused the retired execution")
        execution = replacement
        operation = test.guest_maintenance(update=True)
    with test.context.phase("recover the committed guest and checkpoint"):
        result = test.await_guest_maintenance(operation)
        test.context.verify(result.outcome == "candidate_failed", f"unexpected trial outcome: {result}")
        test.context.verify(result.committed_digest == original.committed_digest,
                            "failed candidate replaced the committed release")
        test.context.verify(test.observe() == execution, "trial recovery replaced the VM")
        test.command(["space-sbxmgr", "checkpoint", test.owned_selector(), "--blocking"], timeout=240)
        test.agent_command("systemctl is-active spaced.service space.target")

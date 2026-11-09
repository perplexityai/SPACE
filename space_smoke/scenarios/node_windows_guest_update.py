"""Manager-owned guest replacement on the Windows recovery seed contract."""

import space_smoke as smoke
from space_smoke.direct_node import DirectNodeRecoveryFixture


@smoke.scenario(
    "node.windows_guest_update",
    sandbox=smoke.SandboxConfig(lifecycle=None),
    api_surfaces=("node",),
    required_tools=("ssh",),
    ignored="requires a Windows updater template and an authorized newer Windows bundle",
    rerun_environment=(
        "SPACE_SMOKE_NODE_RECOVERY", "SPACE_SMOKE_NODE_HOST", "SPACE_SMOKE_NODE_MACHINE_ID",
        "SPACE_SMOKE_NODE_CLIENT_ENV", "SPACE_SMOKE_NODE_TEMPLATE",
        "SPACE_SMOKE_NODE_CONTENT_HASH", "SPACE_SMOKE_NODE_ENVIRONMENT",
        "SPACE_SMOKE_WINDOWS_OPERATIONS_PROBE",
    ),
)
def windows_guest_update(test: DirectNodeRecoveryFixture) -> None:
    """Update, idempotence, idle deferral, forced termination and persistence."""
    with test.context.phase("create an idle Windows execution"):
        test.prepare(firmware=True)
        test.verify_agent_template()
        execution = test.start(memory_mib=8192)
        original = test.guest_maintenance_status()
        test.context.verify(original.version is not None, "template lacks updater identity")
    with test.context.phase("commit a newer guest and replay without restarting"):
        installed = test.await_guest_maintenance(test.guest_maintenance(update=True))
        test.context.verify(installed.outcome == "committed", f"update did not commit: {installed}")
        test.context.verify(installed.committed_digest != original.committed_digest,
                            "policy did not select a different release")
        test.context.verify(test.observe() == execution, "guest update replaced the VM execution")
        repeated = test.await_guest_maintenance(test.guest_maintenance(update=True))
        test.context.verify(repeated.incarnation == installed.incarnation, "same release restarted again")
    with test.context.phase("defer active work, then force its termination"):
        pid = test.start_agent_workload()
        descendant = test.windows_workload_descendant()
        deferred = test.await_guest_maintenance(test.guest_maintenance(update=False))
        test.context.verify(deferred.outcome == "deferred", "normal restart interrupted active work")
        test.agent_command(f"Get-Process -Id {pid},{descendant} | Out-Null")
        forced = test.await_guest_maintenance(test.guest_maintenance(update=False, force=True))
        test.context.verify(forced.outcome == "restarted" and forced.incarnation != installed.incarnation,
                            "force did not replace the guest generation")
        test.agent_command(f"if (Get-Process -Id {pid},{descendant} -ErrorAction SilentlyContinue) {{ throw 'Workload survived' }}")
    with test.context.phase("verify native process and file APIs after replacement"):
        test.verify_windows_agent_operations()
    with test.context.phase("retain the committed release across memory lifecycle"):
        test.command(["space-sbxmgr", "checkpoint", test.owned_selector(), "--blocking"], timeout=240)
        test.agent_command("Write-Output 'checkpoint resumed'")
        test.memory_lifecycle("pause")
        test.context.verify(test.guest_maintenance_status().incarnation is None,
                            "maintenance contacted a paused guest")
        test.memory_lifecycle("resume")
        test.memory_lifecycle("suspend")
        test.memory_lifecycle("restore")
        test.context.verify(test.guest_maintenance_status().committed_digest == installed.committed_digest,
                            "restore lost the committed release")
        test.agent_command("Write-Output 'restore resumed'")
    with test.context.phase("retain the committed release across cold boot"):
        test.stop()
        test.start(memory_mib=8192)
        test.context.verify(test.guest_maintenance_status().committed_digest == installed.committed_digest,
                            "cold boot lost the committed release")
        test.agent_command("Write-Output 'cold boot ready'")

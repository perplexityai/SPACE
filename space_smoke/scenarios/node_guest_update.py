"""Manager-owned guest replacement on an explicitly selected standalone node."""

import space_smoke as smoke
from space_smoke.direct_node import DirectNodeRecoveryFixture


@smoke.scenario(
    "node.guest_update",
    sandbox=smoke.SandboxConfig(lifecycle=None),
    api_surfaces=("node",),
    required_tools=("ssh",),
    ignored="requires a dedicated idle node, qualified selector template and authorized newer guest bundle",
    rerun_environment=(
        "SPACE_SMOKE_NODE_RECOVERY", "SPACE_SMOKE_NODE_HOST", "SPACE_SMOKE_NODE_MACHINE_ID",
        "SPACE_SMOKE_NODE_CLIENT_ENV", "SPACE_SMOKE_NODE_TEMPLATE",
        "SPACE_SMOKE_NODE_CONTENT_HASH", "SPACE_SMOKE_NODE_ENVIRONMENT",
    ),
)
def guest_update(test: DirectNodeRecoveryFixture) -> None:
    """Idle commit, durable receipt, idempotent no-op, force and memory restore."""
    with test.context.phase("create an idle updater-capable execution"):
        test.prepare(rootfs_size_mib=4096)
        execution = test.start(memory_mib=1024)
        original = test.guest_maintenance_status()
        test.context.verify(original.version is not None, "template lacks updater identity")
    with test.context.phase("commit a newer guest through sandbox-manager"):
        operation = test.guest_maintenance(update=True)
        installed = test.await_guest_maintenance(operation)
        test.context.verify(installed.outcome == "committed", f"upgrade did not commit: {installed}")
        test.context.verify(installed.committed_digest != original.committed_digest,
                            "fixture policy did not select a different release")
        test.context.verify(test.observe() == execution, "agent update replaced the VM execution")
    with test.context.phase("recover durable receipt and avoid a duplicate restart"):
        test.restart_managers()
        recovered = test.guest_maintenance_status(operation)
        test.context.verify(recovered.outcome == "committed", "manager restart lost the committed receipt")
        repeated = test.await_guest_maintenance(test.guest_maintenance(update=True))
        test.context.verify(repeated.incarnation == installed.incarnation, "same release restarted again")
    with test.context.phase("busy refusal followed by explicit force"):
        pid = test.start_agent_workload()
        deferred = test.await_guest_maintenance(test.guest_maintenance(update=False))
        test.context.verify(deferred.outcome == "deferred", "normal restart interrupted active work")
        test.agent_command(f"test -e /proc/{pid}")
        forced = test.await_guest_maintenance(test.guest_maintenance(update=False, force=True))
        test.context.verify(forced.outcome == "restarted" and forced.incarnation != installed.incarnation,
                            "force did not replace the agent generation")
        test.agent_command(f"test ! -e /proc/{pid}")
        test.context.verify(test.agent_command("find /var/lib/spaced/slots -name spaced-svc | wc -l") == "2",
                            "guest retained more than two agent copies")
    with test.context.phase("checkpoint resumes customer dispatch"):
        test.command(["space-sbxmgr", "checkpoint", test.owned_selector(), "--blocking"], timeout=240)
        test.context.verify(test.observe() == execution, "checkpoint replaced the VM execution")
        test.agent_command("test -d /proc/self")
    with test.context.phase("updated release survives pause and persisted restore"):
        test.memory_lifecycle("pause")
        dormant = test.guest_maintenance_status()
        test.context.verify(dormant.incarnation is None, "maintenance status contacted a paused guest")
        test.memory_lifecycle("resume")
        test.memory_lifecycle("suspend")
        test.memory_lifecycle("restore")
        restored = test.guest_maintenance_status()
        test.context.verify(restored.committed_digest == installed.committed_digest,
                            "restored sandbox lost the selected release")
        test.agent_command("systemctl is-active spaced.service space.target")


"""The lifecycle conformance matrix shared by Linux and Windows scenarios."""

from space_smoke.direct_node import DirectNodeRecoveryFixture


def exercise_memory_lifecycle(test: DirectNodeRecoveryFixture, *, firmware: bool) -> None:
    """Require the selected scenario's node-operation opt-in before touching the node."""
    with test.context.phase("start a guest process"):
        test.prepare(firmware=firmware)
        if firmware:
            test.verify_agent_template()
        execution = test.start(memory_mib=8192 if firmware else 1024)
        marker = test.memory_marker(start=True)
        if firmware:
            test.verify_ssh_banner()
        else:
            boot = test.write_marker()
    if not firmware:
        with test.context.phase("named full snapshot"):
            test.command(["space-sbxmgr", "snapshot", test.owned_selector(), "--full"], timeout=180)
    for cycle in range(3):
        with test.context.phase(f"resident pause/resume {cycle}"):
            test.context.verify(test.memory_lifecycle("pause") == execution, "pause replaced the VM")
            test.context.verify(test.memory_lifecycle("resume") == execution, "resume replaced the VM")
            marker = test.verify_memory(marker)
        with test.context.phase(f"persisted suspend/restore {cycle}"):
            test.memory_lifecycle("suspend")
            if cycle == 0 and not firmware:
                test.restart_managers()
            restored = test.memory_lifecycle("restore")
            test.context.verify(restored is not None and restored.execution_id != execution.execution_id,
                                "restore reused the suspended execution")
            if restored is None:
                raise AssertionError("restore returned no execution")
            execution = restored
            marker = test.verify_memory(marker)
            if firmware:
                test.verify_ssh_banner()
    if not firmware:
        with test.context.phase("adopt restored execution after manager restart"):
            test.restart_managers()
            test.context.verify(test.verify_marker() == boot, "manager restart changed guest boot or disk")
            marker = test.verify_memory(marker)
            test.memory_lifecycle("pause")
            test.memory_lifecycle("resume")
            marker = test.verify_memory(marker)
    if firmware:
        with test.context.phase("verify native process and file APIs after repeated restore"):
            test.verify_windows_agent_operations()
    for action in ("pause", "suspend"):
        with test.context.phase(f"automatic {action} and wake"):
            test.automatic_memory_lifecycle(action)
            test.memory_lifecycle("wake")
            marker = test.verify_memory(marker)
            if firmware:
                test.verify_ssh_banner()
    with test.context.phase("stop a paused guest and delete the scenario sandbox"):
        test.memory_lifecycle("pause")
        test.stop()
        test.cleanup()
        test.selector = None
        test.create_directory = None

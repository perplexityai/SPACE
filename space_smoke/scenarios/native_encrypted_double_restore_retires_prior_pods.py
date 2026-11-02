"""An encrypted sandbox restores twice, each restore retiring the suspended pod."""

import shlex

import space_smoke as smoke

PATH = "/home/user/data/encrypted-double-restore.txt"
MARKER = "space-smoke-encrypted-double-restore-ok"


# A restored execution's pod is released only after sandbox-manager proves its
# suspend durable. An encrypted upload can outlast the shim Delete deadline
# containerd sets after the VM exits; the later sandbox-stop retry must still
# wait for that proof. If it returns without waiting, the pod stays Terminating
# and the next restore is deferred behind it until the wake fails. Generation 2
# is the first restored execution, so the second cycle is the regression.
# Unlike `local.encrypted_resuspend_preserves_cow`, nothing force-clears a pod.
# It asserts sandbox pods, which the Direct substrate has none of by contract.
@smoke.scenario(
    "native.encrypted_double_restore_retires_prior_pods",
    sandbox=smoke.SandboxConfig(kind_byok=True),
    api_surfaces=("native",),
    substrates=("kubernetes",),
)
def encrypted_double_restore_retires_prior_pods(
    test: smoke.CommandSandboxFixture,
) -> None:
    """A BYOK sandbox survives suspend, restore, suspend and restore with its
    file intact, and each restore leaves only the serving execution's pod."""

    encrypted = test.encrypted_resuspend
    marker = f"{MARKER}-{test.sandbox.sandbox_id}"

    with test.phase("write a file before the first suspend"):
        written = test.run_command_when_ready(
            f"mkdir -p {shlex.quote(PATH.rsplit('/', 1)[0])} && "
            f"printf %s {shlex.quote(marker)} > {shlex.quote(PATH)}"
        )
        test.verify(
            written.exit_code == 0,
            "failed to write the file before encrypted suspend",
            command_stderr=written.stderr,
        )

    for cycle in (1, 2):
        with test.phase(f"seal encrypted suspend {cycle}"):
            node = encrypted.current_node()
            test.suspend_durably()
            encrypted.sealed_snapshot(node)
        with test.phase(f"restore encrypted suspend {cycle}"):
            test.wake_from_durable_suspend()
            observed = test.run_command_when_ready(f"cat {shlex.quote(PATH)}")
            test.expect_command(observed, stdout=marker)
        with test.phase(f"confirm the execution suspended in cycle {cycle} is gone"):
            encrypted.expect_prior_executions_retired()

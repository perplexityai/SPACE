"""A sealed worker refuses every VM activation and still tears down; unsealed, it activates again."""

import space_smoke as smoke


# The seal's refusal, its in-flight accounting and its persistence are crate
# tests (`crates/l40_api/sbx/mgr/server/src/activation_seal/tests.rs` and the
# admission tests beside it), against a fake node-manager. This scenario is
# the seam on a real worker: the public create, resume and wake reach a sealed
# sandbox-manager through the control plane and the shim, none is admitted,
# and the resume comes back with the seal's marker. A create and a restore
# start their VM inside a pod's sandbox creation, so their refusal goes to
# the kubelet and the operation ends without it. A suspend and a delete
# still complete.
#
# A native scenario because a resident pause and its resume are a native-API
# lifecycle (an ECW pause is a durable suspend). The seal and the cordon are
# Kind-only: on a deployed backend `test.activation_seal` raises a
# configuration error, so the run ends inconclusive before it cordons or seals
# anything, and only the fixture's own sandbox is created and deleted. The
# other sandbox worker stays cordoned throughout, so every create and every
# portable restore the scenario makes lands on the sealed worker; run it on a
# bench with exactly two sandbox workers and no other smoke holding the
# worker-cordon Lease. Cleanup unseals the worker before it deletes the
# fixture sandbox and uncordons the other worker.
@smoke.scenario(
    "native.sealed_worker_refuses_activations",
    api_surfaces=("native",),
    # The cordon pins Kubernetes scheduling.
    substrates=("kubernetes",),
    # Two durable suspends, three refused activations and three that succeed.
    timeout_minutes=30,
)
def sealed_worker_refuses_activations(test: smoke.CommandSandboxFixture) -> None:
    """While a worker's sandbox-manager is sealed, a create placed on it, a
    wake of a sandbox paused there and a wake of a sandbox suspended there
    each fail without the worker admitting an activation, the resume with
    the seal's marker in its answer, the paused and suspended
    sandboxes stay paused and suspended, and the seal stays in place without
    admitting any activation; a suspend and a delete on that worker still
    succeed. Once it is unsealed the worker reads unsealed, and a create, a
    resume of a paused sandbox and a wake of a suspended one all succeed
    there."""

    seal = test.activation_seal

    with test.phase("pin the scenario to the fixture sandbox's worker"):
        seal.pin_to_resident_worker()

    with test.phase("stage a suspended and a paused sandbox on that worker"):
        suspended = seal.create_on_worker()
        suspended.suspend()
        seal.expect_lifecycle(suspended.sandbox_id, "suspended")
        test.pause_resident()
        seal.expect_lifecycle(test.sandbox.sandbox_id, "paused")

    with test.phase("seal the worker's sandbox-manager"):
        seal.seal()

    with test.phase("sealed: a create on the worker is refused"):
        # The refusal reaches the kubelet, not the create's operation.
        seal.expect_refused("create", seal.create_on_worker, answer_carries_marker=False)

    with test.phase("sealed: resuming the paused sandbox is refused, and it stays paused"):
        seal.expect_refused("resume", seal.fixture_resident().wake)
        seal.expect_lifecycle(test.sandbox.sandbox_id, "paused")

    with test.phase("sealed: waking the suspended sandbox is refused, and it stays suspended"):
        # A restore also starts in a new pod's sandbox creation.
        seal.expect_refused("wake", suspended.wake, answer_carries_marker=False)
        seal.expect_lifecycle(suspended.sandbox_id, "suspended")

    with test.phase("sealed: a suspend and a delete still succeed"):
        test.suspend_durably()
        seal.expect_lifecycle(test.sandbox.sandbox_id, "suspended")
        test.delete_sandbox(suspended)

    with test.phase("the seal held throughout and admitted nothing"):
        seal.expect_still_sealed()

    with test.phase("unseal the worker"):
        seal.unseal()

    with test.phase("unsealed: a create, a resume and a wake succeed on the worker"):
        created = seal.create_on_worker()
        resident = seal.resident(created)
        resident.pause()
        seal.expect_lifecycle(created.sandbox_id, "paused")
        resident.wake()
        seal.expect_on_worker(created.sandbox_id)
        test.wake_from_durable_suspend()
        seal.expect_on_worker(test.sandbox.sandbox_id)
        test.expect_command(test.run_command_when_ready("printf woke"), stdout="woke")

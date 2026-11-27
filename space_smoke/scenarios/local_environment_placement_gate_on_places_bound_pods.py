"""With the environment placement gate on, a bound create lands only where its environment is usable."""

import space_smoke as smoke
from space_smoke.execution_environment import (
    BUILDER_ROLE,
    E0,
    E1,
    ENVIRONMENT_LABEL_VALUE,
    WORKER_ROLE,
    environment_label_key,
)

# What an unschedulable create answers (`SubstrateError::Unschedulable`).
REFUSED_STATUS = 503
UPGRADE_MARKER_PATH = "/tmp/space-smoke-activation-upgrade.txt"


# The scenario drives the admission itself, in two stages: Firecracker
# 1.17.0 / Linux 6.1.155 (E1) first on the builders only, so its seed can be
# built while no worker runs it, then on the workers. Seed bound to E0, which
# every node runs, is built beside it. Both builds publish no tag, so each
# create names its build's digest, which records the build's binding. With
# the gate on, the E1 create has nowhere to go until the workers run E1, and
# the E0 create places as before.
@smoke.scenario(
    "local.environment_placement_gate_on_places_bound_pods",
    sandbox=smoke.SandboxConfig(timeout=900),
    tier="manual",
    manual_reason=(
        "Rolls the API, admin API and cluster controller to turn the environment placement gate "
        "on, rolls every node-manager to admit Firecracker 1.17.0 / Linux 6.1.155, installs that "
        "runtime on every node from its upstream release, and builds two seeds: run it on a bench "
        "no other scenario shares while it runs."
    ),
    # Worst case: two admission stages of up to 26 minutes each (a DaemonSet
    # rollout of up to about 662 s, 600 s of installs, 300 s of evidence), two
    # 20-minute seed builds, the gate's two writes of up to 17 minutes each
    # (three `set env` calls of up to 30 s, concurrent rollouts of up to
    # 330 s of `rollout status` and 300 s of drain each, and a 300 s
    # readiness wait, before any rollout-pace patch conflict retries), about
    # 8 minutes of creates and runtime reads, and the withdrawal's two
    # DaemonSet rollouts of up to 11 minutes each: about 156 minutes, plus
    # setup, Lease and label reads, inside the 165 the budget leaves after
    # cleanup.
    timeout_minutes=230,
    # On a failure, cleanup deletes the sandboxes, restores the gate (up to
    # 17 minutes, twice if the restore phase itself failed), waits up to
    # five minutes for the one seed build that can be running before deleting
    # its release, and owns the allowlist's two DaemonSet rollouts (about
    # 22 minutes): about 61 minutes at the very worst, 44 in the usual
    # failure, where the restore runs once.
    cleanup_timeout_seconds=3900,
)
def environment_placement_gate_on_places_bound_pods(test: smoke.KindEcw) -> None:
    """With the gate on, a create bound to an environment no worker can run
    is refused as unschedulable and leaves no sandbox, a create bound to E0
    runs E0 on a worker labelled for it, once the workers run the other
    environment its create runs it there, and resuming a suspended E0 sandbox
    after E1 becomes the default and E0 is blocked records and requires E1 on
    the activation pod and boots E1 with disk state intact. Gate, runtime
    policy and allowlists are back to what they were at the end."""

    admission = test.admit_execution_environment(E1.name)
    gate = test.environment_placement_gate

    # Setup's sandbox runs `seed:latest` with the gate off; it isn't part of
    # the claim, and the worker rollouts below shouldn't run under it.
    with test.phase("delete setup's sandbox"):
        test.delete_sandbox()

    with test.phase(f"admit {E1.name} on the builders only"):
        admission.admit([BUILDER_ROLE])

    with test.phase(f"build seed bound to {E1.name}"):
        e1_seed = admission.build_seed()

    e0_seed = test.bound_seed(E0)
    with test.phase(f"build seed bound to {E0.name}"):
        e0_selector = e0_seed.build()

    e1_label = environment_label_key(admission.reference)
    with test.phase(f"no worker can run {E1.name}, every worker can run {E0.name}"):
        workers = set(test.node_manager_nodes([WORKER_ROLE]))
        e1_nodes = set(test.nodes_carrying_label(e1_label, ENVIRONMENT_LABEL_VALUE))
        test.record("workers", sorted(workers))
        test.record("e1_usable_nodes_before_workers", sorted(e1_nodes))
        e0_nodes = set(test.nodes_carrying_label(environment_label_key(e0_seed.reference), ENVIRONMENT_LABEL_VALUE))
        test.verify(bool(workers), "the bench must run workers")
        test.verify(
            not workers & e1_nodes,
            f"a worker already carries {E1.name}'s usable label, so the refusal can't be judged",
            workers_with_label=sorted(workers & e1_nodes),
        )
        test.verify(
            workers <= e0_nodes,
            f"every worker must carry {E0.name}'s usable label",
            workers_without_label=sorted(workers - e0_nodes),
        )

    with test.phase("turn on the environment placement gate and activation runtime upgrades"):
        gate.turn_on(runtime_upgrade_at_activation=True)

    with test.phase(f"an {E0.name}-bound create runs {E0.name} on a worker labelled for it"):
        e0_sandbox = test.sandboxes.create(template=e0_selector)
        render = gate.expect_pod_requires(e0_sandbox.sandbox_id, e0_seed.reference)
        test.verify(render.node_name in workers, "the E0 sandbox must run on a worker", node=render.node_name)
        test.expect_running_environment(e0_sandbox.sandbox_id, E0, step="with the gate on")

    with test.phase(f"delete the {E0.name} sandbox"):
        test.delete_sandbox(e0_sandbox)

    with test.phase(f"an {E1.name}-bound create is refused as unschedulable"):
        # Read again: the gate rollout and the E0 create ran since the census.
        e1_nodes = set(test.nodes_carrying_label(e1_label, ENVIRONMENT_LABEL_VALUE))
        test.record("e1_usable_nodes_before_refusal", sorted(e1_nodes))
        test.verify(
            not workers & e1_nodes,
            f"a worker carries {E1.name}'s usable label before the refusal probe, so it can't be judged",
            workers_with_label=sorted(workers & e1_nodes),
        )
        refused = test.sandboxes.attempt_create(template=e1_seed, refused_statuses=(REFUSED_STATUS,))
        # A create that succeeds answers with the sandbox's access tokens, so
        # only an error body goes into the evidence.
        body = refused.body[:1_000] if refused.status_code >= 400 else "(success body withheld)"
        test.record(
            "refused_create",
            {"status": refused.status_code, "sandbox_id": refused.sandbox_id, "body": body},
        )
        test.verify(
            refused.status_code == REFUSED_STATUS and "unschedulable" in refused.body.lower(),
            f"with no worker able to run {E1.name}, its create must be refused as unschedulable",
            status=refused.status_code,
            body=body,
        )
        test.verify(refused.sandbox_id is None, "a refused create must name no sandbox", sandbox_id=refused.sandbox_id)
        listed = {info.sandbox_id for page in test.sandboxes.list_pages(page_size=50).pages for info in page}
        test.verify(
            listed <= set(test.sandboxes.ids),
            "a refused create must leave no sandbox behind",
            unowned=sorted(listed - set(test.sandboxes.ids)),
        )

    with test.phase(f"admit {E1.name} on the workers"):
        admission.admit([WORKER_ROLE])

    with test.phase(f"the {E1.name}-bound create now runs {E1.name} on a worker labelled for it"):
        e1_sandbox = test.sandboxes.create(template=e1_seed)
        render = gate.expect_pod_requires(e1_sandbox.sandbox_id, admission.reference)
        test.verify(render.node_name in workers, "the E1 sandbox must run on a worker", node=render.node_name)
        admission.expect_running(e1_sandbox.sandbox_id, step="with the gate on")

    with test.phase(f"delete the {E1.name} sandbox"):
        test.delete_sandbox(e1_sandbox)

    with test.phase(f"suspend an {E0.name}-bound sandbox with disk state"):
        upgrading = test.sandboxes.create(template=e0_selector)
        marker = f"activation-upgrade-{upgrading.sandbox_id}"
        upgrading.files.write(UPGRADE_MARKER_PATH, marker)
        gate.expect_pod_requires(upgrading.sandbox_id, e0_seed.reference)
        test.expect_running_environment(upgrading.sandbox_id, E0, step="before activation upgrade")
        upgrading.pause()
        gate.wait_until_restore_portable(upgrading.sandbox_id)

    with test.phase(f"make {E1.name} the default and block {E0.name}"):
        policy_restore = gate.upgrade_default_and_block_source(
            source_ref=e0_seed.reference,
            target_ref=admission.reference,
        )

    with test.phase(f"resuming the suspended {E0.name} sandbox records and requires {E1.name} and boots {E1.name}"):
        resumed = upgrading.connect(timeout=300)
        render = gate.expect_pod_requires(upgrading.sandbox_id, admission.reference)
        test.verify(
            render.node_name in workers,
            "the activation-upgraded sandbox must run on a worker",
            node=render.node_name,
        )
        gate.expect_pod_runtime_upgrade_target(upgrading.sandbox_id, admission.reference)
        admission.expect_running(upgrading.sandbox_id, step="after activation runtime upgrade")
        observed_marker = resumed.files.read(UPGRADE_MARKER_PATH)
        test.verify(
            observed_marker == marker,
            "activation runtime upgrade lost the sandbox's disk state",
            expected=marker,
            observed=observed_marker,
        )

    with test.phase("delete the activation-upgraded sandbox and restore runtime policy"):
        test.delete_sandbox(upgrading)
        policy_restore.release()

    with test.phase("put the environment placement gate back"):
        gate.restore()

    with test.phase(f"delete the {E0.name} seed"):
        e0_seed.delete()

    with test.phase(f"withdraw {E1.name} from the bench"):
        admission.withdraw()

"""An undrain mid-drain cancels the v2 drain on its node: the worker stays cordoned until the node answers, then previews unsealed and admits activations again."""

import space_smoke as smoke

# How long the run watches the undrain wait on a node that doesn't answer:
# several loop ticks past the call's own timeout.
UNANSWERED_SECONDS = 90
# The drain's hold: the resident keeps running through the whole run.
HOLD_SECONDS = 60 * 60


# The seal's forward-only generations and the cancel's tombstone are crate
# tests (`activation_seal/tests.rs`), and only the control plane may call a
# node's `DrainNode`, so no run sends a stale generation; the controller's
# cancel on every tick until the node answers, and its uncordon only then,
# are `node_drain/v2/tests.rs`. This is the round trip on a real worker,
# with node-api cut across the undrain as in
# `native.v2_drain_waits_for_an_unreachable_node`. The API shows no seal: a
# preview that succeeds once the cancel has cleared is what proves the node
# unsealed, since a sealed node refuses one.
@smoke.scenario(
    "native.v2_undrain_unseals_the_worker",
    api_surfaces=("native",),
    substrates=("kubernetes",),
    timeout_minutes=30,
    tier="manual",
    manual_reason="kept manual until it has several consecutive clean runs on Kind, after which it can return to the per-merge set",
)
def v2_undrain_unseals_the_worker(test: smoke.CommandSandboxFixture) -> None:
    """A v2 `:drain` with a hold seals the worker, and the controller stores
    the node's report under the drain's generation; its node-api then stops
    answering and the worker is `:undrain`ed. While the node can't answer,
    its Node stays unschedulable and the cancel stays pending. Once node-api
    answers, the controller cancels the drain on the node, clears the v2
    columns and uncordons the worker; a preview of it then succeeds, which a
    sealed node refuses, and it admits an activation again: with the other
    worker cordoned, the resident's wake lands on it."""

    drain = test.drain_v2

    with drain.v2_controller():
        with test.phase("pin a resident to one worker"):
            # A v2 drain needs the node's instance recorded; re-registering
            # under a fake one deletes the pods there, so the fixture's
            # worker is left alone.
            drain.pin_to_resident_worker(away_from_fixture=True)
            drain.give_fake_instance()
            resident = drain.create_resident()
            drain.release_other_worker()

        with test.phase("drain with engine v2; the node reports under the drain's generation"):
            drain.drain(hold=HOLD_SECONDS)
            drain.wait_for_reported()
            drain.expect_held(resident.sandbox_id)

        with test.phase("node-api stops answering; undrain: the worker stays cordoned"):
            node_api = drain.block("node-api")
            drain.undrain()
            drain.expect_kept_cordoned(watch_seconds=UNANSWERED_SECONDS)

        with test.phase("node-api back: the drain is cancelled, the worker uncordoned; a preview succeeds"):
            node_api.release()
            drain.expect_uncordoned_unsealed()

        with test.phase("activations are accepted again"):
            # The other worker cordoned again, the wake can land only on the
            # undrained one: its activation is what this phase proves.
            drain.cordon_other_worker()
            resident.suspend()
            resident.wake()
            _, active_node = drain.residency(resident.sandbox_id)
            test.verify(
                (active_node or "").lower() == drain.node_id.lower(),
                "the wake didn't land on the undrained worker",
                active_node=active_node,
                drained_node=drain.node_id,
            )
            observed = drain.run_on(resident, "true")
            test.verify(observed.exit_code == 0, "the unsealed worker did not run a restored sandbox")
            drain.release_other_worker()

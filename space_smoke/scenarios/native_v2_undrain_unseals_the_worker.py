"""An undrain mid-drain cancels the v2 drain's epoch: the worker is unsealed and refuses it again."""

import space_smoke as smoke


# The seal's forward-only epochs and the remembered cancel are crate tests
# (`activation_seal/tests.rs`); the controller's cancel is
# `node_drain/v2/tests.rs`. This is the round trip on a real worker.
@smoke.scenario(
    "native.v2_undrain_unseals_the_worker",
    api_surfaces=("native",),
    substrates=("kubernetes",),
    timeout_minutes=30,
)
def v2_undrain_unseals_the_worker(test: smoke.CommandSandboxFixture) -> None:
    """A v2 `:drain` seals the worker under the drain's epoch; `:undrain`
    before it finishes clears the phase, and the controller cancels the epoch
    on the node and clears the v2 columns. The worker is unsealed and admits
    an activation again, and a seal under the cancelled epoch is refused."""

    drain = test.drain_v2

    with drain.v2_controller():
        with test.phase("pin a resident to one worker"):
            drain.pin_to_resident_worker()
            resident = drain.create_resident()
            drain.release_other_worker()

        with test.phase("drain with engine v2; the node reports sealed under the epoch"):
            epoch = drain.drain()
            drain.wait_for_report(
                lambda report: report.get("sealed_epoch") == epoch,
                "the node never reported sealed under the drain's epoch",
                timeout=180,
            )

        with test.phase("undrain: the epoch is cancelled on the node and the worker unsealed"):
            drain.undrain_and_expect_cancelled()

        with test.phase("activations are accepted again"):
            resident.suspend()
            resident.wake()
            observed = drain.run_on(resident, "true")
            test.verify(observed.exit_code == 0, "the unsealed worker did not run a restored sandbox")

        with test.phase("a seal under the cancelled epoch is refused"):
            drain.expect_stale_epoch_refused()

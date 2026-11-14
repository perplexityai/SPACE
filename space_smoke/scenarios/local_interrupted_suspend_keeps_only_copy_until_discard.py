"""An interrupted suspend keeps the sandbox's only copy until an operator discards it."""

import space_smoke as smoke


# The fault holds node-manager for a few seconds while the suspend sits in
# `suspending` (see `space_smoke.retained_only_copy`). Kind's discard window
# is 120 s (`discardAttemptWindowSecs`), which the scenario waits out before
# the discard that must succeed. Do not run concurrently with another
# suspend-bearing smoke on the same node: holding node-manager stalls them.
@smoke.scenario(
    "local.interrupted_suspend_keeps_only_copy_until_discard",
    # Worst cases add to 1200 s: the pause 120 s, the row reaching
    # `suspending` 60 s and `quarantined` 180 s, the deletes 3 x 180 s, Kind's
    # 120 s window plus slack, the row going 60 s, and the control plane's
    # delete and the pod's release 2 x 180 s. Create and the reads come on top.
    sandbox=smoke.SandboxConfig(timeout=1500),
    # Kind's discard window; the release lane renders no test hook and keeps
    # the six-hour window, which no smoke can wait out.
    test_hooks="SPACE_SBX_DISCARD_ATTEMPT_WINDOW_SECS",
)
def interrupted_suspend_keeps_only_copy_until_discard(test: smoke.KindEcw) -> None:
    """A suspend that fails with its runtime gone and no capture kept leaves the
    sandbox quarantined as its only copy: the public delete doesn't complete, the
    node's ordinary delete fails, and the operator's discard refuses while the
    suspend attempt could still finish. Once that window has passed, the
    discard erases the sandbox and its filesystem, and the public delete then
    completes and the sandbox's pod is released."""

    only_copy = test.retained_only_copy

    with test.phase("find the sandbox's node"):
        only_copy.locate()

    with test.phase("fail the suspend with its runtime gone and no capture"):
        only_copy.interrupt_suspend()

    with test.phase("every ordinary delete refuses the only copy"):
        only_copy.expect_ordinary_deletes_refused()

    with test.phase("the discard refuses while the suspend attempt could finish"):
        only_copy.expect_discard_refused_inside_window()

    with test.phase("after the window the discard erases it"):
        only_copy.expect_discard_erases()

    with test.phase("the control plane then deletes it and releases its pod"):
        only_copy.expect_control_plane_releases()

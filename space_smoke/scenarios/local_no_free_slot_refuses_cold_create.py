"""A cold create is refused with a retryable 503 when no node has a free network slot."""

import space_smoke as smoke

# The most slots per node this scenario will fill. Above it the fill stops
# being affordable -- every slot is a real Firecracker sandbox -- so a bench
# whose nodes advertise more fails the precondition rather than trying.
MOST_SLOTS_PER_NODE = 4


# The slot check is a captured, Lease-guarded, kind-only env override whose
# restore is armed before the first write; the fill is run-owned sandboxes with
# cleanup armed before each create, and the one create the recovery probe gets
# admitted carries run metadata the teardown kills by.
@smoke.scenario(
    "local.no_free_slot_refuses_cold_create",
    # One API rollout to enable the check, one to restore it, and one real
    # sandbox per network slot the bench advertises.
    sandbox=smoke.SandboxConfig(timeout=1800),
    test_hooks="networkPoolMaxSlots on the node-manager chart, which this bench must set to a "
    "small count; at the release default of 300 the fill would be thousands of real boots",
)
def no_free_slot_refuses_cold_create(test: smoke.KindEcw) -> None:
    """With every start-eligible node's network slots held by real sandboxes and
    the activation limiter's slot check switched on, a cold create is refused
    with a 503 `Server capacity limit:` naming a free sandbox slot and carrying
    Retry-After; a reconnect to an already-running sandbox is still served; and
    a create is admitted again once a slot comes free."""

    slots = test.no_free_slot

    if not slots.capability_declared():
        # Declared rather than probed: the check leaves no fingerprint the
        # harness can read, so nothing distinguishes a build without the code
        # from one whose code is broken. Recorded under its own name -- a skip
        # is not a pass, and this scenario asserts nothing at all without it.
        test.record("slot_check", "absent: this bench does not declare the slot-check build")
        return

    with test.phase("confirm the bench advertises a fillable slot budget"):
        slots.expect_fillable_slot_budget(most_per_node=MOST_SLOTS_PER_NODE)

    with test.phase("fill every eligible node's network slots"):
        # The fixture's own sandbox already holds one, and the fill counts it:
        # asking for one more than the cluster can place would fail as a create
        # error rather than as the refusal this scenario is about.
        test.wait_until_running()
        slots.fill_slots()

    with test.phase("switch the slot check on"):
        # After the fill, not before: the check refuses exactly the shape the
        # last of the fill's own creates has, so enabling it first would have
        # the fill racing the condition it exists to establish.
        slots.enable_slot_check()

    with test.phase("a cold create is refused with a retryable 503"):
        slots.expect_no_free_slot()

    with test.phase("a reconnect to a running sandbox is still served"):
        slots.expect_reconnect_admitted(test.sandbox.sandbox_id)

    with test.phase("a create is admitted again once a slot comes free"):
        slots.expect_recovery()

    with test.phase("restore the public API slot-check env"):
        slots.restore_slot_check()

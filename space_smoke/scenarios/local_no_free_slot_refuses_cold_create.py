"""A cold create is refused with a retryable 503 when no node has a free network slot."""

import space_smoke as smoke

SCOPED_SLOTS_PER_NODE = 2
MOST_CREATES = 8


# Both cluster knobs (`SPACE_NODEMGR_NETWORK_POOL_MAX_SLOTS` on worker
# node-manager and `SPACE_CLUSTER_ACTIVATION_ADMISSION_SLOT_CHECK_ENABLED` on
# `space-api`) are Lease-guarded, recorded before mutation, and restored in
# scenario phases on exit, so this runs in the routine Kind shard without a
# dedicated bench.
@smoke.scenario(
    "local.no_free_slot_refuses_cold_create",
    required_tools=("kubectl", "docker"),
    sandbox=smoke.SandboxConfig(timeout=1800),
)
def no_free_slot_refuses_cold_create(test: smoke.KindEcw) -> None:
    """With worker network-slot capacity scoped to a small count per node and
    the activation limiter's slot check switched on first, sequential cold
    creates fill the remaining slots until the API refuses with a 503 `Server
    capacity limit:` naming a free sandbox slot and carrying Retry-After, and
    admitted sandboxes (fixture included) match the sum of sandbox-pool nodes'
    `status.allocatable` network slots; a reconnect to an already-running
    sandbox is still served; and freeing one slot lets a create succeed again
    before both cluster knobs are restored."""

    slots = test.no_free_slot

    with slots.scoped_slot_pool():
        with test.phase("scope worker network-slot capacity for this scenario"):
            test.wait_until_running()
            slots.shrink_worker_slots(slots_per_node=SCOPED_SLOTS_PER_NODE)

        with test.phase("switch the slot check on"):
            # Enabled before filling, not after: sequential creates let the
            # API's own slot-shortage 503 decide when eligible capacity is full,
            # and the phase then checks that admitted count against
            # `status.allocatable` instead of mirroring eligibility in Python.
            slots.enable_slot_check()

        with test.phase("fill slots sequentially until a cold create is refused with 503"):
            slots.fill_until_no_free_slot(max_creates=MOST_CREATES)

        with test.phase("a reconnect to a running sandbox is still served"):
            slots.expect_reconnect_admitted(test.sandbox.sandbox_id)

        with test.phase("a create is admitted again once a slot comes free"):
            slots.expect_recovery()

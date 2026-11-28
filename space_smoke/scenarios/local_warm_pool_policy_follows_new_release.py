"""A rolling warm-pool policy moves its pool to the release a rebuild tags `latest`."""

import space_smoke as smoke


# Routine. A run is two minimal SDK builds, two one-slot pool fills and a
# drain: about a minute on a quiet bench. Each wait has its own bound from the
# reconciler's pacing (`space_smoke/warm_pool_policy.py`) and fails there,
# naming what did not happen. Those bounds add up to about 33 minutes (two
# warming timeouts among them) plus the builds, so the budget is raised to 40
# minutes: a run whose waits all run long still fails at a named bound, not
# at the scenario's timeout.
@smoke.scenario("local.warm_pool_policy_follows_new_release", timeout_minutes=40)
def warm_pool_policy_follows_new_release(test: smoke.KindAdmin) -> None:
    """With a rolling warm-pool policy retaining one release of a run-owned
    template, an ECW create of the template claims a ready slot of the
    policy-owned pool for the release its first build tagged `latest`. Once a
    second build with different content moves `latest`, the policy-owned pool
    of the new release gets a ready slot and the old release's is disabled
    within the reconciler's pacing, and a create of the template claims a
    ready slot of the new release's pool and runs the new release's content
    hash and content."""

    policy = test.warm_pool_policy

    with test.phase("node-group placement is off, as Kind ships it"):
        policy.require_placement_off()

    with test.phase("build a run-owned template through the SDK"):
        first = policy.build_release()

    with test.phase("write a rolling policy: one ready slot, one retained release, the default shape"):
        policy.write_policy()

    with test.phase("the policy's pool for the first release gets a ready slot"):
        first_pool = policy.await_ready_pool(first)

    with test.phase("a create of the template claims a warm slot of the first release"):
        policy.expect_warm_claim(first, first_pool)

    with test.phase("build the template again with different content; latest moves"):
        second = policy.build_release()

    with test.phase("the policy follows latest: the new release's pool fills, the old one's is disabled"):
        second_pool = policy.await_follow(first, second)

    with test.phase("a create of the template claims a warm slot of the second release"):
        policy.expect_warm_claim(second, second_pool)

    with test.phase("kill the run's sandboxes"):
        policy.kill_sandboxes()

    with test.phase("disable the policy and wait until the template has no pool configs"):
        policy.disable_policy()

    with test.phase("delete the template"):
        policy.delete_template()

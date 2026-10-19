"""Both API rate limiters reject past their caps, admit when Redis is gone, and recover."""

import space_smoke as smoke

CALLERS = 20
FAIL_OPEN_CALLERS = 4


# The limiters already run against the chart-rendered Redis. The cap override
# is a captured, Lease-guarded, kind-only operation whose restore is armed
# before the first write and recorded on the Lease, where the next scenario
# finds it if this run never restores; the Redis fault is recorded the same
# way and has its scale-back armed before dispatch; every sandbox the probes
# admit carries run metadata the teardown kills by.
# Caps are low because each admitted create boots a real Firecracker VM: at
# the default burst the drain would need sixty.
@smoke.scenario(
    "local.rate_limiters_enforce_and_fail_open",
    # Two API rollouts (enable, restore), a Redis scale-down/up, and about ten
    # real sandbox boots.
    sandbox=smoke.SandboxConfig(timeout=1800),
)
def rate_limiters_enforce_and_fail_open(test: smoke.KindEcw) -> None:
    """With both limiters' caps shrunk and per-client concurrency in enforce
    mode, sequential creates drain the cluster activation bucket to a 429
    `Server rate limit:` with Retry-After; twenty concurrent creates and twenty
    concurrent connects for one client id each draw at least one 429
    `Per-client rate limit:` with Retry-After; once Redis is unreachable
    neither limiter rejects; and once it is back the per-client limiter
    rejects again."""

    limiters = test.rate_limiters

    with test.phase("shrink both limiters' caps on the public API, enforce per-client"):
        limiters.enable_limiters(template=test.sandbox_template_name())

    with test.phase("preflight: one create is admitted or rate-limited"):
        first = limiters.preflight()
        connect_target = first.sandbox_id

    with test.phase("cluster admission: drain the token bucket"):
        drained = limiters.drain_cluster_bucket()
        test.record("cluster_limit_message", drained.message[:120])

    with test.phase("per-client concurrency on create"):
        outcomes = limiters.burst_creates(CALLERS)
        limiters.expect_per_client_limited(outcomes, label="per_client_create")
        connect_target = connect_target or next(iter(limiters.admitted_ids(outcomes)), None)

    with test.phase("per-client concurrency on connect"):
        # The per-client slot is reserved at the top of the connect handler,
        # before the sandbox is fetched, so the cap trips whatever the target's
        # state; the fixture's own sandbox is the fallback target.
        target = connect_target or test.sandbox.sandbox_id
        outcomes = limiters.burst_connects(target, CALLERS)
        limiters.expect_per_client_limited(outcomes, label="per_client_connect")

    with test.phase("fail open: Redis unreachable, no limiter rejects"):
        limiters.take_redis_down()
        outcomes = limiters.burst_creates(FAIL_OPEN_CALLERS)
        limiters.expect_fail_open(outcomes)
        limiters.bring_redis_back()

    with test.phase("recovery: Redis back, the per-client limiter rejects again"):
        limiters.expect_enforcing_again(target)

    with test.phase("restore the public API limiter env"):
        limiters.restore_limiters()

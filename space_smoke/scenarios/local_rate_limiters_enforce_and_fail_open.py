"""Both API rate limiters reject past their caps, admit when Redis is gone, and recover."""

import space_smoke as smoke

CALLERS = 20
FAIL_OPEN_CALLERS = 4
# The control arm stays inside the per-client cap, since it is asserted to
# succeed; the noisy arm has to be able to exceed it.
ISOLATION_CONTROL_CALLERS = 3


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
    test_hooks="SPACE_CLUSTER_ACTIVATION_ADMISSION_BURST, which it shrinks to 2; "
    "the release burst of 60 would need sixty real boots",
)
def rate_limiters_enforce_and_fail_open(test: smoke.KindEcw) -> None:
    """With both limiters' caps shrunk and per-client concurrency in enforce
    mode, sequential creates drain the cluster activation bucket to a 429
    `Server rate limit:` with Retry-After; twenty concurrent creates and twenty
    concurrent connects for one client id each draw at least one 429
    `Per-client rate limit:` with Retry-After; a separately-bucketed control
    client reconnects successfully while a twenty-deep storm for another client
    is over its cap; once Redis is unreachable neither limiter rejects; and once
    it is back the per-client limiter rejects again."""

    limiters = test.rate_limiters

    with test.phase("shrink both limiters' caps on the public API, enforce per-client"):
        limiters.enable_limiters(template=test.sandbox_template_name())

    with test.phase("preflight: one create is admitted or rate-limited"):
        first = limiters.preflight()
        connect_target = first.sandbox_id

    with test.phase("per-client isolation: a control client keeps reconnecting"):
        # The fixture's own sandbox, waited to running, is the target: the
        # round asserts that every control reconnect *succeeds*, so a target
        # that was merely created would make a boot still in progress
        # indistinguishable from the control client being starved. This one
        # predates the limiter overrides entirely, so waiting on it costs no
        # activation token. Running also means no reconnect takes the wake at
        # `lifecycle.rs`'s resume path — though a connect can still escalate
        # to one through `connect_touch_recovery`. Running before the drain is
        # what keeps that escalation affordable, but the margin is thin and
        # worth stating: `enable_limiters` shrinks the activation bucket to a
        # burst of 2 refilling at 0.1/s, and the preflight create just spent
        # one, so roughly one token stands behind 23 concurrent reconnects. An
        # escalation that outran it would take a cluster `Server rate limit:`
        # 429 and fail the success verdict on a healthy limiter. The round
        # counts those separately from per-client rejections, so such a red
        # names its own cause rather than reading as broken isolation.
        #
        # The reconnects carry a 30s timeout, which does *not* shorten this
        # sandbox's own deadline for the phases that reuse it below:
        # `ExtendUntil` reaches `extend_access_deadline`, which returns early
        # when the request is inside the deadline already in force, so it can
        # only push one out.
        test.wait_until_running()
        noisy, control = limiters.isolation_round(
            test.sandbox.sandbox_id, noisy=CALLERS, control=ISOLATION_CONTROL_CALLERS
        )
        limiters.expect_control_isolated(noisy, control)

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

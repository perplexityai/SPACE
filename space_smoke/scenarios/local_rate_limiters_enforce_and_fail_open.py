"""Both API rate limiters reject past their caps and admit when Redis is gone."""

import space_smoke as smoke

CALLERS = 20
FAIL_OPEN_CALLERS = 4


# The shell suite this replaces reconfigured `deploy/space-api` with `kubectl
# set env` and reverted it in a trap. Here the override is a captured,
# Lease-guarded, kind-only operation whose restore is armed before the first
# write, the Redis fault has its scale-back armed before dispatch, and every
# sandbox the probes admit carries run metadata the teardown kills by. Caps
# are low because each admitted create boots a real Firecracker VM. The store
# is opt-in in the bring-up, so the scenario renders it with helm when the
# bench has none and removes what it created.
@smoke.scenario(
    "local.rate_limiters_enforce_and_fail_open",
    required_tools=("helm",),
    # Two API rollouts (enable, restore), a Redis scale-down/up, and about ten
    # real sandbox boots.
    sandbox=smoke.SandboxConfig(timeout=1800),
)
def rate_limiters_enforce_and_fail_open(test: smoke.KindEcw) -> None:
    """With both limiters enabled in enforce mode against Redis, sequential
    creates drain the cluster activation bucket to a 429 `Server rate limit:`
    with Retry-After; twenty concurrent creates and twenty concurrent connects
    for one client id each draw at least one 429 `Per-client rate limit:` with
    Retry-After; and once Redis is unreachable neither limiter rejects."""

    limiters = test.rate_limiters

    with test.phase("provide Redis, then enable both limiters on the public API, low caps, enforce"):
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

    with test.phase("restore the public API limiter env"):
        limiters.restore_limiters()

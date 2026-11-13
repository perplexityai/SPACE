"""Both API rate limiters reject past their caps, admit when Redis is gone, and recover."""

import space_smoke as smoke

CALLERS = 20
FAIL_OPEN_CALLERS = 4
# The control arm stays inside the per-client cap, since it is asserted to
# succeed; the noisy arm has to be able to exceed it.
ISOLATION_CONTROL_CALLERS = 3
# Sized to the activation tokens the wake flood can actually win: the burst
# below is 3 and the preflight create spends one, so two of the wakes the
# per-client gate admits can each load a snapshot. A larger fleet would add
# real VM boots for wakes the cluster bucket would refuse anyway.
WAKE_FLEET = 2
# One above the default 2, so the wake flood has a token to spare after the
# preflight create. `expect_control_isolated` requires the noisy arm to get
# through at least once, and a wake charges a `Restore` activation token on
# top of the per-client slot; at a burst of 2 that verdict would rest on the
# single token the preflight left, with no margin for one more cold
# activation anywhere ahead of it. The drain phase that follows does not get
# more expensive for it -- the flood has already spent part of the bucket by
# the time it runs.
CLUSTER_BURST = 3
# One, so the fleet of two gives the phase exactly one admission and one
# refusal: the smallest round that can show both halves of a plateau.
LIVE_CAP = 1
LIVE_CAP_CONTROL_RECONNECTS = 2


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
    # Two API rollouts (enable, restore), a Redis scale-down/up, and a dozen
    # or so real sandbox boots -- the suspended wake fleet among them.
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
    client keeps reconnecting successfully while a twenty-deep storm for
    another client is over its cap, both while that storm is waking suspended
    sandboxes and while it is reconnecting to a running one; once Redis is
    unreachable every create admits a sandbox; once it is back the per-client
    limiter rejects again; and with the standing configuration restored the
    same flood draws no limiter rejection at all."""

    limiters = test.rate_limiters

    with test.phase("stage a suspended fleet for the wake flood"):
        # Before the caps shrink, deliberately: these are ordinary cold
        # creates, and under the shrunk activation burst they would spend the
        # tokens the flood below needs.
        suspended = limiters.stage_suspended_fleet(WAKE_FLEET)

    with test.phase("shrink both limiters' caps on the public API, enforce per-client"):
        limiters.enable_limiters(
            template=test.sandbox_template_name(), cluster_burst=CLUSTER_BURST
        )

    with test.phase("preflight: one create is admitted or rate-limited"):
        first = limiters.preflight()
        connect_target = first.sandbox_id

    with test.phase("attribution: the activating client is on the pod and the row"):
        # The preflight's sandbox, which is the first create made *after*
        # attribution was switched on -- the fixture's own sandbox predates
        # that write, so its pod would carry no label however well the
        # capability works. No extra boot: this judges a sandbox the preflight
        # already paid for.
        #
        # Declared rather than probed, for the reason in
        # `attribution_available`: the column this capability adds ships ahead
        # of the code that writes it, so its presence says nothing about
        # whether a writer exists.
        if limiters.attribution_available():
            test.verify(
                bool(connect_target),
                "the preflight create was rate-limited, so this phase has no sandbox that "
                "was created while attribution was on",
            )
            limiters.expect_client_attributed(connect_target or "")
        else:
            test.record(
                "client_attribution", "absent: this bench does not declare the attribution build"
            )

    with test.phase("per-client isolation under a wake flood"):
        # The flood the per-client limiter exists for. Every noisy request the
        # gate admits loads a snapshot and holds its slot for the length of
        # that restore, so the bucket stays full for seconds rather than
        # milliseconds -- which the reconnect round below cannot reproduce.
        # The control arm reconnects to the fixture's own running sandbox and
        # nothing else: that is the one request here which takes no activation
        # token, so the per-client gate is the only thing that can stand
        # between it and its answer. See `wake_flood` on why creating or
        # waking in the control arm would move the round onto the cluster
        # limiter.
        test.wait_until_running()
        noisy, control = limiters.wake_flood(
            suspended,
            test.sandbox.sandbox_id,
            noisy=CALLERS,
            control=ISOLATION_CONTROL_CALLERS,
        )
        limiters.expect_control_isolated(noisy, control, label="wake_flood")

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
        # 429 and fail a verdict on a healthy limiter. The round records the
        # cluster rejections of both arms separately from the per-client ones,
        # so such a red carries the evidence that it was the activation bucket
        # and not the per-client gate — but the verdict it trips is still
        # worded for the per-client case, so read those counts before the
        # message.
        #
        # The reconnects carry a 30s timeout, which does *not* shorten this
        # sandbox's own deadline for the phases that reuse it below:
        # `ExtendUntil` reaches `extend_access_deadline`, which returns early
        # when the request is inside the deadline already in force, so it can
        # only push one out.
        #
        # Already waited to running by the wake-flood phase above, which used
        # the same sandbox as its control target; nothing between the two
        # phases can move it out of running.
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

    with test.phase("live cap: one client's live sandboxes plateau at its cap"):
        # Before the Redis fault, because the cap's pending set is in Redis:
        # with the store gone the cap fails open by design, and the phase
        # would be asserting against a guardrail that had switched itself off.
        #
        # Declared rather than probed -- see `CAPABILITIES_ENV`. A bench that
        # claims the capability on a build without it fails this phase rather
        # than passing it, because the refusal simply never comes.
        if limiters.capability_declared("live_cap"):
            suspended = limiters.resuspend_fleet()
            limiters.enable_live_cap(LIVE_CAP)
            woken = limiters.connect_sequentially(suspended)
            # The control client reconnects to the fixture's own running
            # sandbox. That is not an activation, so the cap must never be
            # what answers it, and it is the brief's "a reconnect to an
            # already-running sandbox is never refused".
            control = limiters.connect_sequentially(
                [test.sandbox.sandbox_id] * LIVE_CAP_CONTROL_RECONNECTS,
                client_id=limiters.control_client_id,
            )
            limiters.expect_live_cap_plateau(woken, control)
        else:
            test.record("live_cap", "absent: this bench does not declare the live-cap build")

    with test.phase("fail open: Redis unreachable, every create admits a sandbox"):
        limiters.take_redis_down()
        outcomes = limiters.burst_creates(FAIL_OPEN_CALLERS)
        limiters.expect_fail_open(outcomes)
        limiters.bring_redis_back()

    with test.phase("recovery: Redis back, the per-client limiter rejects again"):
        limiters.expect_enforcing_again(target)

    with test.phase("restore the public API limiter env"):
        limiters.restore_limiters()

    with test.phase("off by default: with no caps configured the flood is admitted"):
        # The converse of every phase above, and the one the enablement work
        # actually risks: that shipping the caps starts refusing traffic a
        # cluster with none set would have taken. Runs last because it needs
        # the standing env back -- the configuration under test here is
        # whatever the chart renders, not anything this scenario set.
        outcomes = limiters.burst_creates(FAIL_OPEN_CALLERS)
        limiters.expect_admitted_by_default(outcomes)

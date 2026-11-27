"""Both API rate limiters reject past their caps, admit when Redis is gone, and recover."""

import space_smoke as smoke

CALLERS = 20
FAIL_OPEN_CALLERS = 4
# The control arm stays inside the per-client cap, since it is asserted to
# succeed; the noisy arm has to be able to exceed it.
ISOLATION_CONTROL_CALLERS = 3
# Sized to the activation tokens the wake flood can actually win: the burst
# below is 3, the preflight create spends at most one, and the flood waits for
# a token first, so at least one and usually two of the wakes the per-client
# gate admits can each load a snapshot. A larger fleet would add
# real VM boots for wakes the cluster bucket would refuse anyway.
WAKE_FLEET = 2
# One above the default 2. `await_activation_token` already guarantees the
# flood one token whatever the burst; the 3 is what lets a second admitted
# wake restore concurrently with the first, since each wake charges a
# `Restore` activation token on top of the per-client slot. The drain phase that follows does not get
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
# create carries run metadata so the teardown can reconcile unanswered creates
# alongside the ids it already knows.
# Caps are low because each admitted create boots a real Firecracker VM: at
# the default burst the drain would need sixty.
@smoke.scenario(
    "local.rate_limiters_enforce_and_fail_open",
    # Three API rollouts (enable, live cap, restore), a Redis scale-down/up,
    # eight to eighteen real sandbox creates, the suspended wake fleet among
    # them, plus the restores the wakes admit (`RateLimiterAssertions`
    # itemizes the spread).
    sandbox=smoke.SandboxConfig(timeout=1800),
    test_hooks="SPACE_CLUSTER_ACTIVATION_ADMISSION_BURST, which it shrinks to 3; "
    "the release burst of 60 would need sixty real boots",
)
def rate_limiters_enforce_and_fail_open(test: smoke.KindEcw) -> None:
    """With both limiters' caps shrunk and per-client concurrency in enforce
    mode, sequential creates drain the cluster activation bucket to a 429
    `Server rate limit:` with Retry-After; twenty concurrent creates and twenty
    concurrent connects for one client id each draw at least one 429
    `Per-client rate limit:` with Retry-After; in each barrier-released round
    where a twenty-deep storm for another client draws per-client 429s — one
    waking suspended sandboxes and one reconnecting to a running one — a
    separately-bucketed control client's reconnects all succeed; a create made
    under a client id records that client on the sandbox row and on each of
    its pods; with a live cap set, one client's wakes of suspended sandboxes
    plateau at the cap under a 429 `Client live sandbox cap reached:`, and
    then that client's reconnect and another client's reconnects to a running
    sandbox, which aren't activations, all succeed; once Redis is
    unreachable every create admits a sandbox; once it is back the per-client
    limiter rejects again; and with the standing configuration restored, when
    it tightens nothing, a burst the shrunk caps would refuse draws no limiter
    rejection."""

    limiters = test.rate_limiters

    with limiters.scoped_limiters():
        with test.phase("stage a suspended fleet for the wake flood"):
            # Under the Lease, so no cooperating run has the activation bucket
            # shrunk while these cold creates spend from it. Before the caps
            # shrink, deliberately: under the shrunk burst they would spend the
            # tokens the flood below needs.
            limiters.acquire_mutation_lease()
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
            # capability works. No extra boot unless the preflight drew the
            # activation 429, which it accepts; then one more create, retried
            # past that 429, gives the phase its sandbox.
            limiters.require_attribution_schema()
            if not connect_target:
                connect_target = limiters.create_until_admitted().sandbox_id
            limiters.expect_client_attributed(connect_target or "")

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
            # The preflight, or the attribution phase's retried create, can leave
            # the bucket empty, and the flood's wakes each need a token.
            limiters.await_activation_token()
            noisy, control = limiters.wake_flood(
                suspended,
                test.sandbox.sandbox_id,
                noisy=CALLERS,
                control=ISOLATION_CONTROL_CALLERS,
            )
            limiters.expect_control_isolated(noisy, control, label="wake_flood")
            limiters.expect_flood_restored(suspended, noisy, label="wake_flood")

        with test.phase("per-client isolation: a control client keeps reconnecting"):
            # The fixture's own sandbox, waited to running, is the target: the
            # round asserts that every control reconnect *succeeds*, so a target
            # that was merely created would make a boot still in progress
            # indistinguishable from the control client being starved. This one
            # predates the limiter overrides entirely, so waiting on it costs no
            # activation token. Running also means no reconnect takes the wake at
            # `lifecycle.rs`'s resume path — though a connect can still escalate
            # to one through `connect_touch_recovery`. Running before the drain is
            # what keeps that escalation affordable, but by here there is no
            # margin left and that is worth stating plainly: `enable_limiters`
            # shrinks the activation bucket to a burst of CLUSTER_BURST refilling
            # at 0.1/s, the preflight create spent one, the attribution phase's
            # retried create spent one more when the preflight drew the 429,
            # `await_activation_token` waited for at least one back before the
            # flood, and the wake flood spent one per fleet member it restored --
            # which is what WAKE_FLEET
            # is sized against. So this round runs on a bucket at or near empty,
            # plus whatever 0.1/s has put back, behind 23 concurrent reconnects.
            # An escalation would take a cluster `Server rate limit:` 429 and fail
            # a verdict on a healthy limiter. The round records the
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

        with test.phase("per-client concurrency on connect"):
            # The per-client slot is reserved at the top of the connect handler,
            # before the sandbox is fetched, so the cap trips whatever the target's
            # state. The fixture's own sandbox is the target: by now a
            # probe-created one can have outlived its 120s idle timeout, and a
            # connect to it would 404 or turn into a wake.
            outcomes = limiters.burst_connects(test.sandbox.sandbox_id, CALLERS)
            limiters.expect_per_client_limited(outcomes, label="per_client_connect")

        with test.phase("live cap: one client's live sandboxes plateau at its cap"):
            # Before the Redis fault, because the cap's pending set is in Redis:
            # with the store gone the cap fails open by design, and the phase
            # would be asserting against a guardrail that had switched itself off.
            suspended = limiters.resuspend_fleet()
            limiters.enable_live_cap(LIVE_CAP)
            # The drain and the create burst can leave the activation bucket
            # empty, and the first wake needs one token from it.
            limiters.await_activation_token()
            woken = limiters.connect_sequentially(suspended)
            # The capped client itself, now at its cap, reconnecting to a running
            # sandbox: no activation, so the cap must admit it.
            limiters.expect_running(test.sandbox, label="live_cap")
            capped_reconnects = limiters.connect_sequentially([test.sandbox.sandbox_id])
            # The control client reconnects to the fixture's own running
            # sandbox. That is not an activation, so the cap must never be
            # what answers it: a reconnect to an already-running sandbox is never
            # refused.
            control = limiters.connect_sequentially(
                [test.sandbox.sandbox_id] * LIVE_CAP_CONTROL_RECONNECTS,
                client_id=limiters.control_client_id,
            )
            limiters.expect_live_cap_plateau(
                woken, control, targets=suspended, capped_reconnects=capped_reconnects
            )

        with test.phase("fail open: Redis unreachable, every create admits a sandbox"):
            limiters.expect_create_capped()
            limiters.take_redis_down()
            outcomes = limiters.burst_creates_at_live_cap(FAIL_OPEN_CALLERS)
            limiters.expect_fail_open(outcomes)
            limiters.bring_redis_back()

        with test.phase("recovery: Redis back, the per-client limiter rejects again"):
            limiters.expect_enforcing_again(test.sandbox.sandbox_id)

        # A failed phase above still restores Redis, the standing env, and the
        # Lease as `scoped_limiters` unwinds inside the scenario's execution
        # budget; this phase accounts for the rollout on the passing path while
        # keeping the Lease across the restored-env check below.
        with test.phase("restore the public API limiter env"):
            limiters.restore_limiters(release_lease=False)

        with test.phase("restored env: a burst the shrunk caps refuse is admitted"):
            # Proves the restore took the scenario's caps away: the shrunk
            # activation burst and live cap would refuse part of this burst.
            # It can't tell per-client observe from enforce, since the burst
            # is under every release-default per-client cap. Runs last because
            # it needs the standing env back. That env is the release defaults
            # on a stock bench but whatever an operator set on any other, and a
            # stricter one would refuse part of this burst correctly, so the
            # burst runs only when the standing env tightens nothing.
            if limiters.judges_release_defaults():
                outcomes = limiters.burst_creates(FAIL_OPEN_CALLERS)
                limiters.expect_restored_env_admits_the_burst(outcomes)

        with test.phase("release the public API mutation lease"):
            limiters.release_mutation_lease()

"""A template runtime-upgrade campaign moves one template, then the rest, and stays paused when paused."""

from dataclasses import asdict

import space_smoke as smoke
from space_smoke.execution_environment import E0, E1
from space_smoke.runtime import InconclusiveSmoke
from space_smoke.runtime_upgrade_campaign import (
    CANARY_HOLD_SECONDS,
    NOT_DEFAULT_HOLD_SECONDS,
    PAUSED_HOLD_SECONDS,
)


# Requires the runtime upgrade campaign admin API and the template driver; the
# scenario ends inconclusive, naming whichever is missing. It turns on what a
# Kind chart leaves off for the run and puts it back
# (`space_smoke/runtime_upgrade_campaign.py`).
@smoke.scenario(
    "local.template_campaign_canary_rolls_and_pauses",
    tier="manual",
    manual_reason=(
        "Turns on the environment placement gate, the campaign loop and plaintext object-store "
        "endpoints for the template driver, rolls every node-manager to admit Firecracker 1.17.0, "
        "and moves the cluster default; a completed campaign leaves each template's E1 twin on the "
        "bench, so a later run is inconclusive until they're gone, and a bench with no default keeps "
        "E0 as its default."
    ),
    # Worst case for the smallest bench with a verdict, three E0 templates:
    # admission up to 38 minutes (two DaemonSet rollouts of up to about 662 s
    # each, 600 s of installs, 300 s of evidence), the settings' two rollouts
    # and back (about 22 minutes), up to 10 minutes for every worker to report
    # reading twins, up to 20 minutes for each of the canary, the wave and the
    # two outcomes the completion waits for, the holds, up to 10 minutes for
    # the twin's pool to fill, three creates (about 15 minutes), and the
    # withdrawal's two rollouts: about 200 minutes, the budget before cleanup.
    # Each further template can add 20 minutes to the completion wait.
    timeout_minutes=200,
    # Cleanup may have to own the default's and the settings' restores and
    # the allowlist's two rollouts.
    cleanup_timeout_seconds=1800,
)
def template_campaign_canary_rolls_and_pauses(test: smoke.KindAdmin) -> None:
    """Started through the admin API, a templates campaign from E0 to E1 moves
    nothing until every worker reads twins and E1 is the default; then it
    moves one template and waits in canary; resumed, it rolls; paused
    mid-wave, it moves nothing more over several loop ticks; resumed again, it
    completes with no failed item. A create then boots its template's twin
    under E1, an ECW create claims a slot of the twin's copy of the warm pool
    the template's original had and runs E1 too, and once the
    default is back on E0, a create boots the original under E0."""

    campaigns = test.template_campaigns
    console = test.console

    with test.phase("the bench serves template campaigns"):
        campaigns.require_served()
        source = campaigns.environment_reference(E0)

    environment = test.admit_execution_environment(E1.name)
    with test.phase(f"admit {E1.name} on every node"):
        environment.admit()

    gate = test.environment_placement_gate
    with test.phase("turn on the gate, the campaign loop and plaintext template storage"):
        gate.turn_on()
        campaigns.turn_on()

    with test.phase(f"{E0.name} is the cluster default"):
        campaigns.keep_default(source)

    template = test.sandbox_template_name()
    with test.phase(f"give {template}'s release a manual warm pool"):
        campaigns.add_pool(template)

    with test.phase("start a templates campaign with a one-template canary"):
        campaigns.require_no_target_releases(environment.reference)
        started = campaigns.start(
            from_environment_ref=source,
            to_environment_ref=environment.reference,
            canary_size=1,
            wave_size=1,
            failure_budget=1,
            reason="space_smoke template campaign",
        )
        test.verify(started.phase == "canary", "a campaign with a canary starts in canary", phase=started.phase)

    with test.phase("every worker reports that it reads twin metadata"):
        campaigns.await_readers()

    with test.phase("nothing moves while the target isn't the default"):
        campaigns.hold(
            lambda c: c.phase == "canary" and c.moved == 0 and c.in_flight == 0,
            "a campaign whose target isn't the default must move nothing",
            seconds=NOT_DEFAULT_HOLD_SECONDS,
        )

    with test.phase(f"make {E1.name} the default"):
        campaigns.set_default(environment.reference, reason="space_smoke: the campaign's target is the default")

    with test.phase("the canary moves one template and waits for a resume"):
        canary = campaigns.await_phase(
            lambda c: c.moved == 1 and c.in_flight == 0, "the canary template has an outcome"
        )
        test.verify(canary.done == 1, "the canary template must be upgraded", campaign=asdict(canary))
        held = campaigns.hold(
            lambda c: c.phase == "canary" and c.moved == 1 and c.in_flight == 0,
            "a canary must wait for a resume after its one template",
            seconds=CANARY_HOLD_SECONDS,
        )
        test.verify(held.failed == 0, "the canary template must not fail", campaign=asdict(held))

    with test.phase("resume into rolling"):
        rolling = campaigns.resume(reason="space_smoke: canary looked good")
        test.verify(rolling.phase == "rolling", "a resumed canary rolls", phase=rolling.phase)

    with test.phase("pause mid-wave"):
        wave = campaigns.await_phase(
            lambda c: c.moved + c.in_flight > 1 or c.pending == 0,
            "the rolling wave leases a template",
        )
        if wave.pending == 0:
            raise InconclusiveSmoke(
                "nothing was left pending once the rolling wave leased; the bench needs more E0 "
                "templates than one wave to show a pause mid-wave"
            )
        paused = campaigns.pause(reason="space_smoke: pause mid-wave")
        test.verify(paused.phase == "paused", "the campaign must pause", phase=paused.phase)
        if paused.pending == 0:
            raise InconclusiveSmoke(
                "the last pending template was leased before the pause landed; the bench needs "
                "more E0 templates to show a pause mid-wave"
            )

    with test.phase("a paused campaign stays paused and moves nothing new"):
        # An item already in flight may still finish, which keeps moved plus
        # in flight where it was; a new lease would raise it.
        ceiling = paused.moved + paused.in_flight
        campaigns.hold(
            lambda c: c.phase == "paused" and c.moved + c.in_flight <= ceiling and c.pending > 0,
            "a paused campaign must stay paused and lease nothing new",
            seconds=PAUSED_HOLD_SECONDS,
        )

    with test.phase("resume to completion"):
        campaigns.resume(reason="space_smoke: finish the campaign")
        completed = campaigns.await_phase(lambda c: c.phase == "completed", "the campaign completes")
        test.verify(
            completed.failed == 0 and completed.pending == 0 and completed.in_flight == 0,
            "a completed campaign has every template done",
            campaign=asdict(completed),
        )
        twins = campaigns.releases_bound_to(environment.reference)
        test.verify(
            twins == completed.done,
            "each template the campaign did must have a new twin bound to the target",
            twins=twins,
            campaign=asdict(completed),
        )

    tenant = console.create_tenant()
    console.create_account(tenant.org_id, "member", slug=tenant.slug)

    with test.phase(f"a create boots its template's twin under {E1.name}"):
        on_twin = console.create_foreign_sandbox(tenant)
        environment.expect_running(on_twin, step="from the twin")
        console.delete_foreign_sandbox(on_twin)

    with test.phase(f"a create from the twin's copied pool runs {E1.name}"):
        twin_pool = campaigns.await_twin_pool(template)
        from_pool = test.create_ecw_sandbox()
        campaigns.expect_claimed(from_pool, twin_pool)
        environment.expect_running(from_pool, step="from the twin's pool")

    with test.phase(f"move the default back to {E0.name}"):
        campaigns.restore_default()

    with test.phase(f"a create boots the original release under {E0.name} again"):
        on_original = console.create_foreign_sandbox(tenant)
        environment.expect_running(on_original, step="from the original", environment=E0)
        console.delete_foreign_sandbox(on_original)

    with test.phase("disable the run's warm pools"):
        campaigns.remove_pools()

    with test.phase("put the bench's settings back"):
        campaigns.restore_settings()
        gate.restore()

    with test.phase(f"withdraw {E1.name} from the bench"):
        environment.withdraw()

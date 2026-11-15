"""A v2 drain preview reports what a drain would find on a worker, and changes nothing there."""

import time
from typing import Any, cast

import space_smoke as smoke
from space_smoke.capabilities import DurableLifecycleSandbox, StoppableLifecycleSandbox


# The preview's plan, and that it seals, starts and writes nothing, are crate
# tests (`service/drain_node/tests.rs`); the admin endpoint's answers are
# `routes/admin/nodes/drain_preview/tests.rs`. This is the round trip on a
# real worker with the cluster controller's v2 loop off; Kind's API runs with
# drain v2 enabled, and a preview needs neither. The blocker is an empty cache directory
# named for a sandbox nothing knows: sandbox-manager's inventory makes a
# quarantined row of it, which the run waits for, so the preview's verdict
# doesn't race the inventory. The run deletes that row and the directory.
@smoke.scenario(
    "native.v2_drain_preview_changes_nothing",
    api_surfaces=("native",),
    substrates=("kubernetes",),
    timeout_minutes=30,
)
def v2_drain_preview_changes_nothing(test: smoke.CommandSandboxFixture) -> None:
    """With the cluster controller's v2 loop off, a worker holds a started
    and a stopped sandbox and a sandbox it quarantined from a cache directory
    no sandbox owned. The admin preview reports the quarantined one blocked,
    the started sandbox's suspend and the stopped one's disk publication as
    what a drain would do, the worker unsealed and a drain unable to finish.
    Right after it, the worker's sandbox-manager still holds both rows in
    their lifecycles. Afterwards nothing has moved: the started sandbox
    still runs, neither gained a snapshot, the node row has no drain phase,
    and a create and a start still land on the worker. Once the quarantined
    sandbox is removed, the preview names it in no blocker, and both
    sandboxes are still in their lifecycles, on the worker too, with no new
    snapshot."""

    drain = test.drain_v2

    with test.phase("pin a started and a stopped sandbox to one worker"):
        drain.pin_to_resident_worker()
        running = cast(DurableLifecycleSandbox, test.sandbox)
        stopped = cast(StoppableLifecycleSandbox, drain.create_resident())
        stopped.stop()
        drain.expect_lifecycles({running.sandbox_id: "started", stopped.sandbox_id: "stopped"})
        full_before = drain.ready_snapshots(running.sandbox_id)
        disk_before = drain.disk_snapshot_ids(stopped.sandbox_id)

    with test.phase("the worker quarantines a cache directory no sandbox owns"):
        orphan, removal = drain.plant_quarantined_sandbox()
        blocker = {"sandbox_id": orphan, "reason": "quarantined"}

    with test.phase("preview: the blocker, and what a drain would do"):
        report: dict[str, Any] = {}
        deadline = time.monotonic() + 120
        while blocker not in (report := drain.preview()).get("blocked", []):
            test.verify(
                time.monotonic() < deadline,
                "the preview never reported the quarantined sandbox blocked",
                report=report,
                blocker=blocker,
            )
            test.wait(5)
        test.record("preview", report)
        drain.expect_node_lifecycles({running.sandbox_id: "started", stopped.sandbox_id: "stopped"})
        pending = {entry.get("sandbox_id"): entry.get("action") for entry in report.get("pending", [])}
        # A suspend waits its turn in the window or for cache disk; a
        # publication only for the window.
        test.verify(
            pending.get(drain.bare_id(running.sandbox_id)) in {"suspend", "queued", "disk"},
            "the preview didn't name the started sandbox's suspend",
            pending=pending,
        )
        test.verify(
            pending.get(drain.bare_id(stopped.sandbox_id)) in {"publish_disk", "queued"},
            "the preview didn't name the stopped sandbox's disk publication",
            pending=pending,
        )
        test.verify(
            report.get("sealed_epoch") is None and report.get("would_finish") is False,
            "the preview reported the worker sealed, or a drain able to finish",
            sealed_epoch=report.get("sealed_epoch"),
            would_finish=report.get("would_finish"),
        )

    with test.phase("nothing moved: no suspend, no backup, no drain phase"):
        drain.expect_lifecycles({running.sandbox_id: "started", stopped.sandbox_id: "stopped"})
        observed = drain.run_on(running, "true")
        test.verify(observed.exit_code == 0, "the started sandbox stopped running after the preview")
        test.verify(
            drain.ready_snapshots(running.sandbox_id) == full_before
            and drain.disk_snapshot_ids(stopped.sandbox_id) == disk_before,
            "a preview left a snapshot behind",
            full_before=full_before,
            disk_before=disk_before,
        )
        test.verify(drain.drain_phase() is None, "a preview put the worker in a drain phase")

    with test.phase("the worker is not sealed: a create and a start land on it"):
        created = drain.create_resident()
        observed = drain.run_on(created, "true")
        test.verify(observed.exit_code == 0, "a sandbox created after the preview didn't run")
        stopped.start()
        drain.expect_lifecycles({stopped.sandbox_id: "started"})

    with test.phase("remove the quarantined sandbox: the preview no longer reports it"):
        removal.release()
        report = drain.preview()
        test.verify(
            all(entry.get("sandbox_id") != orphan for entry in report.get("blocked", [])),
            "the preview still names the removed sandbox in a blocker",
            report=report,
        )
        lifecycles = {running.sandbox_id: "started", stopped.sandbox_id: "started"}
        drain.expect_lifecycles(lifecycles)
        drain.expect_node_lifecycles(lifecycles)
        test.verify(
            drain.ready_snapshots(running.sandbox_id) == full_before
            and drain.disk_snapshot_ids(stopped.sandbox_id) == disk_before,
            "a preview left a snapshot behind",
            full_before=full_before,
            disk_before=disk_before,
        )

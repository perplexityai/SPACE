"""A v2 drain preview reports what a drain would find on a worker, and changes nothing there."""

import time
from typing import Any, cast

import space_smoke as smoke
from space_smoke.capabilities import DurableLifecycleSandbox, StoppableLifecycleSandbox
from space_smoke.drain_v2 import PENDING_RUNNING_FIELD, WAIT_HOLD


# The preview's plan, and that it seals, starts and writes nothing, are crate
# tests (`service/drain_node/tests.rs`); the admin endpoint's answers are
# `routes/admin/nodes/drain_preview/tests.rs`. This is the round trip on a
# real worker with no v2 drain of it recorded; a preview needs no drain. The
# preview models an evacuating drain, as past its deadline, so the running
# sandbox waits on its move rather than a hold. What can't move yet is an
# empty cache directory named for a sandbox nothing knows: sandbox-manager's
# inventory makes a quarantined row of it, which the run waits for, so the
# preview's verdict doesn't race the inventory, and the preview names it
# pending, since a sandbox is never blocked. A pending entry's reason is
# display text, so the run reads only which sandboxes are pending and what
# each waits on. The run deletes that row and the directory.
@smoke.scenario(
    "native.v2_drain_preview_changes_nothing",
    api_surfaces=("native",),
    substrates=("kubernetes",),
    timeout_minutes=30,
)
def v2_drain_preview_changes_nothing(test: smoke.CommandSandboxFixture) -> None:
    """With no v2 drain of it recorded, a worker holds a started and a
    stopped sandbox and a sandbox it quarantined from a cache directory no
    sandbox owned. The admin preview, of an evacuating drain, reports the
    quarantined one pending and no sandbox blocked, the started sandbox
    pending running and waiting on its move, never a hold, and the stopped
    one pending, not running: a drain could not finish yet. Right after it,
    the worker's sandbox-manager still holds both rows in their lifecycles.
    Afterwards nothing has moved: the started sandbox still runs, neither
    gained a snapshot, the node row has no drain phase, and a create and a
    start still land on the worker. Once the quarantined sandbox is removed,
    the preview no longer names it, and both sandboxes are still in their
    lifecycles, on the worker too, with no new snapshot."""

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

        def quarantined_pending(report: dict[str, Any]) -> bool:
            return orphan in drain.pending_entries(report)

    with test.phase("preview: the quarantined sandbox pending, and what an evacuating drain would find"):
        report: dict[str, Any] = {}
        deadline = time.monotonic() + 60
        while not quarantined_pending(report := drain.preview()):
            test.verify(
                time.monotonic() < deadline,
                "the preview never reported the quarantined sandbox pending",
                report=report,
                quarantined=orphan,
            )
            test.wait(5)
        test.record("preview", report)
        drain.expect_nothing_blocked_per_sandbox(report)
        drain.expect_node_lifecycles({running.sandbox_id: "started", stopped.sandbox_id: "stopped"})
        entries = drain.pending_entries(report)
        started_entry = entries.get(drain.bare_id(running.sandbox_id))
        stopped_entry = entries.get(drain.bare_id(stopped.sandbox_id))
        # An evacuating drain holds nothing: the started sandbox waits on its
        # suspend, or on an artifact producer in flight on it.
        test.verify(
            started_entry is not None
            and started_entry.get("wait") != WAIT_HOLD
            and started_entry.get(PENDING_RUNNING_FIELD) is True,
            "the preview didn't report the started sandbox pending, running and not held",
            pending=entries,
        )
        test.verify(
            stopped_entry is not None
            and stopped_entry.get("wait") != WAIT_HOLD
            and stopped_entry.get(PENDING_RUNNING_FIELD) is False,
            "the preview didn't report the stopped sandbox pending and not running",
            pending=entries,
        )
        test.verify(
            not drain.finished(report),
            "the preview reported a drain able to finish with residents still to move",
            report=report,
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
            orphan not in drain.pending_entries(report)
            and all(entry.get("sandbox_id") != orphan for entry in report.get("blocked", [])),
            "the preview still names the removed sandbox",
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

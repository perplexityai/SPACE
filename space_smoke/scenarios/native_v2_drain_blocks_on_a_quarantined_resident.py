"""A resident the node can't make durable blocks a v2 drain until it is undrained."""

import space_smoke as smoke

HOLD_SECONDS = 30


# Kind has no way to quarantine a sandbox, so the worker's drain is told to
# read one resident as quarantined through a test-hooks file
# (`SPACE_DRAIN_NODE_TEST_QUARANTINED_FILE`); everything after that read is
# the production path. The planner's real-quarantine row is
# `service/drain_node/tests.rs`.
@smoke.scenario(
    "native.v2_drain_blocks_on_a_quarantined_resident",
    api_surfaces=("native",),
    substrates=("kubernetes",),
    timeout_minutes=30,
)
def v2_drain_blocks_on_a_quarantined_resident(test: smoke.CommandSandboxFixture) -> None:
    """With a quarantined resident on the worker, the v2 drain reports it
    blocked, never `done`, and the node stays `draining`; `:terminate` is
    refused. `:undrain` cancels the epoch and the resident is left running."""

    drain = test.drain_v2

    with test.phase("pin a resident to one worker and quarantine it for the drain"):
        node = drain.pin_to_resident_worker()
        resident = drain.create_resident()
        control = test._drain_v2_control  # noqa: SLF001
        assert control is not None
        test._context.scope.defer(  # noqa: SLF001
            "clear the drain's test quarantine",
            lambda: control.mark_drain_test_quarantined(node, []),
            critical=True,
        )
        control.mark_drain_test_quarantined(node, [drain.bare_id(resident.sandbox_id)])
        drain.release_other_worker()

    with test.phase("drain with engine v2; the resident is reported blocked"):
        drain.drain()
        report = drain.wait_for_report(
            lambda report: any(
                entry.get("sandbox_id") == drain.bare_id(resident.sandbox_id)
                and "quarantined" in str(entry.get("reason"))
                for entry in report.get("blocked", [])
            ),
            "the node never reported the quarantined resident blocked",
            timeout=180,
        )
        test.record("blocked_report", report)

    with test.phase(f"hold {HOLD_SECONDS} s: never done, still draining, terminate refused"):
        test.wait(HOLD_SECONDS, reason="for v2 drain ticks with a blocked resident")
        report = drain.report()
        node_row = next(n for n in control.admin_nodes() if n.id == drain.node_id)
        test.verify(
            report.get("done") is False and node_row.drain_phase == "draining",
            "a blocked v2 drain reported done or left draining",
            report=report,
            drain_phase=node_row.drain_phase,
        )
        test.verify(
            control.terminate_node(drain.node_id) == 409,
            ":terminate of a blocked v2 drain was not refused",
        )

    with test.phase("undrain: the epoch is cancelled and the resident still runs"):
        control.mark_drain_test_quarantined(node, [])
        drain.undrain_and_expect_cancelled()
        observed = resident.run_command_when_ready("true")
        test.verify(observed.exit_code == 0, "the blocked resident stopped running")

"""A controller refused access to the Lease remains unready without restart loops."""

import space_smoke as smoke


@smoke.scenario(
    "local.leader_election_refusal_does_not_restart",
    required_tools=("kubectl", "docker"),
    telemetry_mode="otel",
    test_hooks="SPACE_CLUSTER_CONTROLLER_LEADER_ELECTION_LEASE_TTL_SECS and "
    "SPACE_CLUSTER_CONTROLLER_LEADER_ELECTION_LEASE_FAILURE_GRACE_SECS at their floors",
)
def leader_election_refusal_does_not_restart(test: smoke.KindAdmin) -> None:
    """A third controller whose Lease calls are refused remains NotReady and
    unrestarted past its full liveness budget and restart visibility margin.
    Two healthy replicas stay Ready without restarting, keep the same Lease
    holder, and only that holder runs reconcilers throughout the observation.
    An independently unreachable contender must spend the shortened budget
    and exit non-zero, proving this run observed the full effective window.
    """
    election = test.leader_election
    with test.phase("own the Kind mutation Lease and shrink the liveness budget"):
        election.take_ownership()
        election.shrink_liveness_budget()
    with test.phase("two Ready replicas and a renewing Lease holder"):
        election.expect_initial_holder()
    with test.phase("place and release the refused contender"):
        election.place_refused_contender()
        election.start_budget_control()
        election.start_refused_contender()
    with test.phase("only the healthy holder runs reconcilers"):
        leader = election.expect_initial_runner()
    with test.phase("the refused contender stays NotReady and unrestarted for its full budget"):
        election.expect_refusal_is_not_a_restart(leader)
    with test.phase("the independent unreachable contender spends the shortened budget"):
        election.expect_budget_control_exited()
    with test.phase("remove the contender and restore the standing budget"):
        election.remove_refused_contender()
        election.restore_liveness_budget()

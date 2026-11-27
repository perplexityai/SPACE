"""Cluster-controller ownership and liveness survive an apiserver outage and a refusal."""

import space_smoke as smoke


@smoke.scenario(
    "local.leader_election_brownout_keeps_one_leader",
    required_tools=("kubectl", "docker"),
    telemetry_mode="otel",
    test_hooks="SPACE_CLUSTER_CONTROLLER_LEADER_ELECTION_LEASE_TTL_SECS and "
    "SPACE_CLUSTER_CONTROLLER_LEADER_ELECTION_LEASE_FAILURE_GRACE_SECS, which it sets to their "
    "floors; at the release TTL and budget each budget wait is minutes longer",
)
def leader_election_brownout_keeps_one_leader(test: smoke.KindAdmin) -> None:
    """Two Ready replicas share one Lease holder and one reconciler runner.
    Cut off from the apiserver, readiness clears and the holder stops before
    its lease can be taken over; once reconnected, two Ready replicas share
    one fresh holder and runner. Throughout, a contender whose Lease calls are
    refused stays NotReady and unrestarted past its liveness budget, and one
    that cannot reach the apiserver spends that budget and exits non-zero.
    """

    election = test.leader_election

    with test.phase("preflight: two replicas, election on, otel; read the tick and export settings"):
        election.take_ownership()

    with test.phase("place the refused contender and start the unreachable budget control"):
        election.start_contenders()

    with test.phase("request the lease TTL's and liveness budget's floors; await the rollout"):
        election.shrink_liveness_budget()

    with test.phase("two Ready replicas and a Lease holder renewing among them"):
        election.expect_initial_holder()

    with test.phase("release the refused contender once a healthy replica renews"):
        election.start_refused_contender()

    with test.phase("one reconciler runner: only the holder's ticks move"):
        election.expect_initial_runner()

    with test.phase("record the readiness markers and cut both replicas off from the apiserver"):
        election.cut_off_the_healthy_pair()

    with test.phase("outage: a running container clears its readiness marker"):
        election.expect_marker_cleared()

    with test.phase("outage: the holder's container ends"):
        election.expect_holder_stops()

    with test.phase("end the outage once a running holder's ticks would show"):
        election.end_the_outage()

    with test.phase("on record: the holder's restart, lease loss and stopped ticks; the standby's unreachable calls"):
        election.expect_outage_recorded()

    with test.phase("recovered: one fresh Lease holder, one reconciler runner, two Ready replicas"):
        election.expect_recovered_leader()

    with test.phase("restore the standing liveness budget; it rolls while the contenders finish"):
        election.restore_liveness_budget()

    with test.phase("the refused contender stays NotReady and unrestarted past its budget"):
        election.expect_refusal_is_not_a_restart()

    with test.phase("the unreachable budget control spends its budget and exits non-zero"):
        election.expect_budget_control_exited()

    with test.phase("remove the contenders; await the restored rollout"):
        election.remove_contenders()
        election.await_restored_budget()

"""Cluster-controller ownership and liveness recover from an apiserver outage."""

import space_smoke as smoke


# The outage and refusal contracts run on independent Kind shards. Each owns
# and restores its fault and liveness budget. Refusal is exercised by
# local.leader_election_refusal_does_not_restart; neither waits through the
# other's independent liveness window.
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
    With the apiserver unavailable, readiness clears, the holder stops before
    its lease can be taken over, and a replica exits non-zero within its
    liveness budget. After recovery, recorded exits and telemetry establish
    why each stopped, and two Ready replicas share one fresh holder and runner.
    """

    election = test.leader_election

    with test.phase("preflight: two replicas, election on, otel; hold the mutation Lease"):
        election.take_ownership()

    with test.phase("request the lease TTL's and liveness budget's floors; await the rollout"):
        election.shrink_liveness_budget()

    with test.phase("two Ready replicas and a Lease holder renewing among them"):
        election.expect_initial_holder()

    with test.phase("one reconciler runner: only the holder's ticks move"):
        election.expect_initial_runner()

    with test.phase("record the readiness markers and take the apiserver away"):
        election.take_the_apiserver_away()

    with test.phase("outage: a running container clears its readiness marker"):
        election.expect_marker_cleared()

    with test.phase("outage: the holder's container ends"):
        election.expect_holder_stops()

    with test.phase("outage: a replica spends its liveness budget and exits"):
        election.expect_budget_exit()

    with test.phase("bring the apiserver back"):
        election.bring_the_apiserver_back()

    with test.phase("the restarts and their reasons are on record"):
        election.expect_outage_recorded()

    with test.phase("recovered: one fresh Lease holder, one reconciler runner, two Ready replicas"):
        election.expect_recovered_leader()

    with test.phase("restore the standing liveness budget"):
        election.restore_liveness_budget()

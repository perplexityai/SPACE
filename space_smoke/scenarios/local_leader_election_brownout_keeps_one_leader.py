"""Cluster-controller leader election keeps one leader through a refusal and an apiserver outage."""

import space_smoke as smoke


# Runs in the implicit inventory, so Kind CI exercises it on every push to
# main and on every PR whose paths select the Kind smoke in Pull CI. It waits
# out the controller's liveness budget twice, once with the control-plane
# apiserver deliberately down and once with a refused contender, at the floor
# of the shortest lease TTL, both requested through the controller's test
# hooks. The faults and the
# budget are captured and undone by this run; the Kind mutation Lease keeps
# cooperating smokes off the cluster meanwhile. See
# space_smoke/leader_election.py for what is read where.
@smoke.scenario(
    "local.leader_election_brownout_keeps_one_leader",
    required_tools=("kubectl", "docker"),
    # Outlasts the run, so the fixture's own sandbox never reaches its idle
    # timeout while the apiserver is down.
    sandbox=smoke.SandboxConfig(timeout=3600),
    telemetry_mode="otel",
    test_hooks="SPACE_CLUSTER_CONTROLLER_LEADER_ELECTION_LEASE_TTL_SECS and "
    "SPACE_CLUSTER_CONTROLLER_LEADER_ELECTION_LEASE_FAILURE_GRACE_SECS, which it sets to their "
    "floors; at the release TTL and budget each budget wait is minutes longer",
)
def leader_election_brownout_keeps_one_leader(test: smoke.KindEcw) -> None:
    """With the liveness budget at its floor for this run, two Ready
    cluster-controller replicas share one Lease holder, which alone moves the
    reconciler tick count in Prometheus. With the apiserver
    down, a running container loses its readiness marker; the holder's
    `lease_lost` is stamped before the Lease could lapse, its ticks stop
    growing by then, and its container ends within the bound its own deadlines
    give; and a replica later spends its budget within the requested ceiling:
    kubelet restarts it with a non-zero exit and Prometheus holds its
    `lease_unreachable{reason="unreachable"}`. Once the apiserver is back, a
    third contender whose lease calls are all refused shows
    `acquire_failed{reason="refused"}`; one fresh holder alone moves the
    reconciler tick count in Prometheus, both replicas are Ready, and the
    contender stays NotReady and is not restarted for a full liveness budget
    while the pair keeps its holder. The standing budget is put back last.
    Expected wall time about 7 minutes."""

    election = test.leader_election

    with test.phase("preflight: two replicas, election on, otel; hold the mutation Lease"):
        election.take_ownership()

    with test.phase("request the lease TTL's and liveness budget's floors; await the rollout"):
        election.shrink_liveness_budget()

    with test.phase("two Ready replicas and a Lease holder renewing among them"):
        election.expect_initial_holder()

    # Placed now, held before its first lease call: after the outage, new
    # pods take a minute or more to reach a kubelet. The runner window below
    # opens before this phase, so the placement costs it nothing.
    with test.phase("place a contender whose lease calls will all be refused"):
        election.place_refused_contender()

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

    # Released now so its liveness-budget window runs under the next two phases.
    with test.phase("start a contender whose lease calls are all refused"):
        election.start_refused_contender()

    with test.phase("the restarts and their reasons are on record"):
        election.expect_outage_recorded()

    with test.phase("recovered: one fresh Lease holder, one reconciler runner, two Ready replicas"):
        leader = election.expect_recovered_leader()

    with test.phase("the refused contender stays NotReady and unrestarted for a liveness budget"):
        election.expect_refusal_is_not_a_restart(leader)

    with test.phase("remove the refused contender"):
        election.remove_refused_contender()

    with test.phase("restore the standing liveness budget"):
        election.restore_liveness_budget()

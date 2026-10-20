"""Cluster-controller leader election keeps one leader through a refusal and an apiserver outage."""

import space_smoke as smoke


# Runs in the implicit inventory, so Kind CI exercises it on every push to
# main and on every PR whose paths select the Kind smoke in Pull CI. It waits
# out the controller's liveness budget twice, once with a refused contender and
# once with the control-plane apiserver deliberately down. Both faults are captured
# and undone by this run; the Kind mutation Lease keeps cooperating smokes off
# the cluster meanwhile. See space_smoke/leader_election.py for what is read
# where.
@smoke.scenario(
    "local.leader_election_brownout_keeps_one_leader",
    required_tools=("kubectl", "docker"),
    # Outlasts the run, so the fixture's own sandbox never reaches its idle
    # timeout while the apiserver is down.
    sandbox=smoke.SandboxConfig(timeout=3600),
    telemetry_mode="otel",
    # Two liveness-budget ceilings (750 s each) are inherent; the fixture's
    # 30 minutes leave no room for setup, recovery and teardown.
    timeout_minutes=45,
)
def leader_election_brownout_keeps_one_leader(test: smoke.KindEcw) -> None:
    """Two Ready cluster-controller replicas share one Lease holder, which
    alone moves the reconciler tick count in Prometheus. A third contender
    whose lease calls
    are all refused shows `acquire_failed{reason="refused"}`, stays NotReady
    and is not restarted for a full liveness budget, while the pair keeps its
    holder. With the apiserver down, a running container loses its readiness
    marker; the holder's `lease_lost` is stamped before the Lease could lapse,
    its ticks stop growing by then, and its container ends within the bound
    its own deadlines give; and a replica later spends its budget: kubelet
    restarts it with a non-zero exit and Prometheus holds its
    `lease_unreachable{reason="unreachable"}`. Once the apiserver is back, one
    fresh holder alone runs the reconcilers and both replicas are Ready.
    Expected wall time about 30 minutes, at most about 35."""

    election = test.leader_election

    with test.phase("preflight: two replicas, election on, otel; hold the mutation Lease"):
        election.take_ownership()

    # Started first so its liveness-budget window runs under the next phase.
    with test.phase("start a contender whose lease calls are all refused"):
        election.start_refused_contender()

    with test.phase("two replicas, one Lease holder, one reconciler runner"):
        leader = election.expect_one_leader(label="initial")

    with test.phase("the refused contender stays NotReady and unrestarted for a liveness budget"):
        election.expect_refusal_is_not_a_restart(leader)

    with test.phase("remove the refused contender"):
        election.remove_refused_contender()

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
        election.expect_one_leader(label="recovered", timeout=300)

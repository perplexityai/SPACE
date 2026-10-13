"""The sandbox namespace's NetworkPolicies hold outside the VM, beneath the gateway."""

import space_smoke as smoke


# Two sandboxes: one to probe from, one whose pod IP it must not reach. The
# guest's own internet check needs the tenant switch on; the namespace policy
# is what is probed, not the gateway's.
@smoke.scenario(
    "local.sandbox_network_isolation_holds",
    sandbox=smoke.SandboxConfig(timeout=600, allow_internet_access=True),
    sandboxes=2,
)
def sandbox_network_isolation_holds(test: smoke.KindEcw) -> None:
    """From a running sandbox pod's network namespace, entered on its node
    outside the VM, Postgres, the Kubernetes API Service, internal-api, the
    object store and another sandbox's pod IP on 9800 are all refused while
    the cluster DNS, and the internet when this host has it, still connect;
    every refused target answers from the control-plane namespace; a pod in
    that namespace cannot open the sandbox pod's 9800; a pod in the sandbox
    namespace that no allow rule selects reaches nothing; and the guest still
    reaches the internet when this host does."""

    isolation = test.sandbox_network_isolation

    with test.phase("locate both sandbox pods and the control-plane targets"):
        isolation.locate()

    with test.phase("control: every target answers from the control-plane namespace"):
        isolation.expect_targets_answer_outside_the_namespace()

    with test.phase("the sandbox pod's network namespace reaches only what the policy allows"):
        isolation.expect_sandbox_pod_confined()

    with test.phase("a pod in another namespace cannot open the sandbox pod's 9800"):
        isolation.expect_sandbox_port_closed_to_other_namespaces()

    with test.phase("the default-deny isolates a pod no allow rule selects"):
        isolation.expect_unselected_pod_isolated()

    with test.phase("the guest still reaches the internet"):
        isolation.expect_guest_internet()

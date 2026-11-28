"""Node-group placement stays inert when off, fails closed without a dedicated group, and isolates dedicated capacity across create and restore."""

import uuid

import space_smoke as smoke


# The failure mode this exists for: a required nodeAffinity term whose values
# don't match the labeler's labels, or a missing dedicated-org taint toleration,
# is a sandbox that sits Pending forever while unit gates stay green. Only real
# Kubernetes Node labels, NoSchedule taints, and kube-scheduler prove it.
@smoke.scenario(
    "local.node_group_boundary_schedules",
    # Three Deployment rollouts (labeler+mirror on, gate on, standing restore),
    # one org_specific ECW template build, and create+pause+restore cycles fit
    # inside the 22 minutes left of the 30-minute fixture budget.
    sandbox=smoke.SandboxConfig(timeout=1800),
    # Failure-path LIFO unwind sweeps admitted sandboxes (~30 s), clears worker
    # taints (~5 s), waits up to 90 s for the 60 s membership mirror tick before
    # deleting the dedicated group, rolls both space-api and
    # space-cluster-controller back + waits for readiness (~180 s), restores
    # worker labels (~5 s), and deletes the org_specific template (~90 s):
    # ~400 s worst case, above the default 120 s cleanup reserve.
    cleanup_timeout_seconds=480,
    rerun_environment=(
        "SPACE_SMOKE_KIND_ADMIN_TOKEN_FILE",
        "SPACE_SMOKE_BUILD_FOREIGN_ORG_ID",
    ),
)
def node_group_boundary_schedules(test: smoke.KindEcw) -> None:
    """By default with the placement gate off, the standing fixture pod carries
    zero node-group affinity terms and zero dedicated-org tolerations; a dedicated
    group is refused with a 409 when no generic group exists; with the labeler and
    membership mirror enabled while the placement gate stays off, generic and
    org-specific creates plus org-specific pause/restore still render zero
    node-group affinity terms and zero dedicated-org tolerations; with the gate on
    and no dedicated group, an org-specific create fails closed with a 409 while a
    generic create renders `In (<default>)` and schedules; and with a dedicated
    group, boundary label, and `space.pplx.net/dedicated-org` `NoSchedule` taint on
    one worker, the owner's org-specific create and restore land on the dedicated
    worker while a foreign org schedules generic work onto untainted shared
    capacity and gets a 404 on the owner's org-specific template."""

    # Register multi-tenant build cleanup before cluster compensations so LIFO
    # unwind sweeps admitted sandboxes before deleting the org_specific template.
    builds = test.template_builds
    groups = test.node_groups
    generic_template = test.sandbox_template_name()
    dedicated_name = f"smoke-dedicated-{uuid.uuid4().hex[:8]}"

    with test.phase("gate off: the fixture pod carries no node-group term or taint toleration"):
        groups.expect_inert_by_default()

    with test.phase("provisioning order: a dedicated group is refused without a generic one"):
        groups.expect_dedicated_refused_without_generic(
            org_id=test.admin_org_id(), name=dedicated_name
        )

    with test.phase("seed run-owned credentials and build an org_specific template"):
        builds.prepare()

    owner_probe = builds.owner_probe()
    foreign_probe = builds.foreign_probe()

    with test.phase("enable labeler and membership mirror while keeping the placement gate off"):
        groups.enable_labeler_with_gate_off()

    with test.phase("the labeler seeds the default group and labels the sandbox workers"):
        default = groups.wait_for_default_group_and_labels(min_workers=2)

    with test.phase("stage 0-2 non-regression: generic, org_specific, and pause/restore stay inert with gate off"):
        groups.expect_stage_zero_to_two_inert(
            owner_probe=owner_probe,
            generic_template=generic_template,
            org_specific_template=builds.base,
        )

    with test.phase("enable the placement gate without a dedicated node group"):
        groups.enable_placement_gate()

    with test.phase("proof 4: org_specific fails closed with 409 while generic renders default and schedules"):
        groups.expect_org_specific_fails_closed_without_dedicated_group(
            owner_probe=owner_probe,
            owner_org_id=builds.owner_org_id,
            org_specific_template=builds.base,
        )
        groups.expect_generic_create_renders_and_schedules(
            default,
            template=generic_template,
            probe=foreign_probe,
            kill_after=True,
        )

    with test.phase("proof 5: dedicated group + taint isolates owner create/restore and fences foreign org"):
        groups.expect_dedicated_isolation_and_restore(
            default,
            dedicated_group_name=dedicated_name,
            owner_org_id=builds.owner_org_id,
            owner_probe=owner_probe,
            foreign_probe=foreign_probe,
            generic_template=generic_template,
            org_specific_template=builds.base,
        )

    with test.phase("restore dedicated group, taints, env, worker labels, and build resources"):
        groups.restore()
        builds.cleanup()

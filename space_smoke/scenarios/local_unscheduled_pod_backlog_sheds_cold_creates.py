"""Unscheduled-pod backlog circuit breaker sheds cold creates before creating pods and recovers once scheduled."""

import space_smoke as smoke
from space_smoke.capabilities import KindColdCreateConfig


@smoke.scenario(
    "local.unscheduled_pod_backlog_sheds_cold_creates",
    sandbox=smoke.SandboxConfig(
        timeout=900,
        kind_cold_create=KindColdCreateConfig(
            vcpu=2,
            memory_mib=1024,
        ),
    ),
    # Failure teardown resumes `kube-scheduler`, settles in-flight creates
    # (<= 30s), retires run-owned sandboxes, restores the `space-api`
    # Deployment env (<= 240s rollout + <= 60s readiness), and releases the
    # mutation Lease last.
    cleanup_timeout_seconds=420,
)
def unscheduled_pod_backlog_sheds_cold_creates(test: smoke.KindEcw) -> None:
    """When `kube-scheduler` falls behind and hasn't evaluated new pods yet,
    `slot_check_enabled` still sees free worker slots while the first cold
    create leaves one `Pending` pod unscheduled;
    `SPACE_CLUSTER_ACTIVATION_ADMISSION_MAX_UNSCHEDULED_PODS=1` sheds follow-up
    cold creates with `503` and `Retry-After` before creating more pods, leaves
    commands on an already-running sandbox unaffected, and admits cold creates
    again once `kube-scheduler` resumes and binds the pod."""

    backlog = test.unscheduled_pod_backlog
    template = test.sandbox_template_selector()

    with test.phase("enable slot check and unscheduled-pod backlog cap of 1 on space-api"):
        backlog.enable_circuit_breaker(max_unscheduled_pods=1)

    with test.phase("pause kube-scheduler and wait for one cold-create pod to sit Pending"):
        backlog.pause_scheduler_and_admit_first_pending_pod(template=template)

    with test.phase("follow-up cold creates are shed with 503 + Retry-After before creating pods"):
        backlog.expect_follow_up_cold_creates_shed_without_pods(template=template)

    with test.phase("existing running sandbox continues executing commands"):
        backlog.expect_running_sandbox_unaffected()

    with test.phase("resume kube-scheduler: waiting create finishes and fresh cold creates recover"):
        backlog.resume_scheduler_and_expect_recovery(template=template)

    with test.phase("restore space-api admission env"):
        backlog.restore()

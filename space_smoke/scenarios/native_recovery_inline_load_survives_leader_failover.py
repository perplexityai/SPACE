"""Inline and queued native work in flight across a controller leader handover is not lost."""

from concurrent.futures import ThreadPoolExecutor

from space_smoke.native_inline import claims, lifecycle, require_inline, suspended_sandbox
from space_smoke.native_recovery import RECOVERY_ENVIRONMENT, KindNativeHardeningFixture, expect_response
from space_smoke.registry import scenario

CREATES = 2
WAKES = 2


@scenario(
    "native_recovery.inline_load_survives_leader_failover",
    api_surfaces=("native",),
    required_tools=("kubectl", "kind"),
    rerun_environment=RECOVERY_ENVIRONMENT,
)
def inline_load_survives_leader_failover(fixture: KindNativeHardeningFixture) -> None:
    """Creates and wakes the API runs inline, and a suspend the dispatcher
    runs, all accepted as every cluster-controller pod is replaced, each
    succeed exactly as requested.
    """

    control = fixture.recovery
    with control.owned_faults():
        require_inline(control, "create_sandbox", "wake_sandbox")
        with fixture.context.phase("prepare suspended and running sandboxes"):
            suspended = [suspended_sandbox(fixture) for _ in range(WAKES)]
            running = fixture.create_ready(timeout=120)
        with fixture.context.phase("admit work while the controller leader is replaced"):
            replaced = control.begin_restart("space-cluster-controller")

            def submit(index: int) -> tuple[str, str, str]:
                if index < CREATES:
                    body = expect_response(fixture, fixture.create(), 202)
                    return body["operation_id"], body["sandbox_id"], "running"
                if index < CREATES + WAKES:
                    sandbox_id = suspended[index - CREATES]
                    return lifecycle(fixture, sandbox_id, "wake"), sandbox_id, "running"
                return lifecycle(fixture, running, "suspend"), running, "suspended"

            with ThreadPoolExecutor(max_workers=CREATES + WAKES + 1) as pool:
                work = list(pool.map(submit, range(CREATES + WAKES + 1)))
            control._wait("space-cluster-controller", control._captures["space-cluster-controller"].replicas,
                          replaced=replaced)
        with fixture.context.phase("await every accepted operation"):
            for operation_id, sandbox_id, state in work:
                fixture.wait_operation(operation_id, timeout=300)
                fixture.wait_sandbox(sandbox_id, state, timeout=180)
            fixture.context.record(
                "failover_claims", {operation_id: claims(fixture, operation_id) for operation_id, _, _ in work}
            )

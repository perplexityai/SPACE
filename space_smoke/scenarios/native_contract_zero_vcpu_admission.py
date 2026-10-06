"""Reject zero-vCPU creates without leaving invalid operations running."""

from space_smoke.native_concurrency import required_string
from space_smoke.native_recovery import (
    RECOVERY_ENVIRONMENT,
    KindNativeHardeningFixture,
)
from space_smoke.registry import scenario


@scenario(
    "native_contract.zero_vcpu_admission",
    api_surfaces=("native",),
    required_tools=("kubectl", "kind"),
    rerun_environment=RECOVERY_ENVIRONMENT,
)
def zero_vcpu_admission(test: KindNativeHardeningFixture) -> None:
    """A valid create with zero vCPUs fails admission with a permanent client error."""

    control = test.recovery
    with control.owned_faults():
        with test.context.phase("verify the template and caller can create a sandbox"):
            # A positive control excludes authentication, template spelling, or
            # unrelated admission failures as evidence of CPU validation.
            test.create_ready()

        with control.operation_age_limit(seconds=5, readiness_seconds=5):
            with test.context.phase("submit zero-vCPU create and retain its admission result"):
                response = test.create({
                    "template": test.config.template,
                    "resources": {"vcpu": 0},
                })
                # Native control-plane errors are a flat PublicError envelope.
                error = response.body
                test.context.record("zero_vcpu_admission", {
                    "status": response.status,
                    "error_code": error.get("code"),
                    "retryable": error.get("retryable"),
                    "operation_id": response.body.get("operation_id"),
                    "sandbox_id": response.body.get("sandbox_id"),
                })

            with test.context.phase("drain accepted work and delete owned sandboxes"):
                # Capture the invalid acceptance before waiting. A later failed
                # operation is cleanup evidence, never admission success.
                if response.status == 202:
                    operation = test.wait_operation(
                        required_string(response.body, "operation_id"),
                        timeout=240,
                        require_success=False,
                    )
                    test.context.record("zero_vcpu_terminal_operation", {
                        key: operation.get(key)
                        for key in ("id", "state", "terminal_error_code")
                    })
                # The guarded phase has a full scenario budget. Routine deferred
                # teardown is too short to absorb invalid-create readiness waits.
                # It remains armed as a backstop and reports retained IDs if this
                # bounded drain cannot establish terminal ownership.
                test.cleanup()

    test.context.verify(
        response.status in (400, 422)
        and error.get("retryable") is False
        and not response.body.get("operation_id")
        and not response.body.get("sandbox_id"),
        "zero-vCPU create must be rejected before an operation or sandbox is accepted",
    )

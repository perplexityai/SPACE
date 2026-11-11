"""Native API create idempotency coverage."""

from collections.abc import Mapping
from dataclasses import asdict

from space_smoke.native_concurrency import (
    ENVIRONMENT,
    CallerTrace,
    ConcurrencySettings,
    new_key,
    request_with_retries,
    required_string,
    run_fanout,
)
from space_smoke.native_hardening import NativeHardeningFixture
from space_smoke.registry import scenario


@scenario(
    "native_concurrency.create_idempotency",
    api_surfaces=("native",),
    rerun_environment=ENVIRONMENT,
)
def create_idempotency(test: NativeHardeningFixture) -> None:
    """Concurrent identical create requests converge on one ready sandbox and operation."""
    settings = ConcurrencySettings.load(test)
    test.context.record("concurrency_settings", asdict(settings))
    key = new_key("create")
    # The fixture stamps stable owned metadata and tracks every accepted create,
    # including unexpected duplicates, before returning to a worker.
    def create(index: int, trace: CallerTrace, deadline: float) -> Mapping[str, object]:
        response = request_with_retries(test, settings, trace, deadline, "create", "POST", "/v1/sandboxes",
                            body={"template": test.config.template}, key=key)
        return {"sandbox_id": required_string(response, "sandbox_id"),
                "operation_id": required_string(response, "operation_id")}
    outcomes = run_fanout(test, settings, "create_fanout", create)
    ids = {(item.value["sandbox_id"], item.value["operation_id"]) for item in outcomes if item.value}
    test.context.verify(len(ids) == 1, "same-key creates produced different sandbox or operation identities")
    sandbox_id, operation_id = next(iter(ids))
    test.wait_operation(str(operation_id))
    test.wait_sandbox(str(sandbox_id))

    changed = test.create(
        {"metadata": {"purpose": "different"}}, idempotency_key=key,
    )
    test.context.verify(
        changed.status == 409 and changed.body.get("code") == "idempotency_conflict",
        "a changed create request reused the concurrent requests' idempotency key",
        status=changed.status,
    )

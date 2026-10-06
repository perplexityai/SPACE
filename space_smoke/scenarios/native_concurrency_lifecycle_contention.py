"""Native API lifecycle contention coverage."""

import time
from collections.abc import Mapping
from dataclasses import asdict
from urllib.parse import quote

from space_smoke.native_concurrency import (
    ENVIRONMENT,
    CallerTrace,
    ConcurrencySettings,
    new_key,
    require,
    request_with_retries,
    required_string,
    run_command,
    run_fanout,
)
from space_smoke.native_hardening import NativeHardeningFixture
from space_smoke.registry import scenario


@scenario(
    "native_concurrency.lifecycle_contention",
    api_surfaces=("native",),
    rerun_environment=ENVIRONMENT,
)
def lifecycle_contention(test: NativeHardeningFixture) -> None:
    """Duplicate lifecycle intents share operations and conflicting intents preserve liveness."""
    settings = ConcurrencySettings.load(test)
    test.context.record("concurrency_settings", asdict(settings))
    sandbox_id = test.create_ready()
    path = f"/v1/sandboxes/{quote(sandbox_id, safe='')}"
    for action, state in (("pause", "paused"), ("wake", "running")):
        key = new_key(action)

        def mutate(index: int, trace: CallerTrace, deadline: float) -> Mapping[str, object]:
            return request_with_retries(test, settings, trace, deadline, action, "POST", path + ":" + action, key=key)

        outcomes = run_fanout(test, settings, action + "run_fanout", mutate)
        ids = {required_string(item.value, "operation_id") for item in outcomes if item.value}
        test.context.verify(len(ids) == 1, f"same-key {action} produced multiple operations")
        test.wait_operation(next(iter(ids)))
        test.wait_sandbox(sandbox_id, state)

    def race(index: int, trace: CallerTrace, deadline: float) -> Mapping[str, object]:
        action = "pause" if index == 0 else "wake"
        endpoint = settings.api_endpoints[index % len(settings.api_endpoints)]
        remaining = deadline - time.monotonic()
        require(remaining > 0, "lifecycle race: batch deadline exceeded")
        response = test.request("POST", path + ":" + action, idempotency_key=new_key("race"),
                                api_url=endpoint, timeout=min(test.config.request_timeout_seconds, remaining))
        trace.observe(action, response.status, 202, endpoint)
        require(response.status in {202, 409, 412}, f"lifecycle race: unexpected HTTP {response.status}")
        return {"status": response.status, "body": response.body}

    outcomes = run_fanout(test, settings, "pause_wake_race", race, count=2)
    terminals = []
    for item in outcomes:
        if item.value and item.value["status"] == 202:
            body = item.value["body"]
            terminal = test.wait_operation(required_string(body, "operation_id"), require_success=False)
            terminals.append({key: terminal.get(key) for key in
                              ("id", "state", "terminal_error_code")})
    test.context.record("race_terminal_operations", terminals)
    # Either concurrent intent may win. A new explicit wake must still converge,
    # and a command must work after the contenders have both become terminal.
    response = test.request("POST", path + ":wake", idempotency_key=new_key("settle"))
    test.context.verify(response.status == 202, "post-race wake was rejected")
    test.wait_operation(required_string(response.body, "operation_id"))
    test.wait_sandbox(sandbox_id)
    stdout, stderr = run_command(test, settings, CallerTrace(0),
                              time.monotonic() + settings.batch_seconds, sandbox_id, "printf alive")
    test.context.verify(stdout == b"alive" and not stderr, "sandbox did not recover after lifecycle contention")

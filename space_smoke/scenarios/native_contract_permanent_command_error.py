"""Permanent command startup errors must not invite unbounded retries."""
from space_smoke.registry import scenario
from space_smoke.native_hardening import NativeHardeningFixture


def _permanent_error(value: object) -> bool:
    return isinstance(value, dict) and value.get("retryable") is False


def _permanent_http(status: int, body: dict[str, object]) -> bool:
    # Authentication, contention, and rate-limit failures do not establish that
    # this command's invalid executable or working directory was diagnosed.
    return (status in (400, 404, 422) or 500 <= status < 600) and _permanent_error(body.get("error"))


@scenario(
    "native_contract.permanent_command_error",
    api_surfaces=("native",),
)
def permanent_command_error(test: NativeHardeningFixture) -> None:
    """Missing executables and working directories yield permanent command failures."""
    sid = test.create_ready()
    outcomes = []
    for label, body in (
        ("executable", {"command": ["/does-not-exist-space-smoke"]}),
        ("cwd", {"command": ["true"], "cwd": "/does-not-exist-space-smoke"}),
    ):
        command = test.request("POST", f"/v1/sandboxes/{sid}/commands", body=body,
                               idempotency_key=test.new_key(label))
        if command.status != 201:
            outcomes.append({"input": label, "phase": "admission", "status": command.status,
                             "permanent": _permanent_http(command.status, command.body)})
            continue
        events = test.events(command.body, timeout=15)
        errors = [event.data for event in events.events if event.data.get("type") == "error"]
        exits = [event.data.get("exit_code") for event in events.events
                 if event.data.get("type") == "process_exited"]
        permanent_stream = (
            events.status == 200
            and not any(event.data.get("type") == "resync_required" for event in events.events)
            and all(_permanent_error(error) for error in errors)
            and (bool(errors) or (len(exits) == 1 and isinstance(exits[0], int) and exits[0] != 0))
        )
        outcomes.append({"input": label, "phase": "attach", "status": events.status,
                         "error_codes": [error.get("code") for error in errors], "exit_codes": exits,
                         "permanent": _permanent_http(events.status, events.body) or permanent_stream})
    test.context.verify(all(item["permanent"] for item in outcomes),
                        "permanent command errors were not actionable terminal failures", outcomes=outcomes)

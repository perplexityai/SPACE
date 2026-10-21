"""Command input rejection without executing malformed commands."""
from space_smoke.registry import scenario
from space_smoke.native_hardening import NativeHardeningFixture

@scenario("native_contract.command_validation", api_surfaces=("native",))
def command_validation(test: NativeHardeningFixture) -> None:
    """Malformed commands, and stdin or pty over server-sent events, fail admission."""
    sid = test.create_ready()
    outcomes = []
    for label, body in (
        ("empty_argv", {"command": []}),
        ("nul_argv", {"command": ["printf", "\u0000"]}),
        ("zero_timeout", {"command": ["true"], "timeout": "0s"}),
    ):
        response = test.request("POST", f"/v1/sandboxes/{sid}/commands", body=body,
                                idempotency_key=test.new_key(label))
        outcomes.append({"input": label, "status": response.status})
    test.context.verify(all(item["status"] in (400, 422) for item in outcomes),
                        "malformed commands were accepted", outcomes=outcomes)

    # Server-sent events carry no input, so a command asking for stdin or a
    # pty is refused before it is created when streamed, and its events stream
    # refuses to start it when it was created without streaming.
    refused = []
    for field, value in (("stdin", True), ("pty", {"rows": 24, "cols": 80})):
        response = test.request("POST", f"/v1/sandboxes/{sid}/commands",
                                body={"command": ["cat"], field: value},
                                idempotency_key=test.new_key(f"stream_{field}"),
                                accept="text/event-stream")
        refused.append({"field": field, "status": response.status,
                        "code": response.body.get("code"),
                        "details": response.body.get("details")})
    test.context.verify(
        all(item["status"] == 400 and item["code"] == "invalid_request"
            and item["details"] == [{"key": "field", "value": item["field"]}]
            for item in refused),
        "a streamed command that asks for input was not refused", outcomes=refused)
    created = test.request("POST", f"/v1/sandboxes/{sid}/commands",
                           body={"command": ["cat"], "stdin": True},
                           idempotency_key=test.new_key("stdin")).require(201)
    events = test.events(created, timeout=30)
    error = events.body.get("error")
    test.context.verify(
        events.status == 400 and isinstance(error, dict)
        and error.get("code") == "invalid_request" and error.get("retryable") is False,
        "the events stream started a process that waits on stdin",
        status=events.status, error=error)

"""Command input rejection without executing malformed commands."""
import json

from space_smoke.registry import scenario
from space_smoke.native_hardening import NativeHardeningFixture

@scenario("native_contract.command_validation", api_surfaces=("native",))
def command_validation(test: NativeHardeningFixture) -> None:
    """Malformed commands and a streamed pty fail admission; streamed stdin reads EOF."""
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

    # Server-sent events carry no input. A pty cannot be closed, so a command
    # asking for one is refused before it is created when streamed, and its
    # events stream refuses to start it when it was created without streaming,
    # even when the command never reads input.
    pty = {"rows": 24, "cols": 80}
    refused = test.request("POST", f"/v1/sandboxes/{sid}/commands",
                           body={"command": ["ls", "--color=auto"], "pty": pty},
                           idempotency_key=test.new_key("stream_pty"),
                           accept="text/event-stream")
    test.context.verify(
        refused.status == 400 and refused.body.get("code") == "invalid_request"
        and refused.body.get("details") == [{"key": "field", "value": "pty"}],
        "a streamed pty command was not refused",
        status=refused.status, body=refused.body)
    created = test.request("POST", f"/v1/sandboxes/{sid}/commands",
                           body={"command": ["sh"], "pty": pty},
                           idempotency_key=test.new_key("pty")).require(201)
    events = test.events(created, timeout=30)
    error = events.body.get("error")
    test.context.verify(
        events.status == 400 and isinstance(error, dict)
        and error.get("code") == "invalid_request" and error.get("retryable") is False,
        "the events stream started a pty process it cannot drive",
        status=events.status, error=error)

    # Stdin starts at end of file instead, so a command that reads it
    # finishes, whether it is streamed or created first and then attached.
    reads_stdin = {"command": ["sh", "-c", "read -r line || exit 7"], "stdin": True}
    streamed = test.request("POST", f"/v1/sandboxes/{sid}/commands", body=reads_stdin,
                            idempotency_key=test.new_key("stream_stdin"),
                            accept="text/event-stream", timeout=30)
    frames = [json.loads(line[5:]) for line in streamed.text.splitlines()
              if line.startswith("data:")]
    streamed_exits = [frame.get("exit_code") for frame in frames
                      if frame.get("type") == "process_exited"]
    created = test.request("POST", f"/v1/sandboxes/{sid}/commands", body=reads_stdin,
                           idempotency_key=test.new_key("stdin")).require(201)
    attached = test.events(created, timeout=30)
    attached_exits = [event.data.get("exit_code") for event in attached.events
                      if event.data.get("type") == "process_exited"]
    test.context.verify(
        streamed.status == 201 and streamed_exits == [7]
        and attached.status == 200 and attached_exits == [7],
        "a stdin command over server-sent events did not read end of file",
        streamed_status=streamed.status, streamed_exits=streamed_exits,
        attached_status=attached.status, attached_exits=attached_exits)

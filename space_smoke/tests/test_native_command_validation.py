"""native_contract.command_validation reads a streamed command's exit whether
or not the API relays its event stream."""

from __future__ import annotations

import json

import pytest

from space_smoke.model import Verdict
from space_smoke.native_hardening import NativeEvent, NativeEvents, NativeResponse
from space_smoke.runtime import execute_scenario
from space_smoke.scenarios.native_contract_command_validation import command_validation

_PTY_REFUSAL = {
    "code": "invalid_request",
    "details": [{"key": "field", "value": "pty"}],
}


def _handle(session_id: str) -> dict[str, object]:
    return {
        "session_id": session_id,
        "attach": {"events_url": f"/v1/sessions/{session_id}/events", "token": "dpt_test"},
    }


def _answer(status: int, body: dict[str, object], *, content_type: str = "application/json",
            text: str = "") -> NativeResponse:
    return NativeResponse(status=status, body=body, headers={"content-type": content_type},
                          elapsed_seconds=0.0, text=text or json.dumps(body))


def _exited(exit_code: int) -> dict[str, object]:
    return {"type": "process_exited", "exit_code": exit_code}


class _Fixture:
    """An API whose commands behave as the contract says; `relay` decides
    whether a command requested as an event stream is answered with it."""

    def __init__(self, context, *, relay: bool, exit_code: int = 7) -> None:
        self.context = context
        self._relay = relay
        self._exit_code = exit_code
        self._sessions = 0

    def create_ready(self) -> str:
        return "sbx_validation"

    def new_key(self, label: str) -> str:
        return label

    def request(self, method, path, *, body=None, idempotency_key=None,
                accept="application/json", timeout=None):
        assert (method, path) == ("POST", "/v1/sandboxes/sbx_validation/commands")
        command = body["command"]
        if not command or "\u0000" in "".join(command) or body.get("timeout") == "0s":
            return _answer(400, {"code": "invalid_request"})
        streamed = accept == "text/event-stream"
        if streamed and body.get("pty"):
            return _answer(400, _PTY_REFUSAL)
        self._sessions += 1
        handle = _handle(f"proc_{'pty' if body.get('pty') else 'stdin'}{self._sessions}")
        if streamed and self._relay:
            frames = [{"type": "command", "command": handle}, _exited(self._exit_code)]
            text = "".join(f"data: {json.dumps(frame)}\n\n" for frame in frames)
            return _answer(201, {}, content_type="text/event-stream", text=text)
        return _answer(201, handle)

    def events(self, command, *, timeout):
        if str(command["session_id"]).startswith("proc_pty"):
            return NativeEvents(400, (), 0.0, {"error": {"code": "invalid_request",
                                                         "retryable": False}}, {})
        return NativeEvents(200, (NativeEvent("1", _exited(self._exit_code)),), 0.0, {}, {})


def _run(**fixture):
    return execute_scenario(
        title=command_validation.title,
        body=lambda context: command_validation.verify(_Fixture(context, **fixture)),
    )


@pytest.mark.parametrize("relay", [True, False])
def test_a_streamed_stdin_command_reads_end_of_file_with_or_without_a_relay(relay):
    result = _run(relay=relay)
    assert result.verdict == Verdict.PASS, result
    assert result.evidence["streamed_relayed"] is relay


def test_an_unrelayed_command_that_does_not_read_end_of_file_fails():
    result = _run(relay=False, exit_code=0)
    assert result.verdict == Verdict.FAIL, result
    assert result.evidence["streamed_exits"] == [0]

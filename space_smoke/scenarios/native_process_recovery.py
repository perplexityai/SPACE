"""Public recovery requests remain fenced across reconnects and memory restore."""

import base64
import json
import time
import uuid

from space_smoke.native_hardening import NativeHardeningFixture
from space_smoke.registry import scenario
from space_smoke.runtime import SubprocessExecutor


def _call(test, command, request, *, error=None, code=None):
    result = SubprocessExecutor().run(
        ["node", "examples/process_recovery.mjs"],
        input_text=json.dumps({
            "dataPlaneUrl": test.config.data_plane_url,
            "attach": command["attach"], "request": request, "timeoutMs": 30000,
            "maxBytes": 16 * 1024 * 1024,
        }), timeout=35,
    )
    test.context.verify(result.returncode == 0, "recovery client did not complete")
    response = json.loads(result.stdout)
    if code is not None:
        test.context.verify(response["error"] and response["error"].get("code") == code,
                            "wrong recovery status", error=response["error"])
    elif error is None:
        test.context.verify(response["error"] is None, "recovery operation failed", error=response["error"])
    else:
        test.context.verify(response["error"] and response["error"].get("reason") == error,
                            "wrong recovery refusal", error=response["error"])
    return response["frames"]


def _command(test, sandbox, discovery, argv, **extra):
    body = {"contract_version": 1,
            "expected_instance_id": discovery["instance_id"],
            "expected_activation_id": discovery["activation_id"],
            "process": {"command": argv, "timeout": "5m", **extra}}
    return test.request("POST", f"/v1/sandboxes/{sandbox}/recovery-commands", body=body,
                        idempotency_key=test.key("recovery-command")).require(201)


@scenario("native.process_recovery", api_surfaces=("native",), required_tools=("node",))
def process_recovery(test: NativeHardeningFixture) -> None:
    """Start replay, cursor reads and sequenced input survive transport reconnects;
    a restored VM rejects handles from its previous activation before effects.
    """
    sandbox = test.create_ready()
    path = f"/v1/sandboxes/{sandbox}"
    discovery = test.request("GET", path + "/process-execution").require(200)
    test.context.verify({"process_recovery_start", "process_recovery_handles", "process_mutation_receipts"}
                        <= set(discovery["capabilities"]), "recovery not advertised")
    test.context.verify(bool(discovery["activation_id"]) and discovery["idempotency_retention_ms"] > 0,
                        "missing recovery retry bounds")
    marker = "/tmp/recovery-" + uuid.uuid4().hex
    command = _command(test, sandbox, discovery,
                       ["sh", "-c", f"echo once >> {marker}; cat; wc -l < {marker}"], stdin=True)
    _call(test, command, {"type": "heartbeat", "seq": 1}, code=3)
    test.request("GET", path + "/processes/" + command["process_id"]).require(404)
    test.request("GET", f"/v1/sessions/{command['session_id']}/events",
                 api_url=test.config.data_plane_url, token=command["attach"]["token"]).require(400)
    test.request("GET", path + "/processes/" + command["process_id"]).require(404)
    started = _call(test, command, {"type": "recovery_start"})
    handle = started[0]["handle"]
    replay = _call(test, command, {"type": "recovery_start"})
    test.context.verify(replay[0]["handle"] == handle, "start retry changed process identity")
    for _ in range(2):
        _call(test, command, {"type": "recovery_input", "handle": handle,
                             "sequence": 1, "data_base64": base64.b64encode(b"once\n").decode()})
    for _ in range(2):
        _call(test, command, {"type": "recovery_close_stdin", "handle": handle, "sequence": 2})
    request = {"type": "recovery_connect", "handle": handle,
               "output_cursor": 0, "resume_after_output_gap": True}
    output = _call(test, command, request)
    again = _call(test, command, request)
    test.context.verify(output == again, "cursor replay consumed output")
    stdout = b"".join(base64.b64decode(frame["data_base64"]) for frame in output
                      if frame["type"] == "recovery_stdout")
    test.context.verify(stdout == b"once\n1\n", "start or input was applied twice", output=stdout.decode())
    test.context.verify(output[-1]["type"] == "recovery_exited" and output[-1]["exit_code"] == 0,
                        "process did not exit successfully")

    with test.context.phase("sequenced signals and fenced PTY resize"):
        signaled = _command(test, sandbox, discovery, ["sleep", "300"])
        signaled_handle = _call(test, signaled, {"type": "recovery_start"})[0]["handle"]
        for _ in range(2):
            _call(test, signaled, {"type": "recovery_signal", "handle": signaled_handle,
                                   "sequence": 1, "signal": 15})
        exited = _call(test, signaled, {"type": "recovery_connect", "handle": signaled_handle,
                                       "output_cursor": 0, "resume_after_output_gap": False})
        test.context.verify(exited[-1]["status"] == "signaled" and exited[-1]["signal"] == 15,
                            "sequenced signal did not reach the process")
        terminal = _command(test, sandbox, discovery, ["sh", "-c", "read line; stty size"],
                            pty={"cols": 80, "rows": 24}, stdin=True)
        terminal_handle = _call(test, terminal, {"type": "recovery_start"})[0]["handle"]
        _call(test, terminal, {"type": "recovery_resize", "handle": terminal_handle,
                               "cols": 72, "rows": 18})
        _call(test, terminal, {"type": "recovery_input", "handle": terminal_handle,
                               "sequence": 1, "data_base64": "eAo="})
        resized = _call(test, terminal, {"type": "recovery_connect", "handle": terminal_handle,
                                         "output_cursor": 0, "resume_after_output_gap": False})
        resized_output = b"".join(base64.b64decode(frame["data_base64"]) for frame in resized
                                   if frame["type"] == "recovery_stdout")
        test.context.verify(b"18 72" in resized_output and resized[-1]["exit_code"] == 0,
                            "fenced resize did not update the PTY")
        _call(test, terminal, {"type": "recovery_input", "handle": handle,
                               "sequence": 3, "data_base64": "eA=="}, code=9)

    with test.context.phase("counted gaps cross the public data plane"):
        # Exceeds the default per-process journal. Wait for exit before reading
        # so an attached primary reader cannot prevent the eviction under test.
        emitted = 8 * 1024 * 1024
        noisy = _command(test, sandbox, discovery, ["head", "-c", str(emitted), "/dev/zero"])
        noisy_handle = _call(test, noisy, {"type": "recovery_start"})[0]["handle"]
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            observed = test.request("GET", path + "/processes/" + noisy_handle["process_id"]).require(200)
            if observed.get("state") == "exited":
                break
            time.sleep(0.1)
        else:
            raise AssertionError("verbose process did not exit")
        frames = _call(test, noisy, {"type": "recovery_connect", "handle": noisy_handle,
                                     "output_cursor": 0, "resume_after_output_gap": True})
        gaps = [frame for frame in frames if frame["type"] == "recovery_output_gap"]
        retained = sum(len(base64.b64decode(frame["data_base64"])) for frame in frames
                       if frame["type"] in {"recovery_stdout", "recovery_stderr"})
        test.context.verify(len(gaps) == 1 and gaps[0]["skipped_events"] > 0,
                            "evicted output was not reported as a gap")
        test.context.verify(gaps[0]["output_bytes_before"] + retained == emitted,
                            "gap and retained bytes do not account for all output")
        test.context.verify(frames[-1]["exit_code"] == 0, "gap replay lost the exit")

    # Keep a process resident in the snapshot. The old start request, input
    # sequence and cursor must all be rejected after restore, not downgraded.
    resident = _command(test, sandbox, discovery, ["cat"], stdin=True)
    resident_handle = _call(test, resident, {"type": "recovery_start"})[0]["handle"]
    for action in ("suspend", "restore"):
        operation = test.request("POST", path + ":" + action,
                                 idempotency_key=test.key(action)).require(202)
        test.wait_operation(operation["operation_id"])
    test.wait_sandbox(sandbox)
    restored = test.request("GET", path + "/process-execution").require(200)
    test.context.verify(restored["activation_id"] != discovery["activation_id"],
                        "memory restore reused the recovery activation")
    for request in [
        {"type": "recovery_start"},
        {"type": "recovery_input", "handle": resident_handle, "sequence": 1, "data_base64": "eA=="},
        {"type": "recovery_connect", "handle": resident_handle, "output_cursor": 0, "resume_after_output_gap": True},
    ]:
        _call(test, resident, request, error="activation_changed")
    fresh = _command(test, sandbox, restored, ["echo", "restored"])
    fresh_handle = _call(test, fresh, {"type": "recovery_start"})[0]["handle"]
    recovered = _call(test, fresh, {"type": "recovery_connect", "handle": fresh_handle,
                                  "output_cursor": 0, "resume_after_output_gap": True})
    test.context.verify(recovered[-1]["exit_code"] == 0, "new activation cannot execute")

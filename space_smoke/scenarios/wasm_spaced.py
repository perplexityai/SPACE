"""Wasmtime through the existing SPACE command and filesystem APIs."""

import json
import uuid

from space_smoke.registry import scenario
from space_smoke.spaced_runtime import SpacedEndpoint, SpacedRpcError, SpacedRuntimeFixture


@scenario(
    "wasm.spaced",
    api_surfaces=("node",),
    tier="manual",
    manual_reason="Requires a task-owned contained spaced agent with the Wasmtime profile",
    timeout_minutes=5,
    required_tools=("grpcurl",),
    rerun_environment=(
        "SPACE_SMOKE_SPACED_SOCKET", "SPACE_SMOKE_SPACED_ENDPOINT", "SPACE_SMOKE_SPACED_SUDO",
    ),
)
def wasm_spaced(test: SpacedRuntimeFixture) -> None:
    """Normal process RPCs execute WASI, retain identities and exchange workspace files."""
    context = test.context
    path = f"/workspace/wasm-spaced-{uuid.uuid4().hex}.txt"
    context.scope.defer(
        "remove task-owned command output",
        lambda: test.call("Filesystem", "Remove", {
            "path": path, "missingOk": True,
        }),
        critical=True,
    )
    with context.phase("negotiate WASI without claiming a Linux guest"):
        if test.endpoint is SpacedEndpoint.HOST:
            ready = test.call("Meta", "WaitReady", {"timeout": "5s"})[0]
            test.call("Meta", "PingGuest", {})
            context.verify(bool(ready["guestVersion"]["version"]), "host omitted guest identity")
        else:
            ready = test.call("Meta", "WaitReady", {
                "timeout": "5s", "hostContractRevision": 1,
            })[0]
            context.verify(ready["ready"], "spaced agent did not become ready")
            context.verify(ready["platform"]["os"] == "GUEST_OS_WASI", "agent misidentified WASI")
            health = test.call("Meta", "Ping", {"drainDiagnostics": True})[0]
            context.verify(
                health.get("memoryUsedBytes") is None,
                "WASI health reported native host memory as sandbox usage",
            )
            context.verify(
                "GUEST_OPERATION_LOOPBACK_FORWARD" not in ready["platform"]["operations"]
                and "GUEST_OPERATION_RESUME" not in ready["platform"]["operations"],
                "WASI advertised unsupported networking or snapshot lifecycle",
            )
        context.verify(
            ready.get("memTotalBytes") is None and ready.get("vcpuCount") is None,
            "WASI readiness reported its native host's resources",
        )
    with context.phase("execute WASI through Process.Start and read through Filesystem.Read"):
        result = test.python(
            "import sys, os, json; from pathlib import Path; "
            f"Path({path!r}).write_text(str(6*7)); "
            "print(json.dumps({'platform': sys.platform, 'injected': os.getenv('LD_PRELOAD')}))",
            env={"LD_PRELOAD": "/workspace/nonexistent.so"},
        )
        context.verify(result.exit_code == 0, "WASI command failed", stderr=result.stderr.decode())
        context.verify(
            json.loads(result.stdout) == {"platform": "wasi", "injected": "/workspace/nonexistent.so"},
            "Python did not receive the isolated guest environment",
        )
        context.verify(test.read_file(path) == b"42", "file API did not see the guest write")
    with context.phase("replay an accepted command without re-executing it"):
        key = f"wasm-replay-{uuid.uuid4().hex}"
        source = f"from pathlib import Path; p=Path({path!r}); p.write_text(p.read_text()+'x'); print(p.read_text())"
        first = test.python(source, key=key)
        second = test.python(source, key=key)
        context.verify(first.exit_code == second.exit_code == 0, "replayed command failed")
        context.verify(first.process_id == second.process_id, "retry minted a second process")
        context.verify(test.read_file(path) == b"42x", "retry executed a mutation twice")
    with context.phase("enforce the normal process timeout and recover"):
        timed_out = test.python("while True: pass", timeout=1)
        context.verify("timedOut" in timed_out.outcome, "command escaped the process timeout")
        context.verify(test.python("print('ready')").stdout == b"ready\n", "agent did not recover")
    with context.phase("cancel a detached command and reconnect to its retained outcome"):
        process_id = str(uuid.uuid4())
        context.scope.defer(
            "stop the task-owned background process",
            lambda: test.kill_if_running(process_id),
            critical=True,
        )
        started = test.start_background({
            "command": ["python3", "-c", "while True: pass"],
            "cwd": "/workspace", "timeout": "30s", "background": True,
            "processId": process_id, "idempotencyKey": f"wasm-cancel-{process_id}",
        })
        context.verify(
            started["processId"] == process_id,
            "background start did not retain the caller's process identity",
        )
        test.call("Process", "SendSignal", {"processId": process_id, "signal": 9})
        replay = test.call("Process", "ConnectProcess", {"processId": process_id})
        outcomes = [event["exit"] for event in replay if "exit" in event]
        context.verify(
            len(outcomes) == 1 and outcomes[0].get("signaled", {}).get("signal") == 9,
            "reconnect did not retain the cancelled process outcome",
        )
    with context.phase("refuse native execution rather than falling back"):
        try:
            test.call("Process", "Start", {
                "command": ["/bin/sh", "-c", "exit 0"],
                "idempotencyKey": f"wasm-no-native-{uuid.uuid4().hex}",
            })
        except SpacedRpcError as error:
            context.verify(
                error.code == 9,
                "native execution did not receive a typed capability refusal",
            )
        else:
            context.verify(False, "native command was accepted by the WASI profile")
        for service, method, request, code in [
            ("Process", "Start", {
                "command": ["python3", "-c", "print(42)"], "user": "root",
                "idempotencyKey": f"wasm-no-root-{uuid.uuid4().hex}",
            }, 7),
            ("Filesystem", "Stat", {"path": "/workspace", "user": "root"}, 9),
        ]:
            try:
                test.call(service, method, request)
            except SpacedRpcError as error:
                context.verify(
                    error.code == code, "account switch was not refused by policy",
                    account_refusal=str(error),
                )
            else:
                context.verify(False, "contained broker accepted a root operation")

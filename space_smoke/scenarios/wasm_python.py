"""Manual smoke for a supervisor configured with a WASI CPython module."""

from typing import Any

from space_smoke.node_runtime import NodeRuntimeFixture
from space_smoke.registry import scenario


@scenario(
    "wasm.python",
    api_surfaces=("node",),
    tier="manual",
    manual_reason="Requires an explicitly configured experimental Wasmtime supervisor",
    timeout_minutes=5,
    rerun_environment=("SPACE_SMOKE_NODE_RUNTIME_COMMAND",),
)
def wasm_python(test: NodeRuntimeFixture) -> None:
    """WASI execution preserves files, bounds output and time, and recovers."""
    context = test.context
    created = test.request({"operation": "create"})
    sandbox_id = created["sandbox_id"]
    deleted = False

    def remove() -> None:
        nonlocal deleted
        if not deleted:
            test.request({"operation": "delete", "sandbox_id": sandbox_id})
            deleted = True

    context.scope.defer("remove task-owned WASI sandbox", remove, critical=True)

    def execute(source: str, timeout: int = 60) -> dict[str, Any]:
        return test.request({
            "operation": "execute",
            "sandbox_id": sandbox_id,
            "argv": ["-c", source],
            "env": {"PYTHONHOME": "/", "PYTHONUNBUFFERED": "1"},
            "timeout_secs": timeout,
        })["execution"]

    with context.phase("run WASI Python and preserve guest files"):
        result = execute(
            "import sys; from pathlib import Path; assert sys.platform == 'wasi'; "
            "Path('/tmp/value.txt').write_text(str(6*7)); print(sys.platform)"
        )
        context.verify(result["exit_code"] == 0 and result["stdout"].strip() == "wasi",
                       "WASI Python did not execute", execution=result)
        downloaded = test.request({
            "operation": "download", "sandbox_id": sandbox_id, "path": "/tmp/value.txt",
        })
        context.verify(downloaded["content"] == "42", "guest file was not preserved")
    with context.phase("report fast output overflow as failure, not truncated success"):
        overflow = execute("import sys; sys.stdout.write('x'*1048577); sys.stdout.flush()")
        context.verify(overflow["exit_code"] == 125 and len(overflow["stdout"]) == 1048576,
                       "output overflow was not rejected",
                       exit_code=overflow["exit_code"], bytes=len(overflow["stdout"]))
    with context.phase("stop execution at its deadline and recover"):
        expired = execute("while True: pass", timeout=1)
        context.verify(expired["exit_code"] == 124 and expired["timed_out"],
                       "execution did not time out")
        recovered = execute("print('ready')")
        context.verify(recovered["exit_code"] == 0 and recovered["stdout"] == "ready\n",
                       "runtime did not recover", execution=recovered)
    remove()
    context.record("sandbox_deleted", deleted)

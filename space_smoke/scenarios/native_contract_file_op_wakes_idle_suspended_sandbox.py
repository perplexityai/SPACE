"""A file call to the data plane wakes a sandbox that idle-suspended under an access wake policy."""

from space_smoke.native_files import FileCalls, data
from space_smoke.native_hardening import NativeHardeningFixture
from space_smoke.registry import scenario

IDLE_SECONDS = 60
# The idle deadline is checked on reconciler ticks, and the suspend uploads a
# snapshot before the sandbox reports suspended.
SUSPEND_BUDGET_SECONDS = 300
# The API waits up to 135 s for the wake it starts while preparing the call,
# and answers a retryable error after that; the write follows the wake.
WAKE_BUDGET_SECONDS = 180
POLICY = {
    "wake": {"on": ["access"]},
    "condition_sets": {
        "access_idle": {"kind": "inactivity", "activity_source": "access"},
    },
    "stages": [
        {"condition_set": "access_idle", "at": f"{IDLE_SECONDS}s", "action": "suspend"},
    ],
}


@scenario("native_contract.file_op_wakes_idle_suspended_sandbox", api_surfaces=("native",))
def file_op_wakes_idle_suspended_sandbox(test: NativeHardeningFixture) -> None:
    """With `wake.on=["access"]`, a `files:write` sent to the data plane after
    an idle suspend wakes the sandbox and lands, without a wake or any other
    call first, and the state written before the suspend is still there."""
    sandbox_id = test.create_ready({"auto_lifecycle": POLICY})
    files = FileCalls(test, sandbox_id, test.config.data_plane_url)
    before = f"/tmp/{test.new_key('before')}"
    after = f"/tmp/{test.new_key('after')}"
    first = files.call("write", {"path": before, "data_base64": data(b"kept")}, key=test.new_key("w"))
    test.context.verify(
        first.status == 200, "the data plane did not write to the running sandbox", status=first.status
    )
    test.wait_sandbox(sandbox_id, "suspended", timeout=SUSPEND_BUDGET_SECONDS)

    with test.context.phase("write to the suspended sandbox through the data plane"):
        written = files.call(
            "write",
            {"path": after, "data_base64": data(b"woken")},
            key=test.new_key("wake-write"),
            timeout=WAKE_BUDGET_SECONDS,
        )
        test.context.record("wake_write_seconds", written.elapsed_seconds)
        test.context.verify(
            written.status == 200 and written.body.get("bytes_written") == len(b"woken"),
            "a file write the wake policy allows did not wake the sandbox and land",
            status=written.status, body=written.body,
        )
    for path, content in ((before, b"kept"), (after, b"woken")):
        read = files.call("read", {"path": path, "length": 16})
        test.context.verify(
            read.status == 200 and read.text == content.decode(),
            "the woken sandbox does not hold what was written",
            path=path, status=read.status,
        )

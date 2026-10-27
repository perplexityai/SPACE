"""Native file write idempotency boundary."""
import base64
from urllib.parse import quote

from space_smoke.native_hardening import NativeHardeningFixture, NativeResponse
from space_smoke.registry import scenario


def _data(content: bytes) -> str:
    return base64.b64encode(content).decode()


def _error_code(response: NativeResponse) -> object:
    error = response.body.get("error")
    return error.get("code") if isinstance(error, dict) else response.body.get("code")


@scenario("native_contract.file_idempotency_conflict", api_surfaces=("native",))
def file_idempotency_conflict(test: NativeHardeningFixture) -> None:
    """A file write's idempotency key replays that write and refuses any other."""
    sandbox_id = test.create_ready()
    files = f"/v1/sandboxes/{quote(sandbox_id, safe='')}/files"
    key = test.new_key("file-write")
    written = {"path": "/tmp/space-smoke-idempotent-a", "data_base64": _data(b"first")}

    def write(body: dict[str, object]) -> NativeResponse:
        return test.request("POST", f"{files}:write", body=body, idempotency_key=key)

    def stat(path: str) -> NativeResponse:
        return test.request("POST", f"{files}:stat", body={"path": path})

    first = write(written)
    test.context.verify(first.status == 200, "file write failed", status=first.status)
    replay = write(written)
    test.context.verify(
        replay.status == 200 and replay.body == first.body,
        "a retried write did not replay its result",
        status=replay.status,
    )

    elsewhere = write({**written, "path": "/tmp/space-smoke-idempotent-b"})
    test.context.verify(
        elsewhere.status == 409 and _error_code(elsewhere) == "idempotency_conflict",
        "a reused key wrote a different path",
        status=elsewhere.status,
    )
    missing = stat("/tmp/space-smoke-idempotent-b")
    test.context.verify(
        missing.status == 404, "a refused write created its path", status=missing.status
    )

    changed = write({**written, "data_base64": _data(b"second write")})
    test.context.verify(
        changed.status == 409 and _error_code(changed) == "idempotency_conflict",
        "a reused key wrote different data",
        status=changed.status,
    )
    kept = stat(str(written["path"])).require(200)
    entry = kept.get("entry")
    size = entry.get("size_bytes") if isinstance(entry, dict) else None
    test.context.verify(
        size == len(b"first"), "a refused write changed the file", size_bytes=size
    )

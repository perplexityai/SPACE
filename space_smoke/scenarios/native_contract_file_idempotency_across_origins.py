"""A file mutation's idempotency key holds across the API and the data plane."""

from space_smoke.native_files import FileCalls, data, error_code, require_distinct_origins
from space_smoke.native_hardening import NativeHardeningFixture
from space_smoke.registry import scenario


@scenario("native_contract.file_idempotency_across_origins", api_surfaces=("native",))
def file_idempotency_across_origins(test: NativeHardeningFixture) -> None:
    """A write first sent to the API is replayed by the data plane under its
    key, and a different write under it is refused there with 409
    `idempotency_conflict`; a rename first sent to the data plane is replayed
    by the API the same way. Neither refused request changes a file.
    Inconclusive when the two origins are one."""
    require_distinct_origins(test)
    sandbox_id = test.create_ready()
    api = FileCalls(test, sandbox_id, test.config.api_url)
    data_plane = FileCalls(test, sandbox_id, test.config.data_plane_url)
    base = f"/tmp/{test.new_key('idempotency')}"
    written = {"path": f"{base}-a", "data_base64": data(b"first"), "parents": True}

    with test.context.phase("write through the API, then replay and change it through the data plane"):
        key = test.new_key("write")
        first = api.call("write", written, key=key)
        test.context.verify(first.status == 200, "the API's write failed", status=first.status)
        replay = data_plane.call("write", written, key=key)
        test.context.verify(
            replay.status == 200 and replay.body == first.body,
            "the data plane did not replay the API's write",
            status=replay.status, body=replay.body, first=first.body,
        )
        changed = data_plane.call("write", {**written, "data_base64": data(b"second write")}, key=key)
        test.context.verify(
            changed.status == 409 and error_code(changed) == "idempotency_conflict",
            "the data plane accepted a different write under the API's key",
            status=changed.status, code=error_code(changed),
        )
        kept = data_plane.call("stat", {"path": written["path"]}).require(200)
        entry = kept.get("entry")
        size = entry.get("size_bytes") if isinstance(entry, dict) else None
        test.context.verify(size == len(b"first"), "a refused write changed the file", size_bytes=size)

    with test.context.phase("rename through the data plane, then replay and change it through the API"):
        key = test.new_key("rename")
        renamed = {"old_path": written["path"], "new_path": f"{base}-b"}
        first = data_plane.call("rename", renamed, key=key)
        test.context.verify(first.status == 200, "the data plane's rename failed", status=first.status)
        replay = api.call("rename", renamed, key=key)
        test.context.verify(
            replay.status == 200 and replay.body == first.body,
            "the API did not replay the data plane's rename",
            status=replay.status, body=replay.body,
        )
        elsewhere = api.call("rename", {**renamed, "new_path": f"{base}-c"}, key=key)
        test.context.verify(
            elsewhere.status == 409 and error_code(elsewhere) == "idempotency_conflict",
            "the API accepted a different rename under the data plane's key",
            status=elsewhere.status, code=error_code(elsewhere),
        )
        missing = api.call("stat", {"path": f"{base}-c"})
        test.context.verify(missing.status == 404, "a refused rename moved the file", status=missing.status)
        moved = api.call("stat", {"path": f"{base}-b"})
        test.context.verify(moved.status == 200, "the replayed rename lost the file", status=moved.status)

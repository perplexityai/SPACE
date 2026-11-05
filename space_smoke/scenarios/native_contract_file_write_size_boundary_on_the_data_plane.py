"""The data plane takes a Native file write up to the API's 16 MiB cap and refuses one past it."""

from space_smoke.native_files import FileCalls, data
from space_smoke.native_hardening import NativeHardeningFixture
from space_smoke.registry import scenario

CAP = 16 * 1024 * 1024
# A 16 MiB write is a ~22 MiB JSON body, sent and decoded whole.
WRITE_TIMEOUT_SECONDS = 120


@scenario("native_contract.file_write_size_boundary_on_the_data_plane", api_surfaces=("native",))
def file_write_size_boundary_on_the_data_plane(test: NativeHardeningFixture) -> None:
    """A `files:write` of exactly 16 MiB sent to the data plane lands whole;
    one byte more is refused with 400 `invalid_request` and writes nothing,
    as the API answers both."""
    sandbox_id = test.create_ready()
    files = FileCalls(test, sandbox_id, test.config.data_plane_url)
    at_cap = f"/tmp/{test.new_key('at-cap')}"
    above = f"/tmp/{test.new_key('above-cap')}"
    # A printable pattern whose period does not divide the read below, so a
    # truncated or shifted write cannot read back the same.
    payload = bytes(97 + index % 23 for index in range(CAP))

    with test.context.phase("write exactly 16 MiB"):
        written = files.call(
            "write",
            {"path": at_cap, "data_base64": data(payload)},
            key=test.new_key("at-cap"),
            timeout=WRITE_TIMEOUT_SECONDS,
        )
        test.context.record("at_cap_seconds", written.elapsed_seconds)
        test.context.verify(
            written.status == 200 and written.body.get("bytes_written") == CAP,
            "a write at the cap was not taken whole",
            status=written.status, body=written.body,
        )
        stat = files.call("stat", {"path": at_cap}).require(200)
        entry = stat.get("entry")
        size = entry.get("size_bytes") if isinstance(entry, dict) else None
        test.context.verify(size == CAP, "the file at the cap has the wrong size", size_bytes=size)
        # The start, the middle and the end, so a chunk lost or reordered
        # anywhere in the body reads back different.
        for where, offset in (("start", 0), ("middle", CAP // 2), ("end", CAP - 1024)):
            chunk = files.call("read", {"path": at_cap, "offset": offset, "length": 1024})
            test.context.verify(
                chunk.status == 200 and chunk.text == payload[offset:offset + 1024].decode(),
                f"the {where} of the file at the cap is not what was written",
                status=chunk.status, offset=offset,
            )

    with test.context.phase("write one byte past 16 MiB"):
        refused = files.call(
            "write",
            {"path": above, "data_base64": data(payload + b"\0")},
            key=test.new_key("above-cap"),
            timeout=WRITE_TIMEOUT_SECONDS,
        )
        error = refused.body.get("error") if isinstance(refused.body.get("error"), dict) else refused.body
        test.context.verify(
            refused.status == 400
            and error.get("code") == "invalid_request"
            and error.get("message") == "file write payload is too large",
            "a write past the cap was not refused as the API refuses it",
            status=refused.status, body=refused.body,
        )
        missing = files.call("stat", {"path": above})
        test.context.verify(missing.status == 404, "a refused write created its file", status=missing.status)

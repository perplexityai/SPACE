"""ECW multipart upload of files larger than one write message."""

from hashlib import sha256

import space_smoke as smoke

MIB = 1024 * 1024
# The data plane streams a staged part to the guest in messages of up to
# 3 MiB. The first file spans three of them; the second starts where the first
# ends in the request's staging and spans two, so a part read from the wrong
# offset or a short message changes the bytes the guest stores.
FILES = (
    ("/tmp/space-smoke/large-upload/first.bin", 7 * MIB),
    ("/tmp/space-smoke/large-upload/second.bin", 3 * MIB + 1),
)


def _content(index: int, size: int) -> bytes:
    """Hashed blocks with no repeating pattern within or across the files, so
    a range read from the wrong place shows up as different bytes."""

    blocks = (
        sha256(f"{index}:{block}".encode()).digest()
        for block in range(size // 32 + 1)
    )
    return b"".join(blocks)[:size]


@smoke.scenario("ecw.large_multipart_upload_round_trip")
def large_multipart_upload_round_trip(test: smoke.Ecw) -> None:
    """Files written together in one multipart Files API request, each larger
    than one write message, are stored byte-exactly at their own paths.
    """

    files = tuple(
        (path, _content(index, size)) for index, (path, size) in enumerate(FILES)
    )

    with test.phase("write both files in one multipart request"):
        test.sandbox.files.write_files(files, request_timeout=120)

    with test.phase("read each file back"):
        for path, expected in files:
            observed = test.sandbox.files.read_bytes(path, request_timeout=120)
            test.verify(
                observed == expected,
                "a file written in a multipart upload changed on its way to the guest",
                path=path,
                expected_bytes=len(expected),
                observed_bytes=len(observed),
                expected_sha256=sha256(expected).hexdigest(),
                observed_sha256=sha256(observed).hexdigest(),
            )

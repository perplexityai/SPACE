"""Native API slow reader coverage."""

import hashlib
from dataclasses import asdict

from space_smoke.native_concurrency import (
    ENVIRONMENT,
    ConcurrencySettings,
    command_output,
)
from space_smoke.native_hardening import NativeHardeningFixture
from space_smoke.registry import scenario


@scenario(
    "native_concurrency.slow_reader",
    api_surfaces=("native",),
    rerun_environment=ENVIRONMENT,
)
def slow_reader(test: NativeHardeningFixture) -> None:
    """A delayed stream reader receives complete output with the expected checksum."""
    settings = ConcurrencySettings.load(test)
    test.context.record("concurrency_settings", asdict(settings))
    sandbox_id = test.create_ready()
    # Each 64-byte record carries its offset, so replacing or reordering equal
    # sized stream chunks cannot pass the checksum as an all-zero payload did.
    payload = b"".join(
        f"{offset:016x}{0:048d}".encode()
        for offset in range(0, settings.output_bytes, 64)
    )[:settings.output_bytes]
    command = (
        f"""awk 'BEGIN {{ for (i=0; i<{settings.output_bytes}; i+=64) printf "%016x%048d", i, 0 }}' """
        f"| head -c {settings.output_bytes}"
    )
    response = test.command(sandbox_id, ["sh", "-c", command], timeout_seconds=60)
    test.context.verify(response.status == 201, "slow-reader command was rejected")
    stream = test.events(response.body, timeout=settings.batch_seconds,
                         initial_read_delay_seconds=min(1, settings.batch_seconds / 10),
                         event_read_delay_seconds=settings.read_delay_seconds)
    test.context.record("slow_reader_http_status", stream.status)
    test.context.verify(stream.status == 200, "slow-reader stream was rejected")
    stdout, stderr = command_output(stream.events, limit=settings.output_bytes)
    digest = hashlib.sha256(stdout).hexdigest()
    expected = hashlib.sha256(payload).hexdigest()
    test.context.record("slow_reader_output", {"bytes": len(stdout), "sha256": digest,
                                              "expected_sha256": expected,
                                              "seconds": stream.elapsed_seconds,
                                              "events": len(stream.events)})
    test.context.verify(len(stdout) == settings.output_bytes and digest == expected and not stderr,
                        "slow-reader output was truncated, duplicated, or corrupted")

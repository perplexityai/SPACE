"""Regression coverage for active ECW loopback-route cleanup."""

import space_smoke as smoke

PORT = 8124
EXPECTED_LINES = tuple(str(ordinal) for ordinal in range(6))
LINE_INTERVAL_SECONDS = 1.0
FORMER_BRIDGE_CLEANUP_WINDOW_SECONDS = 3.0
MINIMUM_STREAM_SECONDS = 5.0


@smoke.scenario(
    "ecw.loopback_route_stays_open",
    sandbox=smoke.sandbox_with_authenticated_public_ports(),
)
def loopback_route_stays_open(test: smoke.Ecw) -> None:
    """An active ECW public-port connection to a loopback-only service stays
    open past the former bridge cleanup window and delivers every delayed line.
    """

    stream = test.serve_delayed_http_stream(
        listening_on=smoke.LOOPBACK_ONLY,
        port=PORT,
        lines=EXPECTED_LINES,
        every_seconds=LINE_INTERVAL_SECONDS,
    )

    stream.establish_public_route()
    test.wait(
        FORMER_BRIDGE_CLEANUP_WINDOW_SECONDS,
        reason="past the former loopback bridge cleanup window",
    )
    response = stream.read_from_public_port(
        "/stream",
        timeout=len(EXPECTED_LINES) * LINE_INTERVAL_SECONDS + 10,
    )

    test.expect_complete_http_stream(
        response,
        status=200,
        lines=EXPECTED_LINES,
        lasting_at_least_seconds=MINIMUM_STREAM_SECONDS,
    )

from __future__ import annotations

import socket

import space_smoke as smoke
from space_smoke.idle_proxy import IdleTimeoutProxy

KEEPALIVE_INTERVAL_SECONDS = 5
PROXY_IDLE_TIMEOUT_SECONDS = 20.0
QUIET_SECONDS = 45


@smoke.scenario("ecw.quiet_process_stream_keepalive")
def quiet_process_stream_keepalive(test: smoke.Ecw) -> None:
    """A command silent past an intermediary's idle timeout keeps its ECW
    response stream open through keepalive events."""

    with (
        test.phase("stream a quiet command through an idle-timeout proxy"),
        IdleTimeoutProxy(
            test.compatibility.envd_address,
            idle_timeout=PROXY_IDLE_TIMEOUT_SECONDS,
        ) as proxy,
    ):
        # An idle control connection shows the proxy drops silent streams.
        with socket.create_connection(proxy.address):
            stream = test.compatibility.stream_command(
                f"sleep {QUIET_SECONDS}; printf done",
                keepalive_interval=KEEPALIVE_INTERVAL_SECONDS,
                dial=proxy.address,
                request_timeout=QUIET_SECONDS + 60,
            )
        idle_closes = proxy.idle_closes

    with test.phase("verify keepalives held the stream open"):
        test.verify(
            stream.error is None
            and stream.events[:1] == ("start",)
            and "keepalive" in stream.events
            and stream.stdout == "done"
            and stream.exit_code == 0,
            "a quiet ECW process stream did not survive the proxy idle timeout",
            stream_error=stream.error,
            events=list(stream.events),
            stdout=stream.stdout,
            exit_code=stream.exit_code,
            longest_silence_seconds=stream.longest_silence_seconds,
            keepalives=stream.events.count("keepalive"),
        )
        test.verify(
            idle_closes == 1,
            "the idle-timeout proxy did not drop its idle control connection",
            idle_closes=idle_closes,
        )

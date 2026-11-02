"""Guest TCP segments carrying MPTCP options never reach the host TCP stack."""

from __future__ import annotations

import json
import shlex
from importlib.resources import files

import space_smoke as smoke

_PROBE_PATH = "/tmp/space-smoke-mptcp-option-probe.py"


@smoke.scenario("ecw.guest_mptcp_segments_dropped")
def guest_mptcp_segments_dropped(test: smoke.Ecw) -> None:
    """An MPTCP SYN gets no host reply while the same plain SYN does.

    No gateway listener speaks MPTCP, so a guest segment carrying TCP option 30
    is never legitimate. The sandbox namespace drops it before its TCP stack
    parses the option; this proves it from the only vantage point a tenant has.
    The plain control establishes that the host does answer a SYN to the
    gateway, which is what makes silence meaningful.
    """

    source = (
        files("space_smoke.guest").joinpath("mptcp_option_probe.py").read_text(encoding="utf-8")
    )
    with test.phase("install mptcp option probe"):
        installed = test.run_command_when_ready(
            f"umask 077 && printf %s {shlex.quote(source)} > {shlex.quote(_PROBE_PATH)}",
            user="root",
            failure_message="mptcp option probe could not be installed",
        )
        test.expect_command(installed, stdout="")

    with test.phase("probe host handling of mptcp segments"):
        result = test.run_command(
            f"python3 {shlex.quote(_PROBE_PATH)}",
            user="root",
            timeout=60,
            failure_message="mptcp option probe did not run",
        )
    test.verify(
        result.exit_code == 0,
        "mptcp option probe failed inside the guest",
        command_exit_code=result.exit_code,
        command_stdout=result.stdout,
        command_stderr=result.stderr,
    )
    try:
        report = json.loads(result.stdout)
    except ValueError:
        report = None
    test.verify(
        isinstance(report, dict),
        "mptcp option probe did not report a JSON object",
        command_stdout=result.stdout,
    )
    assert isinstance(report, dict)
    test.record("mptcp_probe", report)

    test.verify(
        report.get("plain") not in (None, "none"),
        "the plain control SYN got no reply, so the MPTCP SYN cannot be judged",
        plain_reply=report.get("plain"),
    )
    test.verify(
        report.get("mptcp") == "none",
        "an MPTCP SYN from the guest was processed by the host TCP stack",
        reply=report.get("mptcp"),
        plain_reply=report.get("plain"),
    )

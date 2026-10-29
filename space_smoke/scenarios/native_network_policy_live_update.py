"""Native live network-policy update enforcement on a running sandbox."""

from __future__ import annotations

import shlex
from collections.abc import Mapping
from typing import Protocol, cast

import space_smoke as smoke
from space_smoke.capabilities import CommandSandbox


class _NetworkPolicySandbox(CommandSandbox, Protocol):
    def set_network_policy(self, policy: Mapping[str, object]) -> None: ...

    def get_network_policy(self) -> dict[str, object]: ...


# Cloudflare's public resolver serves HTTPS on its IP literal, so the probe
# needs no DNS. It is the destination the network_policy.v1_* suites already
# prove reachable from Kind CI, so a block here is the policy, not the runner.
TARGET = "1.0.0.1"

REACHABLE = "reachable"
BLOCKED = "blocked"

# The PATCH operation succeeds once the desired policy is stored; enforcement
# reaches the running gateway asynchronously through the node reconcile.
_CONVERGENCE_TIMEOUT_SECONDS = 10
_CURL_TIMEOUT_SECONDS = 5
# curl exit codes that mean the connection never produced an HTTP response.
_CURL_NETWORK_FAILURE_EXIT_CODES = "5|6|7|28|35|52|55|56"
_BOOT_ID_COMMAND = "cat /proc/sys/kernel/random/boot_id"


def _deny_by_default(*allowed: str) -> dict[str, object]:
    return {
        "version": "v1",
        "egress": {
            "default_action": "deny",
            "allow": [{"ip": ip} for ip in allowed],
        },
    }


def _probe_command(ip: str) -> str:
    """Print `reachable`, `blocked`, or the unclassified curl evidence."""

    url = shlex.quote(f"https://{ip}/")
    return (
        "set +e; "
        f"code=$(curl -sS -m {_CURL_TIMEOUT_SECONDS} -o /dev/null "
        f"-w '%{{http_code}}' {url} 2>/dev/null); rc=$?; "
        f'if [ "$code" != 000 ] && [ -n "$code" ]; then printf {REACHABLE}; '
        f'else case "$rc" in {_CURL_NETWORK_FAILURE_EXIT_CODES}) '
        f"printf {BLOCKED};; "
        '*) printf "unclassified rc=%s code=%s" "$rc" "$code";; esac; fi'
    )


def _apply_and_verify(
    test: smoke.CommandSandboxFixture,
    sandbox: _NetworkPolicySandbox,
    *,
    allow_target: bool,
) -> None:
    sandbox.set_network_policy(
        _deny_by_default(TARGET) if allow_target else _deny_by_default()
    )
    expected = REACHABLE if allow_target else BLOCKED
    test.wait_until_command_matches(
        _probe_command(TARGET),
        stdout=expected,
        timeout=_CONVERGENCE_TIMEOUT_SECONDS,
        failure_message=f"{TARGET} did not become {expected} after the live update",
    )
    document = sandbox.get_network_policy()
    egress = document.get("egress")
    allow = egress.get("allow") if isinstance(egress, dict) else None
    test.verify(
        isinstance(egress, dict)
        and egress.get("default_action") == "deny"
        and isinstance(allow, list)
        and (({"ip": TARGET} in allow) == allow_target),
        "GET network-policy did not return the updated policy",
        network_policy=document,
    )


@smoke.scenario("native.network_policy_live_update", api_surfaces=("native",))
def network_policy_live_update(test: smoke.CommandSandboxFixture) -> None:
    """PATCHing a running sandbox's network policy changes its egress in place:
    a destination is allowed, revoked, and allowed again, each change is
    enforced and reflected by the read API, and the guest never reboots.
    """

    sandbox = cast(_NetworkPolicySandbox, test.sandbox)

    with test.phase("record guest boot identity"):
        before = test.run_command_when_ready(_BOOT_ID_COMMAND)
        test.verify(
            before.exit_code == 0 and bool(before.stdout.strip()),
            "guest boot id was unreadable",
            command_stdout=before.stdout,
            command_stderr=before.stderr,
        )
        boot_id = before.stdout.strip()
        test.record("boot_id", boot_id)

    with test.phase(f"live update allows {TARGET}"):
        _apply_and_verify(test, sandbox, allow_target=True)

    with test.phase(f"live update revokes {TARGET}"):
        _apply_and_verify(test, sandbox, allow_target=False)

    with test.phase(f"live update allows {TARGET} again"):
        _apply_and_verify(test, sandbox, allow_target=True)

    with test.phase("verify the guest was not restarted"):
        after = test.run_command(_BOOT_ID_COMMAND)
        test.verify(
            after.exit_code == 0 and after.stdout.strip() == boot_id,
            "policy updates restarted the guest instead of applying live",
            boot_id_before=boot_id,
            boot_id_after=after.stdout.strip(),
        )

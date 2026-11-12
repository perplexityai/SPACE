"""Native AWS Secrets Manager credential-injection smoke."""

from __future__ import annotations

import hashlib
import os
import re
import shlex
from collections.abc import Mapping
from importlib.resources import files
from typing import Protocol, cast
from urllib.parse import urlsplit

import space_smoke as smoke
from space_smoke.capabilities import CommandSandbox
from space_smoke.runtime import SmokeConfigurationError


class _CredentialInjectionSandbox(CommandSandbox, Protocol):
    def register_credential_reference(self, secret_arn: str) -> str: ...

    def set_network_policy(self, policy: Mapping[str, object]) -> None: ...

    def retire_credential_reference(self, credential_reference_id: str) -> None: ...


_GUEST_DIGEST_PATH = "/tmp/space-smoke-credential-header-digest.py"


def _required_environment(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        raise SmokeConfigurationError(f"{name} is required")
    return value


def _header_digest_command(url: str, echo_format: str) -> str:
    if echo_format not in {"digest", "json-headers"}:
        raise SmokeConfigurationError(
            "SPACE_SMOKE_ECHO_FORMAT must be 'digest' or 'json-headers'"
        )
    return (
        "curl --fail --silent --show-error --http1.1 --noproxy '*' "
        "--max-time 10 --header 'Cache-Control: no-store' "
        "--header 'Authorization: smoke-guest-placeholder' "
        f"{shlex.quote(url)} | python3 {shlex.quote(_GUEST_DIGEST_PATH)} "
        f"{shlex.quote(echo_format)}"
    )


def _install_header_digest_script(test: smoke.CommandSandboxFixture) -> None:
    source = (
        files("space_smoke.guest")
        .joinpath("credential_header_digest.py")
        .read_text(encoding="utf-8")
    )
    installed = test.run_command_when_ready(
        f"umask 077 && printf %s {shlex.quote(source)} > "
        f"{shlex.quote(_GUEST_DIGEST_PATH)}",
        failure_message="credential digest script could not be installed",
    )
    test.expect_command(installed, stdout="")


@smoke.scenario(
    "native.aws_secret_network_credential_injection",
    api_surfaces=("native",),
    # This scenario needs an explicitly provisioned AWS secret and selected
    # echo endpoint, so keep it out of implicit --all selection. Naming it
    # directly still runs it.
    tier="manual", manual_reason='Requires an explicitly provisioned AWS secret and a selected echo endpoint.',
    rerun_environment=(
        "SPACE_SMOKE_SECRET_ARN",
        "SPACE_SMOKE_ECHO_URL",
        "SPACE_SMOKE_ECHO_FORMAT",
        "SPACE_SMOKE_SECRET_HEADER_SHA256",
    ),
)
def aws_secret_network_credential_injection(
    test: smoke.CommandSandboxFixture,
) -> None:
    """A flat AWS reference injects its secret only after a policy binds it."""

    secret_arn = _required_environment("SPACE_SMOKE_SECRET_ARN")
    echo_url = _required_environment("SPACE_SMOKE_ECHO_URL")
    expected_digest = _required_environment("SPACE_SMOKE_SECRET_HEADER_SHA256")
    if not re.fullmatch(
        r"arn:aws:secretsmanager:[a-z0-9-]+:[0-9]{12}:secret:[^\s]+", secret_arn
    ):
        raise SmokeConfigurationError(
            "SPACE_SMOKE_SECRET_ARN must be a full Secrets Manager ARN"
        )
    if not re.fullmatch(r"[0-9a-f]{64}", expected_digest):
        raise SmokeConfigurationError(
            "SPACE_SMOKE_SECRET_HEADER_SHA256 must be lowercase SHA-256 hex"
        )
    echo = urlsplit(echo_url)
    if (
        echo.scheme != "https"
        or echo.hostname is None
        or echo.port not in (None, 443)
        or echo.username is not None
        or echo.password is not None
        or echo.fragment
    ):
        raise SmokeConfigurationError(
            "SPACE_SMOKE_ECHO_URL must be a credential-free HTTPS URL on port 443"
        )
    guest_digest = hashlib.sha256(b"smoke-guest-placeholder").hexdigest()
    if expected_digest == guest_digest:
        raise SmokeConfigurationError(
            "the injected digest must differ from the guest-provided header digest"
        )

    sandbox = cast(_CredentialInjectionSandbox, test.sandbox)
    baseline_policy: dict[str, object] = {
        "version": "v1",
        "egress": {
            "default_action": "deny",
            # The guest uses these static resolvers to learn the allowed host's IP.
            "allow": [
                {"ip": "8.8.8.8"},
                {"ip": "1.1.1.1"},
                {"domain": echo.hostname},
            ],
        },
        "tls_interception": "on",
    }
    command = _header_digest_command(
        echo_url, os.environ.get("SPACE_SMOKE_ECHO_FORMAT", "digest")
    )

    with test.phase("install credential digest script"):
        _install_header_digest_script(test)

    with test.phase("apply baseline network policy"):
        sandbox.set_network_policy(baseline_policy)
    test.wait_until_command_matches(
        command,
        stdout=guest_digest,
        timeout=90,
        failure_message="origin did not observe the guest-provided header",
    )

    with test.phase("register AWS secret reference"):
        reference_id = sandbox.register_credential_reference(secret_arn)
    test.record("credential_reference_id", reference_id)
    try:
        injected_policy = dict(baseline_policy)
        injected_policy["network_credential_rules"] = [
            {
                "rule_id": "aws-secret-smoke",
                "destination": {
                    "exact_host": echo.hostname,
                    "path_prefix": echo.path or "/",
                },
                "credential_header_bindings": [
                    {
                        "header_name": "authorization",
                        "credential_reference_id": reference_id,
                    }
                ],
                "supported_http_methods": ["GET"],
            }
        ]
        with test.phase("bind reference in sandbox network policy"):
            sandbox.set_network_policy(injected_policy)
        test.wait_until_command_matches(
            command,
            stdout=expected_digest,
            timeout=90,
            failure_message="origin did not observe the injected secret header",
        )
    finally:
        try:
            with test.phase("detach credential reference"):
                sandbox.set_network_policy(baseline_policy)
        except Exception:
            # A failed detach can leave the reference attached. Deleting this
            # run-owned sandbox removes the policy before reference retirement.
            with test.phase("delete sandbox after failed detach"):
                test.delete_sandbox()
            raise
        finally:
            with test.phase("retire credential reference"):
                sandbox.retire_credential_reference(reference_id)

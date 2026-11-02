"""Focused operations and assertions for ECW network-policy contracts."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from space_smoke.capabilities import EcwCreateAttempt
from space_smoke.network_policy_dns import (
    verify_dns_transport as _verify_dns_transport,
)
from space_smoke.network_policy_domain_binding import (
    verify_domain_binding as _verify_domain_binding,
)
from space_smoke.network_policy_fastly import (
    verify_fastly_shared_edge_bypass as _verify_fastly_shared_edge_bypass,
)

if TYPE_CHECKING:
    from space_smoke.fixtures import Ecw


@dataclass(frozen=True)
class NetworkPolicyAssertions:
    """Reusable network-policy observations reached through an ECW sandbox."""

    _test: Ecw

    def verify_live_update(self) -> None:
        """Exercise replacement through management HTTP and real guest traffic."""
        from space_smoke.network_policy_update import verify_live_update

        verify_live_update(self._test)

    def verify_egress(
        self,
        *,
        reachable: Sequence[str] = (),
        blocked: Sequence[str] = (),
    ) -> None:
        """Probe reachable URLs in order before probing blocked URLs in order.

        The reachable-first sequence is part of the contract: domain-policy
        attribution can change after a denied hostname resolves to a shared IP.
        """

        checks = self._test._context.evidence.setdefault("egress_checks", [])
        assert isinstance(checks, list)
        for url in reachable:
            observed = self._test.wait_for_network_access(url)
            checks.append({"expectation": "reachable", **observed.evidence()})
            self._test.expect_network_reachable(observed)
        for url in blocked:
            observed = self._test.observe_https_access(
                url,
                after_seconds=5,
                samples=3,
                every_seconds=1,
            )
            checks.append({"expectation": "blocked", **observed.evidence()})
            self._test.expect_https_blocked(observed)

    def verify_dns_transport(
        self,
        protocol: str,
        *,
        accepted: Sequence[tuple[str, str]] = (),
        refused: Sequence[tuple[str, str]] = (),
        blocked: Sequence[tuple[str, str]] = (),
    ) -> None:
        """Probe real DNS wire traffic through TCP or UDP."""

        _verify_dns_transport(
            self._test,
            protocol,
            accepted=accepted,
            refused=refused,
            blocked=blocked,
        )

    def verify_domain_binding(self) -> None:
        """Verify host identity on DNS-authorized TLS and plaintext HTTP."""

        _verify_domain_binding(self._test)

    def verify_fastly_shared_edge_bypass(self) -> None:
        """Reject alternate authorities and routing metadata on a Fastly IP."""

        _verify_fastly_shared_edge_bypass(self._test)

    def expect_create(
        self,
        observed: EcwCreateAttempt,
        *,
        status: int,
        body_contains: str | None = None,
    ) -> None:
        """Require one exact create status and optional response detail."""

        self._test.verify(
            observed.status_code == status,
            "ECW sandbox create returned the wrong status",
            expected_create_status=status,
            observed_create_status=observed.status_code,
            create_response_body=observed.body[:1_000],
            created_sandbox_id=observed.sandbox_id,
        )
        if body_contains is not None:
            self._test.verify(
                body_contains in observed.body,
                "ECW sandbox create response omitted the expected detail",
                expected_create_body_fragment=body_contains,
                create_response_body=observed.body[:1_000],
            )

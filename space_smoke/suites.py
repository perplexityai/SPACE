"""Versioned suite manifests: fixed scenario lists whose failure rates compare.

A rate is comparable only between runs of the same suite version. A released
version's scenario list never changes; adding, removing or replacing a
scenario is a new version, so a rate cannot improve by dropping coverage.
`space_smoke run --suite NAME` runs the latest version and `NAME@VERSION` a
named one; the run refuses to start unless every listed scenario will run.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Suite:
    name: str
    version: int
    scenarios: tuple[str, ...]

    @property
    def label(self) -> str:
        return f"{self.name}@{self.version}"


SUITES: tuple[Suite, ...] = (
    # Lifecycle faults on the native surface of a Kind bench: reconnect, data
    # plane and API restart, leader failover, controller outage and forced
    # drain of a stopped sandbox.
    Suite(
        "lifecycle-faults",
        1,
        (
            "native_contract.session_reconnect_after_idle_suspend",
            "native_recovery.attached_stream_drains_through_data_plane_restart",
            "native_recovery.command_survives_data_plane_restart",
            "native_recovery.queued_create_survives_api_restart",
            "native_recovery.inline_wake_survives_api_pod_kill",
            "native_recovery.inline_load_survives_leader_failover",
            "native_recovery.stale_queued_work_expires_after_controller_outage",
            "native.forced_drain_re_drains_re_placed_stopped_sandbox",
        ),
    ),
)


def resolve_suite(value: str) -> Suite:
    """The suite `NAME` (its latest version) or `NAME@VERSION` names."""

    name, _, version = value.partition("@")
    versions = [suite for suite in SUITES if suite.name == name]
    if not versions:
        known = ", ".join(sorted({suite.name for suite in SUITES}))
        raise ValueError(f"no suite is named {name!r}; suites: {known}")
    if not version:
        return max(versions, key=lambda suite: suite.version)
    for suite in versions:
        if str(suite.version) == version:
            return suite
    released = ", ".join(str(suite.version) for suite in versions)
    raise ValueError(f"suite {name!r} has no version {version!r}; versions: {released}")

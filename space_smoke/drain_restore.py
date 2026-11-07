"""The drain restore operation's test hold, which a v2 drain scenario scopes to itself.

`SPACE_DRAIN_RESTORE_TEST_HOLD_SECS` holds each claimed drain restore that
long before its node work, so a user call can meet the claim. The controller
reads it per claim, from its process environment, so a scenario overrides
the controller Deployment's env for that key alone (closed key list, bounded
integer seconds) under the controller-mutation Lease, awaits the rollout,
checks the literal on every running controller pod, and puts the captured
standing literal back (`space_smoke.drain_v2.DrainV2Assertions.v2_controller`).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping

DRAIN_RESTORE_HOLD_ENV = "SPACE_DRAIN_RESTORE_TEST_HOLD_SECS"

# The only controller env this scenario may touch. The hook parses a u64 of
# seconds; the bound keeps an override from wedging restores for longer than
# a scenario phase.
DRAIN_RESTORE_HOLD_ENV_KEYS: tuple[str, ...] = (DRAIN_RESTORE_HOLD_ENV,)
MAX_HOLD_SECONDS = 120


def validate_drain_restore_hold_keys(keys: Iterable[str]) -> None:
    unknown = sorted(set(keys) - set(DRAIN_RESTORE_HOLD_ENV_KEYS))
    if unknown:
        raise ValueError(f"refusing non-drain-restore-hold controller env keys: {unknown}")


def validate_drain_restore_hold_overrides(overrides: Mapping[str, str]) -> None:
    """Refuse any key but the hold, and any value but whole seconds in `1..MAX_HOLD_SECONDS`."""

    validate_drain_restore_hold_keys(overrides)
    for key, value in overrides.items():
        if not value.isascii() or not value.isdigit() or not 1 <= int(value) <= MAX_HOLD_SECONDS:
            raise ValueError(f"refusing drain-restore hold value {value!r} for {key}")

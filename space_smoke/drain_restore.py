"""The drain restore operation's controller env, which a v2 drain scenario scopes to itself.

`SPACE_DRAIN_RESTORE_TEST_HOLD_SECS` holds each claimed drain restore that
long before its node work, so a user call can meet the claim.
`SPACE_PUBLIC_OPERATION_DISPATCHER_DRAIN_RESTORE_SLOTS` is how many drain
restores the dispatcher runs at once. The controller reads both from its
process environment, so a scenario overrides the controller Deployment's env
for those keys alone (closed key list, bounded integers) under the
controller-mutation Lease, awaits the rollout, checks the literals on every
running controller pod, and puts the captured standing literals back
(`space_smoke.drain_v2.DrainV2Assertions.v2_controller`).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping

DRAIN_RESTORE_HOLD_ENV = "SPACE_DRAIN_RESTORE_TEST_HOLD_SECS"
DRAIN_RESTORE_SLOTS_ENV = "SPACE_PUBLIC_OPERATION_DISPATCHER_DRAIN_RESTORE_SLOTS"

# The only controller env this scenario may touch. The hook parses a u64 of
# seconds; the bound keeps an override from wedging restores for longer than
# a scenario phase. The slots stay within the Kind chart's execution
# concurrency.
DRAIN_RESTORE_HOLD_ENV_KEYS: tuple[str, ...] = (DRAIN_RESTORE_HOLD_ENV, DRAIN_RESTORE_SLOTS_ENV)
MAX_HOLD_SECONDS = 120
MAX_DRAIN_RESTORE_SLOTS = 16


def validate_drain_restore_hold_keys(keys: Iterable[str]) -> None:
    unknown = sorted(set(keys) - set(DRAIN_RESTORE_HOLD_ENV_KEYS))
    if unknown:
        raise ValueError(f"refusing non-drain-restore controller env keys: {unknown}")


def validate_drain_restore_hold_overrides(overrides: Mapping[str, str]) -> None:
    """Refuse any key but the hold and the slots, and any value but whole
    seconds in `1..MAX_HOLD_SECONDS` or slots in `1..MAX_DRAIN_RESTORE_SLOTS`."""

    validate_drain_restore_hold_keys(overrides)
    for key, value in overrides.items():
        ceiling = MAX_HOLD_SECONDS if key == DRAIN_RESTORE_HOLD_ENV else MAX_DRAIN_RESTORE_SLOTS
        if not value.isascii() or not value.isdigit() or not 1 <= int(value) <= ceiling:
            raise ValueError(f"refusing drain-restore value {value!r} for {key}")

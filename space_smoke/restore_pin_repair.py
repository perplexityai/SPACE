"""The restore-pin admin route and the cluster-controller's restore-pin repair, proven live on Kind.

The admin route `/admin/api/v1/sandboxes/{spec}/restore-pin`
(crates/l40_api/api/src/routes/admin/sandboxes/restore_pin.rs) reads a
sandbox's restore state and lets an operator pin an unpinned suspended
sandbox to one of its captures. The repair loop `restore_pin_repair`
(crates/l40_api/cluster/controller/server/src/restore_pin_repair/) pins such
a row to the artifact its node reports holding. It ships off and reads its
knobs once at startup.

A suspend on this build pins its capture, so only an older build leaves a
suspended row unpinned, and no public path produces one. The scenario
clears the pin through one named control operation, `clear_restore_pin`,
whose statement matches only a `suspended` row pinned to exactly the
artifact the route reported. The repair's knobs are scoped to the scenario
under the controller-mutation Lease the retention scenarios share, with a
closed key list and value vocabulary, and the captured standing values go
back before the body ends.

The observables are the admin route's answers and the sandbox's own data
after resume. The controller's log is attached to a failure as evidence and
never judged.
"""

from __future__ import annotations

import re
import time
from collections.abc import Iterable, Iterator, Mapping
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from space_smoke.capabilities import (
    CallAnswer,
    RestorePinControl,
    RestorePinRepairConfig,
)
from space_smoke.runtime import Compensation, ResourceScope, SmokeConfigurationError

if TYPE_CHECKING:
    from space_smoke.fixtures import KindEcw

REPAIR_ENABLED_ENV = "SPACE_RESTORE_PIN_REPAIR_ENABLED"
REPAIR_TICK_ENV = "SPACE_RESTORE_PIN_REPAIR_TICK_SECS"
REPAIR_BATCH_ENV = "SPACE_RESTORE_PIN_REPAIR_BATCH"

# The only controller env this scenario may touch (`ReconcilerConfig::from_environment`
# in restore_pin_repair/mod.rs reads exactly these), and the closed
# vocabulary its overrides come from.
REPAIR_ENV_KEYS: tuple[str, ...] = (REPAIR_ENABLED_ENV, REPAIR_TICK_ENV, REPAIR_BATCH_ENV)
_BOOLEAN_KEYS = frozenset({REPAIR_ENABLED_ENV})
_BOOLEAN_VALUES = frozenset({"true", "false"})
_COUNT_VALUE = re.compile(r"[1-9][0-9]{0,5}")

# `DEFAULT_TICK`, `DEFAULT_BATCH` and `MAX_BATCH` in restore_pin_repair/mod.rs.
DEFAULT_TICK_SECONDS = 30
DEFAULT_BATCH = 100
MAX_BATCH = 1_000
_U64_MAX = (1 << 64) - 1
_U32_MAX = (1 << 32) - 1
_RUST_UNSIGNED = re.compile(r"\+?[0-9]+")

# The tick the scenario stages: short enough that a few ticks fit the
# scenario, long enough that the loop isn't a busy poll on a slow bench.
SMOKE_TICK_SECONDS = 5

# Every repair-loop record carries the loop's name; the adapter pre-filters
# the controller's log on it for failure evidence.
REPAIR_LOG_NEEDLE = "restore_pin_repair"

# The control operation's one write: clear the pin of a `suspended` row
# pinned to exactly `artifact_id`, all three columns together as the
# identity check constraint requires. It leaves `lifecycle`, so
# `lifecycle_changed_at` (the route's token) doesn't move.
CLEAR_RESTORE_PIN_SQL = """\
UPDATE sandboxes
   SET current_local_artifact_id = NULL,
       current_local_artifact_kind = NULL,
       current_local_artifact_origin = NULL
 WHERE id = '{sandbox_id}'
   AND lifecycle = 'suspended'
   AND current_local_artifact_id = '{artifact_id}'
RETURNING id;
"""

def validate_repair_keys(keys: Iterable[str]) -> None:
    unknown = sorted(set(keys) - set(REPAIR_ENV_KEYS))
    if unknown:
        raise ValueError(f"refusing non-repair controller env keys: {unknown}")


def validate_repair_overrides(overrides: Mapping[str, str]) -> None:
    """Refuse any key off the repair list and any value off the smoke vocabulary."""

    validate_repair_keys(overrides)
    for key, value in overrides.items():
        valid = (
            value in _BOOLEAN_VALUES
            if key in _BOOLEAN_KEYS
            else isinstance(value, str) and _COUNT_VALUE.fullmatch(value) is not None
        )
        if not valid:
            raise ValueError(f"refusing restore-pin repair env value {value!r} for {key}")


def _env_flag(raw: str | None, default: bool) -> bool:
    """`pplx_space_env_vars::env_flag`: trimmed, case-folded, else the default."""

    if raw is None:
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on", "enabled"}:
        return True
    if normalized in {"0", "false", "no", "off", "disabled"}:
        return False
    return default


def _duration_secs(raw: str | None, default: int) -> int:
    """`EnvConfig::duration_secs`: a positive `u64` parse, else the default."""

    if raw is None or _RUST_UNSIGNED.fullmatch(raw) is None:
        return default
    value = int(raw)
    return value if 0 < value <= _U64_MAX else default


def _bounded_u32(raw: str | None, default: int, ceiling: int) -> int:
    """`EnvConfig::bounded_u32`: a `u32` clamped to `ceiling`; zero or a non-number keeps the default."""

    if raw is None or _RUST_UNSIGNED.fullmatch(raw) is None:
        return default
    # A digit string past u32 is `PosOverflow`, which `parse_clamped` clamps.
    value = min(int(raw), _U32_MAX + 1)
    clamped = min(value, ceiling)
    return clamped if clamped > 0 else default


def parse_repair_config(env: Mapping[str, str | None]) -> RestorePinRepairConfig:
    """Parse as `ReconcilerConfig::from_environment` does, knob by knob."""

    return RestorePinRepairConfig(
        enabled=_env_flag(env.get(REPAIR_ENABLED_ENV), False),
        tick_seconds=_duration_secs(env.get(REPAIR_TICK_ENV), DEFAULT_TICK_SECONDS),
        batch=_bounded_u32(env.get(REPAIR_BATCH_ENV), DEFAULT_BATCH, MAX_BATCH),
    )


_RFC3339 = re.compile(
    r"(?P<base>\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?P<fraction>\.\d+)?(?P<offset>Z|[+-]\d\d:\d\d)"
)


def shifted_token(token: str, *, seconds: int) -> str:
    """`token`, an RFC 3339 timestamp, moved by whole `seconds`.

    The fraction and offset are kept verbatim: Python's datetime holds
    microseconds, and the route compares the token to the nanosecond.
    """

    match = _RFC3339.fullmatch(token)
    if match is None:
        raise ValueError(f"not an RFC 3339 timestamp: {token!r}")
    base = datetime.strptime(match["base"], "%Y-%m-%dT%H:%M:%S") + timedelta(seconds=seconds)
    return base.strftime("%Y-%m-%dT%H:%M:%S") + (match["fraction"] or "") + match["offset"]


def capture_ids(state: Mapping[str, Any]) -> list[str]:
    captures = state.get("captures")
    if not isinstance(captures, list):
        return []
    return [
        str(capture.get("local_artifact_id"))
        for capture in captures
        if isinstance(capture, dict) and capture.get("local_artifact_id")
    ]


class RestorePinAssertions:
    """Drive the restore-pin route and a scoped repair loop for the fixture sandbox."""

    def __init__(self, fixture: KindEcw, control: RestorePinControl, scope: ResourceScope) -> None:
        self._fixture = fixture
        self._control = control
        self._scope = scope
        self._release_lease: Compensation | None = None
        self._restore_env: Compensation | None = None
        self._standing: dict[str, str | None] | None = None

    @property
    def _sandbox_id(self) -> str:
        return self._fixture.sandbox.sandbox_id

    # --- the route ---------------------------------------------------------------

    def read(self) -> dict[str, Any]:
        """`GET .../restore-pin`; anything but a 200 fails the scenario."""

        answer = self._control.restore_pin_answer(self._sandbox_id)
        self._fixture.verify(
            answer.status == 200,
            "the admin restore-pin read did not answer 200",
            status=answer.status,
            body=answer.body,
        )
        return answer.body

    def wait_for_suspend_pin(self, *, timeout: float = 180) -> str:
        """Poll the read until the sandbox is `suspended` and pinned; the pin, P."""

        deadline = time.monotonic() + timeout
        while True:
            state = self.read()
            pinned = state.get("pinned_artifact_id")
            if state.get("lifecycle") == "suspended" and isinstance(pinned, str) and pinned:
                self._fixture.record("suspend_pin_state", state)
                self._fixture.verify(
                    pinned in capture_ids(state),
                    "the suspend's pinned capture is not among the captures the route lists",
                    state=state,
                )
                return pinned
            self._fixture.verify(
                time.monotonic() < deadline,
                "the sandbox did not read as suspended and pinned after the suspend",
                state=state,
            )
            self._fixture.wait(2)

    def expect_pin_refused(self, artifact: str, token: str, *, why: str) -> None:
        """A pin of `artifact` with `token` answers 409."""

        answer = self._pin(artifact, token)
        self._fixture.record(f"refused_pin_{why}", {"status": answer.status, "body": answer.body})
        self._fixture.verify(
            answer.status == 409,
            f"the restore-pin route did not refuse a pin with 409 ({why})",
            status=answer.status,
            body=answer.body,
            local_artifact_id=artifact,
            lifecycle_changed_at=token,
        )

    def pin(self, artifact: str, token: str) -> dict[str, Any]:
        """A pin of `artifact` with `token` answers 200 with the sandbox pinned to it."""

        answer = self._pin(artifact, token)
        self._fixture.verify(
            answer.status == 200
            and answer.body.get("pinned_artifact_id") == artifact
            and answer.body.get("lifecycle") == "suspended",
            "the restore-pin route did not pin the unpinned suspended sandbox to its capture",
            status=answer.status,
            body=answer.body,
            local_artifact_id=artifact,
        )
        return answer.body

    def expect_pinned(self, artifact: str) -> dict[str, Any]:
        state = self.read()
        self._fixture.verify(
            state.get("pinned_artifact_id") == artifact and state.get("lifecycle") == "suspended",
            "the restore-pin read does not show the suspended sandbox pinned to the capture",
            expected_artifact=artifact,
            state=state,
        )
        return state

    def _pin(self, artifact: str, token: str) -> CallAnswer:
        return self._control.pin_restore_source_answer(
            self._sandbox_id, {"local_artifact_id": artifact, "lifecycle_changed_at": token}
        )

    # --- the named write -----------------------------------------------------------

    def clear_pin(self, artifact: str) -> dict[str, Any]:
        """Clear the pin of the suspended row pinned to `artifact`; the unpinned read."""

        cleared = self._control.clear_restore_pin(self._sandbox_id, artifact)
        self._fixture.verify(
            cleared,
            "clearing the restore pin matched no suspended row pinned to the capture",
            local_artifact_id=artifact,
        )
        state = self.read()
        self._fixture.verify(
            state.get("lifecycle") == "suspended"
            and state.get("pinned_artifact_id") is None
            and artifact in capture_ids(state),
            "after the pin was cleared, the read does not show an unpinned suspended "
            "sandbox that still lists the capture",
            local_artifact_id=artifact,
            state=state,
        )
        return state

    # --- the repair loop's config ------------------------------------------------

    def expect_repair_off(self) -> None:
        """The running controller pods all run the repair off, under the Lease.

        The Lease is taken first, so a cooperating smoke can't stage the loop
        on between this read and the route's pin. Anything else is a bench
        configuration error: a live loop could pin the row before the route.
        """

        self._hold_lease()
        envs = self._control.running_restore_pin_repair_envs()
        configs = {parse_repair_config(env) for env in envs}
        self._fixture.record("running_repair_envs", envs)
        if len(configs) != 1 or next(iter(configs)).enabled:
            raise SmokeConfigurationError(
                "the running cluster-controller does not run the restore-pin repair off "
                f"on every pod (running envs: {envs}); bring the bench up from this "
                "checkout's chart, which leaves it unset"
            )

    @contextmanager
    def scoped_repair(self) -> Iterator[None]:
        """Put the standing env back as the body unwinds, success or failure alike.

        The scope compensations stay armed as the failure backstop, but the
        cleanup window is too short to own a controller rollout. When the body
        failed, its error propagates: a restore failure is attached as a note.
        """

        try:
            yield
        except BaseException as body_error:
            try:
                self.restore_standing()
            except Exception as restore_error:  # noqa: BLE001 - attached to the body's error below
                self._fixture.record("repair_env_restore_failure", repr(restore_error)[:1_000])
                body_error.add_note(
                    f"restoring the standing restore-pin repair env also failed: {restore_error!r}; "
                    "the scope's restore compensation stays armed"
                )
            raise
        self.restore_standing()

    def stage_repair(self, *, tick_seconds: int = SMOKE_TICK_SECONDS) -> RestorePinRepairConfig:
        """Run the repair on, at `tick_seconds`, on every running controller pod."""

        if not 1 <= tick_seconds <= 60:
            raise ValueError("refusing a restore-pin repair tick outside 1..60 s")
        self._hold_lease()
        standing = self._control.restore_pin_repair_env()
        self._standing = standing
        self._fixture.record("repair_env_before", dict(standing))
        # Armed before the override: a set that half-applies is still put back.
        self._restore_env = self._scope.defer(
            "restore the standing restore-pin repair env",
            lambda: self._control.restore_restore_pin_repair_env(standing),
            critical=True,
        )
        overrides = {REPAIR_ENABLED_ENV: "true", REPAIR_TICK_ENV: str(tick_seconds)}
        self._control.set_restore_pin_repair_env(overrides)
        live = self._control.restore_pin_repair_env()
        expected = {**standing, **overrides}
        self._fixture.verify(
            live == expected,
            "the rolled cluster-controller Deployment does not carry the scenario's repair knobs",
            expected=expected,
            live=live,
        )
        # The set awaits the rollout and the old pods' exit, so every running
        # pod started with these knobs.
        envs = self._control.running_restore_pin_repair_envs()
        configs = {parse_repair_config(env) for env in envs}
        wanted = RestorePinRepairConfig(
            enabled=True, tick_seconds=tick_seconds, batch=parse_repair_config(live).batch
        )
        self._fixture.verify(
            configs == {wanted},
            "the running cluster-controller pods did not start with the scenario's repair knobs",
            wanted=wanted.__dict__,
            running_envs=envs,
        )
        self._fixture.record("repair_config", wanted.__dict__)
        return wanted

    def restore_standing(self) -> None:
        """Put the captured env back now and check it took; then release the Lease.

        The restore compensation is disarmed only once the restore is
        verified, and the Lease stays held while it is armed.
        """

        if self._restore_env is not None:
            standing = self._standing
            if standing is None:
                raise RuntimeError("a restore compensation without a captured standing env")
            self._control.restore_restore_pin_repair_env(standing)
            after = self._control.restore_pin_repair_env()
            self._fixture.verify(
                after == standing,
                "the controller's restore-pin repair env did not come back to its standing values",
                standing=standing,
                after=after,
            )
            self._restore_env.disarm()
            self._restore_env = None
        if self._release_lease is not None:
            release, self._release_lease = self._release_lease, None
            release.release()

    def _hold_lease(self) -> None:
        if self._release_lease is None:
            release = self._control.acquire_controller_mutation()
            self._release_lease = self._scope.defer(
                "release the controller mutation lease", release, critical=True
            )

    # --- the repair -----------------------------------------------------------------

    def wait_for_repair(self, artifact: str, config: RestorePinRepairConfig) -> None:
        """The repair pins the row to `artifact` within a few ticks."""

        # Leader handoff after the rollout and the loop's first, immediate
        # tick being consumed before the first real one both fit the margin.
        budget = config.tick_seconds * 6 + 90
        started = time.monotonic()
        deadline = started + budget
        while True:
            state = self.read()
            if state.get("pinned_artifact_id") == artifact:
                self._fixture.record("repaired_after_seconds", round(time.monotonic() - started, 1))
                self._fixture.record("repaired_state", state)
                return
            self._fixture.verify(
                state.get("pinned_artifact_id") is None,
                "the restore-pin repair loop pinned the sandbox to a capture other than "
                "the one its node holds",
                expected_artifact=artifact,
                state=state,
                failure_evidence=self._repair_evidence,
            )
            self._fixture.verify(
                time.monotonic() < deadline,
                f"the restore-pin repair loop did not pin the unpinned suspended sandbox "
                f"within its {budget} s budget ({config.tick_seconds} s tick)",
                expected_artifact=artifact,
                budget_seconds=budget,
                state=state,
                failure_evidence=self._repair_evidence,
            )
            self._fixture.wait(2)

    def _repair_evidence(self) -> Mapping[str, object]:
        return {"controller_log": self._control.restore_pin_repair_log_tail()}

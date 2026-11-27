"""Checks for native operations the API executes inline, on owned Kind.

With inline execution on, the API that admits a wake or a create claims and
runs it itself when nothing else is pending for its sandbox. Each scenario
here turns it on for its own run (`inline_execution`), so the rest of the
suite runs on the dispatcher, as deployments do by default. Every one stops
the cluster-controller before it acts, so the dispatcher can claim nothing: an
operation that reaches `running` meanwhile is one the API runs. Each then
judges the outcome a client sees: the operation's state and claim events, and
the sandbox's state.

An inline execution can end before a scenario acts on it: a fast runner
finishes a wake in well under a second. A scenario that needs one still
`running` (to kill its API pod, or to hold its permit) sets `hold`, the API's
`SPACE_NATIVE_INLINE_TEST_HOLD_SECS` test hook: each operation the API claims
inline waits with its claim renewed until a run-owned release file appears,
or the fail-safe hold deadline ends. The returned callback releases every
API replica without rolling it out.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager

from space_smoke.native_hardening import NativeTransportError
from space_smoke.native_recovery import (
    KindNativeHardeningFixture,
    NativeRecoveryControl,
    attach_after_recovery,
    expect_response,
    expect_success,
    stdout_bytes,
)
from space_smoke.runtime import SmokeConfigurationError

INLINE_ENABLED_ENV = "SPACE_NATIVE_INLINE_OPERATIONS_ENABLED"
INLINE_KINDS_ENV = "SPACE_NATIVE_INLINE_KINDS"
INLINE_MAX_CONCURRENT_ENV = "SPACE_NATIVE_INLINE_MAX_CONCURRENT"
INLINE_TEST_HOLD_ENV = "SPACE_NATIVE_INLINE_TEST_HOLD_SECS"
INLINE_TEST_RELEASE_ENV = "SPACE_NATIVE_INLINE_TEST_RELEASE_FILE"
TERMINAL = frozenset({"succeeded", "failed", "canceled", "expired"})
TERMINAL_EVENTS = frozenset({"operation.completed", "operation.failed", "operation.canceled", "operation.expired"})
# How long the API may take to answer through its local forward once its
# replacement pods are ready.
API_RECOVERY_TIMEOUT = 45


@contextmanager
def inline_execution(
    fixture: KindNativeHardeningFixture,
    control: NativeRecoveryControl,
    kinds: Iterable[str],
    *,
    permits: int | None = None,
    hold: int | None = None,
) -> Iterator[Callable[[], None]]:
    """Run with the API executing `kinds` inline, with each API pod holding
    `permits` inline permits when given, and with each inline execution held
    `running` until released, bounded by `hold` seconds when given.

    Written under the public API mutation Lease, which records the standing
    values first so a later run repairs them if this one dies, and restored
    on exit. Enter it before `control.owned_faults()`: the API rolls out with
    the new values, and the faults' restore refuses an API whose spec changed.
    """

    harness = control.harness
    overrides = {INLINE_ENABLED_ENV: "true", INLINE_KINDS_ENV: ",".join(kinds)}
    if permits is not None:
        overrides[INLINE_MAX_CONCURRENT_ENV] = str(permits)
    release_file = None
    if hold is not None:
        overrides[INLINE_TEST_HOLD_ENV] = str(hold)
        release_file = f"/tmp/space-inline-{uuid.uuid4().hex}"
        overrides[INLINE_TEST_RELEASE_ENV] = release_file
    release = harness.acquire_public_api_mutation()
    released = fixture.context.scope.defer("release the public API mutation lease", release)
    standing = harness.public_api_env(tuple(overrides))
    fixture.context.record("inline_env_before", dict(standing))

    hold_released = False

    def release_hold() -> None:
        nonlocal hold_released
        if release_file is None or hold_released:
            return
        if "space-api" not in control._captures:
            raise SmokeConfigurationError("inline hold release requires captured API ownership")
        control.release_inline_hold(release_file)
        hold_released = True

    def restore() -> None:
        # Restoring the env replaces the pods, removing their run-owned files
        # and canceling any held executions if the scenario failed before release.
        harness.restore_public_api_env(standing)
        harness.wait_public_api_ready(timeout=240)

    restored = fixture.context.scope.defer("restore the API's inline switches", restore, critical=True)
    with fixture.context.phase(f"run {overrides[INLINE_KINDS_ENV]} inline on the API"):
        harness.set_public_api_env(overrides)
        harness.wait_public_api_ready(timeout=240)
    primary = None
    try:
        yield release_hold
    except BaseException as error:
        primary = error
        raise
    finally:
        try:
            with fixture.context.phase("restore the API's inline switches"):
                restore()
                restored.disarm()
            release()
            released.disarm()
        except Exception as error:
            if primary is None:
                raise
            primary.add_note(f"Inline configuration restoration also failed: {error}")


def terminal_events(fixture: KindNativeHardeningFixture, operation_id: str) -> int:
    """How many terminal events the operation recorded: one once it is done."""

    page = expect_response(
        fixture, fixture.request("GET", f"/v1/operations/{operation_id}/events?limit=1000"), 200
    )
    return sum(1 for event in page.get("data", []) if event.get("event_type") in TERMINAL_EVENTS)


def operation(fixture: KindNativeHardeningFixture, operation_id: str) -> dict:
    return expect_response(fixture, fixture.request("GET", f"/v1/operations/{operation_id}"), 200)


def operation_after_api_loss(fixture: KindNativeHardeningFixture, operation_id: str) -> dict:
    """The operation, read once the API answers again after its pods were
    replaced: until the local forward reaches a new pod, a request can be
    reset or answered 502-504.
    """

    deadline = time.monotonic() + API_RECOVERY_TIMEOUT
    while True:
        try:
            response = fixture.request("GET", f"/v1/operations/{operation_id}")
        except NativeTransportError:
            if time.monotonic() >= deadline:
                raise
        else:
            if response.status not in {502, 503, 504} or time.monotonic() >= deadline:
                return expect_response(fixture, response, 200)
        time.sleep(0.5)


def claims(fixture: KindNativeHardeningFixture, operation_id: str) -> int:
    """How many times the operation was claimed: once per attempt's executor."""

    page = expect_response(
        fixture, fixture.request("GET", f"/v1/operations/{operation_id}/events?limit=1000"), 200
    )
    return sum(1 for event in page.get("data", []) if event.get("event_type") == "operation.claimed")


def wait_state(
    fixture: KindNativeHardeningFixture, operation_id: str, states: Iterable[str], *, timeout: float
) -> str:
    """The first of `states` (or a terminal state) the operation reaches; its state at the deadline."""

    wanted = set(states) | TERMINAL
    deadline = time.monotonic() + timeout
    while True:
        state = operation(fixture, operation_id)["state"]
        if state in wanted or time.monotonic() >= deadline:
            return state
        time.sleep(0.2)


def lifecycle(fixture: KindNativeHardeningFixture, sandbox_id: str, action: str) -> str:
    """`POST /v1/sandboxes/{sandbox_id}:{action}`: the accepted operation."""

    accepted = expect_response(
        fixture,
        fixture.request("POST", f"/v1/sandboxes/{sandbox_id}:{action}", idempotency_key=fixture.key(action)),
        202,
    )
    return accepted["operation_id"]


def queue_suspend_then_wake(
    fixture: KindNativeHardeningFixture, sandbox_id: str
) -> tuple[str, str]:
    """With dispatch stopped and inline permits free, require request ordering.

    Call before filling inline permits: otherwise permit exhaustion, rather
    than the earlier suspend, could explain why the wake stayed queued.
    """

    with fixture.context.phase("queue a suspend, then a wake, with no dispatcher running"):
        suspend = lifecycle(fixture, sandbox_id, "suspend")
        wake = lifecycle(fixture, sandbox_id, "wake")
        # An inline claim is committed before its admission answers.
        states = {
            "suspend": operation(fixture, suspend)["state"],
            "wake": operation(fixture, wake)["state"],
        }
        fixture.context.verify(
            states == {"suspend": "queued", "wake": "queued"},
            "a wake behind a queued suspend must stay queued",
            inline_ordering_states=states,
        )
    return suspend, wake


def expect_ordered_wake(
    fixture: KindNativeHardeningFixture,
    sandbox_id: str,
    suspend: str,
    wake: str,
) -> None:
    """Both queued operations succeed, and their order leaves an executing guest."""

    with fixture.context.phase("await the ordered suspend and wake, then execute"):
        fixture.wait_operation(suspend, timeout=240)
        fixture.wait_operation(wake, timeout=240)
        fixture.wait_sandbox(sandbox_id, timeout=120)
        command = expect_response(
            fixture, fixture.command(sandbox_id, ["sh", "-c", "printf ordered"]), 201
        )
        events = attach_after_recovery(fixture, command)
        expect_success(fixture, events)
        fixture.context.verify(
            stdout_bytes(events) == b"ordered",
            "the wake must run after the suspend, leaving the sandbox running",
        )


def suspended_sandbox(fixture: KindNativeHardeningFixture) -> str:
    """A ready sandbox, suspended through the public API."""

    sandbox_id = fixture.create_ready(timeout=120)
    fixture.wait_operation(lifecycle(fixture, sandbox_id, "suspend"), timeout=180)
    fixture.wait_sandbox(sandbox_id, "suspended", timeout=120)
    return sandbox_id

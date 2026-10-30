"""Checks for native operations the API executes inline, on owned Kind.

With inline execution on, the API that admits a wake or a create claims and
runs it itself when nothing else is pending for its sandbox. Every scenario
here stops the cluster-controller first, so the dispatcher can claim nothing:
an operation that reaches `running` meanwhile is one the API runs. Each then
judges the outcome a client sees: the operation's state and claim events, and
the sandbox's state. They need the inline switches the Kind chart sets, and
are inconclusive where those are off.
"""

from __future__ import annotations

import time
from collections.abc import Iterable, Iterator
from contextlib import contextmanager

from space_smoke.native_recovery import KindNativeHardeningFixture, NativeRecoveryControl, expect_response
from space_smoke.runtime import InconclusiveSmoke

INLINE_ENABLED_ENV = "SPACE_NATIVE_INLINE_OPERATIONS_ENABLED"
INLINE_KINDS_ENV = "SPACE_NATIVE_INLINE_KINDS"
INLINE_MAX_CONCURRENT_ENV = "SPACE_NATIVE_INLINE_MAX_CONCURRENT"
# The spellings `pplx_space_env_vars::parse_env_flag` reads as on.
_TRUE = {"1", "true", "yes", "on", "enabled"}
TERMINAL = frozenset({"succeeded", "failed", "canceled", "expired"})


def require_inline(control: NativeRecoveryControl, *kinds: str) -> None:
    """Inconclusive unless the API runs `kinds` inline."""

    env = control.harness.public_api_env((INLINE_ENABLED_ENV, INLINE_KINDS_ENV, INLINE_MAX_CONCURRENT_ENV))
    control.context.record("inline_env", dict(env))
    enabled = (env[INLINE_ENABLED_ENV] or "").strip().lower() in _TRUE
    listed = {kind.strip() for kind in (env[INLINE_KINDS_ENV] or "").split(",")}
    permits = env[INLINE_MAX_CONCURRENT_ENV]
    if not enabled or not set(kinds) <= listed or permits is not None and permits.strip() == "0":
        raise InconclusiveSmoke(f"the API does not run {', '.join(kinds)} inline on this cluster")


@contextmanager
def inline_permits(
    fixture: KindNativeHardeningFixture, control: NativeRecoveryControl, permits: int
) -> Iterator[None]:
    """Run with each API pod holding `permits` inline permits; `0` turns inline
    execution off, so every operation is queued for the dispatcher.

    Written under the public API mutation Lease, which records the standing
    value first so a later run repairs it if this one dies, and restored on
    exit. Enter it before `control.owned_faults()`: the API rolls out with the
    new value, and the faults' restore refuses an API whose spec changed.
    """

    harness = control.harness
    release = harness.acquire_public_api_mutation()
    released = fixture.context.scope.defer("release the public API mutation lease", release)
    standing = harness.public_api_env((INLINE_MAX_CONCURRENT_ENV,))
    fixture.context.record("inline_permits_before", dict(standing))

    def restore() -> None:
        harness.restore_public_api_env(standing)
        harness.wait_public_api_ready(timeout=240)

    restored = fixture.context.scope.defer("restore the API's inline permits", restore, critical=True)
    with fixture.context.phase(f"give each API pod {permits} inline permits"):
        harness.set_public_api_env({INLINE_MAX_CONCURRENT_ENV: str(permits)})
        harness.wait_public_api_ready(timeout=240)
    yield
    with fixture.context.phase("restore the API's inline permits"):
        restore()
        restored.disarm()
    release()
    released.disarm()


def operation(fixture: KindNativeHardeningFixture, operation_id: str) -> dict:
    return expect_response(fixture, fixture.request("GET", f"/v1/operations/{operation_id}"), 200)


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


def suspended_sandbox(fixture: KindNativeHardeningFixture) -> str:
    """A ready sandbox, suspended through the public API."""

    sandbox_id = fixture.create_ready(timeout=120)
    fixture.wait_operation(lifecycle(fixture, sandbox_id, "suspend"), timeout=180)
    fixture.wait_sandbox(sandbox_id, "suspended", timeout=120)
    return sandbox_id

"""node-ctl's unanswered-template cleanup, proven live on a Kind worker.

ClusterApi answers a worker's template desired-state request only for the
templates the worker has a reason to hold. An org template whose last
sandbox left the worker is omitted from every answer, and node-ctl records
when it first was. After the wait, on a full sync, it asks about that
template again, together with one the same answer named, and, still omitted,
has template-manager delete it with `DeleteIdleTemplate`
(crates/libs/node/ctl/server/src/facets/template_desired_state.rs).

The wait is an hour and is read once at start, so the scenario scopes a
short one to itself: the worker node-ctl DaemonSet's literal for the test
hook is captured, overridden, and put back in a scenario phase, under the
node-ctl mutation Lease. It is not a chart value: on every scenario's bench
it would also delete templates other scenarios plant on a worker and the
cluster does not know.

The observables are what an operator reads on the worker:
`space-tmplmgr list`, the template's digest directories, and the Node's
template-build label and annotation. node-ctl's log is attached to a
failure as evidence and never judged.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from space_smoke.capabilities import UnansweredTemplateEvictionControl
from space_smoke.runtime import Compensation, ResourceScope

if TYPE_CHECKING:
    from space_smoke.fixtures import KindEcw

# node-ctl runs a full sync on every poll tick, the first at start; Kind
# renders the binary's default poll (`DEFAULT_NODE_CTL_POLL_INTERVAL`). A
# delete lands on the first full sync at least the wait after the first
# omitted answer, which can be one sync after the sandbox left, and a sync
# whose clock reads just under the wait defers it one more.
FULL_SYNC_SECONDS = 60
POLL_SECONDS = 5


@dataclass(frozen=True)
class LocalTemplate:
    """What a worker shows for one template while it holds it."""

    node_name: str
    template: str
    digests: tuple[str, ...]
    presence_keys: tuple[str, ...]


class UnansweredTemplateEvictionAssertions:
    """Shorten the wait for the scenario, then watch one worker's copy of one template."""

    def __init__(
        self, fixture: KindEcw, control: UnansweredTemplateEvictionControl, scope: ResourceScope
    ) -> None:
        self._fixture = fixture
        self._control = control
        self._scope = scope
        self._standing: str | None = None
        self._restore_env: Compensation | None = None
        self._release_lease: Compensation | None = None
        self._wait_seconds: int | None = None
        self._since_time = _since_now()

    @property
    def wait_seconds(self) -> int:
        if self._wait_seconds is None:
            raise RuntimeError("the wait has not been shortened")
        return self._wait_seconds

    # --- the scoped wait --------------------------------------------------------

    @contextmanager
    def scoped_wait(self) -> Iterator[None]:
        """Put the standing env back as the body unwinds, success or failure alike.

        The scope compensation stays armed as the backstop, but the cleanup
        window is too short to own a DaemonSet rollout, so the restore runs
        here. A failed body's error is the one that propagates; a restore
        failure is attached to it as a note.
        """

        try:
            yield
        except BaseException as body_error:
            try:
                self.restore_standing()
            except Exception as restore_error:  # noqa: BLE001 - attached to the body's error below
                self._fixture.record("evict_env_restore_failure", repr(restore_error)[:1_000])
                body_error.add_note(
                    f"restoring the standing node-ctl unanswered-template wait also failed: {restore_error!r}; "
                    "the scope's restore compensation stays armed"
                )
            raise
        self.restore_standing()

    def shorten_wait(self, *, seconds: int, workers: tuple[str, ...]) -> float:
        """Set the wait on the worker node-ctl DaemonSet; returns when its pods restarted with it.

        The return value is a monotonic time no earlier than every rolled
        node-ctl's first full sync.
        """

        release = self._control.acquire_node_ctl_mutation()
        self._release_lease = self._scope.defer(
            "release the node-ctl mutation lease", release, critical=True
        )
        standing = self._control.unanswered_template_evict_env()
        self._standing = standing
        self._fixture.record("evict_env_before", standing)
        # No chart sets the key, so a value here is one an earlier run left
        # behind; capturing it as the standing value would make it permanent.
        self._fixture.verify(
            standing is None,
            "the worker node-ctl DaemonSet already carries the unanswered-template wait; "
            "an earlier run likely left it behind, so unset it and rerun",
            standing=standing,
        )
        # Armed before the override: a set that half-applies is still put back.
        self._restore_env = self._scope.defer(
            "restore the standing node-ctl unanswered-template wait",
            lambda: self._control.restore_unanswered_template_evict_env(standing),
            critical=True,
        )
        self._control.set_unanswered_template_evict_after(seconds)
        rolled_at = time.monotonic()
        running = {node: self._control.running_unanswered_template_evict_env(node) for node in workers}
        self._fixture.verify(
            all(value == str(seconds) for value in running.values()),
            "the rolled worker node-ctl pods do not carry the scenario's wait",
            expected=str(seconds),
            running=running,
        )
        self._wait_seconds = seconds
        return rolled_at

    def restore_standing(self) -> None:
        """Put the captured literal back now and check it took; then release the Lease.

        The compensation is disarmed only once the restore is verified, and
        the Lease stays held until then (its release runs after the retry).
        """

        if self._restore_env is not None:
            self._control.restore_unanswered_template_evict_env(self._standing)
            after = self._control.unanswered_template_evict_env()
            self._fixture.verify(
                after == self._standing,
                "the worker node-ctl DaemonSet did not come back to its standing wait",
                standing=self._standing,
                after=after,
            )
            self._restore_env.disarm()
            self._restore_env = None
        if self._release_lease is not None:
            release, self._release_lease = self._release_lease, None
            release.release()

    # --- one worker's copy of the template --------------------------------------

    def other_worker(self, node_name: str) -> str:
        return self._control.other_sandbox_worker_node(node_name)

    def expect_held(self, node_name: str, template: str) -> LocalTemplate:
        """The worker lists the template, has its bytes, and advertises each build."""

        held, copy, seen = self._holding(node_name, template)
        self._fixture.verify(
            held,
            "the worker does not hold the template: listed, on disk and advertised on its Node",
            **seen,
        )
        return copy

    def expect_held_within(self, node_name: str, template: str, *, timeout: float) -> LocalTemplate:
        """`expect_held` once the worker has had `timeout` seconds to pull and advertise the template.

        node-ctl publishes a new build's Node label on its next inventory
        sync, so a template the worker just pulled may not be advertised yet.
        """

        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            held, copy, _ = self._holding(node_name, template)
            if held:
                return copy
            self._fixture.wait(POLL_SECONDS)
        return self.expect_held(node_name, template)

    def _holding(self, node_name: str, template: str) -> tuple[bool, LocalTemplate, dict[str, object]]:
        names = self._control.local_template_names(node_name)
        digests = self._control.template_digest_dirs(node_name, template)
        presence = self._control.node_template_presence(node_name)
        keys = tuple(sorted(key for key, value in presence.annotations.items() if value in digests))
        held = (
            template in names
            and bool(digests)
            and len(keys) == len(digests)
            and set(keys) <= presence.labels
        )
        seen: dict[str, object] = {
            "node": node_name,
            "template": template,
            "listed": template in names,
            "digests": list(digests),
            "presence_keys": list(keys),
            "labelled": sorted(set(keys) & presence.labels),
        }
        return held, LocalTemplate(node_name, template, digests, keys), seen

    def expect_held_until(self, copy: LocalTemplate, deadline: float) -> None:
        """The copy stays, unchanged, on every poll until `deadline` (monotonic)."""

        polls = 0
        while True:
            held = self.expect_held(copy.node_name, copy.template)
            polls += 1
            self._fixture.verify(
                held == copy,
                "the worker's copy of the template changed while its sandbox ran",
                before=asdict(copy),
                after=asdict(held),
                failure_evidence=lambda: self._evidence(copy.node_name),
            )
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                self._fixture.record("held_polls", polls)
                return
            self._fixture.wait(min(15, remaining))

    def expect_deleted_after_wait(self, copy: LocalTemplate, *, since: float) -> float:
        """The worker drops the copy no sooner than the wait after `since`, and within two more syncs.

        Returns the seconds from `since` to the first poll that found it gone.
        """

        wait = self.wait_seconds
        earliest = wait - POLL_SECONDS - 10
        deadline = since + wait + 2 * FULL_SYNC_SECONDS + 30
        while True:
            names = self._control.local_template_names(copy.node_name)
            digests = self._control.template_digest_dirs(copy.node_name, copy.template)
            presence = self._control.node_template_presence(copy.node_name)
            left = {
                "listed": copy.template in names,
                "digests": sorted(set(digests)),
                "labels": sorted(set(copy.presence_keys) & presence.labels),
                "annotations": sorted(set(copy.presence_keys) & set(presence.annotations)),
            }
            elapsed = time.monotonic() - since
            # The local copy is judged on its own: Node metadata can trail it,
            # and an early delete must not hide behind that lag.
            if not left["listed"] and not left["digests"]:
                self._fixture.verify(
                    elapsed >= earliest,
                    "the worker deleted its copy of the template before the wait had passed",
                    wait_seconds=wait,
                    deleted_after_seconds=round(elapsed, 1),
                    failure_evidence=lambda: self._evidence(copy.node_name),
                )
            if not any(left.values()):
                self._fixture.record("deleted_after_seconds", round(elapsed, 1))
                return elapsed
            self._fixture.verify(
                time.monotonic() < deadline,
                "the worker still holds the template the cluster stopped answering for "
                "after the wait and two full syncs",
                wait_seconds=wait,
                waited_seconds=round(elapsed, 1),
                still_present=left,
                failure_evidence=lambda: self._evidence(copy.node_name),
            )
            self._fixture.wait(POLL_SECONDS)

    def _evidence(self, node_name: str) -> dict[str, object]:
        return {"node_ctl_log": self._control.node_ctl_log(node_name, since_time=self._since_time)[-20_000:]}


def _since_now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

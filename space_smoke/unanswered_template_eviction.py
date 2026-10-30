"""node-ctl's unanswered-template cleanup, proven live on a Kind worker.

ClusterApi answers a worker's template desired-state request only for the
templates the worker has a reason to hold. An org template whose last
sandbox left the worker, which the worker still reports holding, comes back
in the answer's `released_templates` instead, and node-ctl records when it
first did. After the wait, on a full sync, node-ctl asks about that template
again and, still released, has template-manager delete it with
`DeleteIdleTemplate` (crates/libs/node/ctl/server/src/facets/
unanswered_templates.rs).

The wait is an hour and is read once at start, so the scenario scopes a
short one to itself: the worker node-ctl DaemonSet's literal for
`SPACE_NODECTL_UNANSWERED_TEMPLATE_EVICT_AFTER_SECS` is captured, overridden,
and put back in a scenario phase, under the node-ctl mutation Lease. It is
not a chart value: on every scenario's bench it would also delete the
dedicated templates other scenarios leave on a worker between sandboxes.

The scenario does not wait for node-ctl's poll. It runs the facet on
demand with `space-nodectl reconcile --facet template-desired-state`, which
runs it at full scope, the only scope that deletes, and reads the facet's
status to know when a run that started after the request has finished (see
`full_sync`). Every timing claim is then made against such a run's monotonic
bounds rather than against the poll period.

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
# run asked for on demand waits behind a poll already running, and node-ctl
# publishes its inventory (the Node's template-build metadata) on the
# outbound sync after either, so a poll period bounds both.
FULL_SYNC_SECONDS = 60
POLL_SECONDS = 5
TEMPLATE_DESIRED_STATE_FACET = "template-desired-state"


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

    def shorten_wait(self, *, seconds: int, workers: tuple[str, ...]) -> None:
        """Set the wait on the worker node-ctl DaemonSet; returns when its pods restarted with it."""

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
        running = {node: self._control.running_unanswered_template_evict_env(node) for node in workers}
        self._fixture.verify(
            all(value == str(seconds) for value in running.values()),
            "the rolled worker node-ctl pods do not carry the scenario's wait",
            expected=str(seconds),
            running=running,
        )
        self._wait_seconds = seconds

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
            self._expect_unchanged(copy, "the worker's copy of the template changed while its sandbox ran")
            polls += 1
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                self._fixture.record("held_polls", polls)
                return
            self._fixture.wait(min(15, remaining))

    def _expect_unchanged(self, copy: LocalTemplate, message: str) -> None:
        held = self.expect_held(copy.node_name, copy.template)
        self._fixture.verify(
            held == copy,
            message,
            before=asdict(copy),
            after=asdict(held),
            failure_evidence=lambda: self._evidence(copy.node_name),
        )

    # --- on-demand full syncs -----------------------------------------------------

    def full_sync(self, node_name: str) -> float:
        """Run the worker's template desired-state facet at full scope now; return once a run
        that started after this call has finished and succeeded, with a monotonic time no
        earlier than its end.

        The facet's status has one-second resolution and also moves for
        node-ctl's own poll. The status read first names the latest run's
        start second; the request goes out a second after that read, so the
        run it starts has a later start second; and the call returns only on
        a finished, successful run with a later start second than the read's,
        which therefore started after the read. That run is the requested one
        or a poll that began in between, which is also a full-scope run.
        Element-scoped runs of this facet come from cluster triggers for a
        template's admin writes or its first appearance in the node's
        inventory, neither of which overlaps the scenario's syncs. A failed
        run fails the call: it may have stopped before the deletes, and a
        check after it would show nothing.
        """

        before = self._control.node_ctl_facet_run(node_name, TEMPLATE_DESIRED_STATE_FACET)
        self._fixture.wait(1)
        self._control.reconcile_node_ctl_facet(node_name, TEMPLATE_DESIRED_STATE_FACET)
        deadline = time.monotonic() + FULL_SYNC_SECONDS
        while True:
            run = self._control.node_ctl_facet_run(node_name, TEMPLATE_DESIRED_STATE_FACET)
            if (
                run.last_started_at > before.last_started_at
                and not run.running
                and run.last_completed_at >= run.last_started_at
            ):
                completed_at = time.monotonic()
                self._fixture.verify(
                    run.last_success_at == run.last_completed_at,
                    "worker node-ctl's on-demand template desired-state run failed",
                    node=node_name,
                    run=asdict(run),
                    failure_evidence=lambda: self._evidence(node_name),
                )
                return completed_at
            self._fixture.verify(
                time.monotonic() < deadline,
                "worker node-ctl did not finish an on-demand template desired-state run "
                f"within {FULL_SYNC_SECONDS} s",
                node=node_name,
                before=asdict(before),
                latest=asdict(run),
                failure_evidence=lambda: self._evidence(node_name),
            )
            self._fixture.wait(0.5)

    def expect_held_past_wait(self, copy: LocalTemplate) -> None:
        """While a sandbox of the template runs: a full sync, the wait, another, and the copy unchanged.

        The second run starts at least the wait after the first finished, so a
        template the first run, or any before it, recorded as left out is due
        in the second, which would delete it.
        """

        first = self.full_sync(copy.node_name)
        self.expect_held_until(copy, first + self.wait_seconds)
        self.full_sync(copy.node_name)
        self._expect_unchanged(copy, "the worker's copy of the template changed while its sandbox ran")

    def expect_held_before_wait(self, copy: LocalTemplate, *, since: float) -> float:
        """After the sandbox left: a full sync, another, and the copy unchanged, all before the wait has passed.

        `since` is a monotonic time no later than the first answer that left
        the template out, which is taken before the sandbox delete. A run
        before `since` plus the wait cannot find the template due, so a copy
        gone by then went early. Returns the first run's end, by which
        node-ctl has recorded the omission.
        """

        wait = self.wait_seconds
        recorded = self.full_sync(copy.node_name)
        early = self.full_sync(copy.node_name)
        held, current, seen = self._holding(copy.node_name, copy.template)
        checked_after = time.monotonic() - since
        # Past the wait a poll may delete the copy, rightly; the check would no
        # longer tell an early delete from that.
        self._fixture.verify(
            checked_after < wait,
            "the syncs after the sandbox left and the check of the copy took longer than the wait, "
            "so they cannot show the worker keeps the template until then; lengthen the scenario's wait",
            wait_seconds=wait,
            checked_after_seconds=round(checked_after, 1),
        )
        self._fixture.verify(
            held and current == copy,
            "the worker deleted or changed its copy of the template before the wait had passed",
            wait_seconds=wait,
            before=asdict(copy),
            seen=seen,
            failure_evidence=lambda: self._evidence(copy.node_name),
        )
        self._fixture.record("early_sync_completed_after_seconds", round(early - since, 1))
        return recorded

    def expect_deleted_after_wait(self, copy: LocalTemplate, *, recorded: float, since: float) -> None:
        """A full sync once the wait has passed since `recorded` deletes the local copy; the Node's
        template-build metadata clears within a poll period after it.

        By `recorded` node-ctl had recorded the omission, so a run starting
        the wait later finds the template due. The builds were last used
        before it too: the sandbox's `GetTemplate` at create, and any touch
        by a node-ctl run, which needs an answer still naming the template,
        so a run that read the cluster before the sandbox was gone; node-ctl
        runs one work item at a time, and the run that ended at `recorded`
        was asked for after that. template-manager's idle bound is the wait,
        so it finds every build idle. `since` dates the delete in the
        evidence.
        """

        wait = self.wait_seconds
        remaining = recorded + wait - time.monotonic()
        if remaining > 0:
            self._fixture.wait(remaining)
        deleting = self.full_sync(copy.node_name)
        names = self._control.local_template_names(copy.node_name)
        digests = self._control.template_digest_dirs(copy.node_name, copy.template)
        self._fixture.verify(
            copy.template not in names and not digests,
            "the worker still holds the template the cluster stopped answering for "
            "after a full sync past the wait",
            wait_seconds=wait,
            listed=copy.template in names,
            digests=list(digests),
            failure_evidence=lambda: self._evidence(copy.node_name),
        )
        self._fixture.record("deleted_after_seconds", round(deleting - since, 1))
        # node-ctl publishes the Node's metadata on the outbound sync after the
        # run, or a poll's if that publication failed.
        deadline = time.monotonic() + FULL_SYNC_SECONDS + POLL_SECONDS
        while True:
            presence = self._control.node_template_presence(copy.node_name)
            left = {
                "labels": sorted(set(copy.presence_keys) & presence.labels),
                "annotations": sorted(set(copy.presence_keys) & set(presence.annotations)),
            }
            if not any(left.values()):
                self._fixture.record(
                    "node_metadata_cleared_after_seconds", round(time.monotonic() - deleting, 1)
                )
                return
            self._fixture.verify(
                time.monotonic() < deadline,
                "the worker's Node still advertises the template's builds a poll period after it deleted them",
                still_present=left,
                failure_evidence=lambda: self._evidence(copy.node_name),
            )
            self._fixture.wait(1)

    def _evidence(self, node_name: str) -> dict[str, object]:
        return {"node_ctl_log": self._control.node_ctl_log(node_name, since_time=self._since_time)[-20_000:]}


def _since_now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

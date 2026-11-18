"""Run a worker node-ctl inbound facet on demand and see that run finish.

A scenario that waits for something node-ctl does on its poll can instead
ask for the facet's run with `space-nodectl reconcile --facet <facet>`, which
runs it at full scope, as a poll does, and read `space-nodectl facet status`
to know when a run that started after the request has finished. The callers
judge the outcome: one fails the scenario on a run that did not finish or
failed, another falls back to waiting for the poll.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass

from space_smoke.capabilities import NodeCtlFacetControl, NodeCtlFacetRun


@dataclass(frozen=True)
class FacetSync:
    """The facet status read before the request, and the latest one read after it."""

    before: NodeCtlFacetRun
    latest: NodeCtlFacetRun
    completed_at: float | None
    """A monotonic time no earlier than the run's end; None when no such run finished in time."""

    @property
    def succeeded(self) -> bool:
        return self.completed_at is not None and self.latest.last_success_at == self.latest.last_completed_at


def sync_node_ctl_facet(
    control: NodeCtlFacetControl,
    node_name: str,
    facet: str,
    *,
    timeout: float,
    wait: Callable[[float], None],
) -> FacetSync:
    """Run `facet` at full scope on that worker now; return once a run that started after
    this call has finished, or once `timeout` seconds have passed without one.

    The facet's status has one-second resolution and also moves for
    node-ctl's own poll. The status read first names the latest run's start
    second; the request goes out a second after that read, so the run it
    starts has a later start second; and only a finished run with a later
    start second than the read's counts, which therefore started after the
    read. That run is the requested one, a poll that began in between, which
    is also a full-scope run, or an element-scoped run a cluster trigger
    started, which callers rule out or tolerate. Raises what the control
    raises when node-ctl cannot be read or did not take the request.
    """

    before = control.node_ctl_facet_run(node_name, facet)
    wait(1)
    control.reconcile_node_ctl_facet(node_name, facet)
    deadline = time.monotonic() + timeout
    while True:
        run = control.node_ctl_facet_run(node_name, facet)
        if (
            run.last_started_at > before.last_started_at
            and not run.running
            and run.last_completed_at >= run.last_started_at
        ):
            return FacetSync(before, run, time.monotonic())
        if time.monotonic() >= deadline:
            return FacetSync(before, run, None)
        wait(0.5)

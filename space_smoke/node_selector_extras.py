"""The sandbox-pod `nodeSelector` extras merge, rendered through create paths and enforced.

The base selector is the worker discriminator label plus every configured
extra (`SPACE_SANDBOX_NODE_SELECTOR_LABELS`), composed in
`crates/libs/common/core/src/k8s/node_selector.rs`. The bench leaves the
variable unset, so every other scenario renders a one-entry selector and the
merge — and the *enforcement* of extras, the part that does any work — had
unit coverage only. `smoke-kind-node-selector-extras.sh` configured extras for
the length of its run and asserted both directions; this module is its typed
form, on one bring-up and one run credential:

1. **positive, api renderer** — with two satisfiable extras configured on all
   renderers, an ECW create's pod carries `spec.nodeSelector` **equal** to the
   discriminator plus every extra, reaches Running, and its node carries every
   entry. Equality, not containment: a dropped extra and a spurious one both
   fail.
2. **positive, cluster-controller renderer** — a *native* create is only
   enqueued by space-api; the cluster controller's public-operation dispatcher
   executes it and renders the pod with its own `SandboxPodConfigValues`, so the
   same assertion on that pod proves the second renderer composes the same
   merge. (The shell suite added this late; the coverage-map gap it closed was
   "the controller is configured but never asserted on".)
3. **negative** — one extra no node carries drives `eligible_start_node_count`
   to a confident zero and cluster admission answers the ECW create with
   `429 Server capacity limit: no eligible sandbox activation nodes …` plus
   `Retry-After`, before any pod is rendered. The same phase asserts, straight
   off the apiserver and independently of the service, that the fleet still
   satisfies the satisfiable set and nothing satisfies the full set, both from
   one node snapshot.

Every renderer gets the variable: `RENDERER_DEPLOYMENTS` is the list, and
`scripts/source_guards/node_selector_extras_smoke.py` checks it against the
Rust `SandboxPodConfigValues::from_env` call sites so a third renderer cannot
appear unnoticed; it also checks `SERVER_CAPACITY_MESSAGE` is still a prefix of
the API's `EmptyCluster` message.

Exclusive bench: with the unsatisfiable extras on all renderers every create
on the cluster is refused for the length of the negative phase.

Not ported, and why (also in the coverage map): the collision branch (an extra
on the discriminator's own key) — with the same value the merged map is
identical whether the override happens or not, and with a different value it
disjoins placement from the registration listers and breaks the bench; it stays
covered by `an_extra_label_on_the_discriminator_key_overrides_the_configured_value`
in the node-selector unit tests. The suite's `deploy_env_live_values` read of
the *running pods'* env is replaced by the Deployment spec literal plus the
rollout wait plus `/readyz`, which is what the typed env override already
proves for the rate limiters and the node groups.
"""

from __future__ import annotations

import secrets
import time
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING

from space_smoke.capabilities import (
    LimiterProbeOutcome,
    NodeSelectorExtrasControl,
    NodeSnapshot,
    RateLimitProbe,
    SandboxPodRender,
)
from space_smoke.runtime import Compensation, ResourceScope

if TYPE_CHECKING:
    from space_smoke.fixtures import KindEcw

SCENARIO_ID = "local.node_selector_extras_render_and_enforce"
SELECTOR_LABELS_ENV = "SPACE_SANDBOX_NODE_SELECTOR_LABELS"
DISCRIMINATOR_KEY_ENV = "SPACE_SANDBOX_NODE_SELECTOR_KEY"
DISCRIMINATOR_VALUE_ENV = "SPACE_SANDBOX_NODE_SELECTOR_VALUE"
ADMISSION_ENABLED_ENV = "SPACE_CLUSTER_ACTIVATION_ADMISSION_ENABLED"
# Every Deployment whose process composes the sandbox node selector. The source
# guard checks this against the Rust call sites.
RENDERER_DEPLOYMENTS: tuple[str, ...] = ("space-api", "space-cluster-controller")
# A prefix of the API's `ClusterAdmissionError::EmptyCluster` message; the
# source guard checks it against `cluster_admission/mod.rs`.
SERVER_CAPACITY_MESSAGE = "Server capacity limit: no eligible sandbox activation nodes"
SSD_PLACEMENT_BLOCKED_LABEL = "space.pplx.net/ssd-placement-blocked"

# Two satisfiable extras: labels the fleet already carries, on keys other than
# the discriminator's. Neither narrows placement on this bench; what they are is
# two further required entries the merge has to carry and the filter has to
# evaluate.
SATISFIED_EXTRAS: tuple[tuple[str, str], ...] = (
    ("space.pplx.net/node-pool", "sandbox"),
    ("kubernetes.io/os", "linux"),
)
# The extra no node carries. Never applied anywhere; the scenario refuses to run
# if any node already has the key.
ABSENT_EXTRA: tuple[str, str] = ("space.pplx.net/selector-extras-absent", "nowhere")


def extras_env(extras: Sequence[tuple[str, str]]) -> str:
    """The comma-separated `key=value` list the services parse."""

    for key, value in extras:
        if not key or "," in key or "=" in key or "," in value:
            raise ValueError(f"refusing extras entry {key!r}={value!r}")
    return ",".join(f"{k}={v}" for k, v in extras)


def expected_selector(
    discriminator: tuple[str, str], extras: Sequence[tuple[str, str]]
) -> dict[str, str]:
    """The map the rendered pod must carry: the discriminator, then every extra layered on top.

    Later keys win, so an extra on the discriminator's own key overrides it —
    the merge rule under test, reimplemented here rather than read back off the
    service, because comparing the composer against itself would pass whatever
    it produced.
    """

    selector = {discriminator[0]: discriminator[1]}
    for key, value in extras:
        selector[key] = value
    return selector


def schedulable_nodes_matching(nodes: Sequence[NodeSnapshot], selector: Mapping[str, str]) -> list[str]:
    """Nodes carrying every entry (equality, no subset or prefix matching) that the filter would count.

    The predicate mirrors `eligible_start_node_count`: Ready, not cordoned, not
    `ssd-placement-blocked`. Taints are the scheduler's and are not counted there
    either.
    """

    return sorted(
        node.name
        for node in nodes
        if node.ready
        and not node.unschedulable
        and node.labels.get(SSD_PLACEMENT_BLOCKED_LABEL) != "true"
        and all(node.labels.get(k) == v for k, v in selector.items())
    )


def render_mismatch(render: SandboxPodRender, expected: Mapping[str, str]) -> str | None:
    """None when the pod's selector equals `expected` exactly; else what differs."""

    rendered = dict(render.node_selector)
    if rendered == dict(expected):
        return None
    missing = sorted(f"{k}={v}" for k, v in expected.items() if rendered.get(k) != v)
    spurious = sorted(f"{k}={v}" for k, v in rendered.items() if k not in expected)
    return f"missing or wrong: {missing}; spurious: {spurious}"


class NodeSelectorExtrasAssertions:
    """Configure extras on every renderer, judge the rendered pods, judge the refusal."""

    def __init__(
        self,
        fixture: KindEcw,
        control: NodeSelectorExtrasControl,
        probe: RateLimitProbe,
        scope: ResourceScope,
    ) -> None:
        self._fixture = fixture
        self._control = control
        self._probe = probe
        self._scope = scope
        self._discriminator: tuple[str, str] | None = None
        self._standing: dict[str, str | None] | None = None
        self._restore_env: Compensation | None = None
        self._release_lease: Compensation | None = None
        self._admitted: list[str] = []
        self._ambiguous_dispatches = 0
        self._metadata = {
            "space_smoke_scenario": SCENARIO_ID,
            "space_smoke_role": "selector-extras-probe",
            "space_smoke_run": secrets.token_hex(8),
        }

    # --- preconditions ----------------------------------------------------------

    def preconditions(self) -> tuple[str, str]:
        """Discriminator read off the api; no node carries the absent key; nothing already composes from our set."""

        env = self._control.renderer_env()
        key = env.get(f"api:{DISCRIMINATOR_KEY_ENV}")
        value = env.get(f"api:{DISCRIMINATOR_VALUE_ENV}")
        self._fixture.verify(
            bool(key) and bool(value),
            "the api Deployment carries no SPACE_SANDBOX_NODE_SELECTOR_KEY/VALUE; this is not the kind stack this scenario configures",
            api_env={k: v for k, v in env.items() if k.startswith("api:")},
        )
        assert key and value
        self._discriminator = (key, value)
        self._fixture.record("discriminator", f"{key}={value}")
        holders = self._control.nodes_carrying_label(ABSENT_EXTRA[0])
        self._fixture.verify(
            holders == [],
            "a node already carries the absent extra's key; the negative phase would not be negative",
            holders=holders,
        )
        satisfied_env = extras_env(SATISFIED_EXTRAS)
        for target in ("api", "controller", "internal-api"):
            standing = env.get(f"{target}:{SELECTOR_LABELS_ENV}")
            self._fixture.verify(
                standing != satisfied_env,
                f"the {target} already composes from the satisfiable extras, so the positive phase would assert nothing this run configured",
                standing=standing,
            )
        self._fixture.record(
            "standing_selector_env", {k: v for k, v in env.items() if k.endswith(SELECTOR_LABELS_ENV)}
        )
        nodes = self._control.nodes_snapshot()
        matching = schedulable_nodes_matching(nodes, expected_selector(self._discriminator, SATISFIED_EXTRAS))
        self._fixture.verify(
            len(matching) >= 1,
            "no schedulable node matches the satisfiable selector; the positive phase could not place a sandbox",
            selector=expected_selector(self._discriminator, SATISFIED_EXTRAS),
            nodes=[n.name for n in nodes],
        )
        self._fixture.record("satisfiable_nodes", matching)
        return self._discriminator

    # --- configure --------------------------------------------------------------

    def configure(self, extras: Sequence[tuple[str, str]], *, rollout_timeout: float = 300) -> None:
        """Write the extras to every renderer (and enable admission on the api); restore armed first.

        The first call captures the standing literals and arms the restore; later
        calls only rewrite. The admission enable is a precondition, not a
        mechanism: the `empty_cluster` gate is inside `prepare_activation_admission`,
        which returns `Ok(false)` when the limiter is off, and the kind charts
        render it enabled — so this sets the value the process already has.
        """

        if self._standing is None:
            release = self._control.acquire_public_api_mutation()
            self._release_lease = self._scope.defer("release the public API mutation lease", release)
            self._scope.defer("kill every sandbox this run admitted", self.sweep_admitted, critical=True)
            standing = self._control.renderer_env()
            self._standing = {
                k: v
                for k, v in standing.items()
                if k.endswith(SELECTOR_LABELS_ENV) or k == f"api:{ADMISSION_ENABLED_ENV}"
            }

            def restore() -> None:
                assert self._standing is not None
                self._control.restore_renderer_env(self._standing)
                self._control.wait_public_api_ready(timeout=rollout_timeout)
                # Recorded from teardown too, so a RESULT that ends before the
                # restore phase still says whether the bench was handed back.
                self._fixture.record("restored_env_by_teardown", dict(self._standing))

            self._restore_env = self._scope.defer("restore the renderers' env", restore, critical=True)
        value = extras_env(extras)
        self._control.set_renderer_env(
            {
                f"api:{SELECTOR_LABELS_ENV}": value,
                f"controller:{SELECTOR_LABELS_ENV}": value,
                f"internal-api:{SELECTOR_LABELS_ENV}": value,
                f"api:{ADMISSION_ENABLED_ENV}": "true",
            }
        )
        self._control.wait_public_api_ready(timeout=rollout_timeout)
        applied = self._control.renderer_env()
        self._fixture.verify(
            all(applied.get(f"{t}:{SELECTOR_LABELS_ENV}") == value for t in ("api", "controller", "internal-api")),
            "a renderer does not carry the extras just written",
            applied={k: v for k, v in applied.items() if k.endswith(SELECTOR_LABELS_ENV)},
            wanted=value,
        )
        self._fixture.record("configured_extras", value)

    # --- positive ---------------------------------------------------------------

    def expect_ecw_create_renders_and_places(self, *, pod_timeout: float = 120) -> str:
        """space-api's render: an ECW create's pod carries exactly the merged selector and runs on a matching node."""

        # The run-owned create stamps the scenario and run identity itself and
        # refuses a caller that supplies them (bench round 1, 2026-09-23); only
        # the role is ours to add. The raw probe and the native create, which
        # bypass that helper, carry the full dict so the sweep can match them.
        sandbox = self._fixture.sandboxes.create(
            metadata={"space_smoke_role": self._metadata["space_smoke_role"]}
        )
        self._judge_render(sandbox.sandbox_id, renderer="space-api (ECW route)", timeout=pod_timeout)
        sandbox.kill()
        self._wait_pod_gone(sandbox.sandbox_id, timeout=pod_timeout)
        return sandbox.sandbox_id

    def expect_native_create_renders_and_places(self, *, template: str, pod_timeout: float = 120) -> str:
        """The cluster controller's render: a native create is executed by its dispatcher; same assertion."""

        # Owed to the sweep from the moment the POST may have committed: the
        # helper can commit the create and then fail or time out polling the
        # operation, and an id learnt only on success would leave that sandbox
        # unswept. Counted as an ambiguous dispatch until the id is known, then
        # owed by id until this phase has killed it itself — a sweep with
        # nothing left to match raises (bench round 3), so the id leaves the
        # list once the kill and the pod's disappearance are confirmed.
        self._ambiguous_dispatches += 1
        sandbox_id = self._control.native_create_sandbox(template=template, metadata=self._metadata)
        self._ambiguous_dispatches -= 1
        self._admitted.append(sandbox_id)
        self._judge_render(sandbox_id, renderer="space-cluster-controller (native route)", timeout=pod_timeout)
        # The kill goes through the ECW probe (the SDK), not the kind harness:
        # bench round 2 died on exactly that misattribution.
        self._probe.kill_sandbox(sandbox_id)
        self._wait_pod_gone(sandbox_id, timeout=pod_timeout)
        self._admitted.remove(sandbox_id)
        return sandbox_id

    def _judge_render(self, sandbox_id: str, *, renderer: str, timeout: float) -> None:
        discriminator = self._require_discriminator()
        expected = expected_selector(discriminator, SATISFIED_EXTRAS)
        deadline = time.monotonic() + timeout
        render: SandboxPodRender | None = None
        while True:
            render = self._control.sandbox_pod_render(sandbox_id)
            if render is not None and render.phase == "Running" and render.node_name:
                break
            self._fixture.verify(
                time.monotonic() < deadline and (render is None or render.phase != "Failed"),
                f"the sandbox pod did not reach Running on a node within the budget [{renderer}]",
                sandbox_id=sandbox_id,
                phase=None if render is None else render.phase,
                node=None if render is None else render.node_name,
            )
            self._fixture.wait(1)
        assert render is not None
        self._fixture.verify(
            not render.pool_role,
            f"the pod came from the warm pool, so it may predate this run's configuration [{renderer}]",
            pool_role=render.pool_role,
        )
        mismatch = render_mismatch(render, expected)
        self._fixture.verify(
            mismatch is None,
            f"the pod's nodeSelector is not the discriminator plus every extra [{renderer}]: {mismatch or ''}",
            rendered=dict(render.node_selector),
            expected=expected,
        )
        assert render.node_name
        labels = self._control.node_labels(render.node_name)
        lacking = sorted(f"{k}={v}" for k, v in expected.items() if labels.get(k) != v)
        self._fixture.verify(
            lacking == [],
            f"the pod's node lacks an entry of its own selector [{renderer}]",
            node=render.node_name,
            lacking=lacking,
        )
        self._fixture.record(
            f"render:{renderer}",
            {"pod": render.pod_name, "node": render.node_name, "selector": dict(render.node_selector)},
        )

    def _wait_pod_gone(self, sandbox_id: str, *, timeout: float, terminating_timeout: float = 30) -> None:
        # A deleting pod is still present until its object is absent; the render
        # skips pods with a deletionTimestamp, so it cannot answer "gone". The
        # shorter terminating deadline attributes a teardown stuck on its node
        # to the phase that killed the sandbox, instead of a later cleanup.
        deadline = time.monotonic() + timeout
        terminating_since: float | None = None
        while pods := self._control.sandbox_pods(sandbox_id):
            now = time.monotonic()
            if all(pod.deleting for pod in pods):
                if terminating_since is None:
                    terminating_since = now
            else:
                terminating_since = None
            evidence = [f"{pod.pod_name} (node {pod.node_name}, phase {pod.phase})" for pod in pods]
            self._fixture.verify(
                terminating_since is None or now - terminating_since < terminating_timeout,
                "the sandbox pod is stuck Terminating after the sandbox was killed; its node has not finished tearing it down",
                sandbox_id=sandbox_id,
                pods=evidence,
                terminating_seconds=None if terminating_since is None else round(now - terminating_since, 1),
            )
            self._fixture.verify(
                now < deadline,
                "the sandbox pod still exists after the sandbox was killed; the negative phase would race its termination",
                sandbox_id=sandbox_id,
                pods=evidence,
            )
            self._fixture.wait(1)

    # --- negative ---------------------------------------------------------------

    def expect_absent_extra_refuses(self, *, timeout: float = 90) -> LimiterProbeOutcome:
        """With one absent extra configured: the fleet still satisfies the old set, nothing the new one, and a create is a capacity 429 before any pod."""

        discriminator = self._require_discriminator()
        nodes = self._control.nodes_snapshot()
        still = schedulable_nodes_matching(nodes, expected_selector(discriminator, SATISFIED_EXTRAS))
        now = schedulable_nodes_matching(
            nodes, expected_selector(discriminator, (*SATISFIED_EXTRAS, ABSENT_EXTRA))
        )
        self._fixture.verify(
            len(still) >= 1,
            "the fleet stopped matching the satisfiable set during the run; a rejection here would prove nothing",
            still_matching=still,
        )
        self._fixture.verify(
            now == [],
            "a node matches the full set including the absent extra; the added extra is not absent",
            matching_full_set=now,
        )
        self._fixture.record("fleet_check", {"satisfiable": still, "with_absent": now})
        before = self._control.sandbox_pod_count()
        outcome = self._probe.race_ecw_request(
            "POST",
            "/sandboxes",
            {
                "templateID": self._fixture.sandbox_template_name(),
                "timeout": 120,
                "metadata": dict(self._metadata),
            },
            client_id="space-smoke",
            timeout=timeout,
        )
        if outcome.sandbox_id:
            self._admitted.append(outcome.sandbox_id)
        elif outcome.status == 0:
            self._ambiguous_dispatches += 1
        after = self._control.sandbox_pod_count()
        # Judged before the status: the fail-open case renders a pod and answers
        # a non-429, and it must be named as the admitted create it is.
        self._fixture.verify(
            after <= before,
            "the create rendered a sandbox pod — it was admitted, so the absent extra did not block placement",
            pods_before=before,
            pods_after=after,
            status=outcome.status,
            response_message=outcome.message,
        )
        self._fixture.verify(
            outcome.status == 429 and outcome.message.startswith(SERVER_CAPACITY_MESSAGE),
            "the create with an absent extra was not refused with the empty-cluster capacity 429",
            status=outcome.status,
            response_message=outcome.message,
        )
        self._fixture.verify(
            bool(outcome.retry_after),
            "the empty-cluster 429 carried no Retry-After header",
            retry_after=outcome.retry_after,
        )
        self._fixture.record(
            "refusal",
            {"status": outcome.status, "retry_after": outcome.retry_after, "message": outcome.message[:200]},
        )
        return outcome

    # --- teardown ---------------------------------------------------------------

    def sweep_admitted(self) -> None:
        """Kill every sandbox still owed to this run, by run metadata (also on an ambiguous dispatch).

        Nothing owed means nothing to reconcile: `kill_matching` raises when no
        sandbox matches, which is the right verdict after an admitted or
        ambiguous create and the wrong one after a run that killed its own.
        """

        if self._admitted or self._ambiguous_dispatches:
            self._probe.kill_matching(self._metadata)

    def restore(self) -> None:
        """Restore the renderers' env now and verify it; the scope no longer needs to."""

        for slot in ("_restore_env", "_release_lease"):
            compensation = getattr(self, slot)
            if compensation is not None:
                setattr(self, slot, None)
                compensation.release()
        if self._standing is not None:
            applied = self._control.renderer_env()
            wanted = dict(self._standing)
            self._fixture.verify(
                all(applied.get(k) == v for k, v in wanted.items()),
                "a renderer does not carry its standing env after the restore",
                applied={k: applied.get(k) for k in wanted},
                wanted=wanted,
            )
            self._fixture.record("restored_env", wanted)

    def _require_discriminator(self) -> tuple[str, str]:
        if self._discriminator is None:
            raise RuntimeError("preconditions() must run first")
        return self._discriminator

"""Drive a template runtime-upgrade campaign through the admin API and judge its phases.

A campaign moves every template release bound to one execution environment
to another by giving each a *twin*: a second release of the same build bound
to the target. The cluster-controller's `runtime_upgrade` loop leases items
under the campaign's pacing and hands them to the template driver. With a
canary size of one, the campaign moves one template and waits in `canary` for
an operator's resume; resumed, it rolls the rest in waves of
`max_in_flight_per_cluster` and completes once a pass finds nothing pending
or in flight. A paused campaign leases nothing new.

The driver moves nothing while any of these holds, so the scenario arranges
each for the run and puts it back afterwards:

- the environment placement gate is `on` wherever a substrate is built
  (`space_smoke/environment_placement_gate.py`), the only mode in which a
  create boots the environment its twin is bound to; the campaign read
  reports `twins_not_booted` otherwise;
- the loop runs (`SPACE_RUNTIME_UPGRADE_RECONCILER_ENABLED`);
- the controller's template driver may use the bench's plain `http://`
  object store (`SPACE_TEMPLATE_STORAGE_ALLOW_PLAINTEXT_ENDPOINTS`), which it
  refuses otherwise;
- every worker reports that its template-manager reads a twin's own
  metadata; the campaign read reports `nodes_dont_read_twins` until then;
- the target is the cluster's default environment. The read doesn't report
  this hold, so the scenario checks that nothing moves before the default
  does.

The controller's two settings are written under the public API mutation
Lease the gate holds while it's on, recorded first and written back in a
phase before the gate's own restore, with a cleanup that does the same if
the run never gets there. They're recorded on that Lease before the write,
as the gate's literals are, so a run killed before its restore leaves the
record, and the Lease's next holder (any Kind scenario's preflight repair)
puts them back. The record is cleared only once a restore reads back.

The scenario judges what the admin API reports for the campaign: its phase,
generation, item counts by phase and `held`
(`GET /admin/api/v1/runtime-upgrade-campaigns/{id}`). Pause and resume are
the operator verbs (`POST .../{id}:pause`, `:resume`), each with a required
reason and the generation it read as `If-Match`. The default moves through
`PUT /admin/api/v1/runtime-environments/default`, under the policy's
generation.

A completed campaign leaves each upgraded release's twin bound to the target
in place. The default goes back to what it was, so creates resolve to the
original release again. A bench with no default yet keeps the source as its
default afterwards: no route clears one, and the source is the environment
nodes run when no default is set. An unfinished campaign is aborted in
cleanup.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from typing import Any, Protocol

from space_smoke.capabilities import ConsoleApiResponse, WarmPoolCandidate
from space_smoke.execution_environment import AdminApi, Judge, SelectableEnvironment, catalog_reference
from space_smoke.runtime import Compensation, InconclusiveSmoke, ResourceScope, SmokeConfigurationError

CAMPAIGNS_PATH = "/admin/api/v1/runtime-upgrade-campaigns"
POLICY_PATH = "/admin/api/v1/runtime-environments"
POOLS_PATH = "/admin/api/v1/warm-pools"
# The run's manual pool on the template's original, which the driver copies
# to the twin: the default shape, the only one a create claims a slot of. An
# ECW create claims one; an admin create never does, so it boots cold.
POOL_SHAPE: Mapping[str, int] = {"vcpu": 2, "memory_mib": 1024}
# A slot warms by cold booting the twin's build under the target.
POOL_READY_TIMEOUT_SECONDS = 600.0
TEMPLATES_KIND = "templates"
FINISHED_PHASES = frozenset({"completed", "aborted"})
# What the scenario sets on the controller for the run, keyed as the adapter
# keys them.
CAMPAIGN_SETTINGS: Mapping[str, str] = {
    "controller:SPACE_RUNTIME_UPGRADE_RECONCILER_ENABLED": "true",
    "controller:SPACE_TEMPLATE_STORAGE_ALLOW_PLAINTEXT_ENDPOINTS": "true",
}
# The loop ticks every 30 s; a paused campaign is watched for several ticks.
LOOP_TICK_SECONDS = 30.0
PAUSED_HOLD_SECONDS = 3 * LOOP_TICK_SECONDS
CANARY_HOLD_SECONDS = 2 * LOOP_TICK_SECONDS
# Before the default moves, nothing may move for this long.
NOT_DEFAULT_HOLD_SECONDS = 2 * LOOP_TICK_SECONDS
# How long a wait may go without an item reaching an outcome. An item can
# cold boot a base snapshot under the target, so this allows a few.
PHASE_TIMEOUT_SECONDS = 1200.0
# Every worker's node-ctl publishes what its template-manager answers with
# its status; after the rollouts, a few publishes.
READERS_TIMEOUT_SECONDS = 600.0
POLL_SECONDS = 5.0
# Campaign reads kept as evidence, the most recent last.
KEPT_READS = 40


class CampaignControl(AdminApi, Protocol):
    """Kind-only operations behind [`TemplateCampaignAssertions`]."""

    def template_campaign_env(self) -> dict[str, str | None]:
        """The literals of `CAMPAIGN_SETTINGS`' keys, None where unset."""
        ...

    def set_template_campaign_env(
        self, values: Mapping[str, str | None], *, record: Mapping[str, str | None] | None = None
    ) -> None:
        """Write those literals (None unsets) and await the rollout, under the gate's Lease.

        `record` goes on the Lease first, for the next holder's repair.
        """
        ...

    def clear_template_campaign_env_record(self) -> None:
        """Drop the record once the standing literals read back."""
        ...

    def warm_pool_claim_pod(self, sandbox_id: str) -> WarmPoolCandidate:
        """The warm-pool slot a sandbox claimed, from its pod's pool labels."""
        ...


@dataclass(frozen=True)
class CampaignSnapshot:
    """One read of a campaign."""

    phase: str
    generation: int
    pending: int
    in_flight: int
    done: int
    failed: int
    skipped: int
    # The hold the read reports, by its reason; None when nothing here holds it.
    held: str | None

    @property
    def moved(self) -> int:
        """Items that reached an outcome."""

        return self.done + self.failed + self.skipped

    @classmethod
    def parse(cls, body: Any) -> CampaignSnapshot:
        if not isinstance(body, dict) or not isinstance(body.get("items"), dict):
            raise SmokeConfigurationError(f"not a campaign: {body!r:.300}")
        items = body["items"]
        held = body.get("held")
        return cls(
            phase=str(body["phase"]),
            generation=int(body["generation"]),
            pending=int(items["pending"]),
            in_flight=int(items["in_flight"]),
            done=int(items["done"]),
            failed=int(items["failed"]),
            skipped=int(items["skipped"]),
            held=str(held["reason"]) if isinstance(held, dict) else None,
        )


class TemplateCampaignAssertions:
    """Start one template campaign, steer it with pause and resume, and read its phases."""

    def __init__(
        self,
        fixture: Judge,
        control: CampaignControl,
        scope: ResourceScope,
        *,
        wait: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._fixture = fixture
        self._control = control
        self._scope = scope
        self._wait = wait
        self._clock = clock
        self._campaign_id: str | None = None
        self._reason: str | None = None
        self._abort: Compensation | None = None
        self._restore_settings: Compensation | None = None
        self._restore_default: Compensation | None = None
        self._pool_release: str | None = None
        self._disable_pools: Compensation | None = None
        self._reads: list[dict[str, object]] = []
        fixture.record("campaign_reads", self._reads)

    @property
    def campaign_id(self) -> str:
        if self._campaign_id is None:
            raise RuntimeError("no campaign has been started")
        return self._campaign_id

    # --- what the bench must serve ----------------------------------------

    def require_served(self) -> None:
        """The campaign API is live and no campaign is unfinished."""

        listed = self._control.console_api("GET", f"{CAMPAIGNS_PATH}?unfinished=true")
        if listed.status == 404:
            raise InconclusiveSmoke(
                "the API serves no /admin/api/v1/runtime-upgrade-campaigns: it predates the "
                "runtime upgrade campaign store, loop and admin API"
            )
        if listed.status != 200 or not isinstance(listed.body, dict):
            raise SmokeConfigurationError(f"listing campaigns answered {listed.status}: {listed.body!r:.300}")
        unfinished = listed.body.get("campaigns") or []
        if unfinished:
            raise SmokeConfigurationError(
                "an unfinished campaign already runs on the bench, so a start would be refused: "
                f"{[entry.get('id') for entry in unfinished if isinstance(entry, dict)]}"
            )

    def environment_reference(self, environment: SelectableEnvironment) -> str:
        """An environment's reference, from the API's catalog."""

        return catalog_reference(self._fixture, self._control, environment)

    # --- the bench's settings ------------------------------------------------

    def turn_on(self) -> None:
        """Set `CAMPAIGN_SETTINGS` for the run, recording what they were; the gate must be on."""

        standing = self._control.template_campaign_env()
        self._fixture.record("template_campaign_env_before", standing)
        if set(standing) != set(CAMPAIGN_SETTINGS):
            raise SmokeConfigurationError(f"the adapter read {sorted(standing)}, not {sorted(CAMPAIGN_SETTINGS)}")
        # No Kind chart sets either, so a literal is what a run left behind
        # without a record to repair it from; taking it as standing would keep
        # it for good.
        left = {key: value for key, value in standing.items() if value is not None}
        if left:
            raise SmokeConfigurationError(
                f"the cluster-controller already sets {left}; no Kind chart sets them, so an earlier "
                "run left them behind; unset them and rerun"
            )
        # Armed before the write, so a write that half-applies is put back.
        # It's registered after the gate's restore, so cleanup runs it first,
        # while the gate still holds the Lease.
        self._restore_settings = self._scope.defer(
            "restore the template campaign settings",
            lambda: self._put_back(standing),
            critical=True,
        )
        self._control.set_template_campaign_env(dict(CAMPAIGN_SETTINGS), record=standing)
        after = self._control.template_campaign_env()
        self._fixture.verify(
            after == dict(CAMPAIGN_SETTINGS),
            "the controller must run with the campaign settings",
            expected=dict(CAMPAIGN_SETTINGS),
            observed=after,
        )

    def _put_back(self, standing: Mapping[str, str | None]) -> None:
        self._control.set_template_campaign_env(standing)
        after = self._control.template_campaign_env()
        if after != dict(standing):
            raise SmokeConfigurationError(
                f"the campaign settings read {after} after the restore, not {dict(standing)}"
            )
        # Only a restore that reads back clears the record; otherwise it stays
        # for the next holder's repair.
        self._control.clear_template_campaign_env_record()

    def restore_settings(self) -> None:
        """Put the recorded settings back, as a phase, before the gate's restore; cleanup retries a failure."""

        if self._restore_settings is not None:
            self._restore_settings.release()
            self._restore_settings = None

    # --- the cluster default -------------------------------------------------

    def _policy(self) -> dict[str, Any]:
        response = self._control.console_api("GET", POLICY_PATH)
        if response.status != 200 or not isinstance(response.body, dict):
            raise SmokeConfigurationError(f"reading the runtime environment policy answered {response.status}")
        return response.body

    def releases_bound_to(self, environment_ref: str) -> int:
        """How many template releases are bound to `environment_ref`, twins included."""

        response = self._control.console_api("GET", f"{POLICY_PATH}/population")
        if response.status != 200 or not isinstance(response.body, dict):
            raise SmokeConfigurationError(f"reading the runtime environment population answered {response.status}")
        counts = [
            entry for entry in response.body.get("environments") or ()
            if isinstance(entry, dict) and entry.get("environment_ref") == environment_ref
        ]
        return int(counts[0].get("template_releases", 0)) if counts else 0

    def require_no_target_releases(self, target_ref: str) -> None:
        """No release is bound to the target yet, so every item must make its twin.

        The driver ends an item done, without making anything, when the
        build already has a release on the target; a bench an earlier run
        left twins on would pass without a twin being made.
        """

        bound = self.releases_bound_to(target_ref)
        self._fixture.record("target_releases_before", bound)
        if bound:
            raise InconclusiveSmoke(
                f"{bound} template releases are already bound to {target_ref}, as an earlier run's twins "
                "are; a campaign would find their builds done without making a twin, so run on a bench "
                "with none"
            )

    def default_environment(self) -> str | None:
        default = self._policy().get("default_environment_ref")
        return str(default) if default is not None else None

    def _put_default(self, environment_ref: str, *, reason: str, strand: bool) -> ConsoleApiResponse:
        policy = self._policy()
        body: dict[str, object] = {"environment_ref": environment_ref, "reason": reason}
        if strand:
            body["strand"] = True
        response = self._control.console_api(
            "PUT", f"{POLICY_PATH}/default", body, if_match=int(policy["generation"])
        )
        self._fixture.record(
            f"set_default_{environment_ref}{'_strand' if strand else ''}",
            {"status": response.status, "body": response.body},
        )
        return response

    def set_default(self, environment_ref: str, *, reason: str) -> None:
        response = self._put_default(environment_ref, reason=reason, strand=False)
        self._verify_default_set(environment_ref, response)

    def _verify_default_set(self, environment_ref: str, response: ConsoleApiResponse) -> None:
        self._fixture.verify(
            response.status == 200,
            f"the API must make {environment_ref} the cluster default",
            status=response.status,
            body=response.body,
        )

    def keep_default(self, source_ref: str) -> None:
        """The source is the default, and the run puts it back.

        A bench with no default gets the source as its first default, which
        it then keeps. One whose default is another environment is
        inconclusive: the scenario can't tell what its creates resolve to.
        """

        standing = self.default_environment()
        self._fixture.record("default_environment_before", standing)
        if standing is None:
            self._first_default(source_ref)
        elif standing != source_ref:
            raise InconclusiveSmoke(
                f"the bench's default environment is {standing}, not the campaign's source {source_ref}"
            )
        self._restore_default = self._scope.defer(
            "move the default back to the campaign's source",
            lambda: self._default_back(source_ref),
            critical=True,
        )

    def _first_default(self, source_ref: str) -> None:
        """Make the source the first default, acknowledging only work that keeps running.

        A first default stops every environment with no executing state, and
        the API refuses it while work may depend on one, counting legacy work
        with no binding among it. Such work runs as the environment it
        matches, the source here. So the scenario passes `strand` only when
        the refusal names no environment that would stop; any other refusal
        fails the run.
        """

        reason = "space_smoke: the template campaign's source is the default"
        response = self._put_default(source_ref, reason=reason, strand=False)
        at_risk = response.body.get("at_risk") if isinstance(response.body, dict) else None
        if response.status == 409 and isinstance(at_risk, dict) and at_risk.get("environments") == []:
            response = self._put_default(source_ref, reason=reason, strand=True)
        self._verify_default_set(source_ref, response)

    def _default_back(self, source_ref: str) -> None:
        if self.default_environment() != source_ref:
            self.set_default(source_ref, reason="space_smoke: the template campaign ran")

    def restore_default(self) -> None:
        """Move the default back to the source, as a phase."""

        if self._restore_default is not None:
            self._restore_default.release()
            self._restore_default = None

    # --- the run's warm pool -------------------------------------------------

    def _pools(self) -> list[dict[str, Any]]:
        response = self._control.console_api("GET", POOLS_PATH)
        if response.status != 200 or not isinstance(response.body, list):
            raise SmokeConfigurationError(f"listing warm pools answered {response.status}")
        return [row for row in response.body if isinstance(row, dict)]

    def add_pool(self, template: str) -> None:
        """Give the release `template` names a manual pool of `POOL_SHAPE`.

        The bench must have no pool of that shape for the template already:
        the run disables every pool of its shape on the template's releases
        afterwards.
        """

        response = self._control.console_api("GET", "/admin/api/v1/template-releases")
        if response.status != 200 or not isinstance(response.body, list):
            raise SmokeConfigurationError(f"listing template releases answered {response.status}")
        named = [
            row for row in response.body
            if isinstance(row, dict) and row.get("template_name") == template
            and "latest" in (row.get("tags") or ())
        ]
        if len(named) != 1:
            raise SmokeConfigurationError(f"expected one release tagged {template}:latest, found {len(named)}")
        release = named[0]
        clashing = [row for row in self._pools() if row.get("template_name") == template and self._run_shape(row)]
        if clashing:
            raise InconclusiveSmoke(
                f"the bench already has a {dict(POOL_SHAPE)} pool for {template}: {[row.get('id') for row in clashing]}"
            )
        self._pool_release = str(release["id"])
        self._disable_pools = self._scope.defer(
            "disable the run's warm pools", lambda: self._disable_run_pools(template), critical=True
        )
        created = self._control.console_api(
            "PUT",
            POOLS_PATH,
            {
                "template_name": template,
                "content_hash": release["content_hash"],
                **POOL_SHAPE,
                "namespace": "sandboxes",
                "target_ready_slots": 1,
                "refill_per_tick": 1,
                "max_warming_slots": 1,
                "enabled": True,
                "encryptable": False,
            },
        )
        self._fixture.record("pool_create", {"status": created.status, "body": created.body})
        self._fixture.verify(
            created.status in (200, 201) and isinstance(created.body, dict)
            and created.body.get("template_release_id") == self._pool_release,
            f"the API must give {template}'s release a manual pool",
            status=created.status,
            body=created.body,
        )

    @staticmethod
    def _run_shape(row: Mapping[str, Any]) -> bool:
        return all(row.get(key) == value for key, value in POOL_SHAPE.items()) and not row.get("encryptable")

    def await_twin_pool(self, template: str) -> str:
        """The twin's copy of the run's pool, once it has a ready slot; its config id."""

        deadline = self._clock() + POOL_READY_TIMEOUT_SECONDS
        while True:
            twins = [
                row for row in self._pools()
                if row.get("template_name") == template and self._run_shape(row)
                and row.get("template_release_id") != self._pool_release
            ]
            self._fixture.record("twin_pools", twins)
            self._fixture.verify(
                len(twins) <= 1, "the twin must have one copy of the run's pool", pools=twins
            )
            if twins and twins[0].get("managed_by") == "manual" and int(twins[0].get("ready_slots") or 0) >= 1:
                return str(twins[0]["id"])
            self._fixture.verify(
                self._clock() < deadline,
                "the twin's copy of the run's pool must get a ready slot",
                pools=twins,
            )
            self._wait(POLL_SECONDS)

    def expect_claimed(self, sandbox_id: str, config_id: str) -> None:
        """The sandbox is a slot of pool config `config_id`."""

        claimed = self._control.warm_pool_claim_pod(sandbox_id)
        self._fixture.record("claimed_pool_slot", asdict(claimed))
        self._fixture.verify(
            claimed.config_id == config_id,
            "a create of the pool's shape must claim the twin's pool",
            expected_config=config_id,
            claimed=asdict(claimed),
        )

    def _disable_run_pools(self, template: str) -> None:
        for row in self._pools():
            if row.get("template_name") == template and self._run_shape(row) and row.get("enabled"):
                response = self._control.console_api("DELETE", f"{POOLS_PATH}/{row['id']}")
                if response.status not in (200, 204, 404):
                    raise RuntimeError(f"disabling warm pool {row['id']} answered {response.status}")

    def remove_pools(self) -> None:
        """Disable the run's pool and its twin copy, as a phase."""

        if self._disable_pools is not None:
            self._disable_pools.release()
            self._disable_pools = None

    # --- the operator verbs -------------------------------------------------

    def start(
        self,
        *,
        from_environment_ref: str,
        to_environment_ref: str,
        canary_size: int,
        wave_size: int,
        failure_budget: int,
        reason: str,
    ) -> CampaignSnapshot:
        # The reason carries a token of this run's own, and the abort is armed
        # before dispatch: if the reply is lost, cleanup finds the campaign by
        # that reason among the unfinished ones (none ran before the start, as
        # `require_served` checked), and never submits anything itself.
        reason = f"{reason} [{uuid.uuid4()}]"
        self._reason = reason
        self._abort = self._scope.defer(
            "abort this run's runtime upgrade campaign", self._abort_ours, critical=True
        )
        response = self._control.console_api(
            "POST",
            CAMPAIGNS_PATH,
            {
                "kind": TEMPLATES_KIND,
                "from_environment_ref": from_environment_ref,
                "to_environment_ref": to_environment_ref,
                "canary_size": canary_size,
                "max_in_flight_per_cluster": wave_size,
                "failure_budget": failure_budget,
                "reason": reason,
            },
        )
        self._fixture.record("campaign_start", {"status": response.status, "body": response.body})
        # The API refuses a kind no cluster-controller driver moves.
        if response.status == 409 and "driver" in str(response.body):
            raise InconclusiveSmoke(
                "the API refuses to start a templates campaign because no cluster-controller "
                "driver moves templates: the template campaign driver isn't deployed"
            )
        self._fixture.verify(
            response.status == 201 and isinstance(response.body, dict) and "id" in response.body,
            "the API must start the templates campaign",
            status=response.status,
            body=response.body,
        )
        self._campaign_id = str(response.body["id"])
        return self.read()

    def pause(self, *, reason: str) -> CampaignSnapshot:
        return self._act("pause", {"reason": reason})

    def resume(self, *, reason: str) -> CampaignSnapshot:
        return self._act("resume", {"reason": reason})

    def _act(self, action: str, body: Mapping[str, object]) -> CampaignSnapshot:
        current = self.read()
        response = self._post_action(action, body, current.generation)
        self._fixture.verify(
            response.status == 200,
            f"the API must {action} the campaign at generation {current.generation}",
            status=response.status,
            body=response.body,
        )
        after = self.read()
        self._fixture.verify(
            after.generation > current.generation,
            f"a {action} must raise the campaign's generation",
            before=current.generation,
            after=after.generation,
        )
        return after

    def _post_action(self, action: str, body: Mapping[str, object], generation: int) -> ConsoleApiResponse:
        return self._control.console_api(
            "POST", f"{CAMPAIGNS_PATH}/{self.campaign_id}:{action}", body, if_match=generation
        )

    # --- reads ----------------------------------------------------------------

    def read(self) -> CampaignSnapshot:
        response = self._control.console_api("GET", f"{CAMPAIGNS_PATH}/{self.campaign_id}")
        if response.status != 200:
            raise SmokeConfigurationError(f"reading campaign {self.campaign_id} answered {response.status}")
        snapshot = CampaignSnapshot.parse(response.body)
        self._reads.append({**asdict(snapshot), "at": round(self._clock(), 1)})
        del self._reads[:-KEPT_READS]
        return snapshot

    def await_readers(self, *, timeout: float = READERS_TIMEOUT_SECONDS) -> CampaignSnapshot:
        """Wait until the read reports no hold: every worker reads twin metadata.

        `twins_not_booted` can't clear by waiting, so it fails at once.
        """

        deadline = self._clock() + timeout
        while True:
            snapshot = self.read()
            if snapshot.held is None:
                return snapshot
            self._fixture.verify(
                snapshot.held != "twins_not_booted",
                "the API must boot twins with the gate on",
                campaign=asdict(snapshot),
            )
            self._fixture.verify(
                self._clock() < deadline,
                "timed out waiting for every worker to report that it reads twin metadata",
                campaign=asdict(snapshot),
            )
            self._wait(POLL_SECONDS)

    def await_phase(
        self, accept: Callable[[CampaignSnapshot], bool], what: str, *, timeout: float = PHASE_TIMEOUT_SECONDS
    ) -> CampaignSnapshot:
        """Wait until `accept` holds; fail once `timeout` passes with no item reaching an outcome.

        A wait that spans several waves keeps going while items keep moving.
        """

        deadline = self._clock() + timeout
        moved: int | None = None
        while True:
            snapshot = self.read()
            if accept(snapshot):
                return snapshot
            if moved is not None and snapshot.moved > moved:
                deadline = self._clock() + timeout
            moved = snapshot.moved
            self._fixture.verify(
                snapshot.phase not in FINISHED_PHASES,
                f"the campaign finished {snapshot.phase} before {what}",
                campaign=asdict(snapshot),
            )
            self._fixture.verify(
                snapshot.phase != "paused",
                f"the campaign paused itself before {what}; its failure budget is spent",
                campaign=asdict(snapshot),
            )
            self._fixture.verify(self._clock() < deadline, f"timed out waiting until {what}", campaign=asdict(snapshot))
            self._wait(POLL_SECONDS)

    def hold(
        self, expect: Callable[[CampaignSnapshot], bool], what: str, *, seconds: float
    ) -> CampaignSnapshot:
        """`expect` holds on every read for `seconds`: several loop ticks."""

        deadline = self._clock() + seconds
        while True:
            snapshot = self.read()
            self._fixture.verify(expect(snapshot), what, campaign=asdict(snapshot))
            if self._clock() >= deadline:
                return snapshot
            self._wait(POLL_SECONDS)

    def _abort_ours(self) -> None:
        """Abort this run's campaign if it's unfinished, found by ID or, failing that, by reason."""

        if self._campaign_id is None:
            listed = self._control.console_api("GET", f"{CAMPAIGNS_PATH}?unfinished=true")
            if listed.status != 200 or not isinstance(listed.body, dict):
                raise RuntimeError(f"listing campaigns answered {listed.status}: {listed.body!r:.300}")
            ours = [
                entry for entry in listed.body.get("campaigns") or ()
                if isinstance(entry, dict) and entry.get("reason") == self._reason
            ]
            if not ours:
                return
            self._campaign_id = str(ours[0]["id"])
        self._abort_if_unfinished()

    def _abort_if_unfinished(self) -> None:
        # The loop raises the generation too (a self-pause, a completion), so
        # an abort can lose one race to it; the second attempt reads again.
        for _ in range(2):
            snapshot = self.read()
            if snapshot.phase in FINISHED_PHASES:
                return
            response = self._post_action(
                "abort", {"reason": "space_smoke cleanup of an unfinished campaign"}, snapshot.generation
            )
            if response.status == 200:
                return
        after = self.read()
        if after.phase not in FINISHED_PHASES:
            raise RuntimeError(
                f"abort of campaign {self.campaign_id} answered {response.status} and it's still "
                f"{after.phase}: {response.body!r:.300}"
            )

"""A rolling warm-pool policy follows its template's newest release.

A policy (`PUT /admin/api/v1/warm-pool-policies`) names a template and a VM
shape. Each pass of the cluster-controller's warm-pool reconciler starts with
the autoscale step: it ranks the template's releases by publication recency,
keeps the newest `retain_releases`, writes a policy-owned
(`managed_by = 'autoscaler'`) pool config for each, and disables the
policy-owned configs of releases that fell out of retention, whose open slots
then drain (`docs/space/design/warm-pools.md`, "Rolling warm-pool
policies"). The path that matters when a template is rebuilt is the move of
its `latest` tag: the policy's pool must follow the new release, so creates of
the template, which resolve to `latest`, claim a warm slot instead of booting
cold.

The scenario builds a run-owned template twice through the public E2B SDK
(the build path of `space_smoke/template_build.py`), each build copying a
marker of its own into the rootfs so the two releases differ in content. It
writes a policy sized for a smoke: one ready slot per retained release, one
retained release, the default create shape. It judges what the admin API
reports (`GET /admin/api/v1/template-releases`, `GET /admin/api/v1/warm-pools`
and `GET /admin/api/v1/sandboxes/{id}`), and a claim by the signal the warm-pool
fixtures use: the sandbox an ECW create returns is the already-started pod
the config's one ready slot held, read from its pool labels before and after
the create (`KindHarness.warm_pool_candidate`, `warm_pool_claim_pod`).

The waits are sized from the reconciler's own pacing, compile-time constants
in `crates/l40_api/cluster/controller/server/src/warm_pool.rs` that the
environment does not change (`Config::from_env` reads none of them); see
`FOLLOW_TIMEOUT_SECONDS` and `READY_TIMEOUT_SECONDS`. They are allowances for
a quiet bench, not worst cases.

A run-owned template built through the SDK is org-specific, so the scenario
needs node-group placement off, as Kind ships it: with the gate on, such a
template gets no pool slot on shared workers, and the scenario reports
inconclusive instead of waiting for one.

The template, the policy and its configs are the run's own, so the scenario
needs no Lease. Cleanup kills the run's sandboxes, disables the policy, waits
until the template has no pool configs left, and deletes the template; the scenario does
each of these as a phase, and the compensations armed before each step are
the backstop when a run fails earlier.
"""

from __future__ import annotations

import secrets
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from typing import Any, Protocol

from space_smoke.capabilities import (
    ConsoleApiResponse,
    EcwSandbox,
    TemplateBuildControl,
    TemplateBuilder,
    TemplateRecipe,
    WarmPoolCandidate,
)
from space_smoke.execution_environment import AdminApi, Judge
from space_smoke.runtime import (
    Compensation,
    InconclusiveSmoke,
    NoWarmPoolPod,
    ResourceScope,
    SmokeConfigurationError,
    cleanup_seconds_left,
)
from space_smoke.template_build import (
    OCI_IMAGE,
    TEMPLATE_DELETE_BUDGET_SECONDS,
    delete_template_and_wait,
)

POLICIES_PATH = "/admin/api/v1/warm-pool-policies"
POOLS_PATH = "/admin/api/v1/warm-pools"
RELEASES_PATH = "/admin/api/v1/template-releases"
SANDBOXES_PATH = "/admin/api/v1/sandboxes"

# The autoscaler's shape for a create recorded without one
# (`AUTOSCALE_DEFAULT_VCPU` and `AUTOSCALE_DEFAULT_MEMORY_MIB` in
# `crates/l30_storage/cluster/pool/src/store.rs`). A create naming no shape
# claims a slot of any enabled shape of its release.
POLICY_SHAPE: Mapping[str, int] = {"vcpu": 2, "memory_mib": 1024}
# One ready slot for the one retained release, filled one slot at a time.
POLICY_SIZING: Mapping[str, int] = {
    "min_ready_slots": 1,
    "max_ready_slots": 1,
    "refill_per_tick": 1,
    "max_warming_slots": 1,
    "retain_releases": 1,
}

# The reconciler's pacing, compile-time constants in
# `crates/l40_api/cluster/controller/server/src/warm_pool.rs`: a pass every
# `DEFAULT_RECONCILE_INTERVAL`, a slot that has not become ready within
# `DEFAULT_WARMING_SLOT_TIMEOUT` deleted, and each cleanup-pending slot's
# execution delete, the pass's one substrate call, bounded by
# `DEFAULT_RECONCILE_GRPC_SUBSTRATE_IO_TIMEOUT`.
RECONCILE_TICK_SECONDS = 5.0
WARMING_SLOT_TIMEOUT_SECONDS = 600.0
SUBSTRATE_IO_TIMEOUT_SECONDS = 120.0
# The autoscale step opens every pass, so a policy write or a tag move shows
# in the configs in the first pass that starts after it. The bound allows a
# tick, a pass already under way held up by one cleanup-pending execution
# delete, and a tick of slack. It is an allowance for a quiet bench, not a
# worst case: a pass deletes its cleanup-pending slots one after another.
FOLLOW_TIMEOUT_SECONDS = 2 * RECONCILE_TICK_SECONDS + SUBSTRATE_IO_TIMEOUT_SECONDS
# With one warming slot at a time, a config's first ready slot is one warming
# attempt: the tick that creates its pod, the attempt itself, which the
# reconciler abandons at the warming timeout, and the tick that promotes it.
READY_TIMEOUT_SECONDS = WARMING_SLOT_TIMEOUT_SECONDS + 2 * RECONCILE_TICK_SECONDS
# Once the policy is disabled, the next pass disables its configs and deletes
# their open slots, and the autoscale step deletes each disabled config once no
# slot of any state is left. A slot a killed sandbox had claimed is deleted in
# the transaction that retires its execution; no backstop removes one that
# delete missed for an hour (`ORPHANED_RECEIPT_DRAIN_GRACE`), so
# `SANDBOX_TEARDOWN_SECONDS` is slack for that teardown, and a drain that times
# out names a slot the delete path left behind.
SANDBOX_TEARDOWN_SECONDS = 300.0
DRAIN_TIMEOUT_SECONDS = SANDBOX_TEARDOWN_SECONDS + FOLLOW_TIMEOUT_SECONDS
# The scope's cleanup budget is min(120 s, 10 % of the scenario timeout);
# the backstop's drain leaves the template delete its own budget inside it.
CLEANUP_DRAIN_BUDGET_SECONDS = 40.0
POLL_SECONDS = 2.0
SANDBOX_TIMEOUT_SECONDS = 600

MARKER_PATH = "/opt/space-smoke-release/marker.txt"
MARKER_FILE = "release-marker.txt"


def release_recipe(marker: str, image: str = OCI_IMAGE) -> TemplateRecipe:
    """The base image plus a file carrying `marker`, so each build is a release of its own.

    No build step runs a command, so the build is the image pull (the image
    `oci_recipe` already pulls on the Kind builders), the copy, the
    guest-bundle assembly and the publish.
    """

    return TemplateRecipe(
        dockerfile=f"FROM {image}\nCOPY {MARKER_FILE} {MARKER_PATH}\n",
        files={MARKER_FILE: marker},
        envs={},
        run_cmds=(),
        start_cmd=None,
        ready_timeout_seconds=0,
    )


def hash_form(content_hash: str) -> str:
    """How a content hash is spelled: its algorithm prefix, or `bare`."""

    prefix, separator, _ = content_hash.partition(":")
    return prefix if separator else "bare"


class WarmPoolPolicyControl(AdminApi, TemplateBuildControl, Protocol):
    """Kind-only operations behind [`WarmPoolPolicyAssertions`]."""

    def warm_pool_candidate(self, config_id: str) -> WarmPoolCandidate:
        """The config's one live pool pod and its sandbox's state on the node."""
        ...

    def warm_pool_claim_pod(self, sandbox_id: str) -> WarmPoolCandidate:
        """The pool pod a sandbox runs in, from its pool labels."""
        ...

    def node_group_env(self) -> Mapping[str, str | None]:
        """The API's and controller's node-group placement settings."""
        ...

    def delete_template_as_admin(self, alias: str) -> str | None:
        """Start the template's delete; its operation id, or None when it is gone."""
        ...

    def operation_state(self, operation_id: str) -> str: ...


class WarmPoolPolicyEcw(TemplateBuilder, Protocol):
    """The public SDK surface: builds, creates, and the kills cleanup needs."""

    def kill_sandbox(self, sandbox_id: str) -> None: ...

    def kill_matching(self, metadata: Mapping[str, str]) -> None: ...


@dataclass(frozen=True)
class Release:
    """One release of the run's template: its `GET template-releases` row and this run's marker."""

    release_id: str
    content_hash: str
    tags: tuple[str, ...]
    marker: str

    @property
    def form(self) -> str:
        return hash_form(self.content_hash)


@dataclass(frozen=True)
class Claim:
    """What a create of the template ran: its pool slot and its release."""

    sandbox_id: str
    config_id: str
    slot_id: str
    content_hash: str


class WarmPoolPolicyAssertions:
    """Build the run's template, steer its policy, and judge which release its pool serves."""

    def __init__(
        self,
        fixture: Judge,
        control: WarmPoolPolicyControl,
        ecw: WarmPoolPolicyEcw,
        scope: ResourceScope,
        *,
        retry_delete: Callable[[Callable[[], None]], int],
        wait: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._fixture = fixture
        self._control = control
        self._ecw = ecw
        self._scope = scope
        # The harness's shared cleanup retry policy for sandbox deletes
        # (`space_smoke.fixtures.retry_sandbox_delete`), passed in because
        # the fixtures module imports this one.
        self._retry_delete = retry_delete
        self._wait = wait
        self._clock = clock
        self.alias = f"space-smoke-policy-{secrets.token_hex(4)}"
        self._releases: list[Release] = []
        self._template_delete: Compensation | None = None
        self._policy_disable: Compensation | None = None
        self._kills: list[Compensation] = []
        self._pool_reads: list[list[dict[str, object]]] = []

    # --- preconditions ---------------------------------------------------------

    def require_placement_off(self) -> None:
        """Node-group placement is off, as Kind ships it; otherwise the run is inconclusive."""

        settings = dict(self._control.node_group_env())
        self._fixture.record("node_group_env", settings)
        if any(_truthy(value) for key, value in settings.items() if key.endswith("PLACEMENT")):
            raise InconclusiveSmoke(
                "node-group placement is on: the run's org-specific template gets no pool "
                "slot on shared workers, so the policy's follow cannot be judged"
            )

    # --- the template's releases ---------------------------------------------

    def build_release(self) -> Release:
        """Build the template again with a new marker; the release `latest` now names."""

        if self._template_delete is None:
            # Armed before the first build, so a failed or ambiguous build
            # cannot leak the template, and before the policy's disable so
            # that unwinds first: a template delete is refused while pools use it.
            self._template_delete = self._scope.defer(
                f"delete the run-owned template {self.alias}", self._delete_template, critical=True
            )
            self._fixture.record("template_alias", self.alias)
        before = {row["id"] for row in self._release_rows()}
        marker = secrets.token_hex(16)
        result = self._ecw.build_template(
            release_recipe(marker),
            alias=self.alias,
            cpu_count=1,
            memory_mb=1024,
            tags=("latest",),
        )
        rows = self._release_rows()
        new = [row for row in rows if row["id"] not in before]
        self._fixture.verify(
            len(new) == 1,
            "a build of the template must publish exactly one new release",
            build_id=result.build_id,
            releases=rows,
        )
        row = new[0]
        release = Release(
            release_id=str(row["id"]),
            content_hash=str(row["content_hash"]),
            tags=tuple(str(tag) for tag in row.get("tags") or ()),
            marker=marker,
        )
        self._fixture.verify(
            "latest" in release.tags,
            "the new release must carry the template's latest tag",
            release=asdict(release),
            releases=rows,
        )
        stale = [
            row for row in rows
            if row["id"] != release.release_id and "latest" in (row.get("tags") or ())
        ]
        self._fixture.verify(
            not stale,
            "only the new release may carry the latest tag",
            release=asdict(release),
            also_latest=stale,
        )
        self._fixture.verify(
            all(earlier.content_hash != release.content_hash for earlier in self._releases),
            "a build with different content must publish a release with a new content hash",
            release=asdict(release),
            earlier=[asdict(earlier) for earlier in self._releases],
        )
        number = len(self._releases) + 1
        self._fixture.record(
            f"release_{number}",
            {**asdict(release), "hash_form": release.form, "build_seconds": result.elapsed_seconds},
        )
        self._releases.append(release)
        return release

    def _release_rows(self) -> list[dict[str, Any]]:
        response = self._control.console_api("GET", RELEASES_PATH)
        if response.status != 200 or not isinstance(response.body, list):
            raise SmokeConfigurationError(f"listing template releases answered {response.status}")
        return [
            row for row in response.body
            if isinstance(row, dict) and row.get("template_name") == self.alias
        ]

    # --- the policy ------------------------------------------------------------

    def _policy_body(self, *, enabled: bool) -> dict[str, object]:
        return {"template_name": self.alias, **POLICY_SHAPE, **POLICY_SIZING, "enabled": enabled}

    def write_policy(self) -> None:
        """Write the run's rolling policy, enabled; its disable is armed first."""

        # Before the write, so a write whose answer is lost is still undone;
        # a disable of a policy that was never written writes a disabled one,
        # which the template delete removes.
        self._policy_disable = self._scope.defer(
            f"disable the policy on {self.alias} and let its pools drain",
            self._disable_and_drain_in_cleanup,
            critical=True,
        )
        response = self._put_policy(enabled=True)
        body = response.body if isinstance(response.body, dict) else {}
        expected = self._policy_body(enabled=True)
        mismatched = {key: body.get(key) for key, value in expected.items() if body.get(key) != value}
        self._fixture.record("policy_write", {"status": response.status, "body": response.body})
        self._fixture.verify(
            response.status == 200 and not mismatched and body.get("runtime") == "fc",
            "the admin API must store the run's rolling policy as written",
            status=response.status,
            mismatched=mismatched,
            body=response.body,
        )

    def _put_policy(self, *, enabled: bool) -> ConsoleApiResponse:
        return self._control.console_api("PUT", POLICIES_PATH, self._policy_body(enabled=enabled))

    # --- the template's pools --------------------------------------------------

    def _pools(self) -> list[dict[str, Any]]:
        response = self._control.console_api("GET", POOLS_PATH)
        if response.status != 200 or not isinstance(response.body, list):
            raise SmokeConfigurationError(f"listing warm pools answered {response.status}")
        rows = [
            row for row in response.body
            if isinstance(row, dict) and row.get("template_name") == self.alias
        ]
        self._pool_reads.append([_pool_summary(row) for row in rows])
        del self._pool_reads[:-20]
        return rows

    @staticmethod
    def _policy_configs(rows: Sequence[Mapping[str, Any]], release: Release) -> list[Mapping[str, Any]]:
        return [
            row for row in rows
            if row.get("template_release_id") == release.release_id
            and row.get("managed_by") == "autoscaler"
            and all(row.get(key) == value for key, value in POLICY_SHAPE.items())
            and not row.get("encryptable")
        ]

    def _policy_config(self, rows: Sequence[Mapping[str, Any]], release: Release) -> Mapping[str, Any] | None:
        configs = self._policy_configs(rows, release)
        self._fixture.verify(
            len(configs) <= 1,
            "a release must have at most one policy-owned config at the policy's shape",
            release=asdict(release),
            configs=[_pool_summary(row) for row in configs],
        )
        return configs[0] if configs else None

    def await_ready_pool(self, release: Release, *, since: float | None = None) -> str:
        """The release's policy-owned config, once enabled with a ready slot; its id."""

        started = self._clock() if since is None else since
        number = self._number(release)
        config_deadline = started + FOLLOW_TIMEOUT_SECONDS
        written_at: float | None = None
        while True:
            config = self._policy_config(self._pools(), release)
            now = self._clock()
            if config is not None and config.get("enabled") is True:
                if written_at is None:
                    written_at = now
                    self._fixture.record(f"release_{number}_config_seconds", round(now - started, 1))
                if int(config.get("ready_slots") or 0) >= 1:
                    self._fixture.record(f"release_{number}_ready_seconds", round(now - started, 1))
                    self._fixture.record(f"release_{number}_config", _pool_summary(config))
                    return str(config["id"])
            if written_at is None:
                self._fixture.verify(
                    now < config_deadline,
                    f"the policy wrote no enabled policy-owned config for release {number} "
                    f"({release.content_hash}) within {FOLLOW_TIMEOUT_SECONDS:g} s",
                    release=asdict(release),
                    pools=self._pool_reads[-1],
                )
            else:
                self._fixture.verify(
                    now < written_at + READY_TIMEOUT_SECONDS,
                    f"the policy-owned config for release {number} got no ready slot "
                    f"within {READY_TIMEOUT_SECONDS:g} s of the first read that showed it enabled",
                    release=asdict(release),
                    pools=self._pool_reads[-1],
                )
            self._wait(POLL_SECONDS)

    def await_follow(self, previous: Release, latest: Release) -> str:
        """After `latest` took the tag: its config fills and `previous`'s stops serving.

        Retention is one release, so the same pass that writes the new
        release's config disables the old one's. The wait is measured from
        the end of the build, the first instant the scenario saw the tag on
        the new release, so it slightly understates the time since the move.
        """

        moved = self._clock()
        number = self._number(latest)
        deadline = moved + FOLLOW_TIMEOUT_SECONDS
        while True:
            rows = self._pools()
            now = self._clock()
            config = self._policy_config(rows, latest)
            old = self._policy_config(rows, previous)
            written = config is not None and config.get("enabled") is True
            retired = old is None or old.get("enabled") is False
            if written and retired:
                self._fixture.record("tag_move_to_follow_seconds", round(now - moved, 1))
                break
            self._fixture.verify(
                now < deadline,
                (
                    f"after latest moved to release {number} ({latest.content_hash}), the policy "
                    f"did not follow it within {FOLLOW_TIMEOUT_SECONDS:g} s: "
                    + (
                        "no enabled policy-owned config names the new release"
                        if not written
                        else "the previous release's policy-owned config is still enabled"
                    )
                ),
                previous=asdict(previous),
                latest=asdict(latest),
                pools=self._pool_reads[-1],
            )
            self._wait(POLL_SECONDS)
        config_id = self.await_ready_pool(latest, since=moved)
        self._fixture.record("tag_move_to_ready_seconds", round(self._clock() - moved, 1))
        return config_id

    # --- creates ---------------------------------------------------------------

    def expect_warm_claim(self, release: Release, config_id: str) -> Claim:
        """A create of the template claims the config's ready slot and runs `release`."""

        number = self._number(release)
        before = self._control.warm_pool_candidate(config_id)
        self._fixture.record(f"release_{number}_candidate", asdict(before))
        self._fixture.verify(
            before.state == "started",
            f"the ready slot of release {number}'s pool must hold a started sandbox",
            candidate=asdict(before),
        )
        sandbox = self._create()
        try:
            after: WarmPoolCandidate | None = self._control.warm_pool_claim_pod(sandbox.sandbox_id)
        except NoWarmPoolPod as error:
            # No pool pod runs the sandbox: the create booted cold. Any other
            # failure to read the pod is not evidence either way, and propagates.
            after = None
            self._fixture.record(f"release_{number}_claim_error", str(error))
        claimed = (
            after is not None
            and before.sandbox_id == sandbox.sandbox_id == after.sandbox_id
            and before.pod_uid == after.pod_uid
            and before.slot_id == after.slot_id
            and after.config_id == config_id
        )
        self._fixture.verify(
            claimed,
            f"a create of {self.alias} must claim the ready slot of release {number}'s "
            "policy-owned pool",
            sandbox_id=sandbox.sandbox_id,
            expected_config=config_id,
            candidate=asdict(before),
            claimed=None if after is None else asdict(after),
        )
        response = self._control.console_api("GET", f"{SANDBOXES_PATH}/{sandbox.sandbox_id}")
        body = response.body if isinstance(response.body, dict) else {}
        content_hash = str(body.get("content_hash") or "")
        self._fixture.record(
            f"release_{number}_sandbox",
            {"status": response.status, "template": body.get("template"), "content_hash": content_hash},
        )
        self._fixture.verify(
            response.status == 200 and content_hash == release.content_hash,
            f"the claimed sandbox must run release {number}",
            sandbox_id=sandbox.sandbox_id,
            expected_content_hash=release.content_hash,
            content_hash=content_hash,
            status=response.status,
        )
        result = sandbox.run(f"cat {MARKER_PATH}", timeout=60)
        self._fixture.verify(
            result.exit_code == 0 and result.stdout == release.marker,
            f"the claimed sandbox must hold release {number}'s content",
            sandbox_id=sandbox.sandbox_id,
            exit_code=result.exit_code,
            expected_marker=release.marker,
            marker=result.stdout[-200:],
            stderr=result.stderr[-500:],
        )
        assert after is not None
        return Claim(sandbox.sandbox_id, after.config_id, after.slot_id, content_hash)

    def _create(self) -> EcwSandbox:
        """An ECW create of the template naming no shape; cleanup kills it.

        Until the create answers, cleanup finds what it made by this call's
        own metadata, so a lost reply leaves nothing behind.
        """

        metadata = {"space_smoke_warm_pool_policy": uuid.uuid4().hex}
        by_metadata = self._scope.defer(
            "kill the policy scenario's sandbox",
            lambda: self._retry_delete(lambda: self._ecw.kill_matching(metadata)),
            critical=True,
        )
        sandbox = self._ecw.create(template=self.alias, timeout=SANDBOX_TIMEOUT_SECONDS, metadata=metadata)
        sandbox_id = sandbox.sandbox_id
        self._kills.append(
            self._scope.defer(
                f"kill sandbox {sandbox_id}",
                lambda: self._retry_delete(lambda: self._ecw.kill_sandbox(sandbox_id)),
                critical=True,
            )
        )
        by_metadata.disarm()
        return sandbox

    # --- teardown --------------------------------------------------------------

    def kill_sandboxes(self) -> None:
        while self._kills:
            self._kills[0].release()
            self._kills.pop(0)

    def disable_policy(self) -> None:
        """Disable the policy and wait, as a phase, until the template has no pool configs."""

        if self._policy_disable is not None:
            # A drain that fails here leaves the backstop armed for cleanup.
            self._disable_and_drain(DRAIN_TIMEOUT_SECONDS)
            self._policy_disable.disarm()
            self._policy_disable = None

    def delete_template(self) -> None:
        if self._template_delete is not None:
            delete, self._template_delete = self._template_delete, None
            delete.release()
        self._fixture.record("template_deleted", self.alias)

    def _disable_and_drain_in_cleanup(self) -> None:
        left = cleanup_seconds_left()
        budget = CLEANUP_DRAIN_BUDGET_SECONDS
        if left is not None:
            budget = min(budget, max(0.0, left - TEMPLATE_DELETE_BUDGET_SECONDS))
        self._disable_and_drain(budget)

    def _disable_and_drain(self, timeout: float) -> None:
        response = self._put_policy(enabled=False)
        if response.status != 200:
            raise RuntimeError(f"disabling the policy on {self.alias} answered {response.status}")
        deadline = self._clock() + timeout
        started = self._clock()
        while True:
            # Drained means no config is left: the autoscaler deletes a disabled
            # config only once no slot of any state remains, including the
            # cleanup-pending ones the listing's counts leave out, and any of
            # those would refuse the template delete.
            rows = self._pools()
            if not rows:
                self._fixture.record("drain_seconds", round(self._clock() - started, 1))
                return
            if self._clock() >= deadline:
                raise RuntimeError(
                    f"the template {self.alias} still had pool configs {timeout:g} s after the "
                    f"policy was disabled: {[_pool_summary(row) for row in rows]}"
                )
            self._wait(POLL_SECONDS)

    def _delete_template(self) -> None:
        delete_template_and_wait(self._control, self.alias, timeout=TEMPLATE_DELETE_BUDGET_SECONDS)

    def _number(self, release: Release) -> int:
        for index, known in enumerate(self._releases):
            if known.release_id == release.release_id:
                return index + 1
        raise RuntimeError(f"release {release.release_id} was not built by this run")


def _truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


def _pool_summary(row: Mapping[str, Any]) -> dict[str, object]:
    keys = (
        "id", "template_release_id", "content_hash", "managed_by", "enabled", "target_ready_slots",
        "warming_slots", "ready_slots", "claimed_slots",
    )
    return {key: row.get(key) for key in keys}


"""Admit a non-default execution environment on a Kind bench, and boot a seed in it.

A Kind node-manager runs with the binary's default execution policy, E0 and
`legacy` (`SPACE_ALLOWED_EXECUTION_ENVIRONMENTS` unset), and bootstrap stages
only E0's Firecracker. The cluster policy can narrow a node's allowlist but
never widen it ("Execution policy and compatible resolution" in
docs/execution-environments.md), so a scenario that runs another environment
has to change both, and only for itself:

1. Under the node-manager mutation Lease, it records the literal
   `SPACE_ALLOWED_EXECUTION_ENVIRONMENTS` on the worker and builder
   node-manager DaemonSets, arms their restore, adds the environment's
   compiled name and awaits both rollouts. The default stays E0, so
   unselected work on the bench keeps running E0.
2. It installs the environment's runtime release on every node that runs a
   node-manager, through runtime-manager (`space-rtmgr runtimes install`) in
   the node's operator shell. Installing only adds bytes, and a compiled
   binding's artifacts are removal-protected, so nothing undoes it.
3. It waits for the evidence that the nodes run it: each node's enforced
   allowlist in `GET /admin/api/v1/runtime-environments/nodes` lists the
   reference, and each Node carries node-ctl's `space.pplx.net/env-...=usable`
   label for it, the label the placement gate reads.

The seed is built through the admin API's system-template build with
`environment` set and no tags published (`POST
/admin/api/v1/system-templates/seed/builds`), so `latest` never moves and a
create names it by digest. Its release is deleted before the allowlist is put
back, and the restore is checked against the recorded literals, in the
scenario's phase and in cleanup alike.

Admission can be staged by node role: a scenario that needs some nodes able
to run the environment and others not (the placement gate's proof) admits
the builders first and the workers later. The first stage records and arms
the restore of every node-manager DaemonSet's allowlist; each stage widens,
installs on and awaits evidence from only its own roles' nodes.

Which environment a sandbox runs is read from the running VM itself: the
version its Firecracker executable (`/proc/<pid>/exe --version`) reports.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Collection, Iterable, Mapping
from dataclasses import dataclass
from functools import partial
from typing import Any, Protocol

from space_smoke.capabilities import ConsoleApiResponse
from space_smoke.runtime import (
    Compensation,
    ResourceScope,
    ScenarioTimedOut,
    SmokeConfigurationError,
    run_concurrently,
)

# Node-manager's policy setting (crates/l40_api/node/runtime/core/src/selection/policy.rs).
ALLOWED_ENVIRONMENTS_ENV = "SPACE_ALLOWED_EXECUTION_ENVIRONMENTS"
# What node-manager runs with when the variable is unset, on x86_64.
DEFAULT_ALLOWED_ENVIRONMENTS = ("e0", "legacy")
# node-ctl's label for each environment a node's latest complete inventory
# lists as usable (`environment_label_key` in crates/l40_api/sandbox/pod).
ENVIRONMENT_LABEL_PREFIX = "space.pplx.net/env-"
ENVIRONMENT_LABEL_DIGEST_HEX = 52
ENVIRONMENT_LABEL_VALUE = "usable"
SEED_BUILD_TERMINAL_STATES = frozenset({"succeeded", "failed", "publish_failed"})
SEED_BUILD_TIMEOUT_SECONDS = 1200.0
# How long cleanup waits for a seed build that's still running.
SEED_CLEANUP_WAIT_SECONDS = 300.0
EVIDENCE_TIMEOUT_SECONDS = 300.0
# How long the running VM's published identity may lag a restore.
RUNNING_IDENTITY_TIMEOUT_SECONDS = 60.0
POLL_SECONDS = 3.0


@dataclass(frozen=True)
class SelectableEnvironment:
    """A compiled Firecracker execution environment, as the API's catalog lists it."""

    name: str
    """Its compiled name, as node-manager's allowlist spells it."""
    runtime_version: str
    kernel_version: str
    runtime: str = "firecracker"
    architecture: str = "x86_64"

    @property
    def runtime_release(self) -> str:
        """The runtime-manager release that installs its runtime."""

        return f"fc-{self.runtime_version}"

    @property
    def executable_version(self) -> str:
        """What a VM running it reports for `<executable> --version`."""

        return f"Firecracker v{self.runtime_version}"


# The compiled bindings in docs/execution-environments.md ("Selection and
# lifecycle"). E1 reuses E0's 6.1.155 kernel, so its runtime is all a node
# lacks.
E1 = SelectableEnvironment("fc-1.17.0-linux-6.1.155", runtime_version="1.17.0", kernel_version="6.1.155")
SELECTABLE_ENVIRONMENTS: Mapping[str, SelectableEnvironment] = {E1.name: E1}
# The default environment, which every node already allows and runs. Not
# selectable for admission; a scenario names it to build seed bound to it.
E0 = SelectableEnvironment("e0", runtime_version="1.15.1", kernel_version="6.1.155")

# The node roles that run a node-manager: sandbox workers and template builders.
WORKER_ROLE = "worker"
BUILDER_ROLE = "builder"
NODE_ROLES = (WORKER_ROLE, BUILDER_ROLE)


def selectable_environment(name: str) -> SelectableEnvironment:
    try:
        return SELECTABLE_ENVIRONMENTS[name]
    except KeyError:
        raise ValueError(
            f"no selectable execution environment {name!r}; known: {sorted(SELECTABLE_ENVIRONMENTS)}"
        ) from None


def environment_label_key(reference: str) -> str:
    """node-ctl's Node label key for an environment reference (`sha256:<hex>`)."""

    algorithm, _, digest = reference.partition(":")
    if not algorithm or len(digest) < ENVIRONMENT_LABEL_DIGEST_HEX:
        raise ValueError(f"not an environment reference: {reference!r}")
    return f"{ENVIRONMENT_LABEL_PREFIX}{algorithm}-{digest[:ENVIRONMENT_LABEL_DIGEST_HEX]}"


def widened_allowlist(standing: str | None, name: str) -> str:
    """The allowlist literal that adds `name` to what node-manager runs with now.

    Unset means node-manager's default, which is spelled out so that adding
    one environment keeps E0 and `legacy`. Order and whitespace of an
    existing literal are kept; node-manager refuses a duplicate, so a literal
    already naming `name` is returned as it is.
    """

    if standing is None:
        return ",".join((*DEFAULT_ALLOWED_ENVIRONMENTS, name))
    entries = [entry.strip() for entry in standing.split(",")]
    if any(not entry for entry in entries):
        raise SmokeConfigurationError(
            f"{ALLOWED_ENVIRONMENTS_ENV}={standing!r} has an empty entry, which node-manager refuses"
        )
    if name in entries:
        return standing
    return f"{standing},{name}"


def latest_tags(releases: Iterable[Mapping[str, Any]]) -> dict[str, str]:
    """Each template's `latest` digest in a `GET /admin/api/v1/template-releases` answer."""

    return {
        entry["template_name"]: entry["content_hash"]
        for entry in releases
        if "latest" in entry.get("tags", [])
    }


def unpromoted_seed_releases(
    releases: Iterable[Mapping[str, Any]], content_hash: str, prior_ids: set[str]
) -> list[Mapping[str, Any]]:
    """The seed releases of `content_hash` a build registered, still tagged only by their digest.

    The only releases a scenario that built seed may delete: new since
    `prior_ids`, and promoted to no tag.
    """

    return [
        entry for entry in releases
        if entry["template_name"] == "seed"
        and entry["content_hash"] == content_hash
        and entry["id"] not in prior_ids
        and set(entry.get("tags", [])) <= {content_hash}
    ]


class Judge(Protocol):
    """Where evidence goes and what fails the run: a fixture, or setup's context."""

    def record(self, name: str, value: object) -> None: ...

    def verify(self, condition: bool, message: str, **evidence: Any) -> None: ...


class AdminApi(Protocol):
    def console_api(
        self,
        method: str,
        path: str,
        body: Mapping[str, object] | None = None,
        *,
        idempotency_key: str | None = None,
    ) -> ConsoleApiResponse:
        """One admin API request as the bench's operator; an error status is returned."""
        ...


class ExecutionEnvironmentControl(AdminApi, Protocol):
    """Kind-only operations behind [`ExecutionEnvironmentAdmission`]."""

    def acquire_node_manager_mutation(self) -> Callable[[], None]:
        """Take the node-manager DaemonSet mutation Lease; returns its release."""
        ...

    def node_manager_allowed_environments(self) -> dict[str, str | None]:
        """Each node-manager DaemonSet's literal allowlist, None where unset."""
        ...

    def set_node_manager_allowed_environments(self, values: Mapping[str, str | None]) -> None:
        """Write each DaemonSet's literal (None unsets) and await its rollout."""
        ...

    def node_manager_daemonsets(self) -> Mapping[str, str]:
        """Each node role's node-manager DaemonSet, keyed by [`NODE_ROLES`]."""
        ...

    def node_manager_nodes(self, roles: Collection[str] = NODE_ROLES) -> tuple[str, ...]:
        """The kube nodes running a node-manager of one of `roles`."""
        ...

    def install_runtime_release(self, node_name: str, release: str) -> str:
        """Install a runtime release through the node's runtime-manager; returns its output."""
        ...

    def nodes_carrying_label(self, key: str, value: str | None = None) -> list[str]:
        """The kube nodes carrying label `key`, with `value` when one is given."""
        ...

    def console_runtime_version(self, sandbox_id: str) -> str:
        """The version the running sandbox's runtime executable reports."""
        ...


def catalog_reference(judge: Judge, control: AdminApi, spec: SelectableEnvironment) -> str:
    """The environment's reference, from the API's catalog of qualified pairs."""

    response = control.console_api("GET", "/admin/api/v1/system-template-environments")
    catalog = response.body if response.status == 200 else None
    entries = catalog.get("environments") if isinstance(catalog, dict) else None
    matches = [
        entry for entry in entries or ()
        if isinstance(entry, dict)
        and entry.get("runtime") == spec.runtime
        and entry.get("runtime_version") == spec.runtime_version
        and entry.get("kernel_version") == spec.kernel_version
        and entry.get("architecture") == spec.architecture
    ]
    judge.verify(
        len(matches) == 1 and isinstance(matches[0].get("environment"), str),
        f"the API's catalog must qualify exactly one {spec.name} pair",
        catalog_status=response.status,
        catalog=response.body,
    )
    return str(matches[0]["environment"])


def expect_runtime(
    judge: Judge,
    control: ExecutionEnvironmentControl,
    spec: SelectableEnvironment,
    sandbox_id: str,
    *,
    step: str,
    wait: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> None:
    """The sandbox's running VM is `spec`'s runtime.

    The pid comes from the sandbox's published observed state, which a node
    publishes after a restore lands, asynchronously to the guest answering.
    So a read right after a restore can still name the previous VM's pid;
    this polls until the running executable reports the environment, for a
    minute plus the read in progress then: up to about three minutes in all
    on a bench that checks its isolation on each call. It judges the last
    version it read; only when no read succeeded is the last read's error
    raised as it is, not judged as the environment.
    """

    deadline = clock() + RUNNING_IDENTITY_TIMEOUT_SECONDS
    observed: str | None = None
    while True:
        try:
            observed = control.console_runtime_version(sandbox_id)
        except ScenarioTimedOut:
            raise
        except RuntimeError:
            # No running identity yet, or a stale pid's exec failing.
            if clock() >= deadline:
                if observed is None:
                    raise
                break
        else:
            if observed == spec.executable_version or clock() >= deadline:
                break
        wait(POLL_SECONDS)
    judge.record(f"runtime_executable_version_{step}", observed)
    judge.verify(
        observed == spec.executable_version,
        f"the sandbox must run {spec.name} {step}",
        expected=spec.executable_version,
        observed=observed,
        sandbox_id=sandbox_id,
    )


class BoundSeed:
    """Seed built bound to one environment under no tag, then deleted.

    Built through the admin API's system-template build with `environment`
    set and `publish_tags` empty, so `latest` never moves and a create names
    it by digest. The delete is armed once the build's job is known.
    """

    def __init__(
        self,
        fixture: Judge,
        control: AdminApi,
        scope: ResourceScope,
        *,
        name: str,
        reference: str,
        evidence: str = "environment_seed",
        wait: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._fixture = fixture
        self._control = control
        self._scope = scope
        self._name = name
        self._reference = reference
        self._evidence = evidence
        self._wait = wait
        self._clock = clock
        self._delete: Compensation | None = None
        self.selector: str | None = None

    @property
    def reference(self) -> str:
        """The environment reference the build binds seed to."""

        return self._reference

    def build(self) -> str:
        """Build seed bound to the environment, publish no tag, arm its delete; returns its selector."""

        if self._delete is not None or self.selector is not None:
            raise RuntimeError(f"the {self._name} seed is already built")
        releases_before = self._api("GET", "/admin/api/v1/template-releases")
        prior_ids = {entry["id"] for entry in releases_before}
        prior_hashes = {entry["content_hash"] for entry in releases_before if entry["template_name"] == "seed"}
        latest_before = latest_tags(releases_before)
        response = self._control.console_api(
            "POST",
            "/admin/api/v1/system-templates/seed/builds",
            {"environment": self._reference, "publish_tags": []},
        )
        self._fixture.verify(
            response.status == 202 and isinstance(response.body, dict) and "job_id" in response.body,
            f"the API must accept a seed build bound to {self._name}",
            status=response.status,
            body=response.body,
        )
        # Armed once the job is known. Cleanup never submits, so a build whose
        # reply was lost goes unrecovered: it leaves a release tagged only by
        # its digest. The seed rootfs isn't byte-reproducible (its mkfs.ext4
        # sets no fixed UUID), so a later build's digest differs from it and
        # its `prior_hashes` check doesn't trip on it.
        job_id = str(response.body["job_id"])
        self._delete = self._scope.defer(
            f"delete the {self._name} seed release",
            lambda: self._delete_release(job_id, prior_ids),
            critical=True,
        )
        deadline = self._clock() + SEED_BUILD_TIMEOUT_SECONDS
        while True:
            status = self._api("GET", f"/admin/api/v1/templates/seed/builds/{job_id}")
            if status.get("state") in SEED_BUILD_TERMINAL_STATES:
                break
            self._fixture.verify(
                self._clock() < deadline, f"the {self._name} seed build did not finish", build=status
            )
            self._wait(POLL_SECONDS)
        self._fixture.record(f"{self._evidence}_build", status)
        self._fixture.verify(
            status.get("state") == "succeeded", f"the {self._name} seed build must succeed", build=status
        )
        content_hash = str(status.get("content_hash") or "")
        releases = self._api("GET", "/admin/api/v1/template-releases")
        new = unpromoted_seed_releases(releases, content_hash, prior_ids)
        # A build whose digest another seed release already holds registers
        # no release of its own, so `seed@<digest>` would boot that release.
        self._fixture.verify(
            len(new) == 1 and content_hash not in prior_hashes,
            f"the {self._name} seed build must register a release of its own digest",
            content_hash=content_hash,
            new_releases=new,
            digest_already_released=content_hash in prior_hashes,
        )
        # Only seed's tag is this build's to move; other templates' promotions
        # can land meanwhile.
        self._fixture.verify(
            latest_tags(releases).get("seed") == latest_before.get("seed"),
            f"building the {self._name} seed must leave seed's latest tag unchanged",
            seed_latest_before=latest_before.get("seed"),
            seed_latest_after=latest_tags(releases).get("seed"),
        )
        self.selector = f"seed@{content_hash}"
        self._fixture.record(self._evidence, self.selector)
        return self.selector

    def delete(self) -> None:
        """Delete the release now, as a scenario phase; a no-op if it was never built."""

        if self._delete is not None:
            delete, self._delete = self._delete, None
            delete.release()

    def _delete_release(self, job_id: str, prior_ids: set[str]) -> None:
        """Delete the build's unpromoted release, waiting a bounded time for a running build."""

        deadline = self._clock() + SEED_CLEANUP_WAIT_SECONDS
        while True:
            status = self._api("GET", f"/admin/api/v1/templates/seed/builds/{job_id}")
            if status.get("state") in SEED_BUILD_TERMINAL_STATES:
                break
            if self._clock() >= deadline:
                raise SmokeConfigurationError(
                    f"seed build {job_id} is still running; delete its release once it finishes"
                )
            self._wait(POLL_SECONDS)
        releases = self._api("GET", "/admin/api/v1/template-releases")
        for entry in unpromoted_seed_releases(releases, str(status.get("content_hash")), prior_ids):
            deleted = self._control.console_api("DELETE", f"/admin/api/v1/template-releases/{entry['id']}")
            if deleted.status == 204:
                continue
            # 409: missing, still tagged `latest`, or still referenced. Only missing is done.
            remaining = {release["id"] for release in self._api("GET", "/admin/api/v1/template-releases")}
            if entry["id"] in remaining:
                raise RuntimeError(
                    f"delete template release {entry['id']} answered {deleted.status}: {deleted.body!r:.300}"
                )

    def _api(self, method: str, path: str) -> Any:
        return admin_json(self._control, method, path)


def admin_json(control: AdminApi, method: str, path: str) -> Any:
    """One admin API request's JSON body; any status but 200 is a configuration failure."""

    response = control.console_api(method, path)
    if response.status != 200:
        raise SmokeConfigurationError(
            f"admin {method} {path.split('?', 1)[0]} answered {response.status}: {response.body!r:.300}"
        )
    return response.body


class AllowlistNotRestored(RuntimeError):
    """A node-manager DaemonSet doesn't carry its recorded allowlist after the restore."""


class ExecutionEnvironmentAdmission:
    """One non-default environment admitted for one scenario, then withdrawn."""

    def __init__(
        self,
        fixture: Judge,
        control: ExecutionEnvironmentControl,
        scope: ResourceScope,
        name: str,
        *,
        wait: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._fixture = fixture
        self._control = control
        self._scope = scope
        self.environment = selectable_environment(name)
        self._wait = wait
        self._clock = clock
        self._reference: str | None = None
        self._standing: dict[str, str | None] | None = None
        self._admitted: set[str] = set()
        self._widened: set[str] = set()
        self._restore: Compensation | None = None
        self._seed: BoundSeed | None = None

    @property
    def reference(self) -> str:
        if self._reference is None:
            raise RuntimeError("the environment has not been resolved")
        return self._reference

    @property
    def admitted_roles(self) -> frozenset[str]:
        """The node roles admitted so far."""

        return frozenset(self._admitted)

    # --- admission ---------------------------------------------------------

    def admit(self, roles: Collection[str] = NODE_ROLES) -> None:
        """Allow and install the environment on the nodes of `roles`, then wait for its evidence there.

        The first call records every node-manager DaemonSet's allowlist and
        arms its restore, so a later call that admits the remaining roles
        widens a DaemonSet whose standing literal is already recorded.
        """

        unknown = sorted(set(roles) - set(NODE_ROLES))
        if unknown or not roles:
            raise ValueError(f"admit needs some of the node roles {NODE_ROLES}, not {sorted(roles)}")
        if self._standing is None:
            self._begin()
        pending = [role for role in NODE_ROLES if role in roles and role not in self._admitted]
        if not pending:
            return
        standing = self._standing
        assert standing is not None
        daemonsets = self._control.node_manager_daemonsets()
        widened = {
            daemonsets[role]: widened_allowlist(standing[daemonsets[role]], self.environment.name)
            for role in pending
        }
        # Marked before the write: one that half-applies is still put back.
        self._widened.update(widened)
        self._control.set_node_manager_allowed_environments(widened)
        self._admitted.update(pending)
        self._fixture.record("execution_environment_admitted_roles", sorted(self._admitted))
        # Each role must have nodes of its own: a union would let one role's
        # nodes stand in for another's that the bench doesn't run.
        missing = [role for role in pending if not self._control.node_manager_nodes([role])]
        if missing:
            raise SmokeConfigurationError(
                f"the bench runs no {'/'.join(missing)} node-manager pod to admit the environment on"
            )
        nodes = self._control.node_manager_nodes(pending)
        release_name = self.environment.runtime_release
        # Each node downloads the release itself, so the installs run at once;
        # an interrupt kills them rather than waiting for them.
        outcomes = run_concurrently(
            [partial(self._control.install_runtime_release, node, release_name) for node in nodes]
        )
        self._fixture.record(
            f"runtime_release_installs_{'_'.join(pending)}",
            {node: (repr(out) if isinstance(out, BaseException) else out)[-2_000:] for node, out in zip(nodes, outcomes)},
        )
        for outcome in outcomes:
            if isinstance(outcome, BaseException):
                raise outcome
        self._await_evidence(nodes, "_".join(pending))

    def _begin(self) -> None:
        """Resolve the reference, take the Lease, record every allowlist and arm its restore."""

        self._reference = catalog_reference(self._fixture, self._control, self.environment)
        self._fixture.record("execution_environment", {"name": self.environment.name, "reference": self._reference})
        release = self._control.acquire_node_manager_mutation()
        release_lease = self._scope.defer(
            "release the node-manager mutation lease", release, critical=True
        )
        standing = self._control.node_manager_allowed_environments()
        self._fixture.record("node_manager_allowlist_before", standing)
        # No Kind chart sets the allowlist, so one that already names the
        # environment is what an earlier run failed to put back; taking it as
        # standing would make it permanent.
        if any(
            value is not None and self.environment.name in (entry.strip() for entry in value.split(","))
            for value in standing.values()
        ):
            raise SmokeConfigurationError(
                f"a node-manager DaemonSet already allows {self.environment.name}; no Kind chart sets "
                f"it, so an earlier run left it behind; unset {ALLOWED_ENVIRONMENTS_ENV} and rerun"
            )
        self._standing = dict(standing)
        # Armed before any write. The restore owns the Lease from here and
        # releases it only once the allowlist is back, so a failed restore
        # keeps the next run out instead of handing it a widened allowlist.
        self._restore = self._scope.defer(
            "restore the node-manager execution allowlist",
            lambda: self._put_back(standing, release),
            critical=True,
        )
        release_lease.disarm()

    def _await_evidence(self, nodes: tuple[str, ...], stage: str) -> None:
        reference = self.reference
        label = environment_label_key(reference)
        deadline = self._clock() + EVIDENCE_TIMEOUT_SECONDS
        while True:
            enforcing = self._nodes_enforcing(reference)
            labelled = set(self._control.nodes_carrying_label(label, ENVIRONMENT_LABEL_VALUE))
            missing_policy = sorted(set(nodes) - enforcing)
            missing_label = sorted(set(nodes) - labelled)
            if not missing_policy and not missing_label:
                self._fixture.record(f"environment_evidence_nodes_{stage}", sorted(nodes))
                return
            self._fixture.verify(
                self._clock() < deadline,
                f"not every node publishes {self.environment.name} as allowed and usable "
                "(a cluster execution policy can also narrow it out of what a node enforces)",
                nodes=sorted(nodes),
                not_enforcing=missing_policy,
                without_usable_label=missing_label,
                label=label,
            )
            self._wait(POLL_SECONDS)

    def _nodes_enforcing(self, reference: str) -> set[str]:
        """Nodes whose fresh published policy allows `reference`, by kube node name."""

        enforcing: set[str] = set()
        after: str | None = None
        while True:
            path = "/admin/api/v1/runtime-environments/nodes" + (f"?after={after}" if after else "")
            page = self._api("GET", path)
            for node in page.get("nodes", ()) if isinstance(page, dict) else ():
                enforced = node.get("enforced") or {}
                name = node.get("substrate_node_id") or node.get("name")
                if (
                    isinstance(name, str)
                    and node.get("status_fresh") is True
                    and reference in (enforced.get("allowed_environment_references") or ())
                ):
                    enforcing.add(name)
            after = page.get("next_after") if isinstance(page, dict) else None
            if not after:
                return enforcing

    # --- the seed ----------------------------------------------------------

    def build_seed(self) -> str:
        """Build seed bound to the environment, publish no tag, arm its delete; returns its selector."""

        if self._seed is not None:
            raise RuntimeError(f"the {self.environment.name} seed is already built")
        self._seed = BoundSeed(
            self._fixture,
            self._control,
            self._scope,
            name=self.environment.name,
            reference=self.reference,
            wait=self._wait,
            clock=self._clock,
        )
        return self._seed.build()

    # --- what runs ---------------------------------------------------------

    def expect_running(self, sandbox_id: str, *, step: str) -> None:
        """The sandbox's running VM is this environment's runtime ([`expect_runtime`])."""

        expect_runtime(
            self._fixture, self._control, self.environment, sandbox_id,
            step=step, wait=self._wait, clock=self._clock,
        )

    # --- withdrawal --------------------------------------------------------

    def _put_back(self, standing: Mapping[str, str | None], release_lease: Callable[[], None]) -> None:
        """Write the recorded literals back, read them again, then release the Lease.

        Only DaemonSets a stage widened are written; every one is read back.
        Raises, keeping the Lease, if they didn't take or a write failed: a
        failed rollout can leave pods running the widened allowlist.
        """

        # One DaemonSet at a time, so a failed write to one still puts the other back.
        failures: list[Exception] = []
        for daemonset, literal in standing.items():
            if daemonset not in self._widened:
                continue
            try:
                self._control.set_node_manager_allowed_environments({daemonset: literal})
            except ScenarioTimedOut:
                raise
            except Exception as error:  # noqa: BLE001 - raised below, after the read-back
                failures.append(error)
        after = self._control.node_manager_allowed_environments()
        if after != standing:
            raise AllowlistNotRestored(
                f"the node-manager DaemonSets carry {after}, not their standing allowlist {dict(standing)}"
            ) from (failures[0] if failures else None)
        if failures:
            raise failures[0]
        release_lease()

    def withdraw(self) -> None:
        """Delete the seed, then put the allowlist back and check it took, as a scenario phase.

        Call it once the sandbox that runs the environment is deleted. Cleanup
        does the same if the scenario never gets here, but a phase judges the
        restore as part of the verdict.
        """

        if self._seed is not None:
            self._seed.delete()
        if self._restore is not None:
            try:
                self._restore.release()
            except AllowlistNotRestored as error:
                # Still armed, so cleanup writes the literals again.
                self._fixture.verify(False, str(error))
            self._restore = None

    def _api(self, method: str, path: str) -> Any:
        return admin_json(self._control, method, path)

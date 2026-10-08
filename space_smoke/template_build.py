"""A public `Template.build` from an OCI image, through the kind builder path, booted and inspected.

The scenario submits a Dockerfile through the E2B SDK, builds and publishes a
bootable template from an OCI image with the SPACE guest contract injected,
and verifies a sandbox created from the alias. It checks the Dockerfile's state,
the SDK's environment and start command, and SPACE's guest services.

The scenario verifies three cross-service contracts:

1. **Owner-pinned status polling.** The kind cluster runs two builders behind
   one Service. The API uses that Service only for the unpinned calls (the
   submit, the file upload); status polls are routed to the builder that
   accepted the job — `find_builder_node(job_id)` then
   `builder_client_for_node`, which resolves the accepting builder's own
   node-api directly (`controllers/template.rs`,
   `substrate/kubernetes/adapter/substrate_adapter.rs`). The scenario submits
   with the Service pinned to one builder, then, once a builder has accepted
   the alias and committed the prepared rootfs, moves the Service to the
   *other* builder for the rest of the build. A build that still completes
   shows the polls did not depend on where the Service pointed, which is the
   owner-pinned routing working; `service_routed_to` records where the Service
   pointed, not who answered the polls (that is not observable from outside
   the API).
2. **Published artefacts.** On the accepting builder, the published template's
   `metadata.json` names the `spaced` bundle version actually installed in the
   rootfs (`/etc/spaced/bundle-version`, read with `debugfs`) and the
   compatibility family derived from it; the `space-builds` containerd namespace
   holds no source-image record, no temporary build image and no temporary
   assembly path once the build is done.
3. **The guest.** A sandbox created from the alias through the public API runs
   checks through the public data plane: Dockerfile
   COPY/RUN/USER/WORKDIR state, Dockerfile and SDK `ENV` in `/etc/environment`,
   the SDK start command's marker, SPACE guest binaries and `spaced-svc` under
   systemd with `space.target` as default, the UID 2000 user, filesystem I/O,
   process execution, rootfs capacity, injected SSH keys, and the source image's
   own userspace (Python 3.12 on bookworm).

Coverage is limited to the Kind OCI-build path and public API/data-plane
operations. It does not exercise node-local CLI/SSH launches, seed-based
`from_template` recipes, or remote deployment targets. ECW pause/resume behavior
is covered by the separate lifecycle scenarios. The SDK version is pinned in
`pyproject.toml`; this scenario does not accept an `E2B_SDK_VERSION` override.
"""

from __future__ import annotations

import secrets
import time
from collections.abc import Mapping
from typing import TYPE_CHECKING

from space_smoke.capabilities import (
    EcwSandbox,
    GuestMetadata,
    TemplateBuildControl,
    TemplateBuildResult,
    TemplateBuilder,
    TemplateRecipe,
)
from space_smoke.runtime import BackgroundWork, Compensation, ResourceScope, background

if TYPE_CHECKING:
    from space_smoke.fixtures import KindEcw

OCI_IMAGE = "docker.io/library/python:3.12-slim-bookworm"
SCENARIO_ID = "local.ecw_template_build_from_oci_boots"

COPY_MARKER_TEXT = "hello from ecw template build smoke\n"
START_MARKER_PATH = "/tmp/ecw-template-build-smoke-start-marker.txt"
START_MARKER_TEXT = "started"
ENV_MARKER_NAME = "ECW_TEMPLATE_BUILD_SMOKE"
ENV_MARKER_VALUE = "1"
OCI_COPY_MARKER_PATH = "/opt/ecw-oci-smoke/copy-marker.txt"
OCI_BUILD_MARKER_PATH = "/opt/ecw-oci-smoke/build-marker.txt"
OCI_USER_MARKER_PATH = "/home/user/ecw-oci-user-marker.txt"
OCI_DOCKERFILE_ENV_NAME = "ECW_OCI_DOCKERFILE_ENV"
OCI_DOCKERFILE_ENV_VALUE = "from-dockerfile"
# Widens the window between acceptance and completion so the Service can be
# moved to the peer while the build is still running (the shell suite's 15 s).
BUILD_DELAY_SECONDS = 15
# The scope's cleanup budget is min(120 s, 10 % of the scenario timeout); every
# compensation here must fit inside it together: settle ≤ 30 s, template delete
# ≤ 60 s, route clear and sandbox kill a few seconds each.
SETTLE_BUDGET_SECONDS = 30.0
TEMPLATE_DELETE_BUDGET_SECONDS = 60.0
# The accepting builder must commit the prepared rootfs within this budget. It
# is probed in short windows so a build that has already failed surfaces its
# own error instead of a missing rootfs at the end of the budget.
HANDOFF_BUDGET_SECONDS = 60.0
HANDOFF_PROBE_WINDOW_SECONDS = 5


def oci_recipe(image: str = OCI_IMAGE) -> TemplateRecipe:
    """The shell suite's Dockerfile-plus-SDK recipe for a public OCI image, unchanged in substance."""

    dockerfile = (
        f"FROM {image}\n"
        f"ENV {OCI_DOCKERFILE_ENV_NAME}={OCI_DOCKERFILE_ENV_VALUE}\n"
        # The Debian fixtures install the SSH client and run a later build
        # step over SSH, which is how the builder drives RUN after the base
        # image is assembled.
        "RUN apt-get update && DEBIAN_FRONTEND=noninteractive "
        "apt-get install -y --no-install-recommends openssh-client </dev/null\n"
        "RUN true\n"
        "WORKDIR /opt/ecw-oci-smoke\n"
        f"COPY copy-marker.txt {OCI_COPY_MARKER_PATH}\n"
        f'RUN test "$(cat {OCI_COPY_MARKER_PATH})" = "{COPY_MARKER_TEXT.rstrip()}"\n'
        f"RUN printf build-step-ran > {OCI_BUILD_MARKER_PATH}\n"
        "USER user\n"
        "WORKDIR /home/user\n"
        f"RUN printf user-step-ran > {OCI_USER_MARKER_PATH}\n"
        "USER root\n"
        "WORKDIR /\n"
    )
    return TemplateRecipe(
        dockerfile=dockerfile,
        files={"copy-marker.txt": COPY_MARKER_TEXT},
        envs={ENV_MARKER_NAME: ENV_MARKER_VALUE},
        run_cmds=(
            (
                f'test "${OCI_DOCKERFILE_ENV_NAME}" = "{OCI_DOCKERFILE_ENV_VALUE}" '
                f'&& test "${ENV_MARKER_NAME}" = "{ENV_MARKER_VALUE}"',
                "root",
            ),
            (f"sleep {BUILD_DELAY_SECONDS}", "root"),
        ),
        start_cmd=f"sh -c 'echo {START_MARKER_TEXT} > {START_MARKER_PATH}; tail -f /dev/null'",
        ready_timeout_seconds=2,
    )


def guest_checks(image: str = OCI_IMAGE) -> dict[str, str]:
    """Shell checks run inside the booted guest; each must exit 0. Order is the report order."""

    checks: dict[str, str] = {}
    if image == OCI_IMAGE:
        checks["source image Python"] = (
            'test "$(readlink -f /usr/local/bin/python3)" = "/usr/local/bin/python3.12" && '
            "python3 -c 'import sys; assert sys.version_info[:2] == (3, 12)'"
        )
        checks["source image OS"] = "grep -q '^VERSION_CODENAME=bookworm$' /etc/os-release"
    checks.update(
        {
            "Dockerfile COPY and RUN": (
                f'test "$(cat {OCI_COPY_MARKER_PATH})" = "{COPY_MARKER_TEXT.rstrip()}" && '
                f'test "$(cat {OCI_BUILD_MARKER_PATH})" = build-step-ran'
            ),
            "Dockerfile USER and WORKDIR": (
                f'test "$(cat {OCI_USER_MARKER_PATH})" = user-step-ran && '
                f'test "$(stat -c %U {OCI_USER_MARKER_PATH})" = user'
            ),
            "Dockerfile and SDK environment": (
                f"grep -qx '{OCI_DOCKERFILE_ENV_NAME}={OCI_DOCKERFILE_ENV_VALUE}' /etc/environment && "
                f"grep -qx '{ENV_MARKER_NAME}={ENV_MARKER_VALUE}' /etc/environment"
            ),
            "SDK start command": f'test "$(cat {START_MARKER_PATH})" = "{START_MARKER_TEXT}"',
            "SPACE guest binaries": (
                "test -x /usr/local/bin/spaced && test -x /usr/local/bin/spaced-svc && "
                "test -s /etc/spaced/version && /usr/local/bin/spaced --version"
            ),
            "SPACE guest service": (
                'test "$(cat /proc/1/comm)" = systemd && '
                "found=; for comm in /proc/[0-9]*/comm; do "
                'test "$(cat "$comm")" = spaced-svc && found=1; done; '
                'test "$found" = 1 && '
                "systemctl --root=/ is-enabled --quiet spaced.service && "
                'test "$(readlink -f /etc/systemd/system/default.target)" = '
                '"/etc/systemd/system/space.target"'
            ),
            "sandbox user": (
                'test "$(id -u user)" = 2000 && '
                "sudo -u user sh -lc 'test \"$HOME\" = /home/user && "
                "touch /home/user/user-owned && "
                "test \"$(stat -c %U /home/user/user-owned)\" = user'"
            ),
            "filesystem IO": (
                "mkdir -p /tmp/oci-smoke/nested && "
                "printf 'one\\ntwo\\nthree\\n' > /tmp/oci-smoke/nested/input && "
                "tr '[:lower:]' '[:upper:]' < /tmp/oci-smoke/nested/input > /tmp/oci-smoke/output && "
                "test \"$(tr '\\n' : < /tmp/oci-smoke/output)\" = ONE:TWO:THREE:"
            ),
            "process execution": (
                "ECW_OCI_SMOKE=works sh -c '"
                'test "$ECW_OCI_SMOKE" = works && test "$(id -u)" = 2000 && '
                "sleep 0.1 & child=$!; wait \"$child\"'"
            ),
            "rootfs capacity": (
                "dd if=/dev/zero of=/tmp/oci-smoke/payload bs=1M count=8 status=none && "
                'test "$(stat -c %s /tmp/oci-smoke/payload)" = 8388608 && rm /tmp/oci-smoke/payload'
            ),
            "SSH injection": (
                "sudo test -s /root/.ssh/authorized_keys && test -s /home/user/.ssh/authorized_keys && "
                "sudo cmp /root/.ssh/authorized_keys /home/user/.ssh/authorized_keys"
            ),
        }
    )
    return checks


def compatibility_family(bundle_version: str) -> str:
    """The family the builder derives from a `spaced` bundle version (the shell oracle)."""

    core = bundle_version.split("+", 1)[0].split("-", 1)[0]
    parts = core.split(".")
    major = parts[0] if parts else ""
    minor = parts[1] if len(parts) > 1 else "0"
    patch = parts[2] if len(parts) > 2 else "0"
    if major == "0":
        return f"0.0.{patch}" if minor == "0" else f"0.{minor}"
    return major


def metadata_matches_bundle(metadata: GuestMetadata) -> str | None:
    """None when the published metadata agrees with the installed bundle; else the mismatch."""

    if not metadata.bundle_version:
        return "the rootfs carries no /etc/spaced/bundle-version"
    if metadata.spaced_version != metadata.bundle_version:
        return "metadata.json spaced_version differs from the installed bundle version"
    expected = compatibility_family(metadata.bundle_version)
    if metadata.compatibility_family != expected:
        return f"metadata.json spaced_compatibility_family is not {expected}"
    return None


class TemplateBuildAssertions:
    """Pin the builders, build through the SDK, inspect the artefacts, boot the result."""

    def __init__(
        self,
        fixture: KindEcw,
        control: TemplateBuildControl,
        builder: TemplateBuilder,
        scope: ResourceScope,
        *,
        image: str = OCI_IMAGE,
    ) -> None:
        self._fixture = fixture
        self._control = control
        self._builder = builder
        self._scope = scope
        self._image = image
        self.alias = f"space-smoke-oci-{secrets.token_hex(4)}"
        self._submit: str | None = None
        self._peer: str | None = None
        self._route_clear: Compensation | None = None
        self._template_delete: Compensation | None = None
        self._kill_sandbox: Compensation | None = None
        self._result: TemplateBuildResult | None = None
        self._sandbox: EcwSandbox | None = None
        self._accepted: str | None = None
        self._settle_build: Compensation | None = None

    # --- builders -------------------------------------------------------------

    def pin_builders(self) -> tuple[str, str]:
        """Two Ready builders; the Service is routed to the first (the route clear is armed first)."""

        builders = self._control.builder_nodes()
        self._fixture.verify(
            len(builders) == 2,
            "the handoff proof needs exactly two builders with a Ready node-api",
            builder_nodes=builders,
        )
        self._submit, self._peer = builders[0], builders[1]
        self._route_clear = self._scope.defer(
            "clear the builder Service route", self._control.clear_builder_service_route, critical=True
        )
        self._control.clear_builder_service_route()
        self._control.route_builder_service("submit", self._submit)
        self._fixture.record("pinned_submit_builder", self._submit)
        self._fixture.record("builder_nodes", list(builders))
        return self._submit, self._peer

    # --- the build ------------------------------------------------------------

    def build_through_sdk(
        self,
        *,
        cpu_count: int = 1,
        memory_mb: int = 1024,
        accept_timeout: float = 60,
        build_timeout: float = 600,
    ) -> TemplateBuildResult:
        """Build on a background thread; move the Service to the peer once the submit builder accepted."""

        submit, peer = self._require_builders()
        self._template_delete = self._scope.defer(
            f"delete the built template {self.alias}", self._delete_template, critical=True
        )
        recipe = oci_recipe(self._image)
        self._fixture.record("template_alias", self.alias)
        self._fixture.record("oci_image", self._image)
        holder: list[TemplateBuildResult] = []

        def build() -> None:
            holder.append(
                self._builder.build_template(
                    recipe,
                    alias=self.alias,
                    cpu_count=cpu_count,
                    memory_mb=memory_mb,
                    tags=("latest", "kind-smoke"),
                )
            )

        work: BackgroundWork = background("public template build", build)
        # Armed after the template delete, so it unwinds before it: on any
        # failure path below (acceptance timeout, missing prepared rootfs, a
        # routing error) the scope must not delete the alias while the SDK is
        # still submitting or building it. It first clears the Service route
        # (cheap, safe at any time) so a stalled build can never leave the
        # cluster pinned, then waits at most SETTLE_BUDGET_SECONDS — inside the
        # scope's cleanup budget, so the template delete still gets its turn.
        # The happy path disarms this once its own join has returned.
        self._settle_build = self._scope.defer(
            "unpin the builder Service and wait for the background build to settle",
            lambda: self._unpin_and_settle(work),
            critical=True,
        )
        deadline = time.monotonic() + accept_timeout
        accepted: str | None = None
        while accepted is None:
            accepted = next(
                (b for b in (submit, peer) if self._control.builder_logged_alias(b, self.alias)),
                None,
            )
            if accepted is not None:
                break
            if not work.running:
                # A build that already ended never reached a builder; join
                # re-raises its failure with the SDK's own message.
                work.join(timeout=1)
            self._fixture.verify(
                time.monotonic() < deadline,
                "no builder accepted the build within the budget",
                pinned_submit_builder=submit,
                builder_nodes=[submit, peer],
                alias=self.alias,
            )
            self._fixture.wait(0.5)
        # The Service pin is best effort: the API keeps its connections to the
        # builder Service alive across requests, so a build can land on the
        # builder a previous run left it talking to (bench 2026-09-23, once in
        # two runs). The proof needs the Service pointed AWAY from the accepting
        # builder while the owner-pinned polls continue, so the reroute target is
        # whichever builder did not accept; whether the pin held is evidence,
        # not a verdict.
        other = peer if accepted == submit else submit
        self._accepted = accepted
        self._fixture.record("accepted_by", accepted)
        self._fixture.record("poll_peer_builder", other)
        self._fixture.record("pinned_submit_honoured", accepted == submit)
        handoff_error = self._await_prepared_rootfs(accepted, work)
        self._fixture.verify(
            handoff_error is None,
            "the accepting builder did not commit a prepared rootfs for the build",
            detail=handoff_error,
        )
        self._control.route_builder_service("poll-peer", other)
        self._fixture.record("service_routed_to", other)
        try:
            work.join(timeout=build_timeout)
        finally:
            if self._route_clear is not None:
                clear, self._route_clear = self._route_clear, None
                clear.release()
        if self._settle_build is not None:
            self._settle_build.disarm()
            self._settle_build = None
        self._fixture.verify(
            bool(holder),
            "the build finished without a result",
            alias=self.alias,
        )
        self._result = holder[0]
        self._fixture.record("build_seconds", self._result.elapsed_seconds)
        self._fixture.record("template_id", self._result.template_id)
        self._fixture.record("build_id", self._result.build_id)
        self._fixture.record("build_log_tail", list(self._result.log_tail))
        return self._result

    def _await_prepared_rootfs(self, builder: str, work: BackgroundWork) -> str | None:
        """None once `builder` committed the build's prepared rootfs; else the probe's failure text.

        A build that ended before the handoff re-raises its own failure, with the
        SDK's message, rather than being reported as a missing rootfs.
        """

        deadline = time.monotonic() + HANDOFF_BUDGET_SECONDS
        while True:
            error = self._control.prepared_rootfs_handoff(
                builder, window_seconds=HANDOFF_PROBE_WINDOW_SECONDS
            )
            if error is None:
                return None
            if not work.running:
                # A build that succeeded without a probe seeing its rootfs
                # still fails the handoff below.
                work.join(timeout=1)
                return error
            if time.monotonic() >= deadline:
                return error

    # --- artefacts on the builder --------------------------------------------

    def expect_guest_metadata_matches_bundle(self) -> None:
        metadata = self._control.published_guest_metadata(self._require_accepted(), self.alias)
        mismatch = metadata_matches_bundle(metadata)
        self._fixture.verify(
            mismatch is None,
            f"published guest metadata does not match the installed bundle: {mismatch or ''}",
            spaced_version=metadata.spaced_version,
            compatibility_family=metadata.compatibility_family,
            bundle_version=metadata.bundle_version,
        )
        self._fixture.record("spaced_bundle_version", metadata.bundle_version)

    def expect_containerd_clean(self) -> None:
        residue = self._control.containerd_residue(self._require_accepted(), self._image)
        self._fixture.verify(
            residue == [],
            "the build left containerd records or temporary paths behind",
            residue=residue,
        )

    # --- the guest ------------------------------------------------------------

    def boot_and_verify_guest(self, *, sandbox_timeout: int = 300) -> str:
        """Create a sandbox from the alias through the public API and run every guest check."""

        sandbox = self._builder.create(
            template=self.alias,
            timeout=sandbox_timeout,
            metadata={
                "space_smoke_scenario": SCENARIO_ID,
                "space_smoke_role": "template-build-boot",
                "space_smoke_run": secrets.token_hex(8),
            },
        )
        self._sandbox = sandbox
        self._kill_sandbox = self._scope.defer(
            f"kill the sandbox booted from {self.alias}", sandbox.kill, critical=True
        )
        self._fixture.record("boot_sandbox_id", sandbox.sandbox_id)
        passed: list[str] = []
        for name, command in guest_checks(self._image).items():
            result = sandbox.run(command, timeout=60)
            self._fixture.verify(
                result.exit_code == 0,
                f"guest check failed: {name}",
                check=name,
                exit_code=result.exit_code,
                stdout=result.stdout[-1_000:],
                stderr=result.stderr[-1_000:],
            )
            passed.append(name)
        self._fixture.record("guest_checks_passed", passed)
        return sandbox.sandbox_id

    # --- teardown -------------------------------------------------------------

    def retire(self, *, timeout: float = 180) -> None:
        """Kill the booted sandbox, then delete the template and wait for its operation."""

        if self._kill_sandbox is not None:
            kill, self._kill_sandbox = self._kill_sandbox, None
            kill.release()
        if self._template_delete is not None:
            delete, self._template_delete = self._template_delete, None
            delete.release()
        self._fixture.record("template_deleted", self.alias)
        del timeout  # the delete waits inside `_delete_template`

    def _unpin_and_settle(self, work: BackgroundWork) -> None:
        """Clear the Service route, then wait (bounded) for the SDK build thread to end.

        The route clear comes first and unconditionally: it is two kubectl calls
        and is what keeps a stalled build from leaving the cluster pinned. The
        thread ending is what makes the template delete safe to issue (the SDK
        is no longer submitting or uploading); a thread still alive at the
        settle budget is a critical cleanup failure, and the delete that follows
        still runs because the wait is bounded well inside the scope's budget.
        A daemon-side job the SDK already accepted is the admin delete
        operation's to reconcile.
        """

        if self._route_clear is not None:
            clear, self._route_clear = self._route_clear, None
            clear.release()
        self._settle(work, timeout=SETTLE_BUDGET_SECONDS)

    @staticmethod
    def _settle(work: BackgroundWork, *, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        while work.running and time.monotonic() < deadline:
            time.sleep(0.5)
        if work.running:
            raise RuntimeError(
                f"the background build was still running {timeout:g} s after cleanup began; "
                "the template delete that follows may race an active build"
            )

    def _delete_template(self, *, timeout: float = TEMPLATE_DELETE_BUDGET_SECONDS) -> None:
        operation_id = self._control.delete_template_as_admin(self.alias)
        if operation_id is None:
            return
        deadline = time.monotonic() + timeout
        while True:
            state = self._control.operation_state(operation_id)
            if state == "succeeded":
                return
            if state in ("failed", "canceled", "expired") or time.monotonic() >= deadline:
                raise RuntimeError(
                    f"template delete operation {operation_id} for {self.alias} ended {state or 'pending'}"
                )
            time.sleep(3)

    def _require_accepted(self) -> str:
        if self._accepted is None:
            raise RuntimeError("build_through_sdk() must run first")
        return self._accepted

    def _require_builders(self) -> tuple[str, str]:
        if self._submit is None or self._peer is None:
            raise RuntimeError("pin_builders() must run first")
        return self._submit, self._peer


def summarize_checks(checks: Mapping[str, str]) -> list[str]:
    return list(checks)

"""Run-owned multi-tenant builds using the public ECW and Admin APIs."""

from __future__ import annotations

import re
import secrets
import time
import uuid
from dataclasses import replace

from space_smoke.adapters import (
    EcwConfig,
    EcwControlPlaneHttpError,
    EcwSdk,
    KindHarness,
    _ecw_control_plane_json,
)
from space_smoke.fixtures import retry_sandbox_delete
from space_smoke.runtime import ResourceScope, ScenarioContext, SmokeConfigurationError


class PrivateTemplateBuildAssertions:
    def __init__(
        self, context: ScenarioContext, kind: KindHarness, config: EcwConfig
    ) -> None:
        self.context = context
        self.kind = kind
        self.config = config
        self.prefix = f"smoke-base-{uuid.uuid4().hex[:16]}"
        self.base = self.prefix + "-private"
        self.marker = secrets.token_hex(16)
        self.marker_path = "/tmp/private-build-sentinel"
        self.resources = ResourceScope()
        self.cleanup_guard = context.scope.defer(
            "clean up multi-tenant build resources", self._cleanup, critical=True
        )
        self.owner: EcwConfig | None = None
        self.foreign: EcwConfig | None = None
        self.digest = ""

    def _key(self, org_id: str, user_id: str) -> EcwConfig:
        key = secrets.token_hex(32)
        self.resources.defer(
            "delete run-owned build key",
            lambda: self.kind.delete_api_key(key),
            critical=True,
        )
        self.kind.seed_api_key(key, org_id=org_id, user_id=user_id)
        return replace(self.config, api_key=key)

    def prepare(self) -> None:
        admin = self.kind.admin_request
        foreign_org = self.kind.environment.get("SPACE_SMOKE_BUILD_FOREIGN_ORG_ID")
        if not foreign_org:
            raise SmokeConfigurationError(
                "set SPACE_SMOKE_BUILD_FOREIGN_ORG_ID to a dedicated test organization"
            )
        foreign_org = str(uuid.UUID(foreign_org))
        owner_id = admin("GET", "cluster")["managing_org_id"]
        owner_users = admin("GET", f"orgs/{owner_id}/users")
        foreign_users = admin("GET", f"orgs/{foreign_org}/users")
        self.context.verify(bool(owner_users), "managing org has no build owner")
        self.context.verify(bool(foreign_users), "foreign test org has no service user")
        self.context.verify(owner_id != foreign_org, "build keys share an organization")
        self.owner = self._key(owner_id, owner_users[0]["id"])
        self.foreign = self._key(foreign_org, foreign_users[0]["id"])
        self.context.record("build_owner_org", owner_id)
        self.context.record("build_foreign_org", foreign_org)
        self._own_template(self.base)
        admin("POST", "templates", {"name": self.base}, expected=(201,))
        admin(
            "PUT",
            f"templates/{self.base}:visibility",
            {"visibility": "org_specific"},
        )
        self._build(
            self.owner,
            self.base,
            "seed:latest",
            f"printf %s {self.marker} > {self.marker_path}",
        )
        alias = self._ecw(self.owner, "GET", f"/templates/aliases/{self.base}")
        self.context.verify(alias["public"] is False, "base is not private")
        releases = admin("GET", "template-releases")
        matches = [row for row in releases if row["template_name"] == self.base]
        self.context.verify(len(matches) == 1, "private base release is ambiguous")
        self.digest = matches[0]["content_hash"]
        self.context.verify(
            re.fullmatch(r"sha256:[0-9a-f]{64}", self.digest) is not None,
            "private base publication has no full immutable digest",
        )
        self.context.record("private_base", self.base)
        self.context.record("private_base_digest", self.digest)

    def build_shared_control(self) -> None:
        assert self.foreign is not None
        name = self.prefix + "-shared-control"
        self._own_template(name)
        self._build(self.foreign, name, "seed:latest", "test -r /etc/os-release")
        self._verify_guest(self.foreign, name, "test -r /etc/os-release")

    def build_owner_children(self) -> None:
        assert self.owner is not None
        command = f'test "$(cat {self.marker_path})" = {self.marker}'
        for selector, base in self._selectors():
            name = self.prefix + "-owner-" + selector
            self._own_template(name)
            self._build(self.owner, name, base, command)
            self._verify_guest(self.owner, name, command)

    def reject_foreign_children(self) -> None:
        assert self.foreign is not None
        for selector, base in (*self._selectors(), ("absent", self.prefix + "-absent")):
            name = self.prefix + "-denied-" + selector
            self._own_template(name)
            spec = self._allocate(self.foreign, name)
            path = f"/templates/{name}/builds/{spec['buildID']}"
            self._expect_not_found(
                self.foreign,
                "POST",
                "/v2" + path,
                {"fromTemplate": base, "steps": [], "force": True},
            )
            self._expect_not_found(self.foreign, "GET", path + "/status")
            self.context.record("denied_" + selector, True)

    def _selectors(self) -> tuple[tuple[str, str], ...]:
        return (
            ("tag", self.base + ":latest"),
            ("digest", self.base + "@" + self.digest),
        )

    def _expect_not_found(self, config, method, path, body=None) -> None:
        try:
            self._ecw(config, method, path, body)
        except EcwControlPlaneHttpError as error:
            self.context.verify(
                error.status == 404, f"expected hidden base, got HTTP {error.status}"
            )
            return
        self.context.verify(False, "foreign or absent private base was accepted")

    @staticmethod
    def _ecw(config: EcwConfig, method: str, path: str, body=None):
        return _ecw_control_plane_json(
            config,
            method,
            path,
            body=body,
            idempotency_key=None,
            expected_status=None,
            timeout=30,
        )

    def _allocate(self, config: EcwConfig, name: str):
        return self._ecw(
            config,
            "POST",
            "/v3/templates",
            {
                "name": name,
                "tags": ["latest"],
                "cpuCount": 1,
                "memoryMB": 1024,
            },
        )

    def _build(self, config: EcwConfig, name: str, base: str, command: str) -> None:
        spec = self._allocate(config, name)
        path = f"/templates/{name}/builds/{spec['buildID']}"
        self._ecw(
            config,
            "POST",
            "/v2" + path,
            {
                "fromTemplate": base,
                "force": True,
                "steps": [{"type": "RUN", "args": [command, "root"]}],
            },
        )
        self.context.record(name + "_build_id", spec["buildID"])
        deadline = time.monotonic() + 600
        while time.monotonic() < deadline:
            status = self._ecw(config, "GET", path + "/status")
            self.context.verify(
                status["status"] != "error", f"build {name} failed: {status}"
            )
            if status["status"] == "ready":
                return
            self.context.wait(2)
        self.context.verify(False, f"build {name} did not finish within 600 seconds")

    def _verify_guest(self, config: EcwConfig, name: str, command: str) -> None:
        sdk = EcwSdk(config)
        metadata = {
            "space_smoke_run": self.prefix,
            "space_smoke_instance": uuid.uuid4().hex,
        }
        created = []

        def delete_sandbox() -> None:
            if created:
                created[0].kill()
            else:
                sdk.kill_matching(metadata)

        cleanup = self.resources.defer(
            "delete derived-template sandbox",
            lambda: retry_sandbox_delete(delete_sandbox),
            critical=True,
        )
        sandbox = sdk.create(template=name, timeout=120, metadata=metadata)
        created.append(sandbox)
        self.context.record(name + "_sandbox_id", sandbox.sandbox_id)
        result = sandbox.run(command)
        self.context.verify(
            result.exit_code == 0, "derived sandbox did not contain expected base files"
        )
        cleanup.release()

    def _own_template(self, name: str) -> None:
        self.resources.defer(
            f"delete run-owned template {name}",
            lambda: self._delete_template(name),
            critical=True,
        )

    def _delete_template(self, name: str) -> None:
        admin = self.kind.admin_request
        response = admin("DELETE", f"templates/{name}", expected=(202, 404))
        if response and "operation_id" in response:
            deadline = time.monotonic() + 90
            while time.monotonic() < deadline:
                status = admin("GET", "operations/" + response["operation_id"])
                if status["state"] == "succeeded":
                    break
                self.context.verify(
                    status["state"] not in {"failed", "canceled", "expired"},
                    f"template cleanup failed: {name}",
                )
                self.context.wait(1)
        admin("GET", f"templates/{name}", expected=(404,))

    def _cleanup(self) -> None:
        failures = self.resources.close()
        self.context.verify(
            not failures,
            "multi-tenant build cleanup failed",
            build_cleanup_failures=[
                f"{failure.name}: {failure.detail}" for failure in failures
            ],
        )
        self.context.record("build_cleanup_complete", True)

    def cleanup(self) -> None:
        self.cleanup_guard.release()

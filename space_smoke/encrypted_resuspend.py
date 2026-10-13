"""A disposable BYOK identity enrolled before the encrypted sandbox is created."""

from __future__ import annotations

import time
import uuid

from space_smoke.capabilities import KindEnvironment
from space_smoke.runtime import ScenarioContext, SmokeConfigurationError


def prepare_byok_identity(
    context: ScenarioContext, kind: KindEnvironment,
) -> tuple[str, str]:
    """Enroll an owned organization through the admin API; never toggle Local.

    Kind's credential authority lazily provisions the org's KEK through its
    key broker. Production credentials and master-key tables are not fixture
    inputs.
    The caller registers key and sandbox cleanup after this identity cleanup,
    so resource teardown finishes before its ownership records are removed.
    """
    kind.require_encrypted_volume_support()
    slug = "byok-smoke-" + uuid.uuid4().hex
    email = slug + "@local"
    org_id: str | None = None
    user_id: str | None = None
    org_pending = False
    user_pending = False
    evidence: dict[str, str] = {"slug": slug}
    context.record("byok_identity", evidence)

    def cleanup() -> None:
        nonlocal org_id
        deadline = time.monotonic() + 30
        while True:
            orgs = kind.admin_request("GET", "orgs")
            if not isinstance(orgs, list):
                raise SmokeConfigurationError("BYOK cleanup received malformed organizations")
            matches = [item for item in orgs if isinstance(item, dict) and item.get("slug") == slug]
            if len(matches) > 1:
                raise SmokeConfigurationError("BYOK cleanup found ambiguous ownership")
            if matches:
                break
            if not org_pending:
                return
            if time.monotonic() >= deadline:
                raise SmokeConfigurationError("BYOK organization create remains ambiguous; inspect recorded slug")
            time.sleep(0.25)
        observed = matches[0].get("id")
        if not isinstance(observed, str) or (org_id is not None and observed != org_id):
            raise SmokeConfigurationError("BYOK cleanup organization identity changed")
        org_id = observed
        deadline = time.monotonic() + 30
        while True:
            users = kind.admin_request("GET", f"orgs/{org_id}/users")
            if not isinstance(users, list):
                raise SmokeConfigurationError("BYOK cleanup received malformed users")
            if users or not user_pending:
                break
            if time.monotonic() >= deadline:
                raise SmokeConfigurationError("BYOK user create remains ambiguous; inspect recorded organization")
            time.sleep(0.25)
        for user in users:
            if not isinstance(user, dict) or user.get("email") != email:
                raise SmokeConfigurationError("BYOK cleanup found unexpected membership")
            identifier = user.get("id")
            if not isinstance(identifier, str) or (user_id is not None and identifier != user_id):
                raise SmokeConfigurationError("BYOK cleanup user identity changed")
            kind.admin_request("DELETE", f"users/{identifier}", expected=(204, 404))
        kind.admin_request("DELETE", f"orgs/{org_id}", expected=(204, 404))
        remaining = kind.admin_request("GET", "orgs")
        if not isinstance(remaining, list) or any(
            isinstance(item, dict) and item.get("id") == org_id for item in remaining
        ):
            raise SmokeConfigurationError("BYOK organization deletion was not confirmed")

    context.scope.defer("delete run-owned BYOK identity", cleanup, critical=True)
    org_pending = True
    org = kind.admin_request("POST", "orgs", {"name": slug, "slug": slug})
    if not isinstance(org, dict) or not isinstance(org.get("id"), str):
        raise SmokeConfigurationError("BYOK organization creation omitted its identity")
    org_id = org["id"]
    org_pending = False
    evidence["org_id"] = org_id
    org = kind.admin_request("PATCH", f"orgs/{org_id}", {"byok_enabled": True})
    if not isinstance(org, dict) or org.get("id") != org_id or org.get("byok_enabled") is not True:
        raise SmokeConfigurationError("BYOK enrollment was not confirmed")
    user_pending = True
    user = kind.admin_request("POST", f"orgs/{org_id}/users", {
        "email": email, "password": uuid.uuid4().hex + "Aa1!",
        "display_name": slug, "role": "member",
    })
    if not isinstance(user, dict) or not isinstance(user.get("id"), str):
        raise SmokeConfigurationError("BYOK user creation omitted its identity")
    user_id = user["id"]
    user_pending = False
    evidence["user_id"] = user_id
    return org_id, user_id

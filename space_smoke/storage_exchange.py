"""The storage exchange's sessions reach exactly the object they were vended for.

With exchanged storage credentials, sandbox workers' volume-manager holds no
object-store keys: for each transfer it asks its node's credential-manager
agent, on the volume socket, for a session scoped to one object, and the
authority vends it through STS with an exact-object session policy. What
keeps a session to that object is the store, which evaluates the policy on
every request. These checks ask for such a session the same way, for one
sandbox's suspend snapshot under read scope, then take it to the store.

Each refusal is judged against controls taken the same way: the session
reads its own object before the refusals and again after them, and a fresh
exchange for that object succeeds at the end, so its keys, token and
signature were good throughout and a refused request is the policy, not a
session or service that stopped working midway. The out-of-scope write uses a small body: versitygw
refuses a write before reading its body and then closes the connection, so a
large body can surface as a reset rather than the 403 (README,
"Object-store credentials"). Whether or not its status arrives, the object is
then read with the root keys and must be unchanged.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

from space_smoke.capabilities import (
    StorageExchangeControl,
    StorageObject,
    StorageRequestResult,
    StorageSession,
)
from space_smoke.runtime import ResourceScope, SmokeConfigurationError

if TYPE_CHECKING:
    from space_smoke.fixtures import KindEcw

READ_SCOPE = "space.storage.read"
TEMPLATE_BUCKET = "templates"
# At most 64 KiB, so a refusal returns its status (see the module docstring).
REFUSED_WRITE_BODY = b"storage exchange scoping smoke: must not be written\n" * 64
SESSION_LIFETIME_SECONDS = 3600
# The authority's answer when a subject asks for an object it does not own
# (StorageExchangeError::UnauthorizedResource, mapped to PERMISSION_DENIED in
# crates/libs/credential/mgr/server/src/service.rs), which the agent forwards.
RESOURCE_DENIED_STATUS = "PERMISSION_DENIED"
RESOURCE_DENIED_DETAIL = "requested storage resource is not authorized for the subject"
SNAPSHOT_READY_TIMEOUT_SECONDS = 300


def _evidence(result: StorageRequestResult) -> dict[str, object]:
    return {
        "operation": result.operation,
        "target": result.target,
        "succeeded": result.succeeded,
        "refused": result.refused,
        "detail": result.detail,
    }


class StorageExchangeScopingAssertions:
    """One read session for one sandbox's snapshot object, against the store."""

    def __init__(self, fixture: KindEcw, control: StorageExchangeControl, scope: ResourceScope) -> None:
        self._fixture = fixture
        self._control = control
        self._scope = scope
        self._node: str | None = None
        self._own: StorageObject | None = None
        self._other: StorageObject | None = None
        self._template: StorageObject | None = None
        self._session: StorageSession | None = None

    def require_exchange_mode(self) -> None:
        mode = self._control.storage_credentials_mode()
        self._fixture.record("storage_credentials_mode", mode)
        if mode != "exchange":
            raise SmokeConfigurationError(
                f"the cluster runs SPACE_KIND_STORAGE_CREDENTIALS={mode}; this scenario needs exchange"
            )
        nodes = self._control.sandbox_worker_nodes()
        self._fixture.verify(bool(nodes), "no sandbox worker runs volume-manager")
        self._node = nodes[0]
        self._fixture.record("exchange_node", self._node)

    def suspend_both(self) -> None:
        """Suspend both sandboxes so each has a snapshot in the store."""

        ids = self._fixture.sandboxes.ids
        if len(ids) < 2:
            raise SmokeConfigurationError("the scoping checks need two sandboxes")
        self._fixture.pause()
        self._fixture.sandboxes.items[1].pause()
        self._own = self._snapshot_object(ids[0])
        self._other = self._snapshot_object(ids[1])
        templates = self._control.bucket_objects(TEMPLATE_BUCKET, limit=1)
        self._fixture.verify(bool(templates), "the templates bucket holds no object to probe")
        self._template = templates[0]
        for name, item in (("own", self._own), ("other_sandbox", self._other), ("template", self._template)):
            self._fixture.record(f"{name}_object", {"arn": item.arn, "etag": item.etag, "size": item.size})

    def _snapshot_object(self, sandbox_id: str) -> StorageObject:
        deadline = time.monotonic() + SNAPSHOT_READY_TIMEOUT_SECONDS
        while True:
            for record in self._control.ready_full_snapshots(sandbox_id):
                objects = [item for item in self._control.snapshot_objects(record) if item.size > 0]
                if objects:
                    return min(objects, key=lambda item: item.size)
            self._fixture.verify(
                time.monotonic() <= deadline,
                "the sandbox's suspend snapshot did not reach the store",
                sandbox_id=sandbox_id,
            )
            self._fixture.wait(2)

    def exchange_read_session(self) -> None:
        """ExchangeToken on the worker's agent volume socket, as volume-manager asks."""

        self._session = self._exchange_own_read_session()

    def _exchange_own_read_session(self) -> StorageSession:
        assert self._node is not None and self._own is not None
        sandbox_id = self._fixture.sandboxes.ids[0]
        outcome = self._control.exchange_storage_credentials(
            self._node, subject=f"sandbox:{sandbox_id}", resource=self._own.arn, scope=READ_SCOPE
        )
        session = outcome.session
        self._fixture.verify(
            session is not None and bool(session.session_token),
            "the storage exchange did not issue a session",
            status=outcome.status,
            detail=outcome.detail,
        )
        assert session is not None
        lifetime = session.expires_at_unix - int(time.time())
        self._fixture.verify(
            0 < lifetime <= SESSION_LIFETIME_SECONDS,
            "the session's expiry is not within the requested hour",
            lifetime_seconds=lifetime,
        )
        self._fixture.record("session_lifetime_seconds", lifetime)
        return session

    def expect_own_object_readable(self) -> None:
        assert self._session is not None
        self._expect_readable(self._session)

    def _expect_readable(self, session: StorageSession) -> None:
        assert self._own is not None
        for operation in ("head", "get"):
            result = self._control.storage_request(session, operation, self._own.bucket, self._own.key)
            self._fixture.verify(
                result.succeeded, "the session could not read the object it was vended for", **_evidence(result)
            )

    def _expect_refused(self, operation: str, item: StorageObject, summary: str, *, key: str | None = None) -> None:
        assert self._session is not None
        result = self._control.storage_request(self._session, operation, item.bucket, key or item.key)
        self._fixture.verify(result.refused, summary, **_evidence(result))
        self._fixture.record(f"refused_{operation}_{item.bucket}", result.detail)

    def expect_other_sandbox_refused(self) -> None:
        assert self._other is not None
        prefix = self._other.key.rsplit("/", 1)[0] + "/"
        self._expect_refused("head", self._other, "the session read another sandbox's snapshot object")
        self._expect_refused("get", self._other, "the session read another sandbox's snapshot object")
        self._expect_refused("list", self._other, "the session listed another sandbox's snapshot", key=prefix)

    def expect_other_bucket_refused(self) -> None:
        assert self._template is not None
        self._expect_refused("head", self._template, "the session read the templates bucket")
        self._expect_refused("get", self._template, "the session read the templates bucket")

    def expect_write_refused_and_object_unchanged(self) -> None:
        assert self._session is not None and self._own is not None
        result = self._control.storage_request(
            self._session, "put", self._own.bucket, self._own.key, body=REFUSED_WRITE_BODY
        )
        self._fixture.verify(result.refused, "a read-scoped session wrote its own object", **_evidence(result))
        after = self._control.head_object(self._own.bucket, self._own.key)
        self._fixture.verify(
            after is not None and after.etag == self._own.etag and after.size == self._own.size,
            "the object changed under a refused write",
            before={"etag": self._own.etag, "size": self._own.size},
            after=None if after is None else {"etag": after.etag, "size": after.size},
        )

    def expect_authority_refuses_another_sandboxs_object(self) -> None:
        """The authority itself vends a sandbox's subject only for its own objects."""

        assert self._node is not None and self._other is not None
        outcome = self._control.exchange_storage_credentials(
            self._node,
            subject=f"sandbox:{self._fixture.sandboxes.ids[0]}",
            resource=self._other.arn,
            scope=READ_SCOPE,
        )
        self._fixture.record("cross_subject_exchange", {"status": outcome.status, "detail": outcome.detail})
        self._fixture.verify(
            outcome.session is None,
            "the authority vended one sandbox's subject for another sandbox's object",
        )
        # Any failure would leave no session; only this one is the refusal.
        self._fixture.verify(
            outcome.status == RESOURCE_DENIED_STATUS and RESOURCE_DENIED_DETAIL in outcome.detail,
            "the cross-subject exchange failed, but not as the authority's resource denial",
            status=outcome.status,
            detail=outcome.detail,
        )

    def expect_controls_hold_after_refusals(self) -> None:
        """The session still reads its object, and a fresh exchange does too."""

        assert self._session is not None
        self._expect_readable(self._session)
        self._expect_readable(self._exchange_own_read_session())

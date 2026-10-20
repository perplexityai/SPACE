"""The storage exchange's sessions reach exactly the object they were vended for.

With exchanged storage credentials, sandbox workers' volume-manager holds no
object-store keys: for each transfer it asks its node's credential-manager
agent, on the volume socket, for a session scoped to one object, and the
authority vends it through STS with an exact-object session policy. What
keeps a session to that object is the store, which evaluates the policy on
every request. These checks ask for such a session the same way, for one
sandbox's suspend snapshot under read scope, then take it to the store.

A builder's agent is asked the same way, as the builder volume-manager asks
for a template pull: for one published file of the sandboxes' template under
read scope. The authority vends it only to a builder whose Kubernetes node it
has recorded, with the same exact-object session policy, so the store refuses
that session another file of the same release (which every broader prefix,
from the release to the whole bucket, would cover) and a sandbox's snapshot,
and, when another template is published, that template's file. The authority
itself refuses to vend the template's subject for another template's file at
all; it decides by the key's template name, so that needs no such object.
Its controls are taken the same way.

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
# The scenario's sandboxes run the default template (SandboxConfig).
SANDBOX_TEMPLATE = "seed"
# Published template files small enough to read whole, which the authority
# vends a template subject (validate_template_resource,
# crates/libs/credential/mgr/server/src/storage_exchange.rs).
TEMPLATE_PROBE_FILES = ("metadata.json", "manifest.json")
# The template name in the authority refusal's key when no other template is
# published. The authority refuses by the key's template name before any
# lookup (validate_template_resource), so the object need not exist.
ABSENT_TEMPLATE = "storage-exchange-scoping-absent"
# A refused target larger than this is only HEAD-probed: a policy broader than
# it should be would otherwise download it whole (a release's rootfs).
REFUSED_GET_MAX_BYTES = 1 << 20
# At most 64 KiB, so a refusal returns its status (see the module docstring).
REFUSED_WRITE_BODY = b"storage exchange scoping smoke: must not be written\n" * 64
SESSION_LIFETIME_SECONDS = 3600
# The authority's answer when a subject asks for an object it does not own
# (StorageExchangeError::UnauthorizedResource, mapped to PERMISSION_DENIED in
# crates/libs/credential/mgr/server/src/service.rs), which the agent forwards.
RESOURCE_DENIED_STATUS = "PERMISSION_DENIED"
RESOURCE_DENIED_DETAIL = "requested storage resource is not authorized for the subject"
SNAPSHOT_READY_TIMEOUT_SECONDS = 300


def _file_name(item: StorageObject) -> str:
    return item.key.rsplit("/", 1)[1]


def _release(item: StorageObject) -> str:
    """A published template file's release prefix, `templates/<name>/<hash>/`."""
    return item.key.rsplit("/", 1)[0] + "/"


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
        self._builder: str | None = None
        self._builder_session: StorageSession | None = None
        self._builder_template: StorageObject | None = None
        self._builder_sibling: StorageObject | None = None
        self._other_template: StorageObject | None = None

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
        builders = self._control.builder_nodes()
        self._fixture.verify(bool(builders), "no builder has a Ready node-api")
        self._builder = builders[0]
        self._fixture.record("builder_exchange_node", self._builder)

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

    def exchange_builder_template_session(self) -> None:
        """ExchangeToken on a builder's agent volume socket, as its volume-manager pulls a template.

        The session is for the smallest probe file of one release of the
        sandboxes' template; the refusals use another file of that release
        and, when there is one, the smallest probe file of another published
        template.
        """

        published: dict[str, list[StorageObject]] = {}
        for item in self._control.bucket_objects(TEMPLATE_BUCKET, limit=1000, prefix="templates/"):
            parts = item.key.split("/")
            if len(parts) == 4:
                published.setdefault(parts[1], []).append(item)
        own = [item for item in published.get(SANDBOX_TEMPLATE, []) if _file_name(item) in TEMPLATE_PROBE_FILES]
        self._fixture.verify(bool(own), "the template has no published file to probe", template=SANDBOX_TEMPLATE)
        self._builder_template = min(own, key=lambda item: item.size)
        release = _release(self._builder_template)
        siblings = [
            item
            for item in published[SANDBOX_TEMPLATE]
            if _release(item) == release and item.key != self._builder_template.key
        ]
        self._fixture.verify(bool(siblings), "the template's release has no other file", release=release)
        # Another probe file if there is one, so the refusal is also read whole.
        self._builder_sibling = min(siblings, key=lambda item: (_file_name(item) not in TEMPLATE_PROBE_FILES, item.size))
        others = [
            item
            for name, items in published.items()
            if name != SANDBOX_TEMPLATE
            for item in items
            if _file_name(item) in TEMPLATE_PROBE_FILES
        ]
        self._other_template = min(others, key=lambda item: item.size) if others else None
        self._fixture.record("published_templates", sorted(published))
        for name, item in (
            ("builder_template", self._builder_template),
            ("builder_sibling", self._builder_sibling),
            ("other_template", self._other_template),
        ):
            self._fixture.record(
                f"{name}_object", None if item is None else {"arn": item.arn, "etag": item.etag, "size": item.size}
            )
        self._builder_session = self._exchange_builder_session()

    def _exchange_builder_session(self) -> StorageSession:
        assert self._builder is not None and self._builder_template is not None
        outcome = self._control.exchange_storage_credentials(
            self._builder,
            subject=f"template:{SANDBOX_TEMPLATE}",
            resource=self._builder_template.arn,
            scope=READ_SCOPE,
        )
        self._fixture.verify(
            outcome.session is not None and bool(outcome.session.session_token),
            "the authority did not vend a builder's template pull",
            builder=self._builder,
            status=outcome.status,
            detail=outcome.detail,
        )
        assert outcome.session is not None
        return outcome.session

    def expect_builder_session_reads_its_file(self) -> None:
        assert self._builder_session is not None
        self._expect_builder_readable(self._builder_session)

    def _expect_builder_readable(self, session: StorageSession) -> None:
        assert self._builder_template is not None
        for operation in ("head", "get"):
            result = self._control.storage_request(
                session, operation, self._builder_template.bucket, self._builder_template.key
            )
            self._fixture.verify(
                result.succeeded, "the builder's session could not read its template file", **_evidence(result)
            )

    def expect_builder_session_refused_beyond_its_file(self) -> None:
        """The store refuses the builder's session its release's other file, a snapshot and another template's file."""

        assert self._builder_session is not None and self._own is not None
        assert self._builder_sibling is not None
        targets = [
            ("sibling", self._builder_sibling, "the builder's session read another file of its template's release"),
            ("snapshot", self._own, "the builder's template session read a sandbox's snapshot"),
        ]
        if self._other_template is not None:
            targets.append(
                ("other_template", self._other_template, "the builder's session read another template's file")
            )
        for name, item, summary in targets:
            operations = ("head", "get") if item.size <= REFUSED_GET_MAX_BYTES else ("head",)
            for operation in operations:
                result = self._control.storage_request(self._builder_session, operation, item.bucket, item.key)
                self._fixture.verify(result.refused, summary, **_evidence(result))
                self._fixture.record(f"builder_refused_{operation}_{name}", result.detail)

    def expect_authority_refuses_another_templates_file(self) -> None:
        """The authority vends a template's subject only for that template's files."""

        assert self._builder is not None and self._builder_template is not None
        if self._other_template is not None:
            resource = self._other_template.arn
        else:
            _, _, digest, file = self._builder_template.key.split("/")
            resource = f"arn:aws:s3:::{self._builder_template.bucket}/templates/{ABSENT_TEMPLATE}/{digest}/{file}"
        outcome = self._control.exchange_storage_credentials(
            self._builder, subject=f"template:{SANDBOX_TEMPLATE}", resource=resource, scope=READ_SCOPE
        )
        self._fixture.record(
            "cross_template_exchange", {"resource": resource, "status": outcome.status, "detail": outcome.detail}
        )
        self._fixture.verify(
            outcome.session is None, "the authority vended one template's subject for another template's file"
        )
        self._fixture.verify(
            outcome.status == RESOURCE_DENIED_STATUS and RESOURCE_DENIED_DETAIL in outcome.detail,
            "the cross-template exchange failed, but not as the authority's resource denial",
            status=outcome.status,
            detail=outcome.detail,
        )

    def expect_builder_controls_hold_after_refusals(self) -> None:
        """The builder's session still reads its file, and a fresh builder exchange does too."""

        assert self._builder_session is not None
        self._expect_builder_readable(self._builder_session)
        self._expect_builder_readable(self._exchange_builder_session())

    def expect_controls_hold_after_refusals(self) -> None:
        """The session still reads its object, and a fresh exchange does too."""

        assert self._session is not None
        self._expect_readable(self._session)
        self._expect_readable(self._exchange_own_read_session())

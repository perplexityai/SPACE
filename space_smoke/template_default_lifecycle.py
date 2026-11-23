"""Native creates against a template's default auto-lifecycle policy.

A native create that names no `auto_lifecycle` and no `idle_timeout` takes the
template's default policy, and suspends after six idle hours when the template
has none; an explicit policy, including the empty disabled `{}`, wins over
both. The public sandbox read reports the outcome as `idle_timeout` and
`idle_action`.

The scenario sets the default on the bench's seed template through the admin
API. That API can set a default but not remove one, so the restore first sets
the disabled policy, waits until every node that published a copy of the
default publishes the disabled one (a node never drops a copy it holds, and a
disabled default is what a template without one already means to every
create path), and only then deletes the desired row. A run killed in between
leaves the row behind, and the next run refuses to start until it is cleared.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Callable, Mapping
from typing import Protocol

from space_smoke.execution_environment import Judge
from space_smoke.runtime import ResourceScope, SmokeConfigurationError

TEMPLATE_DEFAULT_IDLE = "4321s"
# Wakes on access and suspends after `TEMPLATE_DEFAULT_IDLE`: distinct from the
# six-hour fallback, and harmless to any later create that inherits it.
TEMPLATE_DEFAULT_POLICY: Mapping[str, object] = {
    "wake": {"on": ["access"]},
    "condition_sets": {
        "access_idle": {"kind": "inactivity", "activity_source": "access"},
    },
    "stages": [
        {"condition_set": "access_idle", "at": TEMPLATE_DEFAULT_IDLE, "action": "suspend"},
    ],
}
DISABLED_POLICY: Mapping[str, object] = {}
# `idle_timeout` and `idle_action` as the public sandbox read reports them.
SERVER_DEFAULT_IDLE = ("21600s", "suspend")
TEMPLATE_DEFAULT_IDLE_REPORT = (TEMPLATE_DEFAULT_IDLE, "suspend")
DISABLED_IDLE = (None, None)
PUBLISH_TIMEOUT_SECONDS = 180.0


class TemplateDefaultLifecycleControl(Protocol):
    """Kind-only operations behind [`TemplateDefaultLifecycle`]."""

    def native_create_sandbox(
        self,
        *,
        template: str,
        metadata: Mapping[str, str],
        auto_lifecycle: Mapping[str, object] | None = None,
    ) -> str:
        """`POST /v1/sandboxes`; `auto_lifecycle=None` omits the field."""
        ...

    def native_sandbox_idle(self, sandbox_id: str) -> tuple[str | None, str | None]:
        """`idle_timeout` and `idle_action` from the native `/v1` sandbox read."""
        ...

    def template_default_auto_lifecycle(self, template: str) -> Mapping[str, object] | None:
        """The template's desired default policy; None when it has none."""
        ...

    def set_template_default_auto_lifecycle(
        self, template: str, policy: Mapping[str, object]
    ) -> None:
        """`PATCH /admin/api/v1/templates/{template}` with `default_auto_lifecycle`."""
        ...

    def template_default_auto_lifecycle_published(self, template: str) -> bool:
        """Whether every node copy of the template's default equals the desired one."""
        ...

    def delete_template_default_auto_lifecycle(self, template: str) -> None:
        """Delete the desired row; no public path removes a default."""
        ...


class TemplateDefaultLifecycle:
    """Native creates of one template, and its default set for the scenario."""

    def __init__(
        self,
        fixture: Judge,
        control: TemplateDefaultLifecycleControl,
        scope: ResourceScope,
        template: str,
        *,
        kill_sandbox: Callable[[str], None],
        kill_matching: Callable[[Mapping[str, str]], None],
    ) -> None:
        self._fixture = fixture
        self._control = control
        self._scope = scope
        self._template = template
        self._kill_sandbox = kill_sandbox
        self._kill_matching = kill_matching

    def require_no_default(self) -> None:
        standing = self._control.template_default_auto_lifecycle(self._template)
        if standing is not None:
            raise SmokeConfigurationError(
                f"template {self._template} already has a default auto-lifecycle policy "
                f"({standing}); an earlier run left it behind, so delete it and rerun"
            )

    def set_default(self) -> None:
        """Set the template's default to `TEMPLATE_DEFAULT_POLICY`; the restore is armed first."""

        def restore() -> None:
            self._control.set_template_default_auto_lifecycle(self._template, DISABLED_POLICY)
            deadline = time.monotonic() + PUBLISH_TIMEOUT_SECONDS
            while not self._control.template_default_auto_lifecycle_published(self._template):
                if time.monotonic() >= deadline:
                    raise TimeoutError(
                        f"nodes still publish another default for {self._template}; "
                        "its desired row is kept so the next run refuses to start"
                    )
                time.sleep(2)
            self._control.delete_template_default_auto_lifecycle(self._template)
            if self._control.template_default_auto_lifecycle(self._template) is not None:
                raise RuntimeError(f"template {self._template} still has a default")

        self._scope.defer(
            f"remove the default auto-lifecycle policy of {self._template}", restore, critical=True
        )
        self._control.set_template_default_auto_lifecycle(self._template, TEMPLATE_DEFAULT_POLICY)

    def create(self, auto_lifecycle: Mapping[str, object] | None = None) -> str:
        """A native create of the template; the bare sandbox id. Cleanup kills it."""

        metadata = {"space_smoke_template_default_lifecycle": uuid.uuid4().hex}
        by_metadata = self._scope.defer(
            "kill the native sandbox", lambda: self._kill_matching(metadata), critical=True
        )
        sandbox_id = self._control.native_create_sandbox(
            template=self._template, metadata=metadata, auto_lifecycle=auto_lifecycle
        )
        self._scope.defer(
            f"kill native sandbox {sandbox_id}",
            lambda: self._kill_sandbox(sandbox_id),
            critical=True,
        )
        by_metadata.disarm()
        return sandbox_id

    def expect_idle(
        self, sandbox_id: str, expected: tuple[str | None, str | None], message: str
    ) -> None:
        reported = self._control.native_sandbox_idle(sandbox_id)
        self._fixture.verify(
            reported == expected,
            message,
            sandbox_id=sandbox_id,
            idle_timeout=reported[0],
            idle_action=reported[1],
            expected=list(expected),
        )

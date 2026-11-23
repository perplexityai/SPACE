"""A native create without a policy takes the template's default auto-lifecycle policy."""

import space_smoke as smoke
from space_smoke.template_default_lifecycle import (
    DISABLED_IDLE,
    DISABLED_POLICY,
    SERVER_DEFAULT_IDLE,
    TEMPLATE_DEFAULT_IDLE_REPORT,
)


@smoke.scenario("local.native_create_inherits_template_default_lifecycle")
def native_create_inherits_template_default_lifecycle(test: smoke.KindAdmin) -> None:
    """A native create that names no policy suspends after six idle hours while
    the template has no default, and takes the template's default once it has
    one; an explicit empty policy stays disabled despite the default."""

    lifecycle = test.template_default_lifecycle
    lifecycle.require_no_default()

    with test.phase("a create without a policy on a template without a default idles out after six hours"):
        sandbox_id = lifecycle.create()
        lifecycle.expect_idle(
            sandbox_id, SERVER_DEFAULT_IDLE, "a template without a default must keep the six-hour fallback"
        )

    with test.phase("set the template's default auto-lifecycle policy"):
        lifecycle.set_default()

    with test.phase("a create without a policy takes the template's default"):
        sandbox_id = lifecycle.create()
        lifecycle.expect_idle(
            sandbox_id, TEMPLATE_DEFAULT_IDLE_REPORT, "a create without a policy must take the template's default"
        )

    with test.phase("an explicit empty policy stays disabled despite the default"):
        sandbox_id = lifecycle.create(DISABLED_POLICY)
        lifecycle.expect_idle(sandbox_id, DISABLED_IDLE, "an explicit empty policy must win over the template's default")

    with test.phase("remove the template's default auto-lifecycle policy"):
        lifecycle.remove_default()

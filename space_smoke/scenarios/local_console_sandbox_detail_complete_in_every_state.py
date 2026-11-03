"""A sandbox's console page shows what the admin API records, running, suspended and deleted."""

import space_smoke as smoke


@smoke.scenario(
    "local.console_sandbox_detail_complete_in_every_state",
    sandbox=smoke.SandboxConfig(timeout=900),
    browser="chromium",
)
def console_sandbox_detail_complete_in_every_state(test: smoke.KindEcw) -> None:
    """Running, the sandbox's page shows its ID and state, its template,
    its node, its organization by name and its owner by email, each linking
    to its page, offers exactly the lifecycle moves a running sandbox has, and
    lists every operation the admin API records for it, or says there are
    none. Suspended by an operator's admin action, the same page shows it
    unassigned, offers only the moves a suspended sandbox has, and lists that
    suspend. Deleted, it says the sandbox is gone and still lists the
    operations recorded against it. No page reports a browser error outside
    the baseline."""

    console = test.console
    sandbox_id = test.sandbox.sandbox_id

    with test.phase("sign in as the operator"):
        operator = console.sign_in_operator()

    with test.phase("running: the page shows the sandbox"):
        console.expect_detail_complete(operator, sandbox_id, "started")

    with test.phase("suspend the sandbox through the admin API"):
        console.force_action(sandbox_id, "suspend", "suspended")

    with test.phase("suspended: the page shows the sandbox"):
        console.expect_detail_complete(operator, sandbox_id, "suspended")

    with test.phase("delete the sandbox"):
        test.delete_sandbox()

    with test.phase("deleted: the page says so and keeps its operations"):
        console.expect_deleted_detail(operator, sandbox_id)

    with test.phase("no new browser errors"):
        console.expect_no_new_browser_errors()

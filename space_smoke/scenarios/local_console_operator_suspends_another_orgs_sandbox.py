"""The operator suspends another organization's sandbox from the console."""

import space_smoke as smoke


@smoke.scenario(
    "local.console_operator_suspends_another_orgs_sandbox",
    sandbox=smoke.SandboxConfig(timeout=900),
    browser="chromium",
)
def console_operator_suspends_another_orgs_sandbox(test: smoke.KindEcw) -> None:
    """For a running sandbox of an organization other than the one managing
    the cluster, the operator's console page names that organization, and its
    Suspend leaves the sandbox suspended in the admin API, still owned by that
    organization and user, and shown suspended. No page reports a browser
    error outside the baseline."""

    console = test.console

    with test.phase("create another organization, its user and its running sandbox"):
        tenant = console.create_tenant()
        owner = console.create_account(tenant.org_id, "member", slug=tenant.slug)
        foreign = console.create_foreign_sandbox(tenant)

    with test.phase("sign in as the operator"):
        operator = console.sign_in_operator()

    with test.phase("suspend the other organization's sandbox from its page"):
        console.expect_cross_org_suspend(operator, foreign, tenant, owner)

    with test.phase("no new browser errors"):
        console.expect_no_new_browser_errors()

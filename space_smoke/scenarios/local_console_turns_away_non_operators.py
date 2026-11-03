"""Only the managing organization's admins operate the console."""

import space_smoke as smoke


@smoke.scenario(
    "local.console_turns_away_non_operators",
    sandbox=smoke.SandboxConfig(timeout=900),
    browser="chromium",
)
def console_turns_away_non_operators(test: smoke.KindEcw) -> None:
    """An organization admin of another organization, and a member of the
    organization that manages the cluster, each sign in to the console's
    "not for you" notice, with no navigation, on the dashboard and on a deep
    link to the sandbox's page, which does not show the sandbox. The
    operator, an admin of the managing organization, gets the navigation and
    the sandbox's page. No page reports a browser error outside the
    baseline."""

    console = test.console
    sandbox_id = test.sandbox.sandbox_id

    with test.phase("create another organization's admin and a managing-organization member"):
        tenant = console.create_tenant()
        foreign_admin = console.create_account(tenant.org_id, "organization_admin", slug=tenant.slug)
        managing_member = console.create_account(console.managing_org_id(), "member")

    with test.phase("another organization's admin is turned away"):
        console.expect_turned_away(console.sign_in(foreign_admin), sandbox_id)

    with test.phase("a member of the managing organization is turned away"):
        console.expect_turned_away(console.sign_in(managing_member), sandbox_id)

    with test.phase("control: the operator is admitted"):
        console.expect_admitted(console.sign_in_operator(), sandbox_id)

    with test.phase("no new browser errors"):
        console.expect_no_new_browser_errors()

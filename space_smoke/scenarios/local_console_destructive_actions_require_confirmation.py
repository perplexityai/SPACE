"""The console asks before it deletes a sandbox or changes a user's role."""

import space_smoke as smoke


@smoke.scenario(
    "local.console_destructive_actions_require_confirmation",
    sandbox=smoke.SandboxConfig(timeout=900),
    browser="chromium",
)
def console_destructive_actions_require_confirmation(test: smoke.KindEcw) -> None:
    """On a sandbox's page, Delete asks for confirmation; cancelled, the page
    sends no state-changing request and the sandbox stays running; confirmed,
    the sandbox is deleted. On a user's page, choosing another role asks for
    confirmation; cancelled, nothing is sent and the role is unchanged;
    confirmed, the role is the one chosen. The user is a member of another
    organization, so the new role grants no console access. No page reports
    a browser error outside the baseline."""

    console = test.console

    with test.phase("create another organization and a member"):
        tenant = console.create_tenant()
        member = console.create_account(tenant.org_id, "member", slug=tenant.slug)

    with test.phase("sign in as the operator"):
        operator = console.sign_in_operator()

    with test.phase("deleting the sandbox asks first"):
        console.expect_delete_requires_confirmation(operator, test.sandbox.sandbox_id)

    with test.phase("changing the user's role asks first"):
        console.expect_role_change_requires_confirmation(operator, member, "organization_admin")

    with test.phase("no new browser errors"):
        console.expect_no_new_browser_errors()

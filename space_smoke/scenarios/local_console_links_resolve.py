"""The console's links from a sandbox to its organization, owner and node resolve."""

import space_smoke as smoke


@smoke.scenario(
    "local.console_links_resolve",
    sandbox=smoke.SandboxConfig(timeout=900),
    browser="chromium",
)
def console_links_resolve(test: smoke.KindEcw) -> None:
    """From the sandbox list, the operator follows the sandbox's link to its
    page; from there its organization and owner links, the owner's
    organization link, and the nodes list's link to the sandbox's node; and
    from each of those pages a link back to the sandbox. Every link points
    at the page for the object the admin API names, and every page resolves
    rather than showing a not-found or load-failure state. The templates page
    lists the sandbox's template. No page reports a browser error outside the
    baseline."""

    console = test.console

    with test.phase("sign in as the operator"):
        operator = console.sign_in_operator()

    sandbox_id = test.sandbox.sandbox_id

    with test.phase("the list links to the sandbox's page"):
        console.follow_list_to_detail(operator, sandbox_id)

    with test.phase("the organization link resolves and links back"):
        console.follow_organization_links(operator, sandbox_id)

    with test.phase("the owner link resolves, with its organization, and links back"):
        console.follow_owner_links(operator, sandbox_id)

    with test.phase("the nodes list links to the sandbox's node, which links back"):
        console.follow_node_links(operator, sandbox_id)

    with test.phase("the templates page lists the sandbox's template"):
        console.expect_template_listed(operator, sandbox_id)

    with test.phase("no new browser errors"):
        console.expect_no_new_browser_errors()

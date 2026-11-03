"""The console finds a sandbox by every identifier form it accepts."""

import space_smoke as smoke
from space_smoke.console import public_sandbox_id


@smoke.scenario(
    "local.console_sandbox_lookup_accepts_every_id_form",
    sandbox=smoke.SandboxConfig(timeout=900),
    browser="chromium",
)
def console_sandbox_lookup_accepts_every_id_form(test: smoke.KindEcw) -> None:
    """Signed in as the operator, the sandbox's public `sbx_` ID, its bare
    UUID, the UUID without dashes and its pod's name each open the sandbox's
    canonical page from the detail route and, typed into the sandbox list's
    search while the list shows this sandbox and a second one, narrow it to
    exactly this one. A well-formed ID of no sandbox is reported not found by
    both. No page reports a browser error outside the baseline."""

    console = test.console

    with test.phase("read every identifier form of the sandbox"):
        forms = console.sandbox_id_forms(test.sandbox.sandbox_id)
        public_id = forms["public id"]

    with test.phase("provision a second sandbox for the list to tell apart"):
        other_public_id = public_sandbox_id(test.sandboxes.create().sandbox_id)

    with test.phase("sign in as the operator"):
        operator = console.sign_in_operator()

    for form, value in forms.items():
        with test.phase(f"the detail route accepts the {form}"):
            console.expect_detail_route_accepts(operator, form, value, public_id)

    for form, value in forms.items():
        with test.phase(f"the list's search finds the sandbox by its {form}"):
            console.expect_search_finds(operator, form, value, public_id, other_public_id)

    with test.phase("control: an ID of no sandbox is not found"):
        console.expect_unknown_id_not_found(operator)

    with test.phase("no new browser errors"):
        console.expect_no_new_browser_errors()

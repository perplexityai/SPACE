"""Build and boot a seed through the real admin console on an isolated Kind bench."""

import space_smoke as smoke


@smoke.scenario(
    "local.console_seed_runtime_pair",
    browser="chromium",
    timeout_minutes=30,
    tier="manual",
    manual_reason="Opt-in seed build: requires the qualified FC 1.17.0 / Linux 6.1.155 pair installed and admitted on the bench.",
)
def console_seed_runtime_pair(test: smoke.KindAdmin) -> None:
    """The selected qualified pair reaches the builder, produces a bootable
    seed, and leaves latest unchanged. The browser drives the real API; the
    admin API independently judges completion. Only newly registered,
    unpromoted releases and the test sandbox are cleaned."""
    with test.phase("build seed with a qualified runtime/kernel pair"):
        console = test.console
        operator = console.sign_in_operator()
        console.build_seed_with_qualified_pair(operator)
        console.expect_no_new_browser_errors()

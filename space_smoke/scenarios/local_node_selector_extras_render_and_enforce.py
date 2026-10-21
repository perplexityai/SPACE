"""Configured nodeSelector extras render on every renderer's pod and one absent extra blocks placement."""

import space_smoke as smoke
from space_smoke.node_selector_extras import ABSENT_EXTRA, SATISFIED_EXTRAS


# Exclusive bench: with the unsatisfiable extras on both renderers, every create
# on the cluster is refused for the length of the negative phase (pool fills
# included). Two env rewrites and a restore, each a concurrent fast-pace
# rollout of every renderer plus the API readiness wait (~15 s; ~40 s at the
# charts' pace), two real sandbox boots (~5 s each) and one refused create:
# ~65 s on the kind bench.
@smoke.scenario(
    "local.node_selector_extras_render_and_enforce",
    sandbox=smoke.SandboxConfig(timeout=900),
)
def node_selector_extras_render_and_enforce(test: smoke.KindEcw) -> None:
    """With two satisfiable extras configured on both renderers, an ECW create's
    pod (rendered by space-api) and a native create's pod (rendered by the
    cluster controller's dispatcher) each carry `spec.nodeSelector` equal to the
    discriminator plus every extra and run on a node carrying all of them; with
    one extra no node carries added, the fleet still satisfies the old set,
    nothing satisfies the new one, and an ECW create is refused before any pod
    with `429 Server capacity limit: no eligible sandbox activation nodes …`
    and a Retry-After; the renderers' env is restored to what was captured."""

    extras = test.node_selector_extras

    with test.phase("preconditions: discriminator, absent key unheld, extras not already configured"):
        extras.preconditions()

    with test.phase("configure the satisfiable extras on both renderers"):
        extras.configure(SATISFIED_EXTRAS)

    with test.phase("space-api renders the merge: an ECW create's pod carries exactly it and runs"):
        extras.expect_ecw_create_renders_and_places()

    with test.phase("the cluster controller renders the same merge: a native create's pod"):
        extras.expect_native_create_renders_and_places(template=test.sandbox_template_name())

    with test.phase("add an extra no node carries"):
        extras.configure((*SATISFIED_EXTRAS, ABSENT_EXTRA))

    with test.phase("the fleet still satisfies the old set, nothing the new one; the create is a capacity 429"):
        extras.expect_absent_extra_refuses()

    with test.phase("restore the renderers' env"):
        extras.restore()

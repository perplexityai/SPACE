"""Private base files are available only to authorized template builders."""

import space_smoke as smoke


@smoke.scenario(
    "local.private_template_build_isolation",
    rerun_environment=(
        "SPACE_SMOKE_KIND_ADMIN_TOKEN_FILE",
        "SPACE_SMOKE_BUILD_FOREIGN_ORG_ID",
    ),
)
def private_template_build_isolation(test: smoke.KindAdmin) -> None:
    """An owner can derive a working template from its private base, while a
    different authenticated organization cannot build from the same tag or
    digest, including after the owner's successful materialization."""
    builds = test.template_builds
    with test.phase("seed run-owned credentials and build a private base"):
        builds.prepare()
    with test.phase("a second tenant can build from the shared seed"):
        builds.build_shared_control()
    with test.phase("the owner can materialize private base files by tag and digest"):
        builds.build_owner_children()
    with test.phase("another tenant is denied private tag and digest bases"):
        builds.reject_foreign_children()
    with test.phase("delete run-owned templates and credentials"):
        builds.cleanup()

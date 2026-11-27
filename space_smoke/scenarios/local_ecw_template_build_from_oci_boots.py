"""A public `Template.build` from an OCI image completes across a builder handoff and boots."""

import space_smoke as smoke


@smoke.scenario(
    "local.ecw_template_build_from_oci_boots",
)
def ecw_template_build_from_oci_boots(test: smoke.KindAdmin) -> None:
    """With the builder Service pinned to one of two builders, a Dockerfile built
    from a public OCI image through the public E2B SDK is accepted there and its
    prepared rootfs committed; with the Service moved to the other builder for
    the rest of the build, the build still completes (status polls are pinned
    to the accepting builder, not routed through the Service); the published
    `metadata.json` names the installed `spaced` bundle
    and its compatibility family; containerd keeps no source, temporary image or
    assembly residue; and a sandbox created from the alias runs the Dockerfile's
    state, the SDK's environment and start command, SPACE's guest services, the
    UID 2000 user, filesystem I/O, processes, injected SSH keys and the source
    image's Python 3.12 on bookworm."""

    build = test.template_build

    with test.phase("pin the two builders; route the builder Service to the submit builder"):
        build.pin_builders()

    with test.phase("build from the OCI image through the SDK; move the Service off the accepting builder"):
        build.build_through_sdk()

    with test.phase("the published guest metadata names the installed spaced bundle"):
        build.expect_guest_metadata_matches_bundle()

    with test.phase("containerd holds no source image, temporary image or assembly residue"):
        build.expect_containerd_clean()

    with test.phase("a sandbox from the alias boots with the Dockerfile, SDK and SPACE state"):
        build.boot_and_verify_guest()

    with test.phase("kill the booted sandbox; delete the template"):
        build.retire()

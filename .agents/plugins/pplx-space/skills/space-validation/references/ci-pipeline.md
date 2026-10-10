
# Develop the SPACE CI pipeline

GitHub Actions workflows live in `.github/workflows/`; shared runners live in
`ci/scripts/`. Cargo artifacts remain separately cached through sccache.

GitHub Actions jobs get the same kind of image through `ci/scripts/ci-image.sh`.
Its tag hashes `ci/Dockerfile.ci` and `rust-toolchain.toml`, which the image
installs, and only main pushes publish it to `ghcr.io/perplexityai/space-ci`.
Other jobs pull that tag and build locally when it is new or unavailable, so a
change to either file is still tested before it merges. Keep tools that every
run needs, such as cargo-vet and cargo-deny, in the Dockerfile rather than
installing them in a job.

Every third-party image GitHub CI pulls or builds from is listed in
`ci/mirrored-images.txt`, pinned by digest: the test containers, the M1 SDK
check's generator, the Dockerfile bases, the Kind charts' images, and the
observability backends' images. Main
pushes copy any new digest
to `ghcr.io/perplexityai/space-ci-mirror`. Jobs take it from there through
`ci/scripts/mirror-images.sh`: `pull` for images a tool runs, and
`build-contexts` for Dockerfile bases, which overrides a `FROM` image in GitHub
CI only (`SPACE_CI_IMAGE_MIRROR`) and leaves the Dockerfiles unchanged for the
private pipeline. Both fall back to the source. When a Dockerfile, chart or
tool changes an image, change its line there too.

Rust CI checks are owned by `ci/scripts/run-rust-checks.sh` and its
pipeline steps: formatting/error policy, workspace lint and compilation with
all targets/features, and the common-svc feature-isolation check. Preserve the
runner's complete workspace test inventory, including ignored tests and doc
tests. Do not narrow CI selectors because local checks were sufficient for a
focused change.

Pull-request and merge-queue Rust tests reuse the Cargo target directory that
main's cold build saved, and `ci/scripts/rust-target-cache.sh` stamps source
mtimes so Cargo rebuilds only what changed since. That makes every compile-time
input a correctness contract: a file a crate reads while compiling must reach
Cargo through rustc's dep-info (`mod`, `include_str!`, `include_bytes!`) or a
build script's `cargo:rerun-if-changed`, and an environment variable through
`cargo:rerun-if-env-changed` or `env!`. An undeclared input leaves the unit
falsely fresh in PRs; main's cold build is what catches it. Read build outputs
through Cargo (`CARGO_BIN_EXE_*`, its JSON messages), or by the uplifted name of
a target Cargo still builds.

Every graceful Postgres/Redis fixture skip emits a `SPACE-TEST-DEP-SKIP:`
stderr marker and appends to the file named by `SPACE_TEST_DEP_SKIP_LOG` when
set (`crates/libs/db-test-fixtures/src/lib.rs`);
`ci/scripts/run-rust-checks.sh` collects and reports the count on
every run — zero proves no fail-open path was taken. A configured log is
fail-closed: path and write errors fail the affected test so collection
cannot silently be incomplete. The run-scoped nextest Postgres setup is also
fail-closed so a setup outage cannot fan out into opaque per-test failures;
the Redis setup (`ci/scripts/setup-nextest-redis.sh`) is
**deliberately fail-open** and falls back to the counted skip machinery. Do
not weaken the fail-closed paths, and do not "fix the asymmetry" by making
the Redis setup fail-closed.

For deploy-phase template publishing use `$space-template-lifecycle`; for
post-deploy smoke triggering read [smoke testing](smoke-testing.md). Preserve
their ordering after Helm deploy and before validation.

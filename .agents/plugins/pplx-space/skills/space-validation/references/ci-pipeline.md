
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
all targets/features, and the common-svc feature-isolation check. The checks
that run beside nextest live in `ci/scripts/lib/rust-side-checks.sh`, which the
Rust caches' keys do not hash.

The `Rust isolated crates` job (`run-rust-checks.sh isolated-crates`, its own
entry in `rust-static-ci.yml` with its own compiler-cache family) lints
workspace libraries one at a time, so a crate that uses a dependency feature
it gets only through workspace feature unification fails. Its selection and
lints live in `ci/scripts/lib/rust-isolated-crates.sh`, which the cache keys do
not hash either. From `cargo metadata` it selects every library, other than
`pplx-space-common-core` and `pplx-space-common-svc`, that reaches common-core
at most directly: no other workspace member in its normal dependency graph may
reach either of the two. Declared dependencies, optional ones included, decide
first; a library they leave out is decided again from the graph `cargo tree`
resolves for it with all of its features, so an optional dependency nothing
enables does not count. Crates that reach common-core through another crate,
such as everything built on `pplx-space-metrics`, are left out because each
builds that crate's graph too. A short commented `ALSO` list adds crates whose
only other path to common-core is `pplx-space-metrics`, such as
`pplx-space-tokio-runtime`; the check fails on an entry that is not a
workspace library or that the rule does not leave out for that reason. The log
names the libraries left out. Each crate gets its own `cargo clippy` runs with
`--no-default-features` and with `--all-features`, each once with
`--all-targets` and once with `--lib`. The `--lib` runs catch a feature that
only a dev-dependency turns on. A new crate needs no CI edit. The `clippy` mode
does not run this check; to run it locally, from the repository root:

```sh
bash -euo pipefail -c 'source ci/scripts/lib/rust-isolated-crates.sh; run_isolated_crate_checks'
```

Preserve the runner's complete workspace test inventory, including ignored
tests and doc tests. Do not narrow CI selectors because local checks were
sufficient for a focused change.

GitHub's Rust tests job runs that inventory in two containers on one runner,
from one build. `run-rust-checks.sh tests-unprivileged` runs as the CI image's
`space-ci` user (the runner user's uid) with `--cap-drop=ALL`,
`no-new-privileges`, no host network and no Docker socket, on a bridge network
where the runner started Postgres and Redis (`ci/scripts/ci-test-services.sh`);
the nextest setup scripts take those servers' addresses from
`SPACE_NEXTEST_POSTGRES_ADDRESS` and `SPACE_NEXTEST_REDIS_ADDRESS` instead of
starting their own. It builds everything, checks the `privileged` group in
`.config/nextest.toml`, and runs `not group(privileged)`. The check requires
every clause of the group's filter to match at least one test, all of them in
the group; it catches a renamed test, a typo, or another override taking the
test first, each of which would otherwise move a test to the unprivileged half
silently. It cannot find a privileged test that no clause names.
`tests-privileged` then runs `group(privileged)` as root in a `--privileged`
container with the Docker socket but still no host network, from the first
step's binaries through nextest's `--binaries-metadata` and
`--cargo-metadata`, so it compiles nothing. It first confirms it selects the
tests the first step listed, runs the M1 generator check, and returns any
file it created to the runner user on every exit, including one during its
toolchain preflight. The split keeps ordinary tests from mutating the host by
accident; it is not a trust boundary. The unprivileged step mounts the whole
checkout read-write, so it can write everything the root step runs from: the
target directory (the test binaries, the `nextest-*.json` metadata and
`nextest-privileged.txt`) and the scripts and configuration the root step
executes (`ci/scripts/run-rust-checks.sh`, `.config/nextest.toml` and the
nextest setup scripts). A hostile test or build script in the first step
therefore still runs as root on the runner. Add a test to the group when it needs
root, kernel capabilities, devices or its own containers, or when its root
path differs from its unprivileged one (or split it in two); keep such tests
asserting what they need, so a missing one fails the unprivileged step.
`tests` and `all` still run the whole inventory in one process.

A targeted local `cargo nextest run -p <crate>` for Postgres-backed tests also
needs `-p pplx-space-db-test-fixtures`: the run-scoped setup script in
`.config/nextest.toml` executes that crate's `space-nextest-postgres-setup`
binary and fails closed when the run didn't build it. Add `--all-features`
too, as CI does: some test modules rely on dependency features (tokio's
`test-util`, for one) that only the workspace build unifies.

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

Postgres/Redis fixtures fail closed unless the run opts out with
`SPACE_TEST_DEPS_OPTIONAL=1`. Every opted-out skip emits a `SPACE-TEST-DEP-SKIP:`
stderr marker and appends to the file named by `SPACE_TEST_DEP_SKIP_LOG` when
set (`crates/libs/db-test-fixtures/src/lib.rs`);
`ci/scripts/run-rust-checks.sh` collects and reports the count on
every run — zero proves no fail-open path was taken. A configured log is
fail-closed: path and write errors fail the affected test so collection
cannot silently be incomplete. The run-scoped nextest Postgres setup is also
fail-closed so a setup outage cannot fan out into opaque per-test failures;
only an opted-out run without a reachable Docker daemon skips it, with a
marker, leaving each test to fail or skip on its own;
the Redis setup (`ci/scripts/setup-nextest-redis.sh`) is
**deliberately fail-open** and falls back to per-test containers, which fail
closed or, opted out, take the counted skip. Do
not weaken the fail-closed paths, and do not "fix the asymmetry" by making
the Redis setup fail-closed.

For deploy-phase template publishing use `$space-template-lifecycle`; for
post-deploy smoke triggering read [smoke testing](smoke-testing.md). Preserve
their ordering after Helm deploy and before validation.

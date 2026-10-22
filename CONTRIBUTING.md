# Contributing to SPACE

Thank you for your interest in SPACE. SPACE is open source, and we welcome
thoughtful bug reports and proposals. Our maintenance team is still building
the capacity to review and support outside contributions, so we ask you to
discuss most changes before writing code.

## Propose a change

Search existing [issues](https://github.com/perplexityai/SPACE/issues), then
open one describing the problem, why it matters, and your proposed approach.
Include your self-assessed severity and a brief reason for it. If an AI agent
drafted the issue, say so. The SPACE team will triage proposals and decide
which work to take on.

Please wait for a maintainer to agree on the scope before opening a pull
request, except for trivial bug fixes. Agreement to proceed
does not guarantee that a pull request will be merged.

Report suspected vulnerabilities privately as described in
[SECURITY.md](SECURITY.md), rather than in a public issue or pull request.

## Contributor license agreements

Before your first contribution is merged, sign the
[Individual Contributor License Agreement (ICLA)](https://cla-assistant.io/perplexityai/SPACE)
through CLA Assistant, unless your account has a maintainer-approved exemption
or your contribution is covered by a confirmed corporate agreement.

For contributions on behalf of a company, review the
[Corporate Contributor License Agreement (CCLA)](https://gist.github.com/NancyKwongPerplexity/c70de89218671db0de01548bef889c7d)
and contact [space-team@perplexity.ai](mailto:space-team@perplexity.ai) to arrange
signature by an authorized company representative and confirmation of the
covered GitHub accounts.

### Agent-assisted contributions

For agent-assisted work, disclose AI assistance in the pull request description
instead of adding automation-only `Co-authored-by` trailers.

## Before your first commit

Install [pre-commit](https://pre-commit.com/) and the repository hooks in each
clone, using [uv](https://docs.astral.sh/uv/):

```sh
uv tool install pre-commit
pre-commit install --install-hooks
```

The pinned [Gitleaks hook](https://github.com/gitleaks/gitleaks#pre-commit) scans
staged changes before Git creates a commit and redacts detected values. Initial
setup builds the scanner with Go; [pre-commit 3+](https://pre-commit.com/#golang)
can download Go if it is absent, so allow network access for setup. Run
`pre-commit run gitleaks` to check staged changes manually.

Before committing, ask your agent to use the
[space-public-review skill](.agents/plugins/pplx-space/skills/space-public-review/SKILL.md)
on the staged change, or follow it as a manual checklist. Bring private issues
upstream through clean reproductions with synthetic inputs and caller-supplied
configuration. Remove any credentials the hook reports rather than bypassing it.

Known public test fixtures have exact-value, exact-path exceptions in
[.gitleaks.toml](.gitleaks.toml). For a new false positive, establish that it is
intentionally public test material and propose an equally narrow exception with
its reason; do not exclude whole files or test directories.

These checks cover staged changes, not history; review surrounding staged content
because diff scans can miss edits inside existing multiline keys.

## Run the tests

Tests that need Postgres or Redis start them in Docker, so run them with a
reachable Docker daemon. `cargo nextest run` starts one shared Postgres and
Redis per run through the setup scripts in [.config/nextest.toml](.config/nextest.toml);
build `cargo build -p pplx-space-db-test-fixtures --bin space-nextest-postgres-setup`
first when you select packages with `-p`. Other runners start a container per
test. `ci/scripts/run-python-tests.sh` starts the Postgres the Python tests use.

These dependencies are required: when one is unavailable, each test that needs
it fails and names it. To run without them, set `SPACE_TEST_DEPS_OPTIONAL=1`;
those tests then skip, each printing a `SPACE-TEST-DEP-SKIP:` line, and validate
nothing. Opted out, `cargo nextest run` and `ci/scripts/run-python-tests.sh`
also start no Postgres when Docker is not reachable, and say so with the same
marker. A few Rust tests use fixtures that never skip, such as `pg_test_db`,
and fail even then.

CI sets `SPACE_CI_REQUIRE_TEST_DEPS=1`, which overrides the opt-out. Don't set
that variable yourself: the fixtures still read it set to empty or `0` as
optional, for older callers, but the NoCoW host-filesystem tests treat any
value of it as a CI run and fail without their fixture.

Tests that need root or a disposable host filesystem have their own opt-ins,
which this does not turn on; see "Privileged tests and the CI host" in
[docs/rust/space-conventions.md](docs/rust/space-conventions.md).

## Submit a pull request

Keep the change focused and link the relevant issue when there is one. Start
the pull request description from the
[template](.github/pull_request_template.md). It asks what merging the change
does and why, and how you validated it. It also asks for your self-assessed
severity with a brief reason, or a note that severity does not apply, and what
the change could break. Disclose whether an AI agent drafted the pull request
description or contributed to the change, and identify those parts. Size the
description to the risk of the change, not to the effort behind it.

Many readers see the description long after the pull request merges, and they
can see only this repository:

- Describe what merging the pull request does, not how the pull request got
  there. Answer review in comments and commits, and keep the description
  current.
- Link each reference to something a reader can open: an issue, a pull
  request, a commit on `main`, or a file in this repository. Otherwise, state
  the point directly.
- Open an issue for each follow-up you find, and link it. Nobody tracks a
  finding recorded only in a merged pull request's description.
- Before you mark the pull request ready for review, check each claim, commit,
  and result in the description against the current head.

Run `cargo arch` to check the repository structure rules in
[ARCHITECTURE.toml](ARCHITECTURE.toml); [archcheck](crates/tools/archcheck/README.md)
explains each rule and its baseline.

For larger work, consider using `gh stack` to split it into a patch stack.
Keep each pull request small enough for a human reviewer to reason about, but
complete in scope so it can be understood and validated on its own. Explain
how each pull request depends on the others and fits into the full stack.

The SPACE team will triage and review pull requests according to project
priorities and available maintenance capacity. We may decline a contribution,
including a technically sound one, if we cannot responsibly review or maintain
it. We will aim to explain that decision when we can.

## Longer-term participation

If you or your organization are interested in sustained participation, please
contact [space-team@perplexity.ai](mailto:space-team@perplexity.ai).

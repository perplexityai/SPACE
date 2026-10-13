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

## Submit a pull request

Keep the change focused and link the relevant issue when there is one. In the
pull request description, explain the problem, what changed, and how you
validated it. Include your self-assessed severity and a brief reason, or say
when severity does not apply. Disclose whether an AI agent drafted the pull
request description or contributed to the change, and identify those parts.

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

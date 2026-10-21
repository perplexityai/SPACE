---
name: space-code-quality
description: "Write or review SPACE tests, smoke coverage, snapshots, regression fixes, and durable comments, including test selection for configuration-only changes."
---

# Maintain SPACE code quality

Apply these rules across languages. Use the language-specific skill as well when
one matches the change.

## Review-pointed bugs

For an application behavior bug identified by review, add a regression test at
the owning layer when it can faithfully reproduce the issue. Demonstrate the
failure, then apply the fix and rerun it. If reproduction needs unavailable
infrastructure or existing coverage already pins the behavior, report that
evidence instead of adding a surrogate test.

## Tests

- Reject new deployment smoke tests (Kind or deployed) outside the typed
  `space_smoke` framework; allow maintenance of existing untyped ones. Apply the
  [smoke reviewer stop rule](references/test-economy.md#smoke-tests-reviewer-stop-rule).
- Avoid long wall-clock sleeps and artificial per-item latency in routine
  tests. Prefer virtual or injected time for timer behavior, and explicit
  synchronization or bounded condition waits for asynchronous readiness.
  Preserve regression sensitivity: elapsed-time checks must use the same
  controlled clock. Merely shrinking sleeps or pausing Tokio while measuring
  `std::time` can erase coverage. Necessary real-time integration evidence may
  use bounded waits justified by the behavior under test.
- Keep behavioral workloads and assertions in routine CI. Make genuinely long
  real-latency performance runs explicitly opt-in independently of ignored-test
  flags: SPACE CI runs ignored tests.
- Add a test only when it protects meaningful behavior or an invariant not
  already covered. Do not write unit tests that assert on telemetry, including
  emitted logs, Datadog metrics, metric tags, or telemetry schemas. Assert
  meaningful application behavior and invariants instead. A change with no
  coverage gap needs no new test; cite the existing coverage.
- Changes limited to non-application code add no tests. This includes Terraform,
  Helm or Kubernetes manifests, smoke-test definitions or harnesses, and one-off
  operator scripts. Validate them with existing lint, schema, render, syntax,
  dry-run, or targeted operational checks. If the same diff changes application
  code, apply the coverage rules to that application behavior only.
- Test at the lowest layer that can catch the regression. Where a lower layer
  owns a shape, higher layers assert only their own delta and leave a one-line
  delegating comment.
- Variant sweeps are one table, loop, or snapshot test enumerating the whole
  enum, not near-identical functions over a hand-picked subset. Cover an
  adjacent case by extending the existing table before adding a function.
- When review iteration changes a fix, revise the existing regression test's
  inputs; do not accrete a new test per round.
- Do not add [change-detector tests](https://testing.googleblog.com/2015/01/testing-on-toilet-change-detector-tests.html):
  tests derived from the same information as the production code that fail on
  refactors without verifying behavior. A test needs an independent oracle and
  a reachable production behavior. Extracting or grepping a function's source
  to assert that it exists, contains a command or flag, or omits a call is a
  change detector; test the observable behavior at the lowest owning layer or
  delete the test. Source-inspection tests are justified only when they enforce
  a general invariant across present and future resources, justified independently
  of current defaults and falsifiable with valid and invalid examples.
- Do not mirror Helm/YAML/Terraform values, file layout, or Helm's layering and
  rendering semantics in tests without a distinct product contract. Rendering
  YAML or naming a test a security guard does not establish that contract.
- Do not make future behavior green by expecting `Unimplemented`, a panic, or
  another temporary failure. Add the scenario red-first when the production
  path is ready to be implemented.
- Refresh checked-in `insta` snapshots whenever a change affects a user-visible
  serialized shape. Inspect `.snap.new` before promotion and commit the shape
  change with the code.
- Rust unit-test bodies live in sibling module files, not inline
  `#[cfg(test)] mod tests { ... }` blocks. Preserve the exact cfg attributes and
  module path. Read `docs/rust/space-conventions.md` section "Test placement"
  before moving tests or snapshots.

Read [test economy](references/test-economy.md) before adding or reviewing tests,
including for non-application changes, and when choosing a coverage strategy,
test layer, or snapshot. Name the reachable regression, independent contract,
existing coverage gap, and lowest owning layer; zero new tests can be correct.

Database-backed tests use the repository fixtures. When Docker is unavailable,
bring it up rather than accepting dependency skips as validation. Use
`$space-validation` on macOS. Unavailable Postgres and Redis fixtures fail
closed unless `SPACE_TEST_DEPS_OPTIONAL=1` opts out; do not weaken that
behavior or validate with the opt-out set.

## Durable comments

- Explain constraints, rationale, or workarounds. Do not narrate a refactor or
  describe what the current session changed.
- Do not add Linear IDs, bare GitHub PR or issue numbers, or similar tracker
  references to source, chart, shell, YAML, or Dockerfile comments. Put the
  durable substance in the comment and the reference in commit or PR metadata.
- Do not introduce new ticket-tagged TODOs. Remove a pre-existing one when the
  surrounding code is rewritten, but do not churn unrelated comments.

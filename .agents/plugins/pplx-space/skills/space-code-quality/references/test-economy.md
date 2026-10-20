# Test economy: focused tests at the right layer

This file helps choose *how much* test to write and *where*. Rust placement
mechanics live in `docs/rust/space-conventions.md` § Test placement and are not
repeated here.

Use an independent oracle and a reachable production behavior as the criteria
before writing a test. A test derived only from the implementation cannot prove
that the implementation meets its contract.

Tests that restate the type system, re-prove a sibling layer's contract, or
exist to make a diff look thorough cost compile time and review attention,
and they bury the tests that matter when one fails.

## Three coverage questions

1. **What regression does this uniquely catch?** Name a concrete, reachable
   regression and the independent contract or oracle that determines correctness
   (not copied implementation or configured literals). Identify the coverage gap.
   If an existing test already fails when that regression happens, do not write
   a new one —
   point at the existing test in the PR body if reviewers might wonder.
   A change with no coverage gap legitimately adds **zero** tests.
2. **Is the contract already unrepresentable?** Code following
   *parse, don't validate* (`docs/rust/type-driven-design.md`)
   needs no test for states the types exclude. Do not test that a
   `TemplateName` cannot hold a traversal component *and* that every consumer
   of `TemplateName` rejects traversal components — the newtype's own parse
   tests are the single home for that claim.
3. **What is the lowest layer that can catch it?** Test there. A *claim*
   proven identically at two layers is one contract with two maintenance
   bills. This forbids duplicate assertions, not layered contracts: a unit
   test pinning translation logic and a smoke proving the deployed platform
   honors it assert different things and may both exist. Likewise the
   documented flow for infrastructure-dependent bugs — closest unit-level
   invariant now, gap recorded, closed later at the infrastructure layer —
   ends with two tests asserting *different halves* of one story, which is
   fine.

## Non-application changes: validate, do not add tests

Changes limited to Terraform, Helm or Kubernetes manifests, smoke-test
definitions or harnesses, and one-off operator scripts add no tests. Use the
repository's existing lint, schema, render, syntax, dry-run, or targeted
operational checks as appropriate. Update existing test artifacts when the
change itself requires it, but do not add a test of a smoke test, manifest, or
operator script.

This boundary does not relax application-code coverage. If a diff also changes
application code, apply the three coverage questions to that application
behavior. A smoke test may still be the lowest layer for application behavior;
changing the smoke test itself does not require a second test.

## Configuration changes: prove a contract, not the values

Reject tests that merely read Helm/YAML/Terraform values or file layout and
compare them with copied literals, reimplement Helm's layering/rendering
semantics, or assert that configured inputs reappear in rendered output.
Calling `helm template` does not supply an independent oracle; neither do labels
such as "regression", "security", or "deployment guard". The criteria below
govern existing configuration tests and application tests that touch
configuration; they do not override the no-new-tests boundary above.

| Reject | Existing coverage is defensible when |
| --- | --- |
| Freeze endpoint, audience, and enabled values across an environment matrix and claim tokens authenticate. | Exercise the real auth path: accept a valid projected token and reject an invalid audience or disallowed fallback. |
| Recreate values precedence in Python, then assert that Helm emits those values. | Test chart-owned semantic transformation or rejection logic against an independently justified contract, with valid and invalid inputs; do not test Helm itself. |
| Match exact source text or current defaults under a "general safety invariant" label. | Check a general admission/security policy across present and future resources, showing both a violating example that fails and a compliant example that passes. |

A general invariant must be justified by a requirement independent of today's
configuration. It is not permission to freeze environment matrices or static
defaults. Existing runtime/security tests and chart-owned policy checks remain
valuable when they exercise a distinct contract at its lowest owning layer.

Configuration-only changes add **no** tests. Cite applicable existing lint,
schema, or render checks and state what they establish. If the behavior needs
runtime or deployment validation that is unavailable, record that gap; do not
substitute a values mirror and claim behavioral proof.

During an OSS split or repository move, keep meaningful tests with the owning
code or integration boundary. Do not add cross-repository reads or copy private
configuration into public tests to preserve a mirror; reconsider or remove the
mirror within the authorized scope instead.

**Reviewer stop rule:** before retaining or modifying an existing configuration
test, require the reachable regression, independent oracle, coverage gap, and
owning layer. If the only failure is "the configured value or file layout
changed", reject the test; renaming it or wrapping it in a renderer does not fix
the gap.

## Smoke tests: reviewer stop rule

Reject new smoke tests against a SPACE deployment (Kind or deployed) outside the
typed `space_smoke` framework, in any language. Component and substrate smokes
that run without a deployment are out of scope. Inspect new and existing
scripts, inline CI/configuration commands, and embedded shell snippets: a new
test belongs in the typed framework even when added to an existing file.

Existing untyped smoke tests may be updated. A change adds a new test when it
asserts a contract the existing suite didn't already cover. Changes that keep an
existing check working, or strengthen a contract it already asserts, are
maintenance, so new lines, actions, polling, or assertions alone aren't
violations. Don't require piecemeal splitting or immediate migration as a
condition of maintenance.

Allow thin shell wrappers that bootstrap the environment or launch the typed
runner and propagate its exit status. A rejection should identify the new untyped
test and request its typed scenario or framework implementation. The
[smoke guidance](../../space-validation/references/smoke-testing.md#authoring-smoke-coverage)
owns where scenarios live, which boundary rules framework extensions follow, and
the migration contract.

## One contract clause, one test

Test the *contract* — the promise callers rely on — not the implementation's
shape. Symptoms of implementation-shaped testing:

- One test per internal branch or private helper, rather than per observable
  promise. If a refactor that preserves behavior would break the test, the
  test is pinning implementation.
- Asserting intermediate state that no caller can observe.
- A new test per PR iteration: when review changes the fix, rewrite the
  test's *inputs*, don't accrete a second test alongside the first.

Variant sweeps belong in **one** test, and the gold shape is the one the
compiler enforces: an exhaustive `match` over the enum, so adding a variant
fails the build until the test covers it. A hand-written cases table
(`decision_table_covers_every_verdict` in
`crates/libs/cluster/controller/server/src/transient_lifecycle_sweeper/classify/tests.rs`
is the in-tree shape) gets the one-test structure right but cannot falsify
its own name — pair it with an exhaustive `match` or iterator over the
variant set, or its coverage claim is aspirational. Either way, enumerate
**all** of the enum — a hand-picked subset reads as coverage and isn't.
Live example of the failure mode:
`requested_create_reuses_only_stable_sandbox_states`
(`crates/libs/sbx/mgr/server/src/service/tests.rs`), which as of 2026-08
covered 17 of 18 `SandboxState` variants; the missing `Quarantined` arm
sat invisible for weeks precisely because the test looked exhaustive.

For whole-shape pinning, prefer one `insta` snapshot over an assert ladder
**when the shape is itself the caller-facing contract** — a wire message, a
public pod-spec contract, a public API's route set. Rendering YAML alone does
not make it a public contract: a snapshot that only freezes configured defaults
or pass-through values is still a mirror. A ladder of `contains` checks
cannot fail when something unexpected is added; a snapshot can. But a
snapshot of internal rendering with no independent oracle is a mirror
test — the value audit removed exactly that class, including tests that
pinned rendered SQL strings byte-for-byte — so ask first: if the string
changed but behavior didn't, should this fail? Only snapshot where the
answer is yes; for SQL that usually means testing what the query *does*
against Postgres, not what it looks like. Keep
inline asserts for what a snapshot cannot express (negatives, counts, exact
values of run-varying fields via redaction-asserts). Never snapshot a
struct you just built from literals; snapshot *transforms* — the output of
the function under test.

## The layers, and what each one owes

Placement rules live in space-conventions § Test placement; this table is
about *ownership of claims*, not file locations.

| Layer | Owns | Must not |
| --- | --- | --- |
| **Unit** (sibling `tests.rs`) | private invariants, parsing, state machines, pure logic; milliseconds, no containers | reach for Postgres/Redis when the logic is testable without them |
| **DB-backed unit** (`pg_test_db()` / `raw_pg_test_db()`) | SQL semantics *as the contract*: row lifecycles, constraints, query correctness | re-test Rust logic a plain unit test covers; the container is for the SQL |
| **Integration** (`crates/<crate>/tests/`, reserved per space-conventions) | public-API-only guards, binary spawns, multi-file harnesses | become a default home for unit tests; each file is a separately-linked binary |
| **Smoke** (`space_smoke` scenarios and legacy suites on Kind or a live deployment) | cross-service outcomes on a real substrate: does the *platform* do it | re-assert translation/validation logic the unit layers pin; smokes are for substrate truth |

Where a lower layer owns a shape (the golden pod-spec snapshots in
`crates/libs/common/core/src/fixtures/`), higher-layer tests assert only
their *own*
delta and leave the one-line delegating comment — `cluster-controller`'s
pod tests write "golden snapshots, so it is not re-asserted here." Copy
that comment pattern; do not copy the assertions.

## Right-sizing the diff — reviewer heuristics, with the evidence they demand

These are heuristics, not thresholds; what makes them enforceable is the
evidence each one obliges the author to put in the PR:

- **Tests dominating the diff is a prompt, not a verdict.** When most of a
  PR is test code, the PR body names the new contract clauses that justify
  it — or concedes the tests are accreted churn and trims them. Absent
  either, reviewers should ask.
- **Prefer extension over addition.** Covering an adjacent case by adding a
  row to an existing table/matrix/snapshot beats a new test fn; if a new fn
  was chosen anyway, the PR body says why.
- **Arrange sections lean on the shared machinery** — `pplx-space-test-env`,
  `pplx-space-db-test-fixtures` (fixtures, seed constants, node inserts),
  `SimWorkerHarness` (`node-api-sim`), the observability/metrics capture
  layers, and the crate-local helpers where they exist (`test_conn()` in
  sandbox-manager and template-manager, request builders, scenario
  seeders). Hand-rolled ceremony that an existing helper covers is review
  feedback waiting to happen. Builders keep one exhaustive literal per wire
  type; values a test's assertions read stay explicit at the call site.
- **A test's name is a claim its body must be able to falsify.** "Does not
  panic" bodies under "must never tick" names read as coverage and aren't.
  For gate-shaped tests (disabled/refuse paths), seed the work the loop
  would do, give it a paused-clock window, assert the recorders stayed
  empty — and note in the PR body that the assert was mutation-checked
  (gate flipped, test failed, gate restored) so reviewers know it can fail.

## For agents specifically

Tests are not evidence of effort. An agent PR whose application-code diff is
ten lines does not need a matching pile of tests riding along; it needs the one
test the ten lines' contract demands — or zero, with a sentence naming the
existing coverage. When iterating under review, revise the failing test you
already wrote rather than adding a new one per round. And before writing a
Postgres-backed test, ask whether the thing you are proving is the SQL — if it
is the Rust around it, the unit layer is faster to write, faster to run, and
fails more legibly.

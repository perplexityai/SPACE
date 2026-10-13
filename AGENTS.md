# SPACE — Agent Instructions

SPACE is an open-source sandbox platform built on Firecracker microVMs and
licensed under the Apache License, Version 2.0. Keep this file to
repository-wide contracts; task procedures belong in the matching skill,
localized `AGENTS.md`, or maintained runbook.

## Sources of truth

Start with `docs/architecture.md` for the system overview and the owning
crate's documentation for detailed contracts. Setup and first-use guidance
live in `docs/installation.md` and `docs/first-sandbox.md`.

The hosted guide publishes the pages listed in `docs/book/SUMMARY.md` after
they merge. Add each new top-level `docs/*.md` page there, or list it in
`UNPUBLISHED` in `docs/book/build.py` with a reason.

Some documents predate current code. Treat divergence as a finding to resolve,
not automatic proof that the document is stale. Localized `AGENTS.md` files add
constraints for their subtrees.

Repository skills live under `.agents/plugins/pplx-space/skills/` and are
exposed through `.agents/skills/` and `.claude/skills/`. Load a skill only when
its description matches the task.

## Repository contracts

- Never run `git merge`. Advance with `git pull --ff-only` or rebase.
- Route database operations through the owning storage layer. The
  `space-data-store-development` skill defines the store boundary and guarded
  transition exceptions.
- Existing validators and CI guards are the enforcement source. Do not restate
  their implementation in agent instructions.

## Public-safe reproductions

Treat this repository as upstream when investigating a problem from a private
incident, dataset, or deployment. Establish a
[minimal, reproducible example](https://stackoverflow.com/help/minimal-reproducible-example)
using synthetic inputs and generic local configuration before adding evidence to this public
checkout. Keep credentials, raw logs, payloads, dumps, and private operational
values outside it. Use synthetic service names and endpoints; omit private
telemetry and tracker links. Fixtures, filenames, snapshots, comments, and
documentation must all be safe to publish.

Preserve the formats, relationships, and behavior needed to trigger the same
failure. Verify it before the fix and rerun it afterward; reuse existing coverage
or a disposable reproduction where appropriate under the
[space-code-quality test policy](.agents/plugins/pplx-space/skills/space-code-quality/SKILL.md#tests).
If the failure cannot be reproduced without private dependencies, report that
limitation without copying private material or claiming a synthetic test proves the fix.

Install the repository pre-commit hook as described in
[CONTRIBUTING.md](CONTRIBUTING.md#before-your-first-commit) before committing;
do not bypass it to commit credentials. Before committing, follow the
[space-public-review skill](.agents/plugins/pplx-space/skills/space-public-review/SKILL.md)
on the staged change and resolve confirmed disclosures. Report unresolved questions and
coverage gaps. Explain the generic failure and evidence in public commit and PR
text; keep deployment-specific values in private caller configuration.

## Operational safety

Treat live SPACE infrastructure as production-sensitive. Prefer read-only
inspection and normal reconciliation. Destructive operations require explicit
approval from a cluster administrator who understands the exact target, blast
radius, rollback, and verification plan. The `space-infrastructure` skill owns
the execution requirements.

## Completion boundary

Carry authorized implementation through appropriate validation and task-owned
fixture cleanup, then complete any requested commit, push, and PR update.
Disposable test resources may be created and cleaned within that authorized
scope; verify ownership and follow the harness's account and cleanup guards.
Local-dev tests can use real AWS resources. This does not authorize live
infrastructure changes or shared-cache pruning. Report a concrete blocker when
further progress requires permission or an external change.

## Validation

Choose checks that prove the affected behavior. Documentation-only changes do
not require workspace code checks. Dependency skips are not final evidence when
the required services can be started. Use `space-validation` when the correct
environment or evidence tier is not obvious.

## Stable architecture

- Firecracker microVMs provide sandbox isolation.
- gRPC is the inter-service protocol; Unix domain sockets serve node-local
  communication.
- btrfs copy-on-write provides fast sandbox rootfs creation.
- PostgreSQL is the control-plane desired-state store; SQLite stores selected
  on-node state such as template-manager data.
- Mutations persist intent and enqueue node reconciliation. The cluster
  controller drains triggers; periodic node reconciliation is the correctness
  backstop.
- Applicable node-to-node calls use `SPACE_INTERNAL_TOKEN` bearer
  authentication. Cluster Kubernetes authentication may disable static
  fallback; follow the owning chart and runbook.

## Sandbox lifecycle vocabulary

- `create` / `delete`: create or delete a sandbox.
- `stop` / `start`: stop or start without implying snapshot persistence.
- `pause` / `resume`: keep the Firecracker VM resident in memory.
- `suspend` / `restore`: persist VM memory and state, release worker resources,
  and later restore the snapshot.
- `wake`: intentionally accept any valid start, resume, or restore path.

ECW/E2B-compatible `pause` maps to SPACE `suspend`, and ECW `resume` maps to
SPACE `restore`. Do not rewrite ECW lifecycle handlers to SPACE pause/resume or
generic wake semantics.

## Instruction maintenance

`AGENTS.md` is canonical; `CLAUDE.md` is its symlink. Keep the root plus the
largest localized instruction chain below Codex's 32 KiB project-document
budget. Put conditional detail in skills, their references, or existing
runbooks. Plugin packaging rules live in `.agents/plugins/AGENTS.md`.

---
name: space-validation
description: Set up, test, or diagnose SPACE in local, Kind, smoke, load, and CI environments.
---

# Validate SPACE changes

Choose the narrowest environment and suite that proves the affected behavior.
Do not claim Firecracker, jailer, cgroup, namespace, btrfs, shim, or Kubernetes
substrate proof from a macOS-only run.

New smoke tests against a SPACE deployment (Kind or deployed) must be typed
Python `space_smoke` scenarios; existing untyped smoke tests may be maintained in
place. [Smoke testing](references/smoke-testing.md#authoring-smoke-coverage)
owns the authoring and migration rules.

## Select conditional guidance

Read only the references that match the task:

- Devbox bootstrap, macOS setup, on-node sandbox inspection, or
  direct deployment: [local development](references/local-development.md).
- Starting or recovering Docker Desktop on macOS: [Docker Desktop](references/docker-desktop.md),
  then run the bundled `scripts/ensure-docker.sh`.
- Linux/KVM Kind, Tilt iteration, Firecracker proof, or `smoke-kind-*`:
  read `references/kind-devbox-testing.md` if supplied with this skill;
  otherwise use the deployment's documented Kind runbook.
- Local, deployed, post-deploy, or in-cluster smoke suites:
  [smoke testing](references/smoke-testing.md).
- k6 profiles, monitoring, abort, cleanup, or failure diagnosis:
  read `references/load-testing.md` if supplied with this skill; otherwise use
  the deployment's load-testing runbook. Confirm its target, monitoring, abort,
  and cleanup procedures before launching load.
- Buildkite pipelines, dynamic upload, CI test inventory, or CI images:
  [CI pipeline](references/ci-pipeline.md).

Targeted tests are for iteration. Use the owning domain skill's final validation
before committing or opening a PR; `$space-rust-development` owns the complete
Rust command matrix. Dependency skips are not final validation when Docker can
be started. Postgres and Redis fixtures fail closed by default;
`SPACE_TEST_DEPS_OPTIONAL=1` turns them into counted skips.

Use `$space-template-lifecycle` when validation depends on template publishing,
kernel staging, or builder trust. Live cluster mutations remain approval-gated
and follow `$space-infrastructure` when supplied, or the deployment
administration runbook otherwise.


# Develop SPACE locally

Keep the `Tiltfile` (the Kind bring-up, which `infra/space/kind/scripts/up.sh`
starts) and the installation guide aligned whenever a service is added,
removed, or the local architecture changes materially.

Follow `CONTRIBUTING.md` for the development entry points and
`docs/installation.md` for the Linux/KVM runtime prerequisites. See
`lefthook.yaml` for the checks the hook runs.

## Choose the environment

- For production-shaped Kubernetes behavior, use the Kind/KVM harness and
  read `kind-devbox-testing.md` when supplied; otherwise use the deployment's
  Kind runbook. Follow the harness's localized `AGENTS.md` for API and safety
  boundaries.
- Integration tests that need live Postgres or AWS run as typed `space_smoke`
  `local.*` scenarios on the Kind harness; the docker-compose harness is gone.
  Real AWS use stays inside the sandbox account with task-owned, tagged
  resources and reversible teardown.
- On macOS, native smoke tests can prove startup,
  SQLite migrations, sockets, gRPC health, and CLI authentication. They cannot
  prove Firecracker, jailer, cgroups, namespaces, btrfs, or pod startup.
- When container-backed tests need Docker on macOS, read
  [Docker Desktop](docker-desktop.md) and bring the daemon up instead of
  accepting dependency skips.

## Local runtime

Follow `docs/installation.md` to install prerequisites and bring the Kind stack
up with `infra/space/kind/scripts/up.sh`, which starts Tilt. Use `infra/space/kind/README.md` for the ongoing
development loop and `docs/troubleshooting.md` for inspection and recovery.
Direct-node deployments require operator-supplied provisioning and service
management; this repository does not provide a direct-deployment command.

## On-node sandbox management

Use the node-local CLIs for routine development:

```bash
space-sbxmgr create --template seed
space-sbxmgr list
space-ssh <id-prefix>
```

Sandbox deletion remains a destructive operation and requires the approval
specified in the root `AGENTS.md`:

```bash
space-sbxmgr delete <id-prefix>
```

When troubleshooting an existing sandbox, follow `docs/troubleshooting.md` for
public API inspection and sanitized diagnostic evidence. Check recovery and
rollback options before planning a repair, and follow the deployment owner's
incident procedures.

Use `$space-template-lifecycle` for template builds, deployment publishing, or
kernel staging, and read [smoke testing](smoke-testing.md) for end-to-end
validation.

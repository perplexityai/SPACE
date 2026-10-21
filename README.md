# SPACE

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

[Guide](https://space.docs.perplexity.ai/guide/index.html) · [API Reference](https://space.docs.perplexity.ai/api/index.html)

SPACE (Sandboxed Platform for Agentic Code Execution) is a sandbox platform for running code in isolated Firecracker microVMs. Applications use its HTTP API to create Linux environments, execute commands, collect output, and manage sandbox lifecycles.

Use SPACE when your application needs a separate execution environment for an agent, a user workspace, or a code-running job. Your application decides what to run and what results to keep; SPACE manages the sandbox, its processes, and the connection carrying command output.

## How it works

A **template** supplies the starting environment. A **sandbox** is an instance of that environment with its own VM and writable filesystem; multiple commands can run in the same sandbox.

```text
Your application
    |
    +-- Create sandbox from template --> Wait for readiness
    |
    +-- Create command session --------> Attach to start the process
    |                                    Read output and exit result
    |
    +-- Keep, pause, or suspend --------> Wake when needed
    |
    +-- Delete sandbox
```

Sandbox lifecycle changes are asynchronous: the API returns an operation ID that you poll for completion. Command output uses a separate data-plane connection and a session-scoped attachment token.

## Get started

Follow [Installation and configuration](docs/installation.md) to deploy SPACE in a local Kind cluster and authenticate. Then follow [Your first sandbox](docs/first-sandbox.md) to run a command and collect its output. The setup guide covers Kubernetes; the [architecture guide](docs/architecture.md#request-and-execution-flow) also explains how SPACE can run without it.

## Documentation

Read the hosted [Guide](https://space.docs.perplexity.ai/guide/index.html) and [API Reference](https://space.docs.perplexity.ai/api/index.html) online. Both update automatically after changes merge. The links below point to source documentation in this checkout.

| Guide | What you will learn |
|---|---|
| [Versioning and compatibility](docs/versioning.md) | Release progression, supported guests, service stability, and upgrade/rollback requirements. |
| [Architecture](docs/architecture.md) | How templates, sandboxes, processes, sessions, and operations relate; what pause and suspend preserve; how the services fit together. |
| [Installation and configuration](docs/installation.md) | Host prerequisites, local Kind deployment, configuration defaults, authentication, verification, and teardown. |
| [Operator CLI](crates/apps/admin/cli/README.md) | API access, port forwarding, `space-admin-cli login`, nodes, warm pools, and system-template builds. |
| [Your first sandbox](docs/first-sandbox.md) | A complete API workflow, command customization, output handling, inspection, and deletion. |
| [E2B Compatibility Wrapper](docs/ecw.md) | Configure ECW, create a compatibility API key, use the tested E2B Python SDK, and understand supported behavior and limitations. |
| [API reference](docs/api/index.md) | Endpoints, request and response fields, and examples. |
| [Troubleshooting](docs/troubleshooting.md) | Local setup, authentication, lifecycle operations, command streams, and safe retries. |
| [Operator egress profiles](docs/egress-profiles.md) | Configure named policy additions for federated native and ECW creates. |
| [Observability](docs/observability.md) | Telemetry build features, the Rust/Go selector contract, and unchanged image defaults. |
| [Warm pools](docs/warm-pools.md) | Keep booted sandboxes ready on Kubernetes so creates skip a cold boot. |
| [Base snapshots](docs/base-snapshots.md) | Restore sandboxes from a snapshot on a single node, optionally with a node warm pool. |

### Build documentation locally

To browse the guide and API together locally, install [mdBook 0.4.52](https://github.com/rust-lang/mdBook/releases/tag/v0.4.52) and the [API build prerequisites](docs/api/index.md#browse-locally). From a clean checkout, run:

```bash
bash scripts/build-docs.sh
python3 -m http.server 8000 --bind 127.0.0.1 --directory target/docs/site
```

Open <http://127.0.0.1:8000>. The build also requires Git and Python 3.11+. The standalone commands remain `bash scripts/build-guide-docs.sh` and `bash scripts/build-api-docs.sh`; they also support local uncommitted edits.

The API serves its generated reference at `/openapi-m1.json`, with request fields, response schemas, and errors. `/openapi.json` adds the incubating operations that are not hidden. Some incubating operations, including snapshots, checkpoint, and rollback, are hidden: unless the deployment enables them for your organization, they answer as if they did not exist. Neither document lists them for any caller; `/openapi-full.json` documents every operation and requires API credentials.

## SPACE API

The SPACE API manages sandbox lifecycles, runs and controls processes, and streams their output. Fields marked `incubating` in the schema are outside the supported contract.

SPACE is implemented in Rust, but applications interact with it over HTTP and server-sent events. You do not need to embed a Rust library to use it; the first-sandbox walkthrough runs a standard-library-only Python example. Installation and cluster diagnostics use Bash.

## Development

See [DEPENDENCIES.md](DEPENDENCIES.md) when adding or updating third-party code.

After [local installation](docs/installation.md), run `tilt up` from the repository root to rebuild affected images and roll their workloads as files change. The [Kind runbook](infra/space/kind/README.md) covers this development loop, local topology, and template builds.

Use focused unit tests for component logic and the [Kind smoke suites](infra/space/kind/README.md#smoke-test) for runtime behavior. For setup-specific diagnostics, see [host-path troubleshooting](infra/space/kind/README.md#host-paths-and-running-as-a-user-other-than-dev) and [template-build debugging](infra/space/kind/README.md#debugging-a-template-build).

## Repository layout

```text
crates/apps/            CLI tools and service binaries
crates/libs/            Shared Rust libraries
docs/                   Architecture, setup, API walkthrough, and troubleshooting
examples/               Runnable API examples without an SDK dependency
proto/                  gRPC protobuf definitions
templates/firecracker/  Seed builder and template recipes
scripts/                Utility scripts and smoke suites (scripts/tests/)
docker/                 Service Dockerfiles and Rust artifact build
ci/                     CI checks and artifact build scripts
infra/space/kind/        Local Kind configuration, bootstrap, and development runbook
infra/space/aws/reference/  Reference EKS deployment, applied on a schedule
```

## License

SPACE is licensed under the [Apache License, Version 2.0](LICENSE).

See [CONTRIBUTING.md](CONTRIBUTING.md) to propose changes or contribute.

For native API contract, concurrency, recovery, and quarantine coverage, see [Native API hardening](docs/native-api-hardening.md).

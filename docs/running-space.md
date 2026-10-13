# Running SPACE

[Documentation](../README.md)

Set up SPACE on a Linux host, verify the deployment, and diagnose problems before running sandbox workloads.

- [Install and configure locally](installation.md) covers host prerequisites, the Kind deployment, and authentication.
- Look up every supported `SPACE_*` setting in the [Configuration reference](configuration.md).
- Use [Observability](observability.md) to understand metrics, logs, and traces.
- Follow [Troubleshooting](troubleshooting.md) to investigate setup, sandbox, and command failures.
- Keep sandboxes ready on Kubernetes with [warm pools](warm-pools.md).
- Speed up creates on a single node with [base snapshots](base-snapshots.md).
- Secure service-to-service calls with [internal RPC TLS](internal-rpc-tls.md).
- Leave selected TLS streams encrypted with [TLS interception exemptions](tls-interception-exemptions.md).

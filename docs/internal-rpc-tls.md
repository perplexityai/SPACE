# Internal RPC TLS

ClusterApi, NodeApi and BuildManager TCP listeners and callers require authenticated TLS by
default. Unix-domain socket calls continue to use their existing peer-identity
controls. Existing RPC bearer and sandbox route-capability checks still apply.

## Configuration

Mount PEM files readable only by the service identities that need them. Set:

| Variable | Processes | Value |
| --- | --- | --- |
| `SPACE_INTERNAL_RPC_TLS_CERT` | internal-api, node-api and build-manager listeners | Server certificate PEM, including intermediate certificates |
| `SPACE_INTERNAL_RPC_TLS_KEY` | internal-api, node-api and build-manager listeners | Corresponding private key PEM |
| `SPACE_INTERNAL_RPC_TLS_CA` | Every ClusterApi/NodeApi/BuildManager TCP caller | Trusted CA bundle PEM |
| `SPACE_CLUSTER_API_TLS_SERVER_NAME` | ClusterApi callers | Expected certificate DNS name; defaults to the configured endpoint host |
| `SPACE_NODE_API_TLS_SERVER_NAME` | NodeApi callers | Expected certificate DNS name; defaults to the endpoint host |
| `SPACE_BUILD_MANAGER_TLS_SERVER_NAME` | BuildManager callers | Expected certificate DNS name; defaults to the endpoint host |

Set ClusterApi endpoint variables/configuration to `https://...`. Bare NodeApi and BuildManager
addresses become HTTPS; explicitly configured HTTP endpoints are rejected.
When NodeApi endpoints are pod/host IPs, provision certificates with the correct
IP SANs or explicitly configure the expected worker service DNS identity on
callers. A shared worker DNS identity authenticates membership in that service,
not a distinct worker. Registered-endpoint checks authorize transfer addresses;
signed route capabilities and runtime fences constrain requests accepted by
honest workers. They do not authenticate an intercepted connection to the
intended worker or prevent another holder of the shared service identity from
collecting credentials. Use per-node certificate identities when this threat
must be excluded. Adding mTLS alone does not distinguish servers sharing an
identity.
ClusterApi endpoint discovery preserves the original service identity when it
replaces the dial address with a pod IP. It must not authenticate the pod IP as
an unrelated service. HTTPS validation never falls back to plaintext on failure.

This is server-authenticated TLS with existing bearer authorization. The
transport doesn't provision certificates or require client certificates. Use a
separate leaf key per service, or per instance where a shared identity isn't
acceptable; never distribute a server private key to clients.

On EKS, the `space` Terraform module
(`infra/space/aws/eks/modules/space/internal_rpc_tls.tf`) provisions a
per-cluster CA and one leaf certificate each for ClusterApi, NodeApi and
BuildManager. `app_deploy.tf` syncs them into the `space-internal-rpc-ca`,
`space-cluster-api-tls`, `space-node-api-tls` and `space-build-manager-tls`
Secrets.

The leaves carry only short service names (ClusterApi: `space-internal-api` and
`space-cluster-api`; NodeApi: `space-node-api`; BuildManager:
`space-builder-build-manager`) and no IP SANs. A caller whose configured
endpoint host isn't one of those names, such as a node IP, a loopback address, a
fully qualified service name or another short name, sets the matching
`*_TLS_SERVER_NAME`. Every NodeApi listener on a cluster shares the one
`space-node-api` key: that's the shared worker identity described above, not a
per-node identity.

An apply reissues a leaf once it's within 60 days of its one-year expiry, or
past it; the module doesn't schedule applies. Listeners load their identity only
at startup, and External Secrets refreshes these Secrets hourly, so restart
listeners after the Secret has refreshed.

`SPACE_INTERNAL_RPC_INSECURE_DEV=true` explicitly permits HTTP for local test
and development deployments and permits a listener without certificate files.
The checked-in Kind charts set this exception. A partially configured listener
certificate/key pair still fails startup; explicitly configured HTTPS still
requires trust even with the exception. Steady-state production deployments must leave this
exception unset/false. Plaintext Rust fixtures opt in explicitly in isolated
test subprocesses; ordinary `cargo test` and `cargo nextest run` need no global
exception. TLS regression tests inject their own configuration. Do not set the
exception in Cargo's global environment: it would also affect `cargo run`.
For an intentionally plaintext local service run, scope the exception to that
command, for example `SPACE_INTERNAL_RPC_INSECURE_DEV=true cargo run -p <service>`.
Configured-but-empty certificate/key paths still fail instead of downgrading.

## TLS-only listeners and rollback

A listener configured with TLS identity accepts only TLS transport;
plaintext HTTP/2 is rejected even when local development callers permit HTTP.
Discovery advertises HTTPS for TLS-configured listeners and HTTP only for explicit
identity-free development listeners. Clients do not perform protocol fallback.
Listeners admit at most 64 pending TLS handshakes, each with a ten-second
deadline. Established channels do not consume that budget. Shutdown releases
the listening socket even when handshake admission is saturated.

Migrate every caller and operational tool to TLS before deploying TLS-only
listener images. Verify lifecycle, discovery, reconciliation, artifact transfers
and data-plane streams. Existing plaintext connections must drain or reconnect
using TLS before listener compatibility can be removed.

Rollback to an earlier TLS-capable image while keeping TLS callers, listener
identities and trust installed. Restoring plaintext callers requires first
restoring listener images that support both protocols and verifying that
compatibility is available everywhere. Plaintext-only listeners cannot safely
serve callers that still use TLS.

BuildManager uses the same policy for eager CLI connections and NodeApi’s lazy
backend client. Lazy initialization reads trust without opening a connection;
missing or invalid trust fails initialization. The local BuildManager CLI also
needs the CA and, when dialing loopback, the listener’s expected DNS identity.

Server identities are loaded at startup. Restart listeners to install renewed
certificates. NodeApi and BuildManager clients capture trust once per client; ClusterApi discovery
and rotating channels retain their loaded CA bundle. Reconnecting their TCP
sockets does not reload it. Reconstruct these clients, or restart callers, to
change trust. Node-ctl also reconstructs its ClusterApi client when its cache
expires (60 seconds plus jitter); data-plane pools reload when rebuilt. Restarting
callers is the conservative way to converge every path. Use overlapping CA bundles when
rotating issuers: deploy old+new trust, renew listeners, reconnect callers,
verify convergence, then remove old trust. There is no certificate hot reload.
Rotate previously exposed static bearers through the existing secret workflow.

The published source capability manifest `ci/runtime-capabilities.json` exposes
`internal_rpc_tls_only` version 1: a listener configured with TLS identity rejects
plaintext, independent of client development settings. Deployment tooling may
use this contract to distinguish TLS-only images from images that support a
dual-protocol migration. Capability versions describe behavior rather than Rust
source layout; update the declaration together with changes to that behavior.

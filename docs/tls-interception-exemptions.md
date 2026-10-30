# Deployment-configured TLS interception exemptions

[Documentation](../README.md)

TLS interception currently terminates TLS and parses the decrypted stream as
HTTP. Non-HTTP TLS protocols (for example TURN-over-TLS) can therefore receive
an HTTP 400 response and a closed connection. Deployment-configured exemptions
are a stopgap that leaves selected TLS streams encrypted end-to-end, not
protocol-aware interception.

Set `SPACE_TLS_INTERCEPTION_EXEMPTIONS` to the same JSON array on both the API
service and the cluster-controller process that executes queued operations:

```json
[{"domain":"relay.example.test"},{"wildcard_domain":"*.relay.example.test"},
 {"domain":"push.example.test","port":5228}]
```

An entry without `port` exempts every destination port. An entry with `port`
(1 to 65535) exempts only connections to that destination port; the same host
on any other port keeps the policy's interception mode. An all-port entry and a
port-scoped entry for the same host are distinct, not duplicates.

Both processes validate this configuration at initialization, including
cluster-controller standbys before leader election. Unset means disabled;
empty, malformed, invalid-domain, invalid-port, and non-hostname destinations
fail startup.
There is no built-in destination list. Wildcards match leading-label suffixes,
not arbitrary patterns, and do not match the suffix's apex domain.

For sandbox creation and network-policy updates, configured destinations are
appended only when the effective policy has `tls_interception: "on"`. Existing
exemptions are preserved and duplicate additions are skipped. An inherited
template policy becomes a sandbox override only when composition adds an
exemption. The API composes E2B-translated creates; the cluster-controller
composes queued native/admin creates (including admitted warm-pool claims),
policy updates and resets to the template default, and disk fork targets.
Restoring an existing sandbox retains its persisted policy; this setting does
not retroactively rewrite existing sandboxes or template defaults. Policy
updates can apply it to existing sandboxes.

This setting never adds egress allows (including DNS resolver allows) or changes
denies. Exemption does not grant network access. Hostname exemption matching
requires gateway DNS attribution: the guest must resolve via the reserved
resolvers `8.8.8.8` or `1.1.1.1`, and its existing policy must permit the required
DNS and destination traffic. A custom DNS client still qualifies when it sends
queries through those resolvers; SNI alone is not attribution. DNS-over-HTTPS,
other resolvers, expired attribution, or cached addresses without observed DNS
can leave interception enabled.

Before enabling, all policy readers and worker gateways must support
`tls_interception_exemptions`, and before configuring any `port` they must also
support port-scoped exemptions, including the SPACE runtime shim that forwards
a Kubernetes sandbox pod's policy. Older API and cluster-controller processes
fail startup on a configured `port`, and older Rust JSON policy readers reject a
policy that contains one. The pod annotation carries only all-port entries in
`tls_interception_exemptions`; when any entry has a port, the complete ordered
list is in `tls_interception_exemptions_with_ports`. Older lenient shims ignore
the new key and keep intercepting port-scoped destinations. Strict shims that
predate this encoding reject the new key, so cold creates using it fail until
the shim is upgraded. Upgrade the shim and node-ctl first: a sandbox created
or updated through an older one keeps intercepting its port-scoped
destinations until its policy next changes. Older gateways
do not read port-scoped exemptions from the protobuf policy, so they keep
intercepting those destinations rather than exempting every port. Removing the
environment variable from both processes and rolling their replicas stops future composition but does not
remove exemptions already persisted in sandbox overrides; remove those
explicitly to roll back existing sandboxes. Once a `port` is persisted, do not
roll the API, internal API, cluster controller, credential store, template
manager, or sandbox manager back to a version that cannot read port-scoped
exemptions.

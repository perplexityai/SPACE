# Deployment-configured TLS interception exemptions

TLS interception currently terminates TLS and parses the decrypted stream as
HTTP. Non-HTTP TLS protocols (for example TURN-over-TLS) can therefore receive
an HTTP 400 response and a closed connection. Deployment-configured exemptions
are a stopgap that leaves selected TLS streams encrypted end-to-end, not
protocol-aware interception.

Set `SPACE_TLS_INTERCEPTION_EXEMPTIONS` on the API service to a JSON array:

```json
[{"domain":"relay.example.test"},{"wildcard_domain":"*.relay.example.test"}]
```

The API validates this configuration once at startup. Unset means disabled;
empty, malformed, invalid-domain, and non-hostname destinations fail startup.
There is no built-in destination list. Wildcards match leading-label suffixes,
not arbitrary patterns, and do not match the suffix's apex domain.

For sandbox creation and network-policy updates, configured destinations are
appended only when the effective policy has `tls_interception: "on"`. Existing
exemptions are preserved and duplicate additions are skipped. An inherited
template policy becomes a sandbox override only when composition adds an
exemption. Both native and E2B-translated creates, including disk fork targets,
use this composition. Restoring an existing sandbox retains its persisted
policy; this setting does not retroactively rewrite existing sandboxes or
template defaults. Policy updates can apply it to existing sandboxes.

This setting never adds egress allows (including DNS resolver allows) or changes
denies. Exemption does not grant network access. Hostname exemption matching
requires gateway DNS attribution: the guest must resolve via the reserved
resolvers `8.8.8.8` or `1.1.1.1`, and its existing policy must permit the required
DNS and destination traffic. A custom DNS client still qualifies when it sends
queries through those resolvers; SNI alone is not attribution. DNS-over-HTTPS,
other resolvers, expired attribution, or cached addresses without observed DNS
can leave interception enabled.

Before enabling, all policy readers and worker gateways must support
`tls_interception_exemptions`. Removing the environment variable stops future
composition but does not remove exemptions already persisted in sandbox
overrides; remove those explicitly to roll back existing sandboxes.

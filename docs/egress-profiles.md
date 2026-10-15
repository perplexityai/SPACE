# Operator egress profiles

[Documentation](../README.md)

Operators configure named, additive policies with `SPACE_EGRESS_PROFILES` on
API and cluster-controller processes at startup, including standby controllers.
Use the same configuration on both services. Unset or `{}` disables profiles.
For example:

```json
{
  "audit": {
    "default": true,
    "allow": [{"domain": "audit.example.test"}]
  },
  "sensor": {
    "allow": [{"domain": "sensor.example.test"}, {"cidr": "192.0.2.0/24"}],
    "tls_interception_exemptions": [{"domain": "sensor.example.test"}]
  }
}
```

Names contain lowercase letters, digits, and hyphens. `default` defaults to false;
`tls_interception_exemptions` defaults to an empty list. Every profile needs a
nonempty `allow` list. Both lists accept IP, CIDR, and exact domain destinations.
Invalid JSON, duplicate or invalid names, unknown fields, and unsupported
selectors fail startup. Wildcard domains are not accepted.

Authenticated federated native (`POST /v1/sandboxes`) and ECW (`POST /sandboxes`)
creates select profiles with `X-Space-Egress-Profiles: +sensor -audit`.
An absent header selects defaults only. Tokens are separated by single spaces;
empty, malformed, repeated, conflicting, and unknown tokens return HTTP 400.
A non-federated create ignores the header and applies no profiles, including
defaults, while recording an invalid-request admission.

Composition adds allows and exemptions without duplicates. Default-deny policies
also receive the ECW DNS resolver allowances. Explicit tenant denies and platform
denies still win; TLS exemptions do not themselves allow traffic.

The sandbox stores the selected names in `egress_profiles`, a PostgreSQL
text array. Selection is immutable for its lifetime; definitions are not stored.
Fresh creates, tenant policy replacement, and explicit template inheritance resolve
those names against the handling process's current configuration. A missing saved
name returns HTTP 503 without replacing the policy; queued creates retry within
the dispatcher's attempt and deadline limits.
Defaults apply only at admission, never when resolving a saved selection.

Native queued creates persist the names, including an explicit empty array, and
resolve definitions at execution. Nonempty selections participate in idempotency
matching; empty selections retain the pre-profile fingerprint.

Running sandboxes and lifecycle operations (stop/start, pause/resume,
suspend/restore, rollback, and forks) retain the saved effective policy. Forks
also carry the selected names. The original tenant base is not stored. To refresh
destinations, recreate with the intended policy or inherit the template default
(which discards tenant overrides). Replacement resolves current definitions only
against the supplied policy: supply a fresh, uncomposed tenant base. Resubmitting
the saved effective policy retains old profile allows as tenant allows. Keep
selected profiles configured while replacing or inheriting policies.

`space.sandbox.egress_profile_admission{profile,outcome,reason}` reports `ok/ready`
for selected profiles and `failed/invalid_request` or `failed/unconfigured` for
invalid selections and missing saved definitions. Untrusted names at admission
use `profile:unknown`; missing saved definitions use the previously admitted name.
Alert on `failed/unconfigured` for configuration drift.

Upgrade database schemas and queued operation executors before API producers.
Configure every API and cluster-controller replica before callers send selections
or rely on changed definitions. Different definitions may compose different
policies; a worker missing a selected name returns 503 and may exhaust retries.
All policy readers and running gateways must support TLS exemptions before
configuring nonempty exemption lists. Before downgrading, use compatible binaries
to remove exemptions from stored policies or retire the affected resources.
Disabling configuration does not remove saved policies. There is no automatic
refresh or separate definition registry.

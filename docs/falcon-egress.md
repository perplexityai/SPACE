# Optional Falcon egress

[Documentation](../README.md)

The ECW create header `X-Space-Falcon: us2` requests an operator-configured
Falcon destination bundle. Only authenticated federated creates can receive the overlay.
The API has no built-in regional endpoints.

**No renaming is included:** `X-Space-Falcon: us2`,
`SPACE_FALCON_US2_EGRESS_DESTINATIONS`, and the `FalconUs2Egress` type retain
compatibility names. They do not restrict the configured destinations to US-2.
**TODO:** remove the US-2-specific names in a separate coordinated caller and
configuration migration.

Set `SPACE_FALCON_US2_EGRESS_DESTINATIONS` on the API process to a JSON array
of IP addresses, CIDRs, and exact domain destinations. For example, using **synthetic values**:

```json
[{"ip":"192.0.2.10"},{"cidr":"2001:db8:1234::/56"},{"domain":"sensor.example.test"}]
```

The operator must supply the actual destinations approved for their deployment.
Configuration is parsed once at startup. Invalid JSON, invalid addresses,
invalid CIDRs, wildcard domains, and an empty array fail startup. Leave the variable
unset to disable the bundle. Missing configuration or an invalid Falcon header
falls back to Noop admission: creation continues with the original network
policy. Normal authentication, owner/grant validation, and provisioning errors
still apply. Creates without the header are unaffected.

`space.sandbox.falcon_create_admission` counts each Falcon egress admission
with `outcome:ok|failed` and `reason:ready|invalid_request|unconfigured`. The
failure fraction is the failed count divided by the total count over the same
window; the rate of the failed count measures failures per second. A warning
also records each fallback without private request values. This is the SPACE
preparation outcome; the caller separately observes sensor bootstrap and health.

The API adds the configured destinations to the request's network policy, or
the resolved template policy when the request has no override. Explicit denies
and platform hard denies still win. Default-deny policies retain the ECW DNS
resolver allowances. These destinations do not constrain transport or port.
With `SPACE_FALCON_TLS_INTERCEPTION_EXEMPTIONS_ENABLED=true`, composition also
adds them to `tls_interception_exemptions`, so admitted traffic passes through
without TLS termination even when interception is enabled for the sandbox.
Hostname exemptions require a live observed DNS binding to the destination IP at
admission. Established exempt connections retain that verified binding through
DNS expiry, while policy updates still enforce current permissions and exemptions.
IP and CIDR exemptions apply regardless of SNI. Other destinations retain their configured
interception behavior.

The activation setting defaults to `false`; while disabled, composition adds
allowances without adding exemptions. Upgrade every policy reader, including
all API replicas, the internal API, and the credential manager, and every running
worker gateway before enabling it. Follow the
[TLS exemption rollout and rollback steps](architecture.md#tls-interception-exemptions).
The composed policy is persisted with the sandbox. Existing sandboxes need
recreation or an explicit policy update after activation; wake and restore reuse
the saved policy.

For deployments upgrading from an API with built-in endpoints, deploy the
configuration first while retaining the old image. Verify every API replica
has it, then roll out the new image and validate a federated create and sensor
connectivity. Preserve the configured list when rolling back the API image.

Before configuring CIDRs, upgrade every API replica that reads the destination
list. Older parsers reject CIDRs at startup. Restore an IP/domain-only list
before rolling back to such an API image. Existing destination lists and callers
continue to work without changes.

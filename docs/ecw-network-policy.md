# Full network policies through ECW

[Documentation](../README.md)

ECW create requests can carry a complete native `NetworkPolicyV1` JSON document
in the string metadata field `network.policy`. This bridge lets SDK callers
construct one policy for ECW and native create without server-side feature
configuration. Native clients send the same document as `network_policy`.

```json
{
  "metadata": {
    "network.policy": "{\"version\":\"v1\",\"egress\":{\"default_action\":\"allow\",\"allow\":[],\"deny\":[]},\"tls_interception\":\"on\",\"tls_interception_exemptions\":[{\"domain\":\"sensor.example.com\"}]}"
  }
}
```

This is a complete replacement for the template policy, not an overlay. Include
all intended allow/deny rules, TLS settings, credential rules, and DNS resolver
permissions. ECW does not add its implicit DNS permissions to a full policy.
Explicit and platform denies retain their usual precedence. Existing server
policy additions, including deployment-wide TLS exemptions, still apply.

Do not combine `network.policy` with `network`, `allow_internet_access`
(`allowInternetAccess`), or `network.tls_interception`: ECW returns HTTP 400
rather than choosing precedence. SDK callers must omit these fields entirely,
including automatically populated defaults. Malformed policies also return 400.
The bridge consumes `network.policy`; it is not duplicated in sandbox metadata.
Other metadata and create settings retain their existing behavior.

Deploy bridge-capable APIs before enabling callers. Older APIs treat this key
as ordinary metadata and do not enforce its policy. Requests without the key
retain the existing E2B translation and legacy opt-in behavior. Callers can later
switch to native create without changing their policy construction.

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

Credential rules explicitly list the HTTP methods that receive injected headers:
`GET`, `POST`, and `HEAD` are supported. A GET grant does not include HEAD. Deploy
HEAD-capable API and policy consumers, including gateways, before enabling rules
that list HEAD; older consumers reject the new method. Existing GET/POST rules
retain their behavior. Remove HEAD from policies before rolling consumers back to
a version that cannot read it.

## Adding policy fields

On a cold create, the containerd shim converts the pod's network-policy
annotation for sandbox-manager and rejects any field it does not recognize, at
any level, instead of dropping it. Restores keep sandbox-manager's stored policy
and do not read the annotation. Serialize a new field only when it is used, so
policies without it stay readable by every shim. Roll a shim that accepts the
field out to every sandbox node before any API sets it. On a node whose shim
predates the field, those cold creates fail with `InvalidArgument` and count in
`space.shim.operation.error` with `step:parse_network_policy`. Keep the field in
the shim while any pod annotated with it remains.

## Replace a running sandbox's policy

SPACE exposes an authenticated ECW extension at
`/sandboxes/{sandboxID}/network-policy`. It uses the same sandbox ownership
scope as ECW lifecycle operations, including federated subject ownership.
Management credentials remain outside the guest.

The native `PATCH /v1/sandboxes/{sandbox_id}` already accepts network-policy
changes and deduplicates queued operations. Its operation completion means the
desired update was stored; it does not return an exact installed-policy target,
accept an expected policy revision, or wait for forwarding to stop. The native
network-policy GET returns the desired document, not live enforcement evidence.
The ECW extension reuses the existing policy translator, platform composition,
transactional policy write, and reconciliation path, and extends the existing
gateway and sandbox-manager read RPCs with live observations.

Policy receipts are scoped to a sandbox and committed with its desired update.
The shared public-operation store normally expires retry records after 24 hours;
its permanent credential-registration replies have organization lifetime.
Neither provides sandbox-lifetime cleanup. The dedicated receipt table uses a
sandbox foreign key so replay survives later edits without retaining receipts
after the sandbox row is removed.

First `GET` the resource to obtain its opaque `revision`. Then `PUT` a complete
replacement using an `Idempotency-Key` header:

```json
{
  "expectedRevision": "<revision from GET>",
  "network": {"allowOut": ["api.example.com"]},
  "allowInternetAccess": false
}
```

The `network` field is required and uses ECW create's network validation and
translation. `allowInternetAccess` defaults to false. An empty `allowOut` with
that default denies external access; it does not restore template inheritance.
Platform egress profiles and deployment TLS exemptions still compose into the
policy. This extension accepts ECW network settings, not the full-policy
metadata bridge described above.

`202 Accepted` returns `{"target":"<opaque policy target>"}` after committing
both desired state and its retry receipt. It does **not** prove enforcement.
The existing reconciler installs the policy. A stale revision or reuse of a
key with a different request returns `409`. Retrying the same key and request
returns its original target, even after another edit; it never replays the
older mutation. Receipts are retained until the sandbox is deleted.

Poll `GET` until `state` is `applied` and both `desiredTarget` and
`appliedTarget` exactly equal the accepted target. Do not compare generations
using `>=`. The response also includes `revision`, `executionGeneration`,
`gatewayInstance`, and an optional `error`. A different desired target means
another edit superseded the request. `pending`, `failed`, unsupported routes,
and timeouts must not release a caller's execution gate.

An applied observation is a bounded live read of the gateway serving the
current execution. It requires the exact installed policy reference and no
cancelled TCP relays still holding their forwarding lifetime. Gateway policy
installation prunes DNS authorization and resweeps existing TCP flows; UDP
continues to evaluate policy for each datagram; the observation does not wait
for in-flight UDP operations to finish. Both sandbox-manager and the API check
execution identity around the live read. Suspended, unavailable,
replaced, and older gateway executions cannot reuse cached applied fields to
satisfy this check. GET never wakes a sandbox.

The acknowledgment describes the observed instant. It cannot retract bytes
already sent or prevent a later authorized policy edit. A caller that waits
before starting work must check again if its execution identity changes.

Deploy the additive gateway, sandbox-manager, and worker RPC support before
requiring this API. Older components produce no live observation, so the API
stays pending. The receipt migration is additive. Rolling the API back removes
the extension but leaves persisted policy intent intact; do not drop the receipt
table while writers remain active.

Run `network_policy.live_update` through the typed smoke runner to verify live
traffic revocation, preserved access, retry conflicts, explicit deny-all, and
policy persistence across suspend/restore. The scenario requires outbound
access to the public example domains for its positive controls.

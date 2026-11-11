# Intermediate-key broker v2

The `IntermediateKeyBroker` gRPC service is SPACE's provider-neutral boundary
for an external key authority. It supports BYOK without exposing a cloud,
vault, HSM, account, or key-resource model to SPACE. A broker may route to any
number of backing key systems; SPACE sees one logical broker and treats its key
identifiers and wrapped key bytes as opaque.

The wire contract is
[`intermediate_key_broker.proto`](intermediate_key_broker.proto). Generated
Rust client and server bindings live in
`crates/l20_domain/credential/provider/proto`; the SPACE-specific authenticated
client lives in `crates/l40_api/credential/provider/client`.

## Request flow

1. The Credentials Authority calls `IssueIntermediateKey` with a stable
   `TenantRef` and, when required by the broker, typed `AuthorizationEvidence`.
2. The broker authenticates the authority at the transport layer, authorizes
   the request, creates a 32-byte intermediate key, and returns only an opaque
   `key_id` and wrapped key blob. SPACE persists both values verbatim.
3. When the authority needs the plaintext key, it calls
   `UnwrapIntermediateKey` with the same tenant, key ID, and wrapped blob.
4. The broker reauthorizes the request and returns exactly 32 plaintext bytes
   plus a required, nonnegative `max_cache_ttl`. The authority keeps the key in
   zeroizing memory only and applies the minimum of its local limit, the
   authorization-evidence lifetime, and the broker limit. A zero broker limit
   disables caching.
5. The authority generates and wraps data-encryption keys locally. Node-local
   services and sandboxes never call the broker or receive the intermediate
   key.

`TenantRef.namespace` and `TenantRef.id` form the durable cryptographic tenant
binding. They must be stable and compared exactly. A namespace or broker
migration must preserve a route for every historical
`(tenant, key_id, wrapped_key)` tuple while SPACE may retain data protected by
that key.

## Authorization

Transport authentication and `AuthorizationEvidence` serve different roles.
The former authenticates the Credentials Authority connection. The latter is
optional application evidence interpreted only by the broker.

- A self-hosted broker may rely entirely on workload transport identity and
  accept requests with `authorization` absent.
- When evidence is present, `scheme` identifies a versioned validation
  protocol, `subject` identifies its subject when applicable, and `evidence`
  is opaque secret-bearing data.
- A broker must reject missing, empty, malformed, expired, or unsupported
  evidence required by its policy as `AUTHORIZATION_REJECTED`. It must never
  ignore an unknown non-empty scheme.
- Evidence and plaintext or wrapped key bytes must never appear in logs or
  error details.

## Outcomes

Application decisions are returned in the response `oneof`. Non-OK gRPC
statuses are transport failures only and must not be used to report an
application decision.

| Outcome | Meaning | Required caller behavior |
| --- | --- | --- |
| `INVALID_TENANT` | The tenant reference is invalid or unknown. | Fail this issue request without inferring revocation. |
| `INVALID_KEY` | The tenant, key ID, and wrapped blob do not identify a valid key. | Fail this unwrap request without inferring revocation. |
| `AUTHORIZATION_REJECTED` | Authorization evidence is absent, unsupported, expired, malformed, or invalid. | Fail closed; refreshed evidence may be retried. Never trigger revocation cleanup. |
| `ACCESS_REVOKED` | The broker deliberately declares key access revoked. | Fail closed and propagate the typed revocation signal. |
| `UNAVAILABLE` | The broker or a backing service is temporarily unavailable. | Fail closed after any cached key expires and retry according to local policy. |
| Non-OK gRPC status | Transport, proxy, or workload-authentication failure. | Treat as unavailable, never as revocation. |

Missing outcomes, unknown enum values, non-32-byte plaintext, and missing or
negative cache limits are contract violations and are rejected by SPACE.

## Compatibility requirements

- `key_id` is opaque to SPACE, but the current SPACE client requires a
  non-empty UTF-8 value of at most 2,048 bytes with no NUL byte.
- A successful issue creates a key version. Brokers must retain historical
  versions for reads; changing a backing provider, account, region, endpoint,
  or wrapping key must not invalidate existing tuples.
- Retrying an issue after an ambiguous transport failure can create an orphaned
  broker key. Implementations must either make issue retries idempotent or
  safely garbage-collect keys that SPACE never persists.
- Evolve v2 additively. Do not renumber fields or enum values, change existing
  meanings, or reuse removed field numbers or names.

## Conformance

Broker implementations can enable the `conformance` feature on
`pplx-space-credential-provider-proto` and run
`IntermediateKeyBrokerConformance` against a test deployment. The deployment
provides its normal generated gRPC client plus a test-only
`ConformanceControl` adapter that selects each defined failure and simulates a
backing-provider migration. The control adapter is deliberately not part of
the production RPC contract.

Configure the runner with two tenants, an unsupported authorization scheme,
the broker TTL ceiling, and either:

- `TransportIdentityOnly`, which verifies successful requests with
  `authorization` absent; or
- `EvidenceRequired`, which verifies absence is rejected and caps TTL by the
  supplied evidence lifetime.

A passing run verifies absent and unknown authorization behavior, tenant
binding, TTL limits, all in-band failure reasons, fresh issue/rotation
behavior, and historical-key reads before and after backend migration.

### Running the suite

Add the proto crate to the broker test harness with the conformance feature
enabled. A broker developed outside this workspace can pin a SPACE revision:

```toml
[dev-dependencies]
pplx-space-credential-provider-proto = { git = "https://github.com/perplexityai/SPACE", rev = "<commit>", features = ["conformance"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
tonic = "0.14"
```

The broker itself does not need to be written in Rust. The Rust harness only
needs a gRPC `Channel` to a fresh test deployment and an implementation of
`ConformanceControl` for that deployment:

```rust,no_run
use std::time::Duration;

use pplx_space_credential_provider_proto::conformance::{
    AuthorizationProfile, ConformanceConfig, IntermediateKeyBrokerConformance,
};
use pplx_space_credential_provider_proto::v2::{
    intermediate_key_broker_client::IntermediateKeyBrokerClient,
    AuthorizationEvidence, TenantRef,
};

# async fn check<C>(
#     channel: tonic::transport::Channel,
#     controls: &C,
# ) -> Result<(), Box<dyn std::error::Error>>
# where
#     C: pplx_space_credential_provider_proto::conformance::ConformanceControl,
# {
let config = ConformanceConfig::new(
    TenantRef {
        namespace: "urn:example:tenant".into(),
        id: "primary".into(),
    },
    TenantRef {
        namespace: "urn:example:tenant".into(),
        id: "other".into(),
    },
    AuthorizationProfile::TransportIdentityOnly,
    AuthorizationEvidence {
        scheme: "urn:example:unsupported:v1".into(),
        subject: None,
        evidence: b"unsupported".to_vec(),
    },
    Duration::from_secs(120),
)?;

let client = IntermediateKeyBrokerClient::new(channel);
IntermediateKeyBrokerConformance::new(client, config)
    .run(controls)
    .await?;
# Ok(())
# }
```

Use `AuthorizationProfile::EvidenceRequired` instead when the deployment
requires application evidence. Supply valid evidence for both configured
tenants and the validated remaining evidence lifetime.

The test-only control adapter must provide these deterministic operations:

- `set_issue_failure(Some(reason))`: make issue requests return exactly that
  in-band failure; `None` restores normal behavior.
- `set_unwrap_failure(Some(reason))`: make unwrap requests return exactly that
  in-band failure; `None` restores normal behavior.
- `migrate_backing_provider()`: route new issuance through a distinguishable
  backend generation without invalidating previously issued tuples.

Each control call must take effect before it returns. Run the suite against an
isolated fixture because it issues keys and changes simulated backend state.
The unsupported scheme must truly be unsupported by the fixture, and the
configured TTL ceilings must match the fixture policy.

Run a harness in this workspace with:

```sh
cargo test -p <broker-test-package> broker_conforms -- --nocapture
```

For local integration, `crates/test_support/mock/branching/key/provider/svc` contains a
small broker implementation. It is a connectivity fixture, not a conformance
suite or production key authority.

# Hardware-derived deterministic sandbox anchors

This default-off prototype is not production security or rollout approval.
The sole enrollment flag is `space-sandbox-scoped-trust`; the
`split-trust-prototype` build capability and hardware configuration must be
deployed before enabling it.

## Custody and data flow

The platform master is a non-exportable AWS KMS `HMAC_384` key with `AWS_KMS`
origin. KMS HMAC keys support deterministic derivation through `GenerateMac`
([AWS HMAC documentation](https://docs.aws.amazon.com/kms/latest/developerguide/hmac.html)).
Authority obtains one shared seed per loaded key, derives sandbox anchors
locally, and signs gateway intermediates without exporting anchor private keys.

```text
KMS master
  │ seed load
  ▼
Credential authority: shared seed, locally derived sandbox anchor
  │ public anchor / signed gateway intermediate
  ▼
Node credential manager: existing authenticated forwarding
  │
  ▼
Sandbox manager: persist key pin; install public trust
  ├── existing File / Process APIs ──► guest public anchor
  └── signed intermediate ──► gateway, which owns its key and signs leaves
```

The shared seed and sandbox private keys are HSM-derived, not HSM-resident.
Stealing a seed permits offline derivation of every sandbox key under that
master; disabling KMS or deleting the cache does not revoke stolen material.
Authenticated nodes remain globally trusted. No placement/lifecycle database
checks or SQL migrations are added.

## Identity and rotation

Configure one immutable active key ARN, never an alias. New enrollment obtains
the public anchor and concrete key ID through `ExchangeToken`, then persists
that ID in trusted host metadata before installation and readiness.

Subsequent public-anchor requests select the saved key ID, and
`IssueIntermediateCertificate` carries both sandbox UUID and `anchor_key_id`.
Omitting both retains legacy signing; providing only one is invalid. No
private material is returned and no new RPC is introduced.

Same-ID restore preserves the pin. A fork inherits scoped/legacy mode, but
selects the active key for its new UUID and installs a new anchor; captured
application TLS contexts may require restart. Host and snapshot schema4 carry
`scoped_tls_key_id`; its presence encodes scoped enrollment, and older readers
reject the version rather than silently reverting to shared trust.

Rotation provisions a new hardware key and rolls out a new active ARN.
Retain old keys while old sandboxes or snapshots remain restorable. Explicit
pins remain valid through mixed authority configurations; there is no
timestamp-selected generation or shared registry to read or recover.

## Derivation contract

The environment is a stable deployment label of 1-128 ASCII letters, digits,
underscores or hyphens. It must remain unchanged for restorable sandboxes.
The KMS-selected master separates keys; the seed message separates environments:

```text
seed = KMS.GenerateMac(master, HMAC_SHA_384,
    u32be(1) || "space-authority-seed-v4" || 0x00
    || u32be(environment_byte_length) || environment || u32be(384))

material = HMAC_SHA384(seed,
    u32be(1) || "space-sandbox-anchor-v4" || 0x00
    || u32be(16) || uuid_bytes || u32be(384))

scalar = OS2IP(material) mod (P-256 order - 1) + 1
```

Only canonical RFC-variant UUIDv7 identities are accepted. Their timestamps
are sanity-checked, not used to select a key. The framing is a frozen
SP800-108-style contract, not a claim of a validated KDF.

The root is P-256, CA path length1, with fixed 2026-2126 validity and a
positive 20-byte serial derived from UUID framing and public key. RustCrypto
provides deterministic ECDSA only for the anchor self-signature; AWS-LC
handles HMAC, ordinary certificate signing and TLS. This prototype exception
requires a production cryptographic review.

The gateway intermediate has path length0 and at most one-year validity,
bounded by the root's expiry. Each gateway owns a fresh private key, and its
CSR is restricted to the validated P-256 profile.

## Authority configuration and availability

- **`SPACE_ANCHOR_KMS_KEY_ARN`:** active immutable key in the configured account
  and primary region. Historical requests may select only key IDs within that
  account; callers cannot supply an endpoint, region or account ARN.
- **`SPACE_ANCHOR_ENVIRONMENT`:** persistent derivation domain.
- **AWS SDK region configuration:** primary region, normally `AWS_REGION`;
  it must match the active key ARN.
- **`SPACE_ANCHOR_KMS_REPLICA_REGION`:** optional related-region fallback for
  multi-region key IDs. Key material is shared across related keys; policies
  and enabled states remain independent
  ([AWS multi-region keys](https://docs.aws.amazon.com/kms/latest/developerguide/multi-region-keys-overview.html)).

Startup loads the active seed before readiness and keeps it for the process
lifetime. Historical seeds use a bounded 32-entry, singleflight LRU cache.
Failed or cancelled loads are retryable; evicted material zeroizes when its
last owner drops. Warm active signing therefore does not become a hardware
operation because historical restores churn the cache.

Each hardware endpoint attempt has a one-second limit with SDK retries
disabled. The whole credential operation, including admission and blocking
crypto, has an eight-second bound and 32-slot concurrency limit. Warm
authorities can enroll fresh UUIDs during hardware outages; cold startup or
an uncached historical key fails closed. Gateways with issued intermediates
continue local leaf signing during authority outages.

## Runtime validation

The typed `native.split_trust_*` and `local.split_trust_*` scenarios exercise
real Kind, Firecracker, credential binaries and guest TLS. The hardware
scenario uses owned real KMS keys, an independent public-key oracle, regional
failover, all-key outages, cold-authority refusal and active-key rotation.
After rotation it restores an old sandbox under its saved key and creates
a new one under the replacement key.

`space_smoke.anchor_hardware` owns journaled, account-guarded provisioning and
cleanup. Pass its original key ARN and environment into Kind bootstrap using
the configuration above; `install` installs only short-lived scoped runtime
credentials. Run the rotation scenario after the other lifecycle proofs.

Production gates remain: KDF/certificate review, authority-memory and key
governance, trust-store coverage, historical-key recovery and retirement,
load/failure budgets, dependency vetting and a reviewed staged migration.
The prototype does not waive a missing `safe-to-deploy` dependency audit.

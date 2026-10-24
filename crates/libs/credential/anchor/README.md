# Hardware-derived deterministic sandbox anchors

This is a default-off prototype, not production security or rollout approval.
Enable both the `split-trust-prototype` Cargo feature and
`SPACE_SPLIT_TRUST_PROTOTYPE=true`. An authority without the compiled feature
refuses the runtime flag. Ordinary shared-trust behavior remains unchanged when
the feature is off.

## Custody and data flow

The platform-wide master is an AWS KMS HMAC_384 key generated with `AWS_KMS`
origin. Its material is not exported to the authority, Kubernetes, nodes or
guests. AWS documents HMAC keys as HSM-protected and `GenerateMac` as suitable
for deterministic key derivation ([AWS HMAC documentation](https://docs.aws.amazon.com/kms/latest/developerguide/hmac.html)).

```text
public epoch catalog ──► credential authority ──GenerateMac──► cloud HSM
                               │                              master stays here
                               │ scoped signing credentials
                               ▼
                        node credential manager
                         │                     │
                 public anchor          local gateway-CA signing
                         │                     │
                 sandbox manager          network gateway
                         │                     │
              existing File + Process    local leaf signing / TLS
                         │
                  guest public trust
```

The sandbox private key is derived from a scoped HMAC result in authority
memory and delivered over the existing authenticated credential channel to a
bounded, zeroizing node cache. It is **HSM-derived, not HSM-resident**. The guest
receives only the public certificate. Gateway intermediates and leaf signatures
are local; this design does not send each certificate signature to KMS.

There are no new service methods, host/guest implementations, database checks
or migrations. The X.509 profile of existing `ExchangeToken` delivers either a
public certificate or private signing credentials. Existing
`IssueIntermediateCertificate` adds an optional sandbox identity; omission
retains shared issuance, presence requests scoped issuance and never falls back.
Existing credential-manager/authority authentication is the trust boundary.
An authenticated node can request anchors for other sandbox identities. This
does not limit a compromised authenticated node to its currently placed guests.

## Frozen scheme 2

The non-secret `space-anchor-epochs` ConfigMap contains `data["epochs.json"]`:
`{scheme:2, environment, epochs:[{version,since_ms,key_id}]}`. The authority can
only GET this object. The first epoch is version1/since0; versions are consecutive,
activation times strictly increase, and immutable key ARNs cannot be reused.
Aliases are rejected. An older compatible catalog cannot replace the in-memory
view; changes to an already observed epoch are rejected.

For a canonical RFC-variant UUIDv7, its birth timestamp selects the newest epoch
whose activation time is not later than that timestamp. The HMAC_SHA_384 input is:

```text
u32be(1)
|| "space-sandbox-anchor-v2" || 0x00
|| u32be(len(environment_utf8)) || environment_utf8
|| u64be(epoch_version)
|| UUID_bytes[16]
|| u32be(384)
```

This is domain-separated, SP800-108-style counter framing, not a claim of a
validated KDF implementation. One 48-byte result maps to P-256 scalar
`OS2IP(result) mod (n - 1) + 1`. The serial is the first20 bytes of SHA256(input),
with the sign bit cleared and zero excluded. Anchor subject, encoding,
extensions, validity and RFC6979 ECDSA signature are frozen scheme properties.
Changing them requires a new scheme and a trust-migration design.

The self-signed anchor is pathLen1, valid2026-01-01 through2126-01-01. Gateway CAs
are pathLen0, serverAuth-only, and expire no later than one year after issuance
or the anchor expiry. Tests use independently computed public-key/serial vectors
and verify deterministic private keys, certificate bytes, isolation and chains.

RustCrypto P-256 is the approved prototype exception for deterministic anchor
derivation/signing. AWS-LC handles ordinary gateway certificate signing and TLS.
The exception still requires a production security review.

## Latency and availability

- Cache hit: no KMS call. TLS handshakes and leaf signing do not contact KMS.
- Uncached derivation: one `GenerateMac`; up to one additional call to a
  configured related regional replica after primary failure. Clients are reused,
  SDK retries are disabled, and each regional attempt has a one-second bound.
  The public catalog lookup has a two-second bound.
- Node cache: at most4096 entries, 60-second TTL and ten-second sweep.
  Initialization is deduplicated per sandbox, with at most32 concurrent misses.
  No global cache lock is held across network I/O. A slow miss cannot block
  unrelated cached acquisitions. Node acquisition has a20-second overall bound.
- No software-master fallback exists. If every configured KMS region is
  unavailable, new uncached acquisitions fail closed. Already issued gateway
  credentials do not need KMS for ongoing TLS.

Related multi-region HMAC keys have the same material, but key policies,
enabled state and routing are separate. The application performs the fallback;
this is not automatic AWS routing ([AWS multi-region keys](https://docs.aws.amazon.com/kms/latest/developerguide/multi-region-keys-overview.md)).

Fetching currently occurs during the activation barrier after guest readiness,
not in parallel with VM boot. An actual latency report must distinguish direct
KMS request timing from complete sandbox startup and from cached local work.
Do not interpret unit tests or historical emulator timings as real AWS latency
or a production availability SLO.

## Rotation, restore and recovery

Provision a new hardware master and its replica, authorize the workload on both,
verify them, then CAS-append a future epoch. The fixture stages at least120 seconds
ahead, beyond the authority's accepted60-second future-UUID window and bounded
in-flight work. Do not mutate an existing epoch or route history through an alias.
HMAC keys rotate by creating new keys ([AWS key rotation](https://docs.aws.amazon.com/kms/latest/developerguide/rotate-keys.html)).

Retain old keys and catalog entries for every live or restorable sandbox.
Existing UUIDs continue deriving their original private/public key; new UUIDs
after activation use the new master. Retirement is deliberately not automated
without a complete restorable-identity inventory. Emergency replacement of an
existing sandbox's key requires explicit trust migration, not transparent rotation.

Cold activation, new template boots and forks install the sandbox anchor and remove the shared SPACE root
using existing guest file/process APIs. Activation does not publish readiness
until installation succeeds. Resume/restore read and validate existing trust;
they do not rewrite it. With the flag on, an unanchored legacy restore fails
closed; it is not silently upgraded or allowed a shared certificate. This
prototype is for fresh scoped guests, not a mixed-mode rollout onto old guests.
A malformed or foreign anchor also fails closed. The same UUID, epoch catalog and retained HSM key
recreate the same trust after node/authority replacement and cache eviction.

Missing catalog state fails startup. Recover the exact committed non-secret
catalog, not a newly generated master or a guessed latest version. ConfigMap
durability, cross-cluster recovery and stale-backup/rollback protection remain
operator responsibilities; in-memory append-only checks cannot prove a fresh
process was given the newest committed history.

## Real-KMS Kind proof

`space_smoke.anchor_hardware` provisions tagged disposable test resources with
an explicit account/profile, a0600 resource journal, and a runtime role limited
to `GenerateMac`/`DescribeKey` on the owned key ARNs. It installs dedicated
credential environment variables without changing Kind's storage STS identity.
Use the Python `test` dependency group. No credentials, HMAC outputs or private
keys belong in evidence or Git.

Run typed scenarios, not a second shell smoke:

- `native.split_trust_disabled`: default-off shared-TLS compatibility.
- `native.split_trust_tls`: fresh cold sandbox, live in-memory nonce/counter,
  own-anchor TLS and foreign-anchor rejection, resident pause/resume, durable
  suspend/restore, preserved certificate/public key and process identity.
- `local.split_trust_lifecycle`: cold create, fork isolation, same-node and
  cross-node durable restores.
- `local.split_trust_rpc_auth`: authenticated exchange and negative callers.
- `local.split_trust_warm_pool`: claimed warm-slot activation.
- `local.split_trust_authority_recovery`: missing public catalog fails startup;
  exact metadata recovery and restarted authorities reproduce trust.
- `local.split_trust_hardware`: independent KMS/OpenSSL key oracle, measured
  regional MAC calls, fresh uncached acquisition with primary keys disabled,
  append-only rotation and old/new identity checks.

Run the hardware scenario on a fresh epoch1 fixture after the other proofs.
It intentionally refuses to treat an already-rotated fixture as a new rotation
proof. Refresh short-lived fixture credentials before long runs and roll the
authority through the owning harness when its environment must reload.
Cleanup verifies ownership, removes the IAM role and schedules deletion of
replicas before primaries; pending KMS deletion is not immediate deletion.

## Remaining production gates

Production security review, reviewed KMS dependency audit, production IAM and
key/catalog disaster recovery, measured load/quotas/failure behavior, and staged
mixed-version trust-mode migration/rollback are not waived by this prototype.
The default-off flag does not itself make enabling this safe for all existing
guests. No production pin, deployment or100% interception rollout is changed.

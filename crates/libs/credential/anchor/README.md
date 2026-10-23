# Deterministic sandbox anchors: prototype

This crate freezes derivation scheme 1 for the split-trust prototype. It is not
production security approval. The credential authority integration is behind
the default-off `split-trust-prototype` Cargo feature and the explicit
`SPACE_SPLIT_TRUST_PROTOTYPE=true` runtime flag. Setting the runtime flag on an
authority binary built without the feature fails startup.

The stack separates wire contracts, authority, node credential manager,
host/guest installation, and sandbox-manager activation. This authority layer
does not itself activate guest trust or change the ordinary shared issuer.

## Cryptographic contract

- HKDF-SHA256 uses the seed as input, `space-anchor` as salt, and big-endian
  64-bit seed version followed by the 16 UUID bytes as info.
- A 48-byte output maps to P-256 scalar `OS2IP(output) mod (n - 1) + 1`.
- Only RFC-variant UUIDv7 identities are accepted. The greatest retained
  `since_ms` not after the UUID's birth time selects the seed.
- Anchor DER, subject, extensions, validity and deterministic ECDSA signing
  are part of the scheme, not incidental serialization. A change requires
  a new scheme and migration design. Tests include independent key/serial
  vectors and a full-certificate DER SHA-256 golden value.
- The serial is the first 20 bytes of SHA-256(version || UUID), with the sign
  bit cleared and zero excluded. This is an explicit interoperability
  clarification rather than a 32-byte X.509 serial.
- The anchor is a pathLen=1 CA, valid from 2026-01-01 to 2126-01-01. Gateway
  intermediates are pathLen=0, serverAuth-only, with expiry capped at one year
  from issuance and at the anchor's expiry.

## Prototype exception and production gates

RustCrypto P-256 is used only for deterministic anchor derivation/signing.
AWS-LC remains responsible for ordinary gateway certificate signing, TLS,
HKDF, randomness and backup AES-256-GCM. The deterministic-signing provider
exception requires a separate production security review; local test success
does not waive it.

The current integration demonstrates fresh-template deployment, not a mixed
version rollout. Before production, define guest-capability and persistent
trust-mode rollout/rollback rules, validate production KMS/S3 IAM and durability,
and add operational metrics and capacity tests.

Important boundaries of the current prototype:

- Anchor release uses the existing credential-agent RPC authentication and
  allow-list. It does not independently authorize a sandbox's placement or
  execution generation. An authenticated node can request other sandbox
  anchors; per-sandbox certificates alone do not limit a compromised node's
  signing authority under this trust model. No new DB grants or queries are
  introduced by the split-trust path.
- All seed versions are retained, and updates that remove a loaded seed are
  rejected. Automatic retirement is deliberately disabled without a complete
  live/restorable inventory and admission fence.
- Omitted/UNSPECIFIED issuer fields retain legacy SHARED meaning, never ANCHOR.
  Default-off still does not constitute production rollout/security approval.
- The live Kubernetes Secret contains unwrapped keyring JSON. Only the recovery
  copy uses KMS-backed envelope encryption. A provider-neutral live-keyring
  wrapper and environment-bound associated data are not implemented.
- Key release reconciles the keyring synchronously. If rotation is due, it can
  call KMS/S3; this does not yet keep cloud dependencies off the sandbox path.
- Fetch starts after guest readiness, not in parallel with boot. Restore also
  invokes the idempotent install RPC. Overlapped fetch and a strictly cold-boot
  installation contract remain work.
- The prototype removes the platform shared root from upgraded guests. It is
  not the staged dual-trust rollout, does not fall back to shared trust after
  install failure, and cannot prove shared-trust rollback.
- Routine seed staging is CAS-based rather than leader-elected. There is no
  operator `rotate-now` command, dedicated rotation/install metrics, or
  production-cadence rotation soak.

Recovery deliberately requires an explicitly selected encrypted backup object.
S3 can contain backups from losing CAS candidates. Do not infer the committed
keyring from the newest object or highest version alone. The operator must
retain the committed backup reference outside the live Secret; an autonomous
committed-backup catalog and disaster-recovery rollback protection remain
production work.

The local AWS fixture is Moto, not a security-equivalent AWS environment. Its
state is process-local. The integration proof restarts the authorities and
deletes their live Secret; it does not claim survival of the emulator's loss.

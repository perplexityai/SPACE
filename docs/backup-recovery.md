# Retained full-suspend recovery approvals

A recovery approval authorizes publication of an already sealed, validated full
suspend from a specific Kubernetes worker incarnation. It does not seal an
incomplete capture, repair devices, start an upload, clear quarantine, change
placement, or wake the sandbox. Preserve the original retained artifact while
using independently owned staging copies for uploads.

Deploy the additive schema first. Deploy token-aware placement writers, backup
identity enforcement, and evidence-producing upload workers before enabling the
`space-backup-recovery-grants-enabled` flag. Issuance defaults off. Every reader
that admits a backup must understand grant provenance before the first approval:
rolling back to a reader that treats grant-backed attempts as ordinary retry
authority is unsafe. Keep the audit schema on application rollback. Never run
its destructive down migration after issuing approvals.

The managing organization's administrator can use these admin API routes:

- `POST /sandboxes/{sandbox_id}/backup-recovery-grants` approves a manifest.
- `GET /sandboxes/{sandbox_id}/backup-recovery-grants` lists the newest 100
  approvals, including their active/consumed attempt and revocation state. Use
  this after a lost response before retrying approval creation.
- `POST /sandboxes/{sandbox_id}/backup-recovery-grants/{grant_id}/revoke` revokes
  an unconsumed approval and records the actor and reason.

The admin OpenAPI document describes the request. Approval requires an exact
artifact and generation, uploader node ID and Node UID, capture time within the
recorded execution lifetime, capture evidence, reason, and an expiry within
24 hours. The manifest includes an explicit sealed-validation reference, captured
execution metadata and resource shape, template identity, stable metadata binding,
and logical SHA-256/size for every VM artifact. It accepts a complete raw full
suspend or the encrypted snapshot-volume/rootfs-COW pair. Missing, duplicate,
empty, or generated btrfs-send entries are rejected before an approval can
permit upload admission.

Generate the stable metadata digest with the shared
`snapshot_metadata_binding` contract: it hashes canonical version-2 metadata,
excluding generated timestamps, measured sizes, and the local rootfs-send source
ID. Logical file sizes describe uncompressed plaintext inputs; catalog upload
size describes compressed/encrypted VM objects and excludes the metadata object.
Workers report both independently, alongside attempt-specific object keys and
available ETag/version identity. ETags are identity tokens, not content hashes.

| Event | Catalog and approval outcome | Progress owner |
| --- | --- | --- |
| Valid Begin | One active attempt bound to the approved uploader and capture time | Existing upload worker |
| Concurrent second Begin | Refused while the first attempt is not failed | Operator or existing worker |
| Failed attempt before expiry | Failure recorded; a fresh attempt may reuse the live approval | Existing bounded retry policy |
| Expiry, revoke, changed state, newer materialization, or proof mismatch | Ready refused; Failed remains recordable | Operator inspects retained source and audit |
| Ready races revoke | Sandbox lock serializes them; exactly one wins | Winning transaction |
| Valid Ready | Logical/object evidence, Ready, and grant consumption commit together | Normal restore/lifecycle paths |
| Exact consumed replay | Same attempt and evidence remain idempotent | Caller retries lost response |
| New attempt after consumption | Cannot inherit ordinary retry authority, even after attempt pruning | New explicit authority required |

An approval reserves subsequent registrations of its artifact for the approved
uploader, even when a failed ordinary upload records an earlier incarnation.
After expiry, revocation, or consumption, that older registration cannot bypass
the requirement for a new approval. Existing attempts retain their own terminal
reporting authority.

An abandoned higher generation without a materialized pod is not supersession.
A higher active/materialized execution, a replaced source generation, newer
captured state, or a changed lifecycle/placement/artifact witness prevents
publication. Capture timestamps remain immutable across retries; attempt
liveness uses `last_attempt_at`.

The administrator attests the source validation and capture provenance. Worker
hashes bind expected bytes on the honest authenticated-worker path; they do not
independently attest stored plaintext against a compromised uploader. A separate
object-reading verification service would be needed for that threat model.

Upload retries retain their existing bounded deadlines. Expired or revoked grants
never extend them. At exhaustion, the operator owns the next decision; there is
no automatic grant renewal or source deletion. A successful Ready transition is
catalog evidence, not proof of a successful live restore. Validate restore on the
appropriate Linux/KVM substrate before enabling issuance in a deployment.

# Credential Manager storage token exchange

Status: implemented contract

`CredentialManager.ExchangeToken` is the storage-lease boundary for SPACE
host-side consumers. It implements the RFC 8693 request/response shape with
one deliberately narrow profile: short-lived AWS credentials for one S3 object
or one canonical SPACE artifact prefix. It never returns the authority's
ambient or statically configured AWS credentials.

## Request contract

Every request must provide all of the following. Missing, malformed,
unknown, or duplicated values are rejected; an authorization or validation
failure never falls back to broader credentials.

| Field | Required value |
| --- | --- |
| `grant_type` | `urn:ietf:params:oauth:grant-type:token-exchange` |
| `audience` | `s3.amazonaws.com` |
| `requested_token_type` | `urn:ietf:params:oauth:token-type:aws-credentials` |
| `resource` | An exact `arn:aws:s3:::<bucket>/<key>` or canonical prefix ARN ending in `/*`; bucket-root access is unsupported |
| `scope` | A non-empty set of `space.storage.read` and/or `space.storage.write` |
| `subject_token` | The resource identity described below |
| `subject_token_type` | The matching subject-token type described below |
| `requested_expires_in` | `3600`; callers do not refresh credentials during a transfer |

The subject forms are:

- `urn:space:token-type:subject-id`: canonical `sandbox:<uuid>` or
  `template:<name>`.
- `urn:space:token-type:build-input-sha256`: a 64-character lowercase SHA-256
  digest for one content-addressed builder input.

Direct authenticated services must omit `actor_token` and `actor_token_type`;
the authority derives the actor from the authenticated transport. The node
credential agent sets `actor_token_type=urn:space:token-type:service` and an
actor token containing the locally authorized service name. It strips and
overwrites both incoming actor fields before forwarding. The authority accepts
that delegation only when the transport identity is the node credential agent.

Tenant identity is not caller-controlled. Sandbox subjects resolve their
organization from the sandbox row. Write authorization accepts durable node
attribution from active or target placement, the requested snapshot's
`source_node_id`, or the latest Kubernetes execution. That union is required
because Retire and suspend completion clear placement before queued uploads
necessarily exchange credentials. Read-only durable artifacts are portable to
another authenticated Volume Manager for restore and fork. Template subjects
resolve their optional owning organization from the template row. A supplied
resource cannot select or override a tenant.

## Authorization matrix

| Effective actor | Subject | Allowed scope |
| --- | --- | --- |
| Volume Manager | Sandbox | read and/or write |
| Volume Manager | Template | read only |
| Template Manager | Template | read only |
| Build Manager | Template | read and/or write |
| Build Manager | Build input digest | read and/or write |

All other combinations are denied. In particular, Sandbox Manager cannot
receive storage credentials, and template publication is unavailable to
read-only consumers.

Sandbox resources must use either the canonical
`suspend_snapshots/<shard>/<sandbox>/<snapshot>` layout or the maintained fork
layout beneath it,
`.../<source-snapshot>/fork/<source-sandbox>/<fork-snapshot>`. Every sandbox
component must name the subject sandbox. Kubernetes writes must match a
durably attributed node name and UID, except a prepared `creating` fork
snapshot: fork copy is deliberately portable to an assigned ready worker and
is restricted to that recorded snapshot URI. An organization-specific
`snapshot_base_object_storage_uri`, when valid, fixes the bucket and outer
prefix. The exact URI stored in `snapshot_storage_locations` is also accepted,
so changing the organization default does not strand older snapshots.

Template resources must use the canonical
`templates/<template>/<content-hash>` layout and name an existing template.
Build-input resources must name exactly
`build-inputs/sha256/<subject-digest>`. Exact-object requests additionally
accept only the maintained artifact filenames. Template reads also accept the
legacy `rootfs.ext4` artifact because older manifests legitimately publish
only that filename.

## Issuance and response

The authority selects an organization-specific `aws-sts` role for sandbox
storage when one is registered in `org_storage_vend_identities`; otherwise it
uses `SPACE_CREDMGR_S3_VEND_ROLE_ARN`. Template and build-input artifacts use
the platform storage role because their storage URI is platform-owned even
when the template registry row has a tenant owner. The scoped path calls STS
`AssumeRole` for one hour with an inline session policy containing object
actions on only the validated object/prefix plus `s3:ListBucket` on the bucket
conditioned to that same key/prefix. The latter preserves S3's missing-object
404 behavior without permitting a bucket-wide listing. The complete minified
policy is checked against STS's 2,048-byte plaintext limit before the network
call. The role's identity policy and the session policy intersect, so neither
can widen the other.

`scope` in the response is the canonical effective scope. `expires_in` is
calculated from the expiry returned by STS, and `AwsCredentials.expires_at_unix`
reports that same absolute expiry. No refresh token is issued.

The SDK default chain may supply the authority's credential **to STS**, but
default-chain and static credential values are never themselves vended. If no
valid vend role exists, exchange fails closed.

## Audit and failure behavior

Every authorization denial emits a `credential_security_audit` event with the
authenticated transport actor and a redacted error class. Successful issuance
and post-authorization STS failures include actor, derived tenant, subject,
resource bucket/key, effective scope, decision, outcome, and expiry where
available. Events never contain subject/actor bearer tokens, access keys,
secret keys, session tokens, or the inline policy.

Store failures are server failures and contention is retryable. An invalid
organization storage URI can still serve an exact historical URI recorded for
the requested snapshot; otherwise it, an unsupported vend provider, or an
invalid role ARN is a failed configuration precondition rather than an
`INTERNAL` error. Unknown subjects, cross-node writes, actor spoofing,
cross-subject resources, and privilege escalation are authorization denials.
Malformed or unsupported wire values are client errors.

## Authority configuration

`SPACE_CREDMGR_S3_VEND_ROLE_ARN` enables storage credential exchange. The
authority parses it once at startup; a whitespace-corrupted or otherwise
invalid ARN, or a set role without `DATABASE_URL`, prevents the authority
becoming ready. When it is unset the authority wires no storage authorization
store and every request fails with `FAILED_PRECONDITION`, so a deployment
without a storage-vend role issues no storage credentials. Leaving the role
unset is also the emergency stop for all storage credential issuance.
The retired `SPACE_CREDMGR_SCOPED_EXCHANGE_TOKEN_MODE` no longer selects
behavior. A retained `disabled` value alongside a vend role fails startup so an
upgrade cannot reverse that emergency stop; other retained values only log a
warning.

Every request needs a JWT or UDS caller identity. A static-token TCP transport
resolves as `Plaintext`, which is never authorized. Callers that omit
`requested_expires_in`, including older node agents that drop the field when
re-encoding a request, are rejected with `INVALID_ARGUMENT`.

## Deployment prerequisites

Scoped sandbox authorization reads the requested snapshot's node attribution
and recorded storage URI through the authority's existing `space_creds` pool.
Role bootstrap grants that login read/write DML on every application table, so
no additional database grant or migration is required.

Callers and node agents must send the scoped request contract before they
reach an authority. Rolling a caller back to a build that predates the
contract makes its storage exchanges fail.

//! Parse and authorize the storage-specific RFC 8693 token exchange contract.

use pplx_space_common_core::types::SnapshotId;

use std::fmt;

use pplx_space_common_svc::core::host::linux::identities::users;
use pplx_space_common_svc::core::types::{
    ContentHash, OrgId, SandboxId, SnapshotArtifact, SubjectId, TemplateArtifact, TemplateName,
    snapshot_shard,
};
use pplx_space_credmgr_client::{
    AWS_CREDENTIALS_TOKEN_TYPE, BUILD_INPUT_SHA256_TOKEN_TYPE, SERVICE_ACTOR_TOKEN_TYPE,
    STORAGE_READ_SCOPE, STORAGE_SESSION_TTL_SECONDS, STORAGE_TOKEN_AUDIENCE, STORAGE_WRITE_SCOPE,
    SUBJECT_ID_TOKEN_TYPE, TOKEN_EXCHANGE_GRANT_TYPE,
};

use crate::auth::{ClientService, ResolvedCaller};
use crate::proto::ExchangeTokenRequest;
use crate::storage_vend_store::{StorageVendProvider, StorageVendStore, StorageVendStoreError};

const MAX_STS_POLICY_BYTES: usize = 2_048;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct StorageActions {
    read: bool,
    write: bool,
}

impl StorageActions {
    fn parse(raw: Option<&str>) -> Result<Self, StorageExchangeError> {
        let raw = raw.ok_or(StorageExchangeError::Missing("scope"))?;
        if raw.is_empty() {
            return Err(StorageExchangeError::EmptyScope);
        }
        let mut actions = Self {
            read: false,
            write: false,
        };
        for scope in raw.split(' ') {
            if scope.is_empty() {
                return Err(StorageExchangeError::Malformed("scope"));
            }
            match scope {
                STORAGE_READ_SCOPE if !actions.read => actions.read = true,
                STORAGE_WRITE_SCOPE if !actions.write => actions.write = true,
                STORAGE_READ_SCOPE | STORAGE_WRITE_SCOPE => {
                    return Err(StorageExchangeError::Malformed("scope"));
                }
                _ => return Err(StorageExchangeError::Unsupported("scope")),
            }
        }
        Ok(actions)
    }

    pub(crate) fn effective_scope(self) -> String {
        match (self.read, self.write) {
            (true, true) => format!("{STORAGE_READ_SCOPE} {STORAGE_WRITE_SCOPE}"),
            (true, false) => STORAGE_READ_SCOPE.to_string(),
            (false, true) => STORAGE_WRITE_SCOPE.to_string(),
            (false, false) => unreachable!("StorageActions cannot be empty after parsing"),
        }
    }

    fn includes_write(self) -> bool {
        self.write
    }

    fn iam_actions(self) -> Vec<&'static str> {
        let mut actions = Vec::with_capacity(3);
        if self.read {
            actions.push("s3:GetObject");
        }
        if self.write {
            actions.push("s3:PutObject");
            actions.push("s3:AbortMultipartUpload");
        }
        actions
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct S3Resource {
    bucket: String,
    key: String,
    prefix: bool,
}

impl S3Resource {
    fn parse(raw: &str) -> Result<Self, StorageExchangeError> {
        let body = raw
            .strip_prefix("arn:aws:s3:::")
            .ok_or(StorageExchangeError::Malformed("resource"))?;
        let (bucket, raw_key) = body
            .split_once('/')
            .ok_or(StorageExchangeError::Malformed("resource"))?;
        if !valid_bucket(bucket) {
            return Err(StorageExchangeError::Malformed("resource"));
        }
        let (key, prefix) = match raw_key.strip_suffix("/*") {
            Some(prefix) => (prefix, true),
            None => (raw_key, false),
        };
        if key.is_empty()
            || key.len() > 1_900
            || key.contains('*')
            || key.contains('\\')
            || key.chars().any(char::is_control)
            || key
                .split('/')
                .any(|segment| segment.is_empty() || matches!(segment, "." | ".."))
        {
            return Err(StorageExchangeError::Malformed("resource"));
        }
        Ok(Self {
            bucket: bucket.to_string(),
            key: key.to_string(),
            prefix,
        })
    }

    pub(crate) fn bucket(&self) -> &str {
        &self.bucket
    }

    pub(crate) fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn policy_arn(&self) -> String {
        if self.prefix {
            format!("arn:aws:s3:::{}/{}/{}", self.bucket, self.key, "*")
        } else {
            format!("arn:aws:s3:::{}/{}", self.bucket, self.key)
        }
    }

    fn bucket_policy_arn(&self) -> String {
        format!("arn:aws:s3:::{}", self.bucket)
    }

    fn list_prefix_condition(&self) -> serde_json::Value {
        if self.prefix {
            serde_json::json!({
                "StringLike": {
                    "s3:prefix": [self.key, format!("{}/*", self.key)],
                },
            })
        } else {
            serde_json::json!({
                "StringEquals": { "s3:prefix": self.key },
            })
        }
    }

    fn is_within(&self, bucket: &str, base_prefix: &str) -> bool {
        if self.bucket != bucket {
            return false;
        }
        let base = base_prefix.trim_matches('/');
        base.is_empty()
            || self.key == base
            || self
                .key
                .strip_prefix(base)
                .is_some_and(|suffix| suffix.starts_with('/'))
    }
}

fn valid_bucket(bucket: &str) -> bool {
    (3..=63).contains(&bucket.len())
        && bucket.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'-')
        })
        && bucket
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        && bucket
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BuildInputDigest(String);

impl BuildInputDigest {
    fn parse(raw: &str) -> Result<Self, StorageExchangeError> {
        if raw.len() == 64
            && raw
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            Ok(Self(raw.to_string()))
        } else {
            Err(StorageExchangeError::Malformed("subject_token"))
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum StorageSubject {
    Resource(SubjectId),
    BuildInput(BuildInputDigest),
}

impl StorageSubject {
    fn parse(token: Option<&str>, token_type: Option<&str>) -> Result<Self, StorageExchangeError> {
        let token = token.ok_or(StorageExchangeError::Missing("subject_token"))?;
        match token_type.ok_or(StorageExchangeError::Missing("subject_token_type"))? {
            SUBJECT_ID_TOKEN_TYPE => token
                .parse()
                .map(Self::Resource)
                .map_err(|_| StorageExchangeError::Malformed("subject_token")),
            BUILD_INPUT_SHA256_TOKEN_TYPE => BuildInputDigest::parse(token).map(Self::BuildInput),
            _ => Err(StorageExchangeError::Unsupported("subject_token_type")),
        }
    }

    pub(crate) fn audit_kind(&self) -> &'static str {
        match self {
            Self::Resource(SubjectId::Sandbox(_)) => "sandbox",
            Self::Resource(SubjectId::Template(_)) => "template",
            Self::BuildInput(_) => "build-input",
        }
    }

    pub(crate) fn audit_id(&self) -> String {
        match self {
            Self::Resource(subject) => subject.to_string(),
            Self::BuildInput(digest) => digest.0.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct VendRoleArn(String);

impl VendRoleArn {
    pub(crate) fn parse(raw: impl Into<String>) -> Result<Self, StorageExchangeError> {
        let raw = raw.into();
        if crate::iam_role_arn::is_iam_role_arn(&raw) {
            Ok(Self(raw))
        } else {
            Err(StorageExchangeError::InvalidVendRole)
        }
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug)]
struct ParsedExchange {
    actor: ClientService,
    subject: StorageSubject,
    resource: S3Resource,
    actions: StorageActions,
    ttl_seconds: u32,
}

#[derive(Clone, Debug)]
pub(crate) struct AuthorizedExchange {
    pub(crate) actor: ClientService,
    pub(crate) subject: StorageSubject,
    pub(crate) resource: S3Resource,
    pub(crate) actions: StorageActions,
    pub(crate) ttl_seconds: u32,
    pub(crate) organization_id: Option<OrgId>,
    role_override: Option<VendRoleArn>,
}

impl AuthorizedExchange {
    pub(crate) fn vend_role(
        &self,
        default_role: Option<&VendRoleArn>,
    ) -> Result<VendRoleArn, StorageExchangeError> {
        match &self.role_override {
            Some(role) => Ok(role.clone()),
            None => default_role
                .cloned()
                .ok_or(StorageExchangeError::MissingVendRole),
        }
    }

    pub(crate) fn session_policy(&self) -> Result<String, StorageExchangeError> {
        let policy = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": self.actions.iam_actions(),
                    "Resource": self.resource.policy_arn(),
                },
                {
                    "Effect": "Allow",
                    "Action": "s3:ListBucket",
                    "Resource": self.resource.bucket_policy_arn(),
                    "Condition": self.resource.list_prefix_condition(),
                },
            ],
        });
        let policy = policy.to_string();
        if policy.len() > MAX_STS_POLICY_BYTES {
            return Err(StorageExchangeError::PolicyTooLarge);
        }
        Ok(policy)
    }
}

pub(crate) async fn authorize_exchange(
    store: &dyn StorageVendStore,
    caller: &ResolvedCaller,
    request: ExchangeTokenRequest,
) -> Result<AuthorizedExchange, StorageExchangeError> {
    let parsed = parse_request(caller, request)?;
    authorize_actor(&parsed)?;
    let subject_resource = validate_subject_resource(&parsed.subject, &parsed.resource)?;

    let mut organization_id = None;
    let mut role_override = None;
    match &parsed.subject {
        StorageSubject::Resource(SubjectId::Sandbox(sandbox_id)) => {
            let SubjectResource::Snapshot(snapshot) = subject_resource else {
                unreachable!("sandbox validation returns a snapshot identity")
            };
            let context = store
                .sandbox_context(*sandbox_id, snapshot.snapshot_id)
                .await?
                .ok_or(StorageExchangeError::UnknownSubject)?;
            if let ResolvedCaller::K8sJwt(identity) = caller {
                let node_matches = context.nodes.iter().any(|node| {
                    node.name == *identity.node().node_name()
                        && node.uid == *identity.node().node_uid()
                });
                let portable_write_matches =
                    context
                        .portable_snapshot_write
                        .as_ref()
                        .is_some_and(|storage| {
                            parsed
                                .resource
                                .is_within(&storage.bucket, &storage.key_prefix)
                        });
                // Durable reads are allowed on a different authenticated node:
                // cross-node restore and fork copy deliberately consume an
                // already-published artifact away from its source. Mutations
                // remain fenced to durable active/target/execution/snapshot
                // attribution, including Retire salvage after placement clears.
                if !node_matches && parsed.actions.includes_write() && !portable_write_matches {
                    return Err(StorageExchangeError::WrongNode);
                }
            }
            if !context.snapshot_storage.is_empty()
                && !context.snapshot_storage.iter().any(|storage| {
                    parsed
                        .resource
                        .is_within(&storage.bucket, &storage.key_prefix)
                })
            {
                return Err(StorageExchangeError::UnauthorizedResource);
            }
            if context.snapshot_storage_configuration_invalid && context.snapshot_storage.is_empty()
            {
                return Err(StorageExchangeError::InvalidStorageConfiguration);
            }
            organization_id = Some(context.organization_id);
            if let Some(identity) = store.org_vend_identity(context.organization_id).await? {
                match identity.provider {
                    StorageVendProvider::AwsSts => {
                        role_override = Some(VendRoleArn::parse(identity.identity)?);
                    }
                }
            }
        }
        StorageSubject::Resource(SubjectId::Template(template)) => {
            organization_id = store
                .template_owner(template)
                .await?
                .ok_or(StorageExchangeError::UnknownSubject)?;
            let SubjectResource::TemplateRelease(hash) = subject_resource else {
                unreachable!("template validation returns the release hash")
            };
            // A registered release is immutable. The builder vends write
            // credentials before RegisterTemplateRelease, so first publication
            // and upload retries pass, while a rewrite of any already-published
            // release prefix (including the platform's system templates) is
            // refused no matter which template the caller names.
            if parsed.actions.includes_write()
                && store.template_release_published(template, &hash).await?
            {
                return Err(StorageExchangeError::ReleaseAlreadyPublished);
            }
        }
        StorageSubject::BuildInput(_) => {}
    }

    Ok(AuthorizedExchange {
        actor: parsed.actor,
        subject: parsed.subject,
        resource: parsed.resource,
        actions: parsed.actions,
        ttl_seconds: parsed.ttl_seconds,
        organization_id,
        role_override,
    })
}

fn parse_request(
    caller: &ResolvedCaller,
    request: ExchangeTokenRequest,
) -> Result<ParsedExchange, StorageExchangeError> {
    if request.grant_type != TOKEN_EXCHANGE_GRANT_TYPE {
        return Err(StorageExchangeError::Unsupported("grant_type"));
    }
    if request.audience.as_deref() != Some(STORAGE_TOKEN_AUDIENCE) {
        return Err(if request.audience.is_some() {
            StorageExchangeError::Unsupported("audience")
        } else {
            StorageExchangeError::Missing("audience")
        });
    }
    if request.requested_token_type != AWS_CREDENTIALS_TOKEN_TYPE {
        return Err(StorageExchangeError::Unsupported("requested_token_type"));
    }
    let ttl_seconds = request
        .requested_expires_in
        .ok_or(StorageExchangeError::Missing("requested_expires_in"))?;
    if ttl_seconds != STORAGE_SESSION_TTL_SECONDS {
        return Err(StorageExchangeError::Unsupported("requested_expires_in"));
    }
    let actor = effective_actor(
        caller,
        request.actor_token.as_deref(),
        request.actor_token_type.as_deref(),
    )?;
    Ok(ParsedExchange {
        actor,
        subject: StorageSubject::parse(
            request.subject_token.as_deref(),
            request.subject_token_type.as_deref(),
        )?,
        resource: S3Resource::parse(&request.resource)?,
        actions: StorageActions::parse(request.scope.as_deref())?,
        ttl_seconds,
    })
}

fn effective_actor(
    caller: &ResolvedCaller,
    actor_token: Option<&str>,
    actor_token_type: Option<&str>,
) -> Result<ClientService, StorageExchangeError> {
    match caller {
        ResolvedCaller::K8sJwt(identity)
            if identity.service() == ClientService::NodeCredentialAgent =>
        {
            if actor_token_type != Some(SERVICE_ACTOR_TOKEN_TYPE) {
                return Err(if actor_token_type.is_some() {
                    StorageExchangeError::Unsupported("actor_token_type")
                } else {
                    StorageExchangeError::Missing("actor_token_type")
                });
            }
            let actor = actor_token
                .ok_or(StorageExchangeError::Missing("actor_token"))?
                .parse()
                .map_err(|_| StorageExchangeError::Malformed("actor_token"))?;
            if matches!(
                actor,
                ClientService::VolumeManager | ClientService::TemplateManager
            ) {
                Ok(actor)
            } else {
                Err(StorageExchangeError::UnauthorizedActor)
            }
        }
        ResolvedCaller::K8sJwt(identity) => {
            require_no_actor_claim(actor_token, actor_token_type)?;
            Ok(identity.service())
        }
        ResolvedCaller::UdsPeer(identity) => {
            require_no_actor_claim(actor_token, actor_token_type)?;
            if identity.uid() == users::SPACE_VOLMGR.uid
                || identity.uid() == users::SPACE_SBXMGR.uid
            {
                Ok(ClientService::VolumeManager)
            } else if identity.uid() == users::SPACE_TMPLMGR.uid {
                Ok(ClientService::TemplateManager)
            } else {
                Err(StorageExchangeError::UnauthorizedActor)
            }
        }
        ResolvedCaller::Plaintext => Err(StorageExchangeError::UnauthorizedActor),
    }
}

fn require_no_actor_claim(
    actor_token: Option<&str>,
    actor_token_type: Option<&str>,
) -> Result<(), StorageExchangeError> {
    if actor_token.is_some() || actor_token_type.is_some() {
        Err(StorageExchangeError::ActorSpoofing)
    } else {
        Ok(())
    }
}

fn authorize_actor(parsed: &ParsedExchange) -> Result<(), StorageExchangeError> {
    let authorized = match (parsed.actor, &parsed.subject) {
        (ClientService::VolumeManager, StorageSubject::Resource(SubjectId::Sandbox(_))) => true,
        (
            ClientService::VolumeManager | ClientService::TemplateManager,
            StorageSubject::Resource(SubjectId::Template(_)),
        ) => !parsed.actions.includes_write(),
        (ClientService::BuildManager, StorageSubject::Resource(SubjectId::Template(_))) => true,
        (ClientService::BuildManager, StorageSubject::BuildInput(_)) => true,
        _ => false,
    };
    authorized
        .then_some(())
        .ok_or(StorageExchangeError::UnauthorizedActor)
}

fn validate_subject_resource(
    subject: &StorageSubject,
    resource: &S3Resource,
) -> Result<SubjectResource, StorageExchangeError> {
    match subject {
        StorageSubject::Resource(SubjectId::Sandbox(sandbox_id)) => {
            validate_snapshot_resource(*sandbox_id, resource).map(SubjectResource::Snapshot)
        }
        StorageSubject::Resource(SubjectId::Template(template)) => {
            validate_template_resource(template, resource).map(SubjectResource::TemplateRelease)
        }
        StorageSubject::BuildInput(digest) => {
            validate_build_input_resource(digest, resource).map(|()| SubjectResource::BuildInput)
        }
    }
}

/// What the validated resource key identifies within the subject.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SubjectResource {
    Snapshot(SnapshotResource),
    /// The release hash segment of `templates/<name>/<hash>[/<file>]`.
    TemplateRelease(ContentHash),
    BuildInput,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SnapshotResource {
    snapshot_id: SnapshotId,
}

fn validate_snapshot_resource(
    sandbox_id: SandboxId,
    resource: &S3Resource,
) -> Result<SnapshotResource, StorageExchangeError> {
    let segments: Vec<_> = resource.key.split('/').collect();
    let marker = segments
        .iter()
        .rposition(|segment| *segment == "suspend_snapshots")
        .ok_or(StorageExchangeError::UnauthorizedResource)?;
    let Some(tail) = segments.get(marker + 1..) else {
        return Err(StorageExchangeError::UnauthorizedResource);
    };
    let expected_id = sandbox_id.to_string();
    let expected_shard = format!("{:02x}", snapshot_shard(&expected_id));
    let snapshot_id = match (resource.prefix, tail) {
        (true, [shard, sandbox, snapshot]) => (*shard == expected_shard && *sandbox == expected_id)
            .then(|| snapshot.parse::<uuid::Uuid>().ok())
            .flatten(),
        (false, [shard, sandbox, snapshot, artifact]) => (*shard == expected_shard
            && *sandbox == expected_id
            && valid_snapshot_artifact(artifact))
        .then(|| snapshot.parse::<uuid::Uuid>().ok())
        .flatten(),
        (
            true,
            [
                shard,
                sandbox,
                source_snapshot,
                "fork",
                fork_source_sandbox,
                fork_snapshot,
            ],
        ) => (*shard == expected_shard
            && *sandbox == expected_id
            && *fork_source_sandbox == expected_id
            && source_snapshot.parse::<uuid::Uuid>().is_ok())
        .then(|| fork_snapshot.parse::<uuid::Uuid>().ok())
        .flatten(),
        (
            false,
            [
                shard,
                sandbox,
                source_snapshot,
                "fork",
                fork_source_sandbox,
                fork_snapshot,
                artifact,
            ],
        ) => (*shard == expected_shard
            && *sandbox == expected_id
            && *fork_source_sandbox == expected_id
            && source_snapshot.parse::<uuid::Uuid>().is_ok()
            && valid_snapshot_artifact(artifact))
        .then(|| fork_snapshot.parse::<uuid::Uuid>().ok())
        .flatten(),
        _ => None,
    };
    snapshot_id
        .map(|snapshot_id| SnapshotResource {
            snapshot_id: SnapshotId::from_uuid(snapshot_id),
        })
        .ok_or(StorageExchangeError::UnauthorizedResource)
}

fn valid_snapshot_artifact(value: &str) -> bool {
    [
        SnapshotArtifact::Rootfs,
        SnapshotArtifact::RootfsBtrfsSend,
        SnapshotArtifact::Memory,
        SnapshotArtifact::VmState,
        SnapshotArtifact::EncryptedSnapshotVolume,
        SnapshotArtifact::EncryptedRootfsCow,
        SnapshotArtifact::Metadata,
    ]
    .into_iter()
    .any(|artifact| artifact.file_name() == value)
}

fn validate_template_resource(
    template: &TemplateName,
    resource: &S3Resource,
) -> Result<ContentHash, StorageExchangeError> {
    let segments: Vec<_> = resource.key.split('/').collect();
    let marker = segments
        .iter()
        .rposition(|segment| *segment == "templates")
        .ok_or(StorageExchangeError::UnauthorizedResource)?;
    let Some(tail) = segments.get(marker + 1..) else {
        return Err(StorageExchangeError::UnauthorizedResource);
    };
    let hash = match (resource.prefix, tail) {
        (true, [name, hash]) if *name == template.as_str() => hash.parse::<ContentHash>().ok(),
        (false, [name, hash, file])
            if *name == template.as_str()
                && (*file == "rootfs.ext4"
                    || TemplateArtifact::PUBLISHED
                        .into_iter()
                        .any(|artifact| artifact.file_name() == *file)) =>
        {
            hash.parse::<ContentHash>().ok()
        }
        _ => None,
    };
    hash.ok_or(StorageExchangeError::UnauthorizedResource)
}

fn validate_build_input_resource(
    digest: &BuildInputDigest,
    resource: &S3Resource,
) -> Result<(), StorageExchangeError> {
    let segments: Vec<_> = resource.key.split('/').collect();
    let marker = segments
        .iter()
        .rposition(|segment| *segment == "build-inputs")
        .ok_or(StorageExchangeError::UnauthorizedResource)?;
    let Some(tail) = segments.get(marker + 1..) else {
        return Err(StorageExchangeError::UnauthorizedResource);
    };
    if resource.prefix || !matches!(tail, ["sha256", value] if *value == digest.0) {
        return Err(StorageExchangeError::UnauthorizedResource);
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum StorageExchangeError {
    #[error("missing required token-exchange field {0}")]
    Missing(&'static str),
    #[error("malformed token-exchange field {0}")]
    Malformed(&'static str),
    #[error("unsupported token-exchange field {0}")]
    Unsupported(&'static str),
    #[error("storage credential scope must not be empty")]
    EmptyScope,
    #[error("direct callers cannot assert an actor token")]
    ActorSpoofing,
    #[error("caller is not authorized for the requested storage access")]
    UnauthorizedActor,
    #[error("requested storage resource is not authorized for the subject")]
    UnauthorizedResource,
    #[error("storage subject does not exist")]
    UnknownSubject,
    #[error("sandbox is not assigned to the authenticated node incarnation")]
    WrongNode,
    #[error("template release is already published and cannot be rewritten")]
    ReleaseAlreadyPublished,
    #[error("storage credential vending is not configured")]
    MissingVendRole,
    #[error("configured storage vend role ARN is invalid")]
    InvalidVendRole,
    #[error("the organization's snapshot storage configuration is invalid")]
    InvalidStorageConfiguration,
    #[error("generated STS session policy exceeds the 2048-byte limit")]
    PolicyTooLarge,
    #[error(transparent)]
    Store(#[from] StorageVendStoreError),
}

impl StorageExchangeError {
    pub(crate) fn audit_class(&self) -> &'static str {
        match self {
            Self::Missing(_) => "missing_field",
            Self::Malformed(_) => "malformed_field",
            Self::Unsupported(_) => "unsupported_field",
            Self::EmptyScope => "empty_scope",
            Self::ActorSpoofing => "actor_spoofing",
            Self::UnauthorizedActor => "unauthorized_actor",
            Self::UnauthorizedResource => "unauthorized_resource",
            Self::UnknownSubject => "unknown_subject",
            Self::WrongNode => "wrong_node",
            Self::ReleaseAlreadyPublished => "release_already_published",
            Self::MissingVendRole => "missing_vend_role",
            Self::InvalidVendRole => "invalid_vend_role",
            Self::InvalidStorageConfiguration => "invalid_storage_configuration",
            Self::PolicyTooLarge => "policy_too_large",
            Self::Store(_) => "store_failure",
        }
    }
}

impl fmt::Display for AuthorizedExchange {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} {}", self.actor, self.subject.audit_kind())
    }
}

#[cfg(test)]
mod tests;

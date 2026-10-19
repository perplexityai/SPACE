use pplx_space_common_core::types::SnapshotId;

use std::collections::HashMap;

use pplx_space_common_svc::core::types::{
    ContentHash, K8sNodeName, K8sNodeUid, OrgId, SandboxId, StorageUri, TemplateName,
    snapshot_shard,
};

use super::*;
use crate::auth::K8sIdentity;
use crate::storage_vend_store::{SandboxNodeIdentity, SandboxStorageContext, StorageVendIdentity};

#[derive(Default)]
struct TestStore {
    sandboxes: HashMap<SandboxId, SandboxStorageContext>,
    templates: HashMap<TemplateName, Option<OrgId>>,
    published: Vec<(TemplateName, ContentHash)>,
    identities: HashMap<OrgId, StorageVendIdentity>,
}

#[tonic::async_trait]
impl StorageVendStore for TestStore {
    async fn sandbox_context(
        &self,
        sandbox_id: SandboxId,
        _snapshot_id: SnapshotId,
    ) -> Result<Option<SandboxStorageContext>, StorageVendStoreError> {
        Ok(self.sandboxes.get(&sandbox_id).cloned())
    }

    async fn template_owner(
        &self,
        template: &TemplateName,
    ) -> Result<Option<Option<OrgId>>, StorageVendStoreError> {
        Ok(self.templates.get(template).copied())
    }

    async fn template_release_published(
        &self,
        template: &TemplateName,
        hash: &ContentHash,
    ) -> Result<bool, StorageVendStoreError> {
        Ok(self
            .published
            .iter()
            .any(|(name, published)| name == template && published == hash))
    }

    async fn org_vend_identity(
        &self,
        organization_id: OrgId,
    ) -> Result<Option<StorageVendIdentity>, StorageVendStoreError> {
        Ok(self.identities.get(&organization_id).cloned())
    }
}

fn caller(service: ClientService, node_name: &str, node_uid: &str) -> ResolvedCaller {
    ResolvedCaller::K8sJwt(
        K8sIdentity::from_token_review(
            service,
            "credential-client",
            "credential-client-uid",
            node_name,
            node_uid,
        )
        .unwrap(),
    )
}

fn template_request(template: &TemplateName, scope: &str) -> ExchangeTokenRequest {
    ExchangeTokenRequest {
        grant_type: TOKEN_EXCHANGE_GRANT_TYPE.to_string(),
        resource: format!(
            "arn:aws:s3:::space-snapshots/templates/{template}/sha256:{}/*",
            "ab".repeat(32)
        ),
        audience: Some(STORAGE_TOKEN_AUDIENCE.to_string()),
        scope: Some(scope.to_string()),
        requested_token_type: AWS_CREDENTIALS_TOKEN_TYPE.to_string(),
        subject_token: Some(format!("template:{template}")),
        subject_token_type: Some(SUBJECT_ID_TOKEN_TYPE.to_string()),
        actor_token: None,
        actor_token_type: None,
        requested_expires_in: Some(STORAGE_SESSION_TTL_SECONDS),
        copy_source_resource: None,
    }
}

fn sandbox_request(sandbox_id: SandboxId) -> ExchangeTokenRequest {
    let sandbox = sandbox_id.to_string();
    let snapshot = uuid::Uuid::now_v7();
    ExchangeTokenRequest {
        grant_type: TOKEN_EXCHANGE_GRANT_TYPE.to_string(),
        resource: format!(
            "arn:aws:s3:::space-snapshots/tenant/suspend_snapshots/{:02x}/{sandbox}/{snapshot}/*",
            snapshot_shard(&sandbox)
        ),
        audience: Some(STORAGE_TOKEN_AUDIENCE.to_string()),
        scope: Some(STORAGE_WRITE_SCOPE.to_string()),
        requested_token_type: AWS_CREDENTIALS_TOKEN_TYPE.to_string(),
        subject_token: Some(format!("sandbox:{sandbox}")),
        subject_token_type: Some(SUBJECT_ID_TOKEN_TYPE.to_string()),
        actor_token: None,
        actor_token_type: None,
        requested_expires_in: Some(STORAGE_SESSION_TTL_SECONDS),
        copy_source_resource: None,
    }
}

fn sandbox_context(org: OrgId) -> SandboxStorageContext {
    SandboxStorageContext {
        organization_id: org,
        nodes: vec![node_identity("worker-a", "worker-a-uid")],
        snapshot_storage: vec![
            StorageUri::parse("s3://space-snapshots/tenant?region=us-east-1").unwrap(),
        ],
        snapshot_storage_configuration_invalid: false,
        portable_snapshot_write: None,
    }
}

fn copy_request(sandbox: SandboxId) -> ExchangeTokenRequest {
    let mut request = sandbox_request(sandbox);
    let prefix = request.resource.trim_end_matches("/*");
    let source = format!("{prefix}/rootfs.btrfs-send");
    let destination = format!(
        "{prefix}/fork/{sandbox}/{}/rootfs.btrfs-send",
        uuid::Uuid::now_v7()
    );
    request.resource = destination;
    request.copy_source_resource = Some(source);
    request.scope = Some(format!("{STORAGE_READ_SCOPE} {STORAGE_WRITE_SCOPE}"));
    request
}

#[tokio::test]
async fn copy_exchange_rejects_broader_or_unattributed_resources() {
    let sandbox = SandboxId::new();
    let base = copy_request(sandbox);
    let store = TestStore {
        sandboxes: HashMap::from([(sandbox, sandbox_context(OrgId::new()))]),
        ..Default::default()
    };
    let peer = caller(ClientService::VolumeManager, "worker-a", "worker-a-uid");
    let cases = [
        ("source prefix", {
            let mut request = base.clone();
            let source = request.copy_source_resource.as_mut().unwrap();
            *source = format!("{}/*", source.rsplit_once('/').unwrap().0);
            request
        }),
        ("destination prefix", {
            let mut request = base.clone();
            request.resource = format!("{}/*", request.resource.rsplit_once('/').unwrap().0);
            request
        }),
        ("different source subject", {
            let mut request = base.clone();
            let source = request.copy_source_resource.as_mut().unwrap();
            *source = source.replace(&sandbox.to_string(), &SandboxId::new().to_string());
            request
        }),
        ("source outside recorded storage", {
            let mut request = base.clone();
            let source = request.copy_source_resource.as_mut().unwrap();
            *source = source.replace("/tenant/", "/outside/");
            request
        }),
        ("source equals destination", {
            let mut request = base.clone();
            request.copy_source_resource = Some(request.resource.clone());
            request
        }),
        ("read-only destination", {
            let mut request = base.clone();
            request.scope = Some(STORAGE_READ_SCOPE.to_string());
            request
        }),
        ("write-only destination", {
            let mut request = base.clone();
            request.scope = Some(STORAGE_WRITE_SCOPE.to_string());
            request
        }),
    ];
    for (label, request) in cases {
        assert!(
            authorize_exchange(&store, &peer, request).await.is_err(),
            "{label} must not vend copy credentials"
        );
    }
    let error = authorize_exchange(
        &store,
        &caller(ClientService::VolumeManager, "worker-b", "worker-b-uid"),
        base,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(error, StorageExchangeError::WrongNode),
        "portable reads must not authorize an unattributed destination write"
    );
}

#[tokio::test]
async fn portable_copy_vends_read_only_source_and_exact_destination_permissions() {
    let sandbox = SandboxId::new();
    let mut request = sandbox_request(sandbox);
    let source_prefix = request.resource.trim_end_matches("/*").to_string();
    let source = format!("{source_prefix}/rootfs.btrfs-send");
    let destination_prefix = format!("{source_prefix}/fork/{sandbox}/{}", uuid::Uuid::now_v7());
    let destination = format!("{destination_prefix}/rootfs.btrfs-send");
    request.resource = destination.clone();
    request.copy_source_resource = Some(source.clone());
    request.scope = Some(format!("{STORAGE_READ_SCOPE} {STORAGE_WRITE_SCOPE}"));
    let mut context = sandbox_context(OrgId::new());
    context.portable_snapshot_write = Some(
        StorageUri::parse(&format!(
            "s3://{}?region=us-east-1",
            destination_prefix.trim_start_matches("arn:aws:s3:::")
        ))
        .unwrap(),
    );
    let store = TestStore {
        sandboxes: HashMap::from([(sandbox, context)]),
        ..Default::default()
    };
    let authorized = authorize_exchange(
        &store,
        &caller(ClientService::VolumeManager, "worker-b", "worker-b-uid"),
        request,
    )
    .await
    .expect("source reads and prepared destination writes are portable independently");
    let policy: serde_json::Value =
        serde_json::from_str(&authorized.session_policy().unwrap()).unwrap();
    assert_eq!(policy["Statement"].as_array().unwrap().len(), 4);
    let source_statements: Vec<_> = policy["Statement"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|statement| statement["Resource"] == source)
        .collect();
    assert_eq!(
        source_statements.len(),
        1,
        "copy must grant the source object without widening the destination prefix"
    );
    assert_eq!(
        source_statements[0]["Action"],
        serde_json::json!(["s3:GetObject"])
    );
    assert_eq!(
        policy["Statement"][3]["Condition"],
        serde_json::json!({
            "StringEquals": {"s3:prefix": source.strip_prefix("arn:aws:s3:::space-snapshots/").unwrap()}
        })
    );
    assert_eq!(policy["Statement"][0]["Resource"], destination);
    assert_eq!(
        policy["Statement"][0]["Action"],
        serde_json::json!(["s3:GetObject", "s3:PutObject", "s3:AbortMultipartUpload"])
    );
}

fn node_identity(name: &str, uid: &str) -> SandboxNodeIdentity {
    SandboxNodeIdentity {
        name: K8sNodeName::parse(name).unwrap(),
        uid: K8sNodeUid::parse(uid).unwrap(),
    }
}

#[test]
fn required_and_closed_exchange_fields_fail_closed() {
    assert_eq!(STORAGE_SESSION_TTL_SECONDS, 60 * 60);

    let template = TemplateName::parse("seed").unwrap();
    let base = template_request(&template, STORAGE_READ_SCOPE);
    let direct = caller(ClientService::TemplateManager, "worker-a", "worker-a-uid");

    let cases = [
        ("missing grant type", {
            let mut request = base.clone();
            request.grant_type.clear();
            request
        }),
        ("bucket-root resource", {
            let mut request = base.clone();
            request.resource = "arn:aws:s3:::space-snapshots".into();
            request
        }),
        ("missing audience", {
            let mut request = base.clone();
            request.audience = None;
            request
        }),
        ("wrong audience", {
            let mut request = base.clone();
            request.audience = Some("sts.amazonaws.com".into());
            request
        }),
        ("empty scope", {
            let mut request = base.clone();
            request.scope = Some(String::new());
            request
        }),
        ("unsupported scope", {
            let mut request = base.clone();
            request.scope = Some("space.storage.admin".into());
            request
        }),
        ("duplicated scope", {
            let mut request = base.clone();
            request.scope = Some(format!("{STORAGE_READ_SCOPE} {STORAGE_READ_SCOPE}"));
            request
        }),
        ("missing requested token type", {
            let mut request = base.clone();
            request.requested_token_type.clear();
            request
        }),
        ("missing subject", {
            let mut request = base.clone();
            request.subject_token = None;
            request
        }),
        ("missing subject type", {
            let mut request = base.clone();
            request.subject_token_type = None;
            request
        }),
        ("missing ttl", {
            let mut request = base.clone();
            request.requested_expires_in = None;
            request
        }),
        ("short ttl", {
            let mut request = base.clone();
            request.requested_expires_in = Some(STORAGE_SESSION_TTL_SECONDS - 1);
            request
        }),
        ("overlong ttl", {
            let mut request = base.clone();
            request.requested_expires_in = Some(STORAGE_SESSION_TTL_SECONDS + 1);
            request
        }),
    ];
    for (name, request) in cases {
        assert!(parse_request(&direct, request).is_err(), "{name}");
    }
}

#[tokio::test]
async fn template_publication_is_builder_only() {
    let template = TemplateName::parse("seed").unwrap();
    let org = OrgId::new();
    let role = "arn:aws:iam::123456789012:role/platform-templates";
    let store = TestStore {
        templates: HashMap::from([(template.clone(), Some(org))]),
        ..Default::default()
    };
    let write = template_request(&template, STORAGE_WRITE_SCOPE);

    for service in [ClientService::TemplateManager, ClientService::VolumeManager] {
        let error = authorize_exchange(
            &store,
            &caller(service, "worker-a", "worker-a-uid"),
            write.clone(),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, StorageExchangeError::UnauthorizedActor));
    }

    let authorized = authorize_exchange(
        &store,
        &caller(ClientService::BuildManager, "builder", "builder-uid"),
        write,
    )
    .await
    .expect("build-manager may publish the named template");
    assert_eq!(authorized.organization_id, Some(org));
    assert_eq!(
        authorized
            .vend_role(Some(&VendRoleArn::parse(role.to_string()).unwrap()))
            .unwrap()
            .as_str(),
        role
    );
}

/// A registered release is immutable: the builder identity may publish a
/// template once per hash and never rewrite an existing release prefix, no
/// matter which template it names. Reads of the published release and the
/// first publication of a new hash are unaffected.
#[tokio::test]
async fn builder_cannot_rewrite_a_published_release() {
    let template = TemplateName::parse("seed").unwrap();
    let published_hash: ContentHash = format!("sha256:{}", "ab".repeat(32)).parse().unwrap();
    let store = TestStore {
        templates: HashMap::from([(template.clone(), None)]),
        published: vec![(template.clone(), published_hash)],
        ..Default::default()
    };
    let builder = caller(ClientService::BuildManager, "builder", "builder-uid");

    let error = authorize_exchange(
        &store,
        &builder,
        template_request(&template, STORAGE_WRITE_SCOPE),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        error,
        StorageExchangeError::ReleaseAlreadyPublished
    ));

    let mut object = template_request(&template, STORAGE_WRITE_SCOPE);
    object.resource =
        format!("arn:aws:s3:::space-snapshots/templates/{template}/{published_hash}/rootfs.ext4");
    let error = authorize_exchange(&store, &builder, object)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        StorageExchangeError::ReleaseAlreadyPublished
    ));

    let mut unpublished = template_request(&template, STORAGE_WRITE_SCOPE);
    unpublished.resource = format!(
        "arn:aws:s3:::space-snapshots/templates/{template}/sha256:{}/*",
        "cd".repeat(32)
    );
    authorize_exchange(&store, &builder, unpublished)
        .await
        .expect("first publication of a new hash is allowed");

    authorize_exchange(
        &store,
        &caller(ClientService::TemplateManager, "worker-a", "worker-a-uid"),
        template_request(&template, STORAGE_READ_SCOPE),
    )
    .await
    .expect("published releases stay readable");
}

#[tokio::test]
async fn sandbox_exchange_binds_tenant_node_resource_and_org_role() {
    let sandbox = SandboxId::new();
    let other_sandbox = SandboxId::new();
    let org = OrgId::new();
    let role = "arn:aws:iam::123456789012:role/tenant-storage";
    let store = TestStore {
        sandboxes: HashMap::from([(sandbox, sandbox_context(org))]),
        identities: HashMap::from([(
            org,
            StorageVendIdentity {
                provider: StorageVendProvider::AwsSts,
                identity: role.to_string(),
            },
        )]),
        ..Default::default()
    };
    let volume = caller(ClientService::VolumeManager, "worker-a", "worker-a-uid");

    let authorized = authorize_exchange(&store, &volume, sandbox_request(sandbox))
        .await
        .unwrap();
    assert_eq!(authorized.organization_id, Some(org));
    assert_eq!(authorized.vend_role(None).unwrap().as_str(), role);

    let wrong_node = caller(
        ClientService::VolumeManager,
        "worker-a",
        "replacement-worker-uid",
    );
    assert!(matches!(
        authorize_exchange(&store, &wrong_node, sandbox_request(sandbox))
            .await
            .unwrap_err(),
        StorageExchangeError::WrongNode
    ));

    let mut cross_tenant = sandbox_request(sandbox);
    cross_tenant.resource = sandbox_request(other_sandbox).resource.clone();
    assert!(matches!(
        authorize_exchange(&store, &volume, cross_tenant)
            .await
            .unwrap_err(),
        StorageExchangeError::UnauthorizedResource
    ));

    let mut wrong_tenant_bucket = sandbox_request(sandbox);
    wrong_tenant_bucket.resource =
        wrong_tenant_bucket
            .resource
            .replacen("space-snapshots", "another-tenant", 1);
    assert!(matches!(
        authorize_exchange(&store, &volume, wrong_tenant_bucket)
            .await
            .unwrap_err(),
        StorageExchangeError::UnauthorizedResource
    ));
}

#[tokio::test]
async fn durable_snapshot_reads_are_portable_but_writes_remain_node_fenced() {
    let sandbox = SandboxId::new();
    let org = OrgId::new();
    let store = TestStore {
        sandboxes: HashMap::from([(sandbox, sandbox_context(org))]),
        ..Default::default()
    };
    let mut read = sandbox_request(sandbox);
    read.scope = Some(STORAGE_READ_SCOPE.to_string());
    read.resource = read.resource.trim_end_matches('*').to_string() + "metadata.json";
    let other_node = caller(ClientService::VolumeManager, "worker-b", "worker-b-uid");

    authorize_exchange(&store, &other_node, read)
        .await
        .expect("an authenticated restore worker may read an exact durable snapshot object");
    assert!(matches!(
        authorize_exchange(&store, &other_node, sandbox_request(sandbox))
            .await
            .unwrap_err(),
        StorageExchangeError::WrongNode
    ));
}

#[tokio::test]
async fn fork_artifact_layout_is_bound_to_the_source_subject() {
    let sandbox = SandboxId::new();
    let org = OrgId::new();
    let source_snapshot = uuid::Uuid::now_v7();
    let fork_snapshot = uuid::Uuid::now_v7();
    let sandbox_text = sandbox.to_string();
    let fork_prefix = format!(
        "tenant/suspend_snapshots/{:02x}/{sandbox}/{source_snapshot}/fork/{sandbox}/{fork_snapshot}",
        snapshot_shard(&sandbox_text)
    );
    let store = TestStore {
        sandboxes: HashMap::from([(
            sandbox,
            SandboxStorageContext {
                portable_snapshot_write: Some(
                    StorageUri::parse(&format!(
                        "s3://space-snapshots/{fork_prefix}?region=us-east-1"
                    ))
                    .unwrap(),
                ),
                ..sandbox_context(org)
            },
        )]),
        ..Default::default()
    };
    let mut request = sandbox_request(sandbox);
    request.resource = format!("arn:aws:s3:::space-snapshots/{fork_prefix}/*");

    authorize_exchange(
        &store,
        &caller(ClientService::VolumeManager, "worker-b", "worker-b-uid"),
        request.clone(),
    )
    .await
    .expect("a prepared fork snapshot may be copied by an assigned worker");

    let mut unrecorded_destination = request.clone();
    unrecorded_destination.resource = unrecorded_destination.resource.replace(
        &source_snapshot.to_string(),
        &uuid::Uuid::now_v7().to_string(),
    );
    assert!(matches!(
        authorize_exchange(
            &store,
            &caller(ClientService::VolumeManager, "worker-b", "worker-b-uid"),
            unrecorded_destination,
        )
        .await
        .unwrap_err(),
        StorageExchangeError::WrongNode
    ));

    request.resource = request.resource.replace(
        &format!("/fork/{sandbox}/"),
        &format!("/fork/{}/", SandboxId::new()),
    );
    assert!(matches!(
        authorize_exchange(
            &store,
            &caller(ClientService::VolumeManager, "worker-a", "worker-a-uid"),
            request,
        )
        .await
        .unwrap_err(),
        StorageExchangeError::UnauthorizedResource
    ));
}

#[tokio::test]
async fn legacy_uncompressed_template_artifact_remains_readable() {
    let template = TemplateName::parse("seed").unwrap();
    let store = TestStore {
        templates: HashMap::from([(template.clone(), None)]),
        ..Default::default()
    };
    let mut request = template_request(&template, STORAGE_READ_SCOPE);
    request.resource = request.resource.trim_end_matches('*').to_string() + "rootfs.ext4";

    authorize_exchange(
        &store,
        &caller(ClientService::TemplateManager, "worker-a", "worker-a-uid"),
        request,
    )
    .await
    .expect("legacy manifests may name rootfs.ext4 directly");
}

#[test]
fn actor_claims_are_accepted_only_from_the_node_agent() {
    let template = TemplateName::parse("seed").unwrap();
    let mut request = template_request(&template, STORAGE_READ_SCOPE);
    request.actor_token = Some(ClientService::TemplateManager.to_string());
    request.actor_token_type = Some(SERVICE_ACTOR_TOKEN_TYPE.to_string());

    for (uid, expected_actor) in [
        (users::SPACE_VOLMGR.uid, ClientService::VolumeManager),
        (users::SPACE_TMPLMGR.uid, ClientService::TemplateManager),
    ] {
        let peer =
            ResolvedCaller::UdsPeer(pplx_space_common_svc::PeerIdentity::new(uid, uid, None));
        assert_eq!(
            parse_request(&peer, template_request(&template, STORAGE_READ_SCOPE))
                .unwrap()
                .actor,
            expected_actor,
        );
        assert!(matches!(
            parse_request(&peer, request.clone()).unwrap_err(),
            StorageExchangeError::ActorSpoofing
        ));
    }

    for uid in [0, users::SPACE_SBXMGR.uid] {
        let peer =
            ResolvedCaller::UdsPeer(pplx_space_common_svc::PeerIdentity::new(uid, uid, None));
        assert!(matches!(
            parse_request(&peer, template_request(&template, STORAGE_READ_SCOPE)).unwrap_err(),
            StorageExchangeError::UnauthorizedActor
        ));
    }

    let direct = caller(ClientService::BuildManager, "builder", "builder-uid");
    assert!(matches!(
        parse_request(&direct, request.clone()).unwrap_err(),
        StorageExchangeError::ActorSpoofing
    ));

    let agent = caller(
        ClientService::NodeCredentialAgent,
        "worker-a",
        "worker-a-uid",
    );
    assert_eq!(
        parse_request(&agent, request).unwrap().actor,
        ClientService::TemplateManager
    );

    let mut builder_claim = template_request(&template, STORAGE_WRITE_SCOPE);
    builder_claim.actor_token = Some(ClientService::BuildManager.to_string());
    builder_claim.actor_token_type = Some(SERVICE_ACTOR_TOKEN_TYPE.to_string());
    assert!(matches!(
        parse_request(&agent, builder_claim).unwrap_err(),
        StorageExchangeError::UnauthorizedActor
    ));

    let missing_actor = template_request(&template, STORAGE_READ_SCOPE);
    assert!(matches!(
        parse_request(&agent, missing_actor).unwrap_err(),
        StorageExchangeError::Missing("actor_token_type")
    ));
}

/// A denial is audited with what the request asked for, so each part parses on
/// its own: a bad part (or a request `parse_request` refuses early) leaves the
/// others intact, and an actor is recorded only as `effective_actor` resolves
/// it, never as the caller claims it.
#[test]
fn requested_exchange_parses_each_part_independently() {
    let sandbox = SandboxId::new();
    let base = sandbox_request(sandbox);
    let key = base
        .resource
        .strip_prefix("arn:aws:s3:::space-snapshots/")
        .and_then(|resource| resource.strip_suffix("/*"))
        .unwrap()
        .to_string();
    let claiming = |actor: ClientService| {
        let mut request = base.clone();
        request.actor_token = Some(actor.to_string());
        request.actor_token_type = Some(SERVICE_ACTOR_TOKEN_TYPE.to_string());
        request
    };
    let agent = caller(
        ClientService::NodeCredentialAgent,
        "worker-a",
        "worker-a-uid",
    );
    let uds =
        |uid| ResolvedCaller::UdsPeer(pplx_space_common_svc::PeerIdentity::new(uid, uid, None));

    let cases = [
        (
            "agent forwarding a volume-manager claim",
            agent.clone(),
            claiming(ClientService::VolumeManager),
            Some(ClientService::VolumeManager),
            true,
        ),
        (
            "agent forwarding a disallowed actor claim",
            agent.clone(),
            claiming(ClientService::BuildManager),
            None,
            true,
        ),
        (
            "build-manager naming an actor claim",
            caller(ClientService::BuildManager, "builder", "builder-uid"),
            claiming(ClientService::VolumeManager),
            None,
            true,
        ),
        (
            "direct volume-manager spoofing an actor claim",
            uds(users::SPACE_VOLMGR.uid),
            claiming(ClientService::TemplateManager),
            None,
            true,
        ),
        (
            "direct volume-manager",
            uds(users::SPACE_VOLMGR.uid),
            base.clone(),
            Some(ClientService::VolumeManager),
            true,
        ),
        (
            "UDS uid that is no storage actor",
            uds(users::SPACE_SBXMGR.uid),
            base.clone(),
            None,
            true,
        ),
        (
            "unsupported audience",
            agent.clone(),
            {
                let mut request = claiming(ClientService::VolumeManager);
                request.audience = Some("sts.amazonaws.com".into());
                request
            },
            Some(ClientService::VolumeManager),
            true,
        ),
        (
            "malformed resource",
            agent.clone(),
            {
                let mut request = claiming(ClientService::VolumeManager);
                request.resource = format!("arn:aws:s3:::space-snapshots/{key}/../*");
                request
            },
            Some(ClientService::VolumeManager),
            false,
        ),
    ];

    for (name, caller, request, actor, resource_parses) in cases {
        let requested = RequestedExchange::parse(&caller, &request);
        assert_eq!(requested.actor, actor, "{name}");
        assert_eq!(
            requested.subject,
            Some(StorageSubject::Resource(SubjectId::Sandbox(sandbox))),
            "{name}"
        );
        assert_eq!(
            requested
                .resource
                .as_ref()
                .map(|resource| (resource.bucket(), resource.key())),
            resource_parses.then_some(("space-snapshots", key.as_str())),
            "{name}"
        );
        assert_eq!(
            requested.actions.map(StorageActions::effective_scope),
            Some(STORAGE_WRITE_SCOPE.to_string()),
            "{name}"
        );
    }
}

#[test]
fn session_policy_is_exact_and_enforces_the_sts_size_limit() {
    let template = TemplateName::parse("seed").unwrap();
    let parsed = parse_request(
        &caller(ClientService::BuildManager, "builder", "builder-uid"),
        template_request(
            &template,
            &format!("{STORAGE_READ_SCOPE} {STORAGE_WRITE_SCOPE}"),
        ),
    )
    .unwrap();
    let authorized = AuthorizedExchange {
        actor: parsed.actor,
        subject: parsed.subject,
        resource: parsed.resource,
        actions: parsed.actions,
        ttl_seconds: parsed.ttl_seconds,
        organization_id: None,
        role_override: None,
        copy_source: None,
    };
    let policy: serde_json::Value =
        serde_json::from_str(&authorized.session_policy().unwrap()).unwrap();
    assert!(
        authorized.session_policy().unwrap().len() < 700,
        "a normal prefix policy must leave substantial headroom below STS's 2048-byte limit"
    );
    assert_eq!(
        policy,
        serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": ["s3:GetObject", "s3:PutObject", "s3:AbortMultipartUpload"],
                    "Resource": authorized.resource.policy_arn(),
                },
                {
                    "Effect": "Allow",
                    "Action": "s3:ListBucket",
                    "Resource": "arn:aws:s3:::space-snapshots",
                    "Condition": {
                        "StringLike": {
                            "s3:prefix": [
                                authorized.resource.key(),
                                format!("{}/*", authorized.resource.key()),
                            ],
                        },
                    },
                },
            ],
        })
    );

    let oversized = AuthorizedExchange {
        resource: S3Resource {
            bucket: "space-snapshots".into(),
            key: format!("{}/object", "a".repeat(1_890)),
            prefix: false,
        },
        ..authorized
    };
    assert!(matches!(
        oversized.session_policy().unwrap_err(),
        StorageExchangeError::PolicyTooLarge
    ));
}

#[test]
fn ambient_or_static_credentials_are_never_a_vend_result() {
    let template = TemplateName::parse("seed").unwrap();
    let parsed = parse_request(
        &caller(ClientService::BuildManager, "builder", "builder-uid"),
        template_request(&template, STORAGE_READ_SCOPE),
    )
    .unwrap();
    let authorized = AuthorizedExchange {
        actor: parsed.actor,
        subject: parsed.subject,
        resource: parsed.resource,
        actions: parsed.actions,
        ttl_seconds: parsed.ttl_seconds,
        organization_id: None,
        role_override: None,
        copy_source: None,
    };
    assert!(matches!(
        authorized.vend_role(None).unwrap_err(),
        StorageExchangeError::MissingVendRole
    ));
}

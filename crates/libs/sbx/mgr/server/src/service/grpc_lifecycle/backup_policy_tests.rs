//! Characterization of the artifact-backup admission, network-policy, and
//! `Kill` handler contracts that are reachable without host runtime state.
//!
//! Fixtures fail closed: no passing case reaches node-manager, vol-mgr, the
//! template-manager socket, or a real cluster-api endpoint. The one exception
//! under regression is `Kill`: if the restart drain stops refusing at cleanup
//! admission, a fatal signal to a started or paused row attempts its
//! node-manager dispatch before the refusal bound fails the test.

use super::{ArtifactBackupHandoff, ArtifactBackupSubmission, SubmitSuspendSnapshotOutcome};
use crate::db::PolicyAuthority;
use crate::proto::enqueue_artifact_backup_request::Source;
use crate::service::SandboxManagerService;
use crate::service::activation_support::internal_request;
use crate::service::network_policy::EffectiveNetworkPolicy;
use crate::service::tests::service_tests::insert_test_sandbox;
use crate::state::SandboxState;
use pplx_space_common_core::node_drain_signals::{
    REUPLOAD_ARTIFACT_MISSING_MARKER, REUPLOAD_SANDBOX_UNKNOWN_MARKER,
};
use pplx_space_common_core::types::{
    ArtifactKind, ArtifactOrigin, BackupAttemptId, LocalArtifactId, SandboxId, SnapshotName,
};
use pplx_space_net_common::NetworkPolicyV1;
use pplx_space_net_common::proto as policy_proto;
use pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat;
use std::ffi::OsStr;
use std::sync::Arc;

/// Environment of a node wired to a cluster API through the given endpoint and
/// static token, with the token file pointing at a missing test-owned path.
/// Every cluster-api connect therefore fails at the token probe, before it
/// dials the (unused) loopback endpoint.
fn cluster_api_env<'a>(
    endpoint: &'a str,
    token: &'a str,
    token_path: &'a std::path::Path,
) -> Vec<(&'static str, Option<&'a OsStr>)> {
    vec![
        (
            pplx_space_node_api_client::cluster::CLUSTER_API_ENDPOINT_ENV,
            Some(OsStr::new(endpoint)),
        ),
        (
            pplx_space_node_api_client::cluster::CLUSTER_API_ENDPOINT_ENV_LEGACY,
            None,
        ),
        (
            pplx_space_common_svc::bearer_token::SPACE_INTERNAL_TOKEN_ENV,
            Some(OsStr::new(token)),
        ),
        (
            pplx_space_common_svc::bearer_token::SPACE_CLUSTER_API_TOKEN_PATH_ENV,
            Some(token_path.as_os_str()),
        ),
        ("SPACE_SNAPSHOT_BUCKET", None),
        ("SPACE_SNAPSHOT_REGION", None),
    ]
}

const LOOPBACK_ENDPOINT: &str = "http://127.0.0.1:9";
const STATIC_TOKEN: &str = "characterization-token";

fn snapshot_upload_jobs(service: &SandboxManagerService, sandbox_id: &str) -> usize {
    let db = service.db_conn().unwrap();
    pplx_space_common_jobs::db::list_jobs(&db, Some(sandbox_id), None, 0)
        .unwrap()
        .into_iter()
        .filter(|job| job.kind == crate::snapshot_upload_jobs::SNAPSHOT_UPLOAD_JOB_KIND)
        .count()
}

fn artifact_backup_request(
    sandbox_id: &str,
    kind: crate::proto::ArtifactKind,
    source: Option<Source>,
) -> tonic::Request<crate::proto::EnqueueArtifactBackupRequest> {
    internal_request(crate::proto::EnqueueArtifactBackupRequest {
        sandbox_id: sandbox_id.to_string(),
        expected_artifact_kind: kind as i32,
        source,
    })
}

#[tokio::test]
async fn artifact_backup_admission_refuses_sources_it_cannot_back_up() {
    let token_dir = tempfile::tempdir().unwrap();
    let token_path = token_dir.path().join("cluster-api.token");
    if !pplx_space_test_env::enter_test_process(&cluster_api_env(
        LOOPBACK_ENDPOINT,
        STATIC_TOKEN,
        &token_path,
    )) {
        return;
    }
    use crate::proto::ArtifactKind::{Disk, Full, Unspecified};

    enum Fixture {
        /// The requested sandbox has no row.
        Absent,
        /// A stopped sandbox with no published artifacts.
        Bare,
        /// A published disk artifact, owned by this sandbox or another one.
        DiskArtifact { owned: bool },
        /// A named full snapshot (no `d-`/`f-` kind prefix) of this sandbox.
        NamedFull,
        /// A suspended sandbox whose current instance is pinned.
        PinnedSuspend,
    }
    // Sources the handler resolves to a published artifact of the requested
    // kind must prove remote readiness before any enqueue. The cluster-api
    // credential is unreadable, so that proof is unavailable (retryable).
    let cases: [(
        &str,
        Fixture,
        crate::proto::ArtifactKind,
        Option<Source>,
        tonic::Code,
    ); 11] = [
        (
            "blank id",
            Fixture::Bare,
            Disk,
            None,
            tonic::Code::InvalidArgument,
        ),
        (
            "unspecified kind",
            Fixture::Bare,
            Unspecified,
            None,
            tonic::Code::InvalidArgument,
        ),
        (
            "unknown sandbox",
            Fixture::Absent,
            Disk,
            None,
            tonic::Code::NotFound,
        ),
        (
            "unparseable artifact id",
            Fixture::Bare,
            Disk,
            Some(Source::LocalArtifactId("not-an-artifact".into())),
            tonic::Code::InvalidArgument,
        ),
        (
            "unpublished artifact id",
            Fixture::Bare,
            Disk,
            Some(Source::LocalArtifactId(LocalArtifactId::new().to_string())),
            tonic::Code::FailedPrecondition,
        ),
        (
            "another sandbox's artifact",
            Fixture::DiskArtifact { owned: false },
            Disk,
            None,
            tonic::Code::FailedPrecondition,
        ),
        (
            "artifact of the other kind",
            Fixture::DiskArtifact { owned: true },
            Full,
            None,
            tonic::Code::FailedPrecondition,
        ),
        (
            "unparseable snapshot name",
            Fixture::Bare,
            Disk,
            Some(Source::SnapshotName("nested/name".into())),
            tonic::Code::InvalidArgument,
        ),
        (
            "named artifact of the other kind",
            Fixture::NamedFull,
            Disk,
            Some(Source::SnapshotName("base".into())),
            tonic::Code::FailedPrecondition,
        ),
        (
            "named artifact awaiting readiness proof",
            Fixture::NamedFull,
            Full,
            Some(Source::SnapshotName("base".into())),
            tonic::Code::Unavailable,
        ),
        (
            "pinned current instance awaiting readiness proof",
            Fixture::PinnedSuspend,
            Full,
            Some(Source::CurrentInstance(
                crate::proto::CurrentInstanceSource {},
            )),
            tonic::Code::Unavailable,
        ),
    ];
    for (case, fixture, kind, source, expected) in cases {
        let conn = crate::db::test_conn();
        let id = SandboxId::new().to_string();
        let artifact_dir = tempfile::tempdir().unwrap();
        let artifact_path = artifact_dir.path().to_str().unwrap();
        let mut source = source;
        match fixture {
            Fixture::Absent => {}
            Fixture::Bare => insert_test_sandbox(&conn, &id, SandboxState::Stopped),
            Fixture::DiskArtifact { owned } => {
                insert_test_sandbox(&conn, &id, SandboxState::Stopped);
                let other = SandboxId::new().to_string();
                insert_test_sandbox(&conn, &other, SandboxState::Stopped);
                let artifact_id = LocalArtifactId::new();
                crate::db::publish_named_local_artifact(
                    &conn,
                    if owned { &id } else { &other },
                    artifact_id,
                    ArtifactKind::Disk,
                    &SnapshotName::parse("d-disk").unwrap(),
                    artifact_path,
                )
                .unwrap();
                source = Some(Source::LocalArtifactId(artifact_id.to_string()));
            }
            Fixture::NamedFull => {
                insert_test_sandbox(&conn, &id, SandboxState::Stopped);
                crate::db::publish_named_local_artifact(
                    &conn,
                    &id,
                    LocalArtifactId::new(),
                    ArtifactKind::Full,
                    &SnapshotName::parse("base").unwrap(),
                    artifact_path,
                )
                .unwrap();
            }
            Fixture::PinnedSuspend => {
                insert_test_sandbox(&conn, &id, SandboxState::Suspended);
                assert!(
                    crate::db::adopt_suspended_local_artifact(
                        &conn,
                        &id,
                        LocalArtifactId::new(),
                        artifact_path,
                    )
                    .unwrap()
                );
            }
        }
        let service = SandboxManagerService::for_test(conn);
        let requested_id = if case == "blank id" { "  " } else { &id };

        let error = service
            .enqueue_artifact_backup(artifact_backup_request(requested_id, kind, source))
            .await
            .expect_err(case);

        assert_eq!(error.code(), expected, "{case}: {error}");
        // The drain promoter treats these markers as terminal; none of these
        // refusals may carry them.
        for marker in [
            REUPLOAD_SANDBOX_UNKNOWN_MARKER,
            REUPLOAD_ARTIFACT_MISSING_MARKER,
        ] {
            assert!(!error.message().contains(marker), "{case}: {error}");
        }
    }
}

/// Empty cluster-api wiring is retryable, operator-fixable configuration. It
/// is refused before sandbox lookup, so the drain promoter finds no terminal
/// marker in the backup refusal. Eviction's reading of the matching reupload
/// refusal is pinned against its classifier in `crate::sandbox_eviction`.
async fn assert_refused_as_missing_wiring() {
    let service = SandboxManagerService::for_test(crate::db::test_conn());
    let unknown = SandboxId::new().to_string();
    let enqueue = service
        .enqueue_artifact_backup(artifact_backup_request(
            &unknown,
            crate::proto::ArtifactKind::Disk,
            None,
        ))
        .await
        .unwrap_err();
    assert_eq!(enqueue.code(), tonic::Code::FailedPrecondition, "{enqueue}");
    assert!(
        !enqueue.message().contains(REUPLOAD_SANDBOX_UNKNOWN_MARKER),
        "{enqueue}"
    );
}

#[tokio::test]
async fn backup_admission_treats_an_empty_cluster_api_token_as_missing() {
    let token_dir = tempfile::tempdir().unwrap();
    let token_path = token_dir.path().join("cluster-api.token");
    if !pplx_space_test_env::enter_test_process(&cluster_api_env(
        LOOPBACK_ENDPOINT,
        "",
        &token_path,
    )) {
        return;
    }
    assert_refused_as_missing_wiring().await;
}

#[tokio::test]
async fn backup_admission_treats_an_empty_cluster_api_endpoint_as_missing() {
    let token_dir = tempfile::tempdir().unwrap();
    let token_path = token_dir.path().join("cluster-api.token");
    if !pplx_space_test_env::enter_test_process(&cluster_api_env("", STATIC_TOKEN, &token_path)) {
        return;
    }
    assert_refused_as_missing_wiring().await;
}

fn upload_config(bucket: &str) -> crate::snapshot_upload::SnapshotUploadConfig {
    crate::snapshot_upload::SnapshotUploadConfig::from_uri(&format!(
        "s3://{bucket}/prefix?region=us-east-1"
    ))
    .unwrap()
}

/// Write a published artifact directory whose metadata names no artifact
/// identity, so any identity in the durable intent came from the submission.
fn write_artifact(dir: &std::path::Path, sandbox_id: &str, files: &[&str]) {
    let execution = pplx_space_common_core::types::SnapshotExecutionMetadata::new(
        pplx_space_common_core::types::ExecutionMetadata::initial_firecracker(
            std::env::consts::ARCH.parse().unwrap(),
            vec![],
        ),
    )
    .unwrap();
    std::fs::create_dir_all(dir).unwrap();
    for file in files {
        std::fs::write(dir.join(file), b"artifact").unwrap();
    }
    std::fs::write(
        dir.join(crate::snapshot_upload::SNAPSHOT_METADATA_FILENAME),
        serde_json::to_vec(&serde_json::json!({
            "version": crate::snapshot_upload::SNAPSHOT_METADATA_VERSION,
            "execution": execution.execution(),
            "sandbox_id": sandbox_id,
            "size_bytes": 0,
            "created_at": "2026-10-10T00:00:00Z",
            "source": {"runtime": "fc", "template_id": "seed"},
        }))
        .unwrap(),
    )
    .unwrap();
}

#[tokio::test]
async fn durable_artifact_backup_persists_the_submitted_artifact_identity() {
    // A regressed handoff that began the backup remotely would stop at the
    // unreadable cluster-api token instead of using ambient host wiring.
    let token_dir = tempfile::tempdir().unwrap();
    let token_path = token_dir.path().join("cluster-api.token");
    if !pplx_space_test_env::enter_test_process(&cluster_api_env(
        LOOPBACK_ENDPOINT,
        STATIC_TOKEN,
        &token_path,
    )) {
        return;
    }
    const FULL: &[&str] = &["vmstate", "memory", "rootfs.ext4"];
    struct Case {
        name: &'static str,
        kind: ArtifactKind,
        origin: ArtifactOrigin,
        snapshot_name: Option<&'static str>,
        files: &'static [&'static str],
        metadata: Option<&'static [u8]>,
        request_bucket: Option<&'static str>,
        default_bucket: Option<&'static str>,
        /// Bucket of the persisted intent; `None` expects no intent at all.
        persisted_bucket: Option<&'static str>,
    }
    let base = Case {
        name: "",
        kind: ArtifactKind::Full,
        origin: ArtifactOrigin::Suspend,
        snapshot_name: None,
        files: FULL,
        metadata: None,
        request_bucket: Some("request-bucket"),
        default_bucket: Some("default-bucket"),
        persisted_bucket: Some("request-bucket"),
    };
    let cases = [
        Case {
            name: "request override wins over the node default",
            ..base
        },
        Case {
            name: "named disk artifact on the node default",
            kind: ArtifactKind::Disk,
            origin: ArtifactOrigin::Snapshot,
            snapshot_name: Some("d-disk"),
            files: &["rootfs.ext4"],
            request_bucket: None,
            persisted_bucket: Some("default-bucket"),
            ..base
        },
        Case {
            name: "no destination configured",
            request_bucket: None,
            default_bucket: None,
            persisted_bucket: None,
            ..base
        },
        Case {
            name: "missing captured file",
            files: &["vmstate", "memory"],
            persisted_bucket: None,
            ..base
        },
        Case {
            name: "unparseable published metadata",
            metadata: Some(b"{truncated"),
            persisted_bucket: None,
            ..base
        },
    ];
    for case in cases {
        let conn = crate::db::test_conn();
        let id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &id, SandboxState::Suspended);
        let mut service = SandboxManagerService::for_test(conn);
        service.snapshot_upload_config = case.default_bucket.map(|b| Arc::new(upload_config(b)));
        let sandbox = service.lookup_sandbox(&id).unwrap();
        let root = tempfile::tempdir().unwrap();
        let snapshot_dir = root.path().join("artifact");
        write_artifact(&snapshot_dir, &id, case.files);
        if let Some(metadata) = case.metadata {
            std::fs::write(
                snapshot_dir.join(crate::snapshot_upload::SNAPSHOT_METADATA_FILENAME),
                metadata,
            )
            .unwrap();
        }
        let request_override = case.request_bucket.map(upload_config);
        let local_artifact_id = LocalArtifactId::new();
        let backup_attempt_id = BackupAttemptId::new();
        let dedup_key = local_artifact_id.to_string();

        let outcome = service
            .submit_artifact_backup(
                &sandbox,
                ArtifactBackupSubmission {
                    local_artifact_id,
                    artifact_kind: case.kind,
                    artifact_origin: case.origin,
                    snapshot_name: case.snapshot_name,
                    backup_attempt_id,
                    snapshot_dir: snapshot_dir.to_str().unwrap(),
                    request_override: request_override.as_ref(),
                    format: SuspendSnapshotFormat::Full,
                    dedup_key: Some(&dedup_key),
                    handoff: ArtifactBackupHandoff::DurableWorker,
                    execution_generation: None,
                },
            )
            .await;

        let persisted = service
            .snapshot_upload_worker()
            .find_job_by_dedup_key(
                crate::snapshot_upload_jobs::SNAPSHOT_UPLOAD_JOB_KIND,
                &dedup_key,
            )
            .unwrap();
        let Some(bucket) = case.persisted_bucket else {
            if case.default_bucket.is_none() && case.request_bucket.is_none() {
                assert!(
                    matches!(outcome, Ok(SubmitSuspendSnapshotOutcome::NoUploadConfig)),
                    "{}: {outcome:?}",
                    case.name
                );
            } else {
                assert!(outcome.is_err(), "{}: {outcome:?}", case.name);
            }
            assert!(persisted.is_none(), "{}", case.name);
            assert_eq!(snapshot_upload_jobs(&service, &id), 0, "{}", case.name);
            continue;
        };
        let Ok(SubmitSuspendSnapshotOutcome::Enqueued {
            job_id,
            backup_attempt_id: returned_attempt,
        }) = outcome
        else {
            panic!("{}: {outcome:?}", case.name);
        };
        let job = persisted.expect(case.name);
        assert_eq!(job.job_id, job_id, "{}", case.name);
        assert_eq!(
            job.subject_id.as_deref(),
            Some(id.as_str()),
            "{}",
            case.name
        );
        assert_eq!(
            returned_attempt,
            backup_attempt_id.to_string(),
            "{}",
            case.name
        );
        let payload = job
            .payload_as::<crate::snapshot_upload::SnapshotUploadJobs>()
            .unwrap();
        assert_eq!(
            payload.snapshot_id,
            backup_attempt_id.to_string(),
            "{}",
            case.name
        );
        assert_eq!(payload.config.bucket, bucket, "{}", case.name);
        assert!(
            payload.storage_uri.starts_with(&format!("s3://{bucket}/")),
            "{}: {}",
            case.name,
            payload.storage_uri
        );
        assert!(
            payload.job_ids.is_empty(),
            "{}: the durable worker owns every remote step",
            case.name
        );
        assert_eq!(
            payload.meta.local_artifact_id,
            Some(local_artifact_id),
            "{}",
            case.name
        );
        assert_eq!(payload.meta.artifact_kind, Some(case.kind), "{}", case.name);
        assert_eq!(
            payload.meta.artifact_origin,
            Some(case.origin),
            "{}",
            case.name
        );
        assert_eq!(
            payload
                .meta
                .artifact_snapshot_name
                .as_ref()
                .map(SnapshotName::as_str),
            case.snapshot_name,
            "{}",
            case.name
        );
        assert!(payload.meta.execution.is_some(), "{}", case.name);
    }
}

#[derive(Clone, Copy, Debug)]
enum Egress {
    Allow,
    Deny,
    /// Default deny with one allow rule, one deny rule, and one credential
    /// rule (which requires TLS interception).
    Ruled,
}

const RULED_ALLOW_DOMAIN: &str = "api.example.com";
const RULED_DENY_CIDR: &str = "203.0.113.0/24";
const RULED_CREDENTIAL_RULE: &str = "example-api";
const RULED_CREDENTIAL_PORT: u16 = 8443;
const RULED_CREDENTIAL_PATH: &str = "/v1";
const RULED_CREDENTIAL_HEADER: &str = "authorization";
const RULED_CREDENTIAL_REFERENCE: &str = "01920000-0000-7000-8000-000000000001";

/// The wire form of each policy, written out as the expected value.
fn wire_policy(egress: Egress) -> policy_proto::NetworkPolicyV1 {
    use policy_proto::egress_destination_v1::Destination;
    let version = policy_proto::NetworkPolicyVersion::V1 as i32;
    let rule_less = |action: policy_proto::EgressDefaultAction| policy_proto::NetworkPolicyV1 {
        version,
        egress_default_action: action as i32,
        ..Default::default()
    };
    match egress {
        Egress::Allow => rule_less(policy_proto::EgressDefaultAction::Allow),
        Egress::Deny => rule_less(policy_proto::EgressDefaultAction::Deny),
        Egress::Ruled => policy_proto::NetworkPolicyV1 {
            version,
            egress_default_action: policy_proto::EgressDefaultAction::Deny as i32,
            allow: vec![policy_proto::EgressDestinationV1 {
                destination: Some(Destination::Domain(RULED_ALLOW_DOMAIN.to_string())),
            }],
            deny: vec![policy_proto::EgressDestinationV1 {
                destination: Some(Destination::Cidr(RULED_DENY_CIDR.to_string())),
            }],
            tls_interception: policy_proto::TlsInterception::On as i32,
            network_credential_rules: vec![policy_proto::NetworkCredentialRuleV1 {
                rule_id: RULED_CREDENTIAL_RULE.to_string(),
                destination: Some(policy_proto::NetworkCredentialDestinationV1 {
                    exact_host: RULED_ALLOW_DOMAIN.to_string(),
                    port: Some(u32::from(RULED_CREDENTIAL_PORT)),
                    path_prefix: Some(RULED_CREDENTIAL_PATH.to_string()),
                }),
                credential_header_bindings: vec![policy_proto::CredentialHeaderBindingV1 {
                    header_name: RULED_CREDENTIAL_HEADER.to_string(),
                    credential_reference_id: RULED_CREDENTIAL_REFERENCE.to_string(),
                }],
                supported_http_methods: vec![
                    policy_proto::NetworkCredentialHttpMethodV1::Get as i32,
                ],
            }],
            tls_interception_exemptions: Vec::new(),
        },
    }
}

/// The desired-row form of each policy, written out as the expected value.
fn stored_policy(egress: Egress) -> NetworkPolicyV1 {
    use pplx_space_net_common::{
        CredentialHeaderBindingV1, EgressDefaultAction, EgressDestinationV1, EgressPolicyV1,
        NetworkCredentialDestinationV1, NetworkCredentialHttpMethodV1, NetworkCredentialRuleV1,
        NetworkPolicyVersion, TlsInterception,
    };
    match egress {
        Egress::Allow => NetworkPolicyV1::allow_all(),
        Egress::Deny => NetworkPolicyV1::deny_all(),
        Egress::Ruled => NetworkPolicyV1 {
            version: NetworkPolicyVersion::V1,
            egress: EgressPolicyV1 {
                default_action: EgressDefaultAction::Deny,
                allow: vec![EgressDestinationV1::Domain(
                    RULED_ALLOW_DOMAIN.parse().unwrap(),
                )],
                deny: vec![EgressDestinationV1::Cidr(RULED_DENY_CIDR.parse().unwrap())],
            },
            tls_interception: TlsInterception::On,
            network_credential_rules: vec![NetworkCredentialRuleV1 {
                rule_id: RULED_CREDENTIAL_RULE.to_string(),
                destination: NetworkCredentialDestinationV1 {
                    exact_host: RULED_ALLOW_DOMAIN.parse().unwrap(),
                    port: RULED_CREDENTIAL_PORT,
                    path_prefix: Some(RULED_CREDENTIAL_PATH.to_string()),
                },
                credential_header_bindings: vec![CredentialHeaderBindingV1 {
                    header_name: RULED_CREDENTIAL_HEADER.to_string(),
                    credential_reference_id: RULED_CREDENTIAL_REFERENCE.parse().unwrap(),
                }],
                supported_http_methods: vec![NetworkCredentialHttpMethodV1::Get],
            }],
            tls_interception_exemptions: Vec::new(),
        },
    }
}

fn wire_authority(authority: PolicyAuthority) -> i32 {
    match authority {
        PolicyAuthority::Cluster => policy_proto::PolicyAuthority::Cluster as i32,
        PolicyAuthority::Local => policy_proto::PolicyAuthority::Local as i32,
    }
}

/// Seed the `seed` template's owner policy, so inherited lookups never fetch
/// from template-manager. Callers pause the clock so the entry cannot reach
/// its refresh deadline mid-test.
async fn service_with_template_policy(
    conn: rusqlite::Connection,
    egress: Egress,
    generation: u64,
) -> SandboxManagerService {
    let service = SandboxManagerService::for_test(conn);
    let policy = EffectiveNetworkPolicy::new(
        stored_policy(egress),
        generation,
        PolicyAuthority::Cluster,
        true,
    );
    service
        .template_network_policies
        .get_or_fetch_with("seed", move |_| async move { Ok(policy) })
        .await
        .unwrap();
    service
}

async fn drain_service_tasks(service: &SandboxManagerService) {
    service.shutdown_token.cancel();
    service.task_tracker.close();
    service.task_tracker.wait().await;
}

fn set_policy_request(
    sandbox_id: &str,
    policy: Option<Egress>,
    inherit_template: bool,
    policy_generation: u64,
    authority: i32,
) -> tonic::Request<crate::proto::SetNetworkPolicyRequest> {
    internal_request(crate::proto::SetNetworkPolicyRequest {
        sandbox_id: sandbox_id.to_string(),
        policy: policy.map(wire_policy),
        inherit_template,
        policy_generation,
        authority,
        sync_to_gateway: false,
    })
}

#[tokio::test(start_paused = true)]
async fn set_network_policy_reports_the_committed_decision_and_effective_policy() {
    use pplx_space_net_common::proto::NetworkPolicyApplyDecision::{Apply, Skip};
    let cluster = policy_proto::PolicyAuthority::Cluster as i32;
    let local = policy_proto::PolicyAuthority::Local as i32;
    let conn = crate::db::test_conn();
    let id = SandboxId::new().to_string();
    insert_test_sandbox(&conn, &id, SandboxState::Stopped);
    let service = service_with_template_policy(conn, Egress::Deny, 3).await;

    struct Step {
        name: &'static str,
        policy: Option<Egress>,
        inherit_template: bool,
        generation: u64,
        authority: i32,
        decision: pplx_space_net_common::proto::NetworkPolicyApplyDecision,
        /// Effective policy, its generation and authority, and whether it is
        /// inherited from the template; also the desired row afterwards.
        effective: (Egress, u64, PolicyAuthority, bool),
    }
    for step in [
        Step {
            name: "an unversioned override takes the next generation",
            policy: Some(Egress::Deny),
            inherit_template: false,
            generation: 0,
            authority: cluster,
            decision: Apply,
            effective: (Egress::Deny, 1, PolicyAuthority::Cluster, false),
        },
        Step {
            name: "repeating the override without a live gateway changes nothing",
            policy: Some(Egress::Deny),
            inherit_template: false,
            generation: 0,
            authority: cluster,
            decision: Skip,
            effective: (Egress::Deny, 1, PolicyAuthority::Cluster, false),
        },
        Step {
            name: "an override carries its allow, deny, and credential rules",
            policy: Some(Egress::Ruled),
            inherit_template: false,
            generation: 0,
            authority: cluster,
            decision: Apply,
            effective: (Egress::Ruled, 2, PolicyAuthority::Cluster, false),
        },
        Step {
            name: "a local override supersedes the cluster override",
            policy: Some(Egress::Allow),
            inherit_template: false,
            generation: 7,
            authority: local,
            decision: Apply,
            effective: (Egress::Allow, 0, PolicyAuthority::Local, false),
        },
        Step {
            name: "a cluster update cannot displace a local override",
            policy: Some(Egress::Deny),
            inherit_template: false,
            generation: 9,
            authority: cluster,
            decision: Skip,
            effective: (Egress::Allow, 0, PolicyAuthority::Local, false),
        },
        Step {
            name: "clearing the local override restores the template default",
            policy: None,
            inherit_template: true,
            generation: 0,
            authority: local,
            decision: Apply,
            effective: (Egress::Deny, 3, PolicyAuthority::Cluster, true),
        },
    ] {
        let response = service
            .set_network_policy(set_policy_request(
                &id,
                step.policy,
                step.inherit_template,
                step.generation,
                step.authority,
            ))
            .await
            .unwrap_or_else(|error| panic!("{}: {error}", step.name))
            .into_inner();

        let (egress, generation, authority, inherited) = step.effective;
        assert_eq!(
            response.apply_decision, step.decision as i32,
            "{}",
            step.name
        );
        assert_eq!(
            response.effective_policy,
            Some(wire_policy(egress)),
            "{}",
            step.name
        );
        assert_eq!(response.effective_generation, generation, "{}", step.name);
        assert_eq!(
            response.authority,
            wire_authority(authority),
            "{}",
            step.name
        );
        assert_eq!(response.inherited, inherited, "{}", step.name);
        assert_eq!(
            response.source_template,
            if inherited { "seed" } else { "" },
            "{}",
            step.name
        );
        let row = service.lookup_sandbox(&id).unwrap();
        assert_eq!(
            row.network_policy_override,
            (!inherited).then(|| stored_policy(egress)),
            "{}",
            step.name
        );
        assert_eq!(
            row.network_policy_authority,
            if inherited {
                PolicyAuthority::Cluster
            } else {
                authority
            },
            "{}",
            step.name
        );
    }

    // An equal generation naming a different policy is a conflict, refused
    // without changing the desired row.
    service
        .set_network_policy(set_policy_request(
            &id,
            Some(Egress::Deny),
            false,
            4,
            cluster,
        ))
        .await
        .unwrap();
    let conflict = service
        .set_network_policy(set_policy_request(
            &id,
            Some(Egress::Allow),
            false,
            4,
            cluster,
        ))
        .await
        .unwrap_err();
    assert_eq!(conflict.code(), tonic::Code::FailedPrecondition);
    let row = service.lookup_sandbox(&id).unwrap();
    assert_eq!(
        row.network_policy_override,
        Some(NetworkPolicyV1::deny_all())
    );
    assert_eq!(row.network_policy_generation, 4);
    drain_service_tasks(&service).await;
}

#[tokio::test]
async fn set_network_policy_rejects_malformed_requests_without_mutation() {
    let cluster = policy_proto::PolicyAuthority::Cluster as i32;
    let allow = Some(Egress::Allow);
    let conn = crate::db::test_conn();
    let id = SandboxId::new().to_string();
    insert_test_sandbox(&conn, &id, SandboxState::Stopped);
    let service = SandboxManagerService::for_test(conn);
    for (name, request, expected) in [
        (
            "unknown sandbox",
            set_policy_request(&SandboxId::new().to_string(), allow, false, 1, cluster),
            tonic::Code::NotFound,
        ),
        (
            "both a policy and template inheritance",
            set_policy_request(&id, allow, true, 1, cluster),
            tonic::Code::InvalidArgument,
        ),
        (
            "neither a policy nor template inheritance",
            set_policy_request(&id, None, false, 1, cluster),
            tonic::Code::InvalidArgument,
        ),
        (
            "unknown authority",
            set_policy_request(&id, allow, false, 1, 99),
            tonic::Code::InvalidArgument,
        ),
    ] {
        let error = service.set_network_policy(request).await.unwrap_err();
        assert_eq!(error.code(), expected, "{name}: {error}");
        let row = service.lookup_sandbox(&id).unwrap();
        assert!(row.network_policy_override.is_none(), "{name}");
        assert_eq!(row.network_policy_generation, 0, "{name}");
        assert_eq!(
            row.network_policy_authority,
            PolicyAuthority::Cluster,
            "{name}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn get_network_policy_reports_each_sandbox_policy_source() {
    use pplx_space_net_common::proto::NetworkPolicySource;
    let conn = crate::db::test_conn();
    let [overridden, inherited, orphaned] =
        std::array::from_fn::<_, 3, _>(|_| SandboxId::new().to_string());
    for id in [&overridden, &inherited, &orphaned] {
        insert_test_sandbox(&conn, id, SandboxState::Stopped);
    }
    crate::db::set_network_policy_fields(
        &conn,
        &overridden,
        Some(&NetworkPolicyV1::deny_all()),
        0,
        PolicyAuthority::Local,
    )
    .unwrap();
    assert!(
        crate::db::mark_network_policy_applied(
            &conn,
            &inherited,
            crate::db::NetworkPolicyAppliedSource::Template,
            2,
        )
        .unwrap()
    );
    conn.execute(
        "UPDATE sandboxes SET template = ?1 WHERE id = ?2",
        rusqlite::params![
            pplx_space_common_core::types::ORPHANED_SANDBOX_TEMPLATE,
            orphaned
        ],
    )
    .unwrap();
    let service = service_with_template_policy(conn, Egress::Allow, 5).await;

    let response = service
        .get_network_policy(internal_request(crate::proto::GetNetworkPolicyRequest {
            sandbox_ids: vec![
                overridden.clone(),
                format!(" {inherited} "),
                String::new(),
                overridden.clone(),
                orphaned.clone(),
            ],
        }))
        .await
        .unwrap()
        .into_inner();

    let observed = response
        .policies
        .iter()
        .map(|detail| {
            (
                detail.sandbox_id.as_str(),
                detail.effective_policy.clone(),
                detail.inherited,
                detail.source_template.as_str(),
                detail.effective_generation,
                detail.authority,
                detail.applied_generation,
                detail.applied_source,
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        observed,
        [
            (
                overridden.as_str(),
                Some(wire_policy(Egress::Deny)),
                false,
                "",
                0,
                policy_proto::PolicyAuthority::Local as i32,
                0,
                NetworkPolicySource::Unspecified as i32,
            ),
            (
                inherited.as_str(),
                Some(wire_policy(Egress::Allow)),
                true,
                "seed",
                5,
                policy_proto::PolicyAuthority::Cluster as i32,
                2,
                NetworkPolicySource::Template as i32,
            ),
            // An orphaned sandbox has no template owner to inherit from.
            (
                orphaned.as_str(),
                Some(wire_policy(Egress::Deny)),
                false,
                "",
                0,
                policy_proto::PolicyAuthority::Local as i32,
                0,
                NetworkPolicySource::Unspecified as i32,
            ),
        ]
    );

    let error = service
        .get_network_policy(internal_request(crate::proto::GetNetworkPolicyRequest {
            sandbox_ids: vec![overridden.clone(), SandboxId::new().to_string()],
        }))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::NotFound);
    drain_service_tasks(&service).await;
}

/// A refusal returns promptly. A blocking kill that passes its barrier
/// dispatches to node-manager and then waits on lifecycle cleanup, which has no
/// test-scale deadline; the bound turns that regression into a failure naming
/// the barrier instead of a hang. It is generous so a loaded runner cannot
/// reach it.
const KILL_REFUSAL_BOUND: std::time::Duration = std::time::Duration::from_secs(30);

async fn kill(
    service: &SandboxManagerService,
    id: &str,
    signal: u32,
    case: &str,
    barrier: &str,
) -> tonic::Status {
    let request = internal_request(crate::proto::KillRequest {
        sandbox_id: id.to_string(),
        signal,
        blocking: true,
    });
    tokio::time::timeout(KILL_REFUSAL_BOUND, service.kill(request))
        .await
        .unwrap_or_else(|_| {
            panic!(
                "{case}: kill was not refused within {KILL_REFUSAL_BOUND:?}; \
                 regressed barrier: {barrier}"
            )
        })
        .expect_err("kill must be refused")
}

#[tokio::test]
async fn kill_refuses_signals_it_cannot_deliver_before_cleanup_admission() {
    // Every service drains for restart, so a fatal signal that passes the state
    // guard is refused at cleanup admission, before any node-manager dispatch.
    // Non-fatal signals skip admission and go straight to node-manager once
    // admitted, so each non-fatal case has a barrier besides the guard it pins:
    // "signal zero" uses a stopped row, refused by the signal check and again by
    // the state guard; the other non-fatal case names an unknown sandbox, which
    // lookup refuses before either guard.
    let cleanup_untouched = |service: &SandboxManagerService, id: &str, state, case: &str| {
        let db = service.db_conn().unwrap();
        let row = crate::db::get_sandbox(&db, id).unwrap();
        assert_eq!(row.map(|row| row.state), state, "{case}");
        assert!(
            pplx_space_common_jobs::db::find_active_job(&db, crate::db::cleanup_jobs::KIND, id)
                .unwrap()
                .is_none(),
            "{case}"
        );
        assert!(
            crate::db::cleanup_episode(&db, id).unwrap().is_none(),
            "{case}"
        );
    };
    let service_with = |state: Option<SandboxState>| {
        let conn = crate::db::test_conn();
        let id = SandboxId::new().to_string();
        if let Some(state) = state {
            insert_test_sandbox(&conn, &id, state);
        }
        let service = SandboxManagerService::for_test(conn);
        service.begin_draining_for_restart();
        (service, id)
    };

    for state in SandboxState::all() {
        let deliverable = matches!(state, SandboxState::Started | SandboxState::Paused);
        for signal in [9, 15] {
            let (service, id) = service_with(Some(state));
            let case = format!("{state} signal {signal}");
            let barrier = if deliverable {
                "restart drain at cleanup admission"
            } else {
                "state guard"
            };
            let error = kill(&service, &id, signal, &case, barrier).await;
            if deliverable {
                // Only the restart drain stops these, asking the caller to
                // retry on the successor.
                assert!(
                    pplx_space_sbx_mgr_client::is_restart_retry_on_successor(&error),
                    "{case}: {error:?}"
                );
            } else {
                assert_eq!(
                    error.code(),
                    tonic::Code::FailedPrecondition,
                    "{case}: {error}"
                );
            }
            cleanup_untouched(&service, &id, Some(state), &case);
        }
    }

    for (case, state, signal, closed, barrier, expected) in [
        (
            "unknown sandbox",
            None,
            9,
            false,
            "sandbox lookup",
            tonic::Code::NotFound,
        ),
        (
            "signal zero",
            Some(SandboxState::Stopped),
            0,
            false,
            "signal check",
            tonic::Code::InvalidArgument,
        ),
        // Fatal signals need cleanup admission, so a closing service refuses
        // them as retryable before inspecting the sandbox.
        (
            "SIGKILL after shutdown",
            Some(SandboxState::Stopped),
            9,
            true,
            "closed cleanup request permit",
            tonic::Code::Unavailable,
        ),
        (
            "SIGTERM after shutdown",
            Some(SandboxState::Stopped),
            15,
            true,
            "closed cleanup request permit",
            tonic::Code::Unavailable,
        ),
        // Non-fatal signals are not gated by cleanup admission.
        (
            "non-fatal signal after shutdown",
            None,
            1,
            true,
            "sandbox lookup",
            tonic::Code::NotFound,
        ),
    ] {
        let (service, id) = service_with(state);
        if closed {
            service.shutdown_token.cancel();
        }
        let error = kill(&service, &id, signal, case, barrier).await;
        assert_eq!(error.code(), expected, "{case}: {error}");
        cleanup_untouched(&service, &id, state, case);
    }
}

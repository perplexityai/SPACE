use super::*;
use crate::proto::ExecutionInventoryUnavailable;
use pplx_space_common_core::sql;
use pplx_space_db_test_fixtures::{insert_ready_worker_node, try_pg_test_db};
use pplx_space_execution_common::proto::{RuntimeIdentity, RuntimeInstallation, RuntimeKind};
use pplx_space_test_env::TestResult;
use prost::Message;

// A deployed reader knows field 8 but not the observation envelope at field 11.
#[derive(Clone, PartialEq, Message)]
struct LegacyPublication {
    #[prost(message, optional, tag = "8")]
    execution_inventory: Option<NodeExecutionInventory>,
}

fn installed() -> NodeExecutionInventory {
    NodeExecutionInventory {
        policy: None,
        runtimes: vec![RuntimeInstallation {
            runtime: Some(RuntimeIdentity {
                kind: RuntimeKind::Firecracker.into(),
                version: "1.15.1".into(),
            }),
            ..Default::default()
        }],
        components: vec![],
        environments: None,
    }
}

fn complete(inventory: NodeExecutionInventory) -> ExecutionInventoryObservation {
    ExecutionInventoryObservation {
        outcome: Some(Outcome::Complete(inventory)),
    }
}

fn unavailable() -> ExecutionInventoryObservation {
    ExecutionInventoryObservation {
        outcome: Some(Outcome::Unavailable(ExecutionInventoryUnavailable {})),
    }
}

#[test]
fn old_readers_keep_inventory_but_cannot_manufacture_observation_support() {
    let inventory = installed();
    let request = crate::proto::PublishNodeStateRequest {
        execution_inventory: Some(inventory.clone()),
        execution_observation: Some(complete(inventory.clone())),
        ..Default::default()
    };
    let old = LegacyPublication::decode(request.encode_to_vec().as_slice()).unwrap();
    assert_eq!(old.execution_inventory, Some(inventory));
    let forwarded =
        crate::proto::PublishNodeStateRequest::decode(old.encode_to_vec().as_slice()).unwrap();
    assert!(matches!(
        decode(
            forwarded.execution_observation.as_ref(),
            forwarded.execution_inventory.as_ref(),
        )
        .unwrap(),
        Some(Report::Legacy(_))
    ));
    let request = crate::proto::PublishNodeStateRequest {
        execution_observation: Some(unavailable()),
        ..Default::default()
    };
    let old = LegacyPublication::decode(request.encode_to_vec().as_slice()).unwrap();
    assert!(old.execution_inventory.is_none());

    // An unrecognized future outcome must not decode as a complete empty set.
    let unknown = ExecutionInventoryObservation::decode(&b"\x1a\x00"[..]).unwrap();
    assert!(matches!(
        decode(Some(&unknown), None),
        Err(ObservationError::MissingOutcome)
    ));
}

#[test]
fn invalid_or_conflicting_reports_never_become_positive_evidence() {
    let empty = NodeExecutionInventory::default();
    let installed = installed();
    for (observation, legacy) in [
        (ExecutionInventoryObservation::default(), None),
        (complete(empty.clone()), Some(&installed)),
        (unavailable(), Some(&empty)),
    ] {
        decode(Some(&observation), legacy).unwrap_err();
    }
    assert!(decode(None, None).unwrap().is_none());
    assert!(matches!(
        decode(None, Some(&empty)).unwrap(),
        Some(Report::Legacy(_))
    ));
    for legacy in [None, Some(&installed)] {
        assert!(matches!(
            decode(Some(&complete(installed.clone())), legacy).unwrap(),
            Some(Report::Complete(_))
        ));
    }
}

async fn publish_with_status(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    node_id: NodeId,
    observation: Option<&ExecutionInventoryObservation>,
    legacy: Option<&NodeExecutionInventory>,
) -> Result<(), Status> {
    let prepared = prepare(tx, node_id, observation, legacy).await?;
    let outcome = publish(tx, prepared).await?;
    crate::status::upsert_node_status_from_publish(
        tx,
        node_id,
        None,
        None,
        crate::status::SandboxInventoryPrune::Skipped,
        crate::status::LocalBackupInventoryReport::Absent,
        outcome,
    )
    .await
}

async fn observed(
    pool: &sqlx::PgPool,
    node_id: NodeId,
) -> Result<(Option<String>, Option<String>, i64), sqlx::Error> {
    sqlx::query_as(sql!(
        "SELECT execution_inventory_status, execution_inventory_received_at::text,
                (SELECT COUNT(*) FROM node_runtime_installations WHERE node_id = $1)
         FROM node_status_snapshots WHERE node_id = $1",
    ))
    .bind(node_id)
    .fetch_one(pool)
    .await
}

#[tokio::test]
async fn observation_and_projection_follow_publication_outcomes_atomically() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    let mut tx = db.pool.begin().await?;
    publish_with_status(&mut tx, node_id, None, None).await?;
    tx.commit().await?;
    assert_eq!(observed(&db.pool, node_id).await?, (None, None, 0));
    let installed = installed();
    let empty = NodeExecutionInventory::default();
    // Both deployment orders, rollback to a legacy producer, and empty success
    // must remain distinct from a failed collection and an unrelated publish.
    for (report, legacy, status, count) in [
        (None, Some(installed.clone()), "legacy", 1),
        (
            Some(complete(installed.clone())),
            Some(installed.clone()),
            "complete",
            1,
        ),
        (Some(unavailable()), None, "unavailable", 1),
        (Some(complete(empty)), None, "complete", 0),
        (None, Some(installed), "legacy", 1),
    ] {
        let mut tx = db.pool.begin().await?;
        publish_with_status(&mut tx, node_id, report.as_ref(), legacy.as_ref()).await?;
        tx.commit().await?;
        let actual = observed(&db.pool, node_id).await?;
        assert_eq!(actual.0.as_deref(), Some(status));
        assert_eq!(actual.2, count);

        let mut tx = db.pool.begin().await?;
        publish_with_status(&mut tx, node_id, None, None).await?;
        tx.commit().await?;
        assert_eq!(observed(&db.pool, node_id).await?, actual);
    }
    let before = observed(&db.pool, node_id).await?;
    let mut tx = db.pool.begin().await?;
    publish_with_status(
        &mut tx,
        node_id,
        Some(&complete(NodeExecutionInventory::default())),
        None,
    )
    .await?;
    tx.rollback().await?;
    assert_eq!(observed(&db.pool, node_id).await?, before);
    let malformed = complete(NodeExecutionInventory {
        policy: None,
        runtimes: vec![RuntimeInstallation::default()],
        components: vec![],
        environments: None,
    });
    let mut tx = db.pool.begin().await?;
    let error = publish_with_status(&mut tx, node_id, Some(&malformed), None)
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    tx.rollback().await?;
    assert_eq!(observed(&db.pool, node_id).await?, before);
    Ok(())
}

#[tokio::test]
async fn empty_publications_serialize_until_status_receipt_commits() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    let empty = complete(NodeExecutionInventory::default());
    let mut first = db.pool.begin().await?;
    // No status or artifact rows are needed to serialize a replacement.
    let prepared = prepare(&mut first, node_id, Some(&empty), None).await?;
    let outcome = publish(&mut first, prepared).await?;
    let mut second = db.pool.begin().await?;
    sqlx::query(sql!("SET LOCAL lock_timeout = '20ms'"))
        .execute(&mut *second)
        .await?;
    let result = prepare(&mut second, node_id, Some(&empty), None).await;
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("concurrent empty publication did not wait for the node lock"),
    };
    assert_eq!(error.code(), tonic::Code::Internal);
    second.rollback().await?;
    crate::status::upsert_node_status_from_publish(
        &mut first,
        node_id,
        None,
        None,
        crate::status::SandboxInventoryPrune::Skipped,
        crate::status::LocalBackupInventoryReport::Absent,
        outcome,
    )
    .await?;
    first.commit().await?;
    let mut retry = db.pool.begin().await?;
    publish_with_status(&mut retry, node_id, Some(&unavailable()), None).await?;
    retry.commit().await?;
    assert_eq!(
        observed(&db.pool, node_id).await?.0.as_deref(),
        Some("unavailable")
    );
    Ok(())
}

fn populated_inventory() -> NodeExecutionInventory {
    use pplx_space_execution_common::proto::{
        ArtifactComponent, ArtifactComponentInstallation, ArtifactComponentKind, CpuArchitecture,
        GuestOperatingSystem, GuestPlatform, StateFormatCapability, StateFormatIdentity,
    };
    let mut inventory = installed();
    inventory.runtimes[0].supported_guests = vec![GuestPlatform {
        os: GuestOperatingSystem::Linux.into(),
        architecture: CpuArchitecture::X8664.into(),
    }];
    inventory.runtimes[0].state_formats = vec![StateFormatCapability {
        identity: Some(StateFormatIdentity {
            format: "firecracker-snapshot".into(),
            version: "9.0.0".into(),
        }),
        can_create: true,
        can_load: true,
    }];
    inventory.components = vec![ArtifactComponentInstallation {
        component: Some(ArtifactComponent {
            kind: ArtifactComponentKind::Kernel.into(),
            role: None,
            version: Some("6.1.155".into()),
            digest: Some(format!("sha256:{}", "42".repeat(32))),
            format: Some("vmlinux".into()),
        }),
        default_for_new: true,
        legacy_default: true,
    }];
    inventory
}

async fn projection_rows(
    pool: &sqlx::PgPool,
    node_id: NodeId,
) -> Result<Vec<(String, String, String)>, sqlx::Error> {
    // xmin catches UPDATE and delete/reinsert churn even when values are equal.
    sqlx::query_as(sql!(
        "SELECT 'runtime', to_jsonb(r)::text, xmin::text
         FROM node_runtime_installations r WHERE node_id = $1
         UNION ALL SELECT 'guest', to_jsonb(g)::text, xmin::text
         FROM node_runtime_guests g WHERE node_id = $1
         UNION ALL SELECT 'format', to_jsonb(f)::text, xmin::text
         FROM node_runtime_state_formats f WHERE node_id = $1
         UNION ALL SELECT 'component', to_jsonb(c)::text, xmin::text
         FROM node_artifact_components c WHERE node_id = $1
         ORDER BY 1, 2"
    ))
    .bind(node_id)
    .fetch_all(pool)
    .await
}

async fn commit_inventory(
    pool: &sqlx::PgPool,
    node_id: NodeId,
    inventory: NodeExecutionInventory,
) -> TestResult {
    let mut tx = pool.begin().await?;
    publish_with_status(&mut tx, node_id, Some(&complete(inventory)), None).await?;
    tx.commit().await?;
    Ok(())
}

#[tokio::test]
async fn repeated_samples_refresh_receipts_without_rewriting_the_projection() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    let inventory = populated_inventory();
    commit_inventory(&db.pool, node_id, inventory.clone()).await?;
    let before = projection_rows(&db.pool, node_id).await?;
    assert_eq!(before.len(), 4);
    sqlx::query(sql!(
        "UPDATE node_status_snapshots SET execution_inventory_received_at = to_timestamp(0)
         WHERE node_id = $1"
    ))
    .bind(node_id)
    .execute(&db.pool)
    .await?;
    let receipt_before = observed(&db.pool, node_id).await?.1;

    // Reordered duplicate entries have the same set/union semantics as the
    // normalized projection, including split true flags within one report.
    let mut duplicate = inventory.clone();
    duplicate.runtimes[0].state_formats[0].can_create = false;
    duplicate.runtimes.push(inventory.runtimes[0].clone());
    duplicate.runtimes[1].state_formats[0].can_load = false;
    duplicate.components[0].default_for_new = false;
    duplicate.components.push(inventory.components[0].clone());
    duplicate.components[1].legacy_default = false;
    for mut sample in [inventory.clone(), duplicate] {
        sample.runtimes.reverse();
        sample.components.reverse();
        commit_inventory(&db.pool, node_id, sample).await?;
        assert_eq!(projection_rows(&db.pool, node_id).await?, before);
        assert_ne!(observed(&db.pool, node_id).await?.1, receipt_before);
    }

    // The cluster's stored state is authoritative for reconciliation: a lost
    // projection row must be repaired even if the publisher repeats its sample.
    sqlx::query(sql!("DELETE FROM node_runtime_guests WHERE node_id = $1"))
        .bind(node_id)
        .execute(&db.pool)
        .await?;
    commit_inventory(&db.pool, node_id, inventory).await?;
    let repaired = projection_rows(&db.pool, node_id).await?;
    assert_eq!(repaired.len(), 4);
    for (old, new) in before.iter().zip(&repaired) {
        assert_eq!(old.0, new.0);
        assert_eq!(old.1, new.1);
        if old.0 != "guest" {
            assert_eq!(old.2, new.2);
        }
    }
    Ok(())
}

#[tokio::test]
async fn changed_samples_revoke_capabilities_and_remove_absent_rows() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    enum Change {
        RevokeFlags,
        RemoveGuest,
        RemoveFormat,
        RemoveComponent,
        RemoveRuntime,
        Empty,
    }
    for change in [
        Change::RevokeFlags,
        Change::RemoveGuest,
        Change::RemoveFormat,
        Change::RemoveComponent,
        Change::RemoveRuntime,
        Change::Empty,
    ] {
        let mut inventory = populated_inventory();
        commit_inventory(&db.pool, node_id, inventory.clone()).await?;
        let expected_kinds = match change {
            Change::RevokeFlags => {
                inventory.runtimes[0].state_formats[0].can_create = false;
                inventory.runtimes[0].state_formats[0].can_load = false;
                inventory.components[0].default_for_new = false;
                inventory.components[0].legacy_default = false;
                vec!["component", "format", "guest", "runtime"]
            }
            Change::RemoveGuest => {
                inventory.runtimes[0].supported_guests.clear();
                vec!["component", "format", "runtime"]
            }
            Change::RemoveFormat => {
                inventory.runtimes[0].state_formats.clear();
                vec!["component", "guest", "runtime"]
            }
            Change::RemoveComponent => {
                inventory.components.clear();
                vec!["format", "guest", "runtime"]
            }
            Change::RemoveRuntime => {
                inventory.runtimes.clear();
                vec!["component"]
            }
            Change::Empty => {
                inventory = NodeExecutionInventory::default();
                vec![]
            }
        };
        commit_inventory(&db.pool, node_id, inventory).await?;
        let rows = projection_rows(&db.pool, node_id).await?;
        assert_eq!(
            rows.iter().map(|row| row.0.as_str()).collect::<Vec<_>>(),
            expected_kinds
        );
        if matches!(change, Change::RevokeFlags) {
            for (kind, content, _) in rows {
                let content: serde_json::Value = serde_json::from_str(&content)?;
                for &flag in match kind.as_str() {
                    "component" => &["default_for_new", "legacy_default"][..],
                    "format" => &["can_create", "can_load"][..],
                    _ => &[],
                } {
                    assert_eq!(content[flag], serde_json::Value::Bool(false));
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn policy_receipts_clear_unknown_evidence_and_commit_atomically() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node = NodeId::new();
    insert_ready_worker_node(&db.pool, node).await?;
    let reference = format!("sha256:{}", "11".repeat(32));
    let valid = pplx_space_execution_common::proto::ExecutionPolicyObservation {
        schema_version: 1,
        legacy_unbound_allowed: true,
        allowed_environment_references: vec![reference.clone()],
        default_environment_reference: Some(reference),
        applied_generation: None,
        cluster_policy_unreadable: false,
    };
    let invalid = pplx_space_execution_common::proto::ExecutionPolicyObservation {
        schema_version: 99,
        ..valid.clone()
    };
    // A node enforcing cluster generation 5, whose cluster default was refused
    // after its configured default was removed.
    let applied = pplx_space_execution_common::proto::ExecutionPolicyObservation {
        schema_version: 2,
        default_environment_reference: None,
        applied_generation: Some(5),
        ..valid.clone()
    };
    let unreadable = pplx_space_execution_common::proto::ExecutionPolicyObservation {
        schema_version: 2,
        legacy_unbound_allowed: false,
        allowed_environment_references: vec![],
        default_environment_reference: None,
        applied_generation: None,
        cluster_policy_unreadable: true,
    };
    for (policy, unavailable_report, expected) in [
        (Some(valid.clone()), false, true),
        (Some(invalid), false, false),
        (Some(applied.clone()), false, true),
        (Some(unreadable.clone()), false, true),
        (Some(valid.clone()), false, true),
        (None, false, false),
        (Some(valid), false, true),
        (None, true, false),
    ] {
        let published = policy.as_ref().map(|policy| policy.applied_generation);
        let unreadable_published = policy
            .as_ref()
            .is_some_and(|policy| policy.cluster_policy_unreadable);
        let report = if unavailable_report {
            unavailable()
        } else {
            complete(NodeExecutionInventory {
                policy,
                ..Default::default()
            })
        };
        let mut tx = db.pool.begin().await?;
        publish_with_status(&mut tx, node, Some(&report), None).await?;
        tx.commit().await?;
        let read = || {
            sqlx::query_scalar::<
                _,
                Option<
                    sqlx::types::Json<pplx_space_common_core::types::ExecutionPolicyObservation>,
                >,
            >(sql!(
                "SELECT execution_policy FROM node_status_snapshots WHERE node_id = $1"
            ))
            .bind(node)
        };
        let before = read().fetch_one(&db.pool).await?;
        assert_eq!(before.is_some(), expected);
        if let Some(stored) = &before {
            assert_eq!(Some(stored.applied_generation()), published);
            assert_eq!(stored.cluster_policy_unreadable(), unreadable_published);
        }
        let receipt = || {
            sqlx::query_scalar::<_, Option<time::OffsetDateTime>>(sql!(
                "SELECT execution_policy_received_at FROM node_status_snapshots WHERE node_id = $1"
            ))
            .bind(node)
        };
        let received_at = receipt().fetch_one(&db.pool).await?;
        assert_eq!(received_at.is_some(), expected);
        let mut tx = db.pool.begin().await?;
        publish_with_status(&mut tx, node, None, None).await?;
        tx.commit().await?;
        assert_eq!(read().fetch_one(&db.pool).await?, before);
        let mut tx = db.pool.begin().await?;
        publish_with_status(&mut tx, node, Some(&unavailable()), None).await?;
        tx.rollback().await?;
        assert_eq!(read().fetch_one(&db.pool).await?, before);
        assert_eq!(receipt().fetch_one(&db.pool).await?, received_at);
        // The inventory receipt is independent storage. Updating it alone
        // must not refresh policy evidence (this does not simulate an old writer).
        sqlx::query(sql!(
            "UPDATE node_status_snapshots SET execution_inventory_received_at = clock_timestamp()
             WHERE node_id = $1"
        ))
        .bind(node)
        .execute(&db.pool)
        .await?;
        assert_eq!(receipt().fetch_one(&db.pool).await?, received_at);
    }
    Ok(())
}

/// What a current node publishes on top of `inventory`: E0 known and usable,
/// and a sort key on each runtime.
fn with_observation(mut inventory: NodeExecutionInventory) -> NodeExecutionInventory {
    use pplx_space_common_core::types::VersionSortKey;
    use pplx_space_execution_common::environment::{
        EnvironmentDefinition, EnvironmentObservation, KnownEnvironment,
    };
    let e0 = EnvironmentDefinition::from_document(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../../../proto/execution/v1/fixtures/e0.json"
    )))
    .unwrap()
    .into_runtime_neutral()
    .unwrap();
    let reference = e0.reference();
    let observation = EnvironmentObservation::new(
        [KnownEnvironment::new(
            e0,
            Some(VersionSortKey::new(vec![1, 15, 1]).unwrap()),
            [],
            [],
        )
        .unwrap()],
        [reference],
    )
    .unwrap();
    for runtime in &mut inventory.runtimes {
        runtime.version_sort_key = vec![1, 15, 1];
    }
    inventory.environments = Some((&observation).into());
    inventory
}

/// The inventory messages as deployed cluster-api replicas decode them,
/// before environment observations and runtime sort keys existed.
mod deployed {
    use prost::Message;

    #[derive(Clone, PartialEq, Message)]
    pub(super) struct RuntimeInstallation {
        #[prost(message, optional, tag = "1")]
        pub runtime: Option<pplx_space_execution_common::proto::RuntimeIdentity>,
        #[prost(message, repeated, tag = "2")]
        pub supported_guests: Vec<pplx_space_execution_common::proto::GuestPlatform>,
        #[prost(message, repeated, tag = "3")]
        pub state_formats: Vec<pplx_space_execution_common::proto::StateFormatCapability>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub(super) struct NodeExecutionInventory {
        #[prost(message, repeated, tag = "1")]
        pub runtimes: Vec<RuntimeInstallation>,
        #[prost(message, repeated, tag = "2")]
        pub components: Vec<pplx_space_execution_common::proto::ArtifactComponentInstallation>,
        #[prost(message, optional, tag = "3")]
        pub policy: Option<pplx_space_execution_common::proto::ExecutionPolicyObservation>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub(super) struct ExecutionInventoryObservation {
        #[prost(message, optional, tag = "1")]
        pub complete: Option<NodeExecutionInventory>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub(super) struct Publication {
        #[prost(message, optional, tag = "8")]
        pub execution_inventory: Option<NodeExecutionInventory>,
        #[prost(message, optional, tag = "11")]
        pub execution_observation: Option<ExecutionInventoryObservation>,
    }
}

#[test]
fn deployed_readers_drop_environment_observations_from_both_inventories_alike() {
    let inventory = with_observation(installed());
    let request = crate::proto::PublishNodeStateRequest {
        execution_inventory: Some(inventory.clone()),
        execution_observation: Some(complete(inventory.clone())),
        ..Default::default()
    };
    let old = deployed::Publication::decode(request.encode_to_vec().as_slice()).unwrap();
    let legacy = old.execution_inventory.unwrap();
    let observed = old.execution_observation.unwrap().complete.unwrap();
    // Deployed decode() rejects a complete report that differs from field 8.
    assert_eq!(legacy, observed);
    assert_eq!(legacy.runtimes.len(), 1);
    assert_eq!(legacy.runtimes[0].runtime, inventory.runtimes[0].runtime);
    assert_eq!(legacy.policy, inventory.policy);
}

#[test]
fn current_readers_accept_observations_and_old_nodes_without_them() {
    use pplx_space_execution_common::environment::EnvironmentObservation;
    let current = with_observation(installed());
    assert!(matches!(
        decode(Some(&complete(current.clone())), Some(&current)).unwrap(),
        Some(Report::Complete(_))
    ));
    let observation = EnvironmentObservation::from_inventory(&current)
        .unwrap()
        .unwrap();
    assert_eq!(observation.known().len(), 1);
    assert_eq!(observation.usable().len(), 1);
    // A node built before observations sends both copies without them.
    let old = installed();
    let wire = crate::proto::PublishNodeStateRequest {
        execution_inventory: Some(old.clone()),
        execution_observation: Some(complete(old)),
        ..Default::default()
    };
    let decoded =
        crate::proto::PublishNodeStateRequest::decode(wire.encode_to_vec().as_slice()).unwrap();
    let Some(Report::Complete(inventory)) = decode(
        decoded.execution_observation.as_ref(),
        decoded.execution_inventory.as_ref(),
    )
    .unwrap() else {
        panic!("an old node's complete report stays complete");
    };
    assert_eq!(
        EnvironmentObservation::from_inventory(inventory).unwrap(),
        None
    );
}

#[tokio::test]
async fn observations_leave_the_inventory_projection_unchanged() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    insert_ready_worker_node(&db.pool, node_id).await?;
    commit_inventory(&db.pool, node_id, populated_inventory()).await?;
    let before = projection_rows(&db.pool, node_id).await?;
    assert_eq!(before.len(), 4);
    commit_inventory(&db.pool, node_id, with_observation(populated_inventory())).await?;
    assert_eq!(projection_rows(&db.pool, node_id).await?, before);
    assert_eq!(
        observed(&db.pool, node_id).await?.0.as_deref(),
        Some("complete")
    );
    Ok(())
}

//! Characterization of [`insert_unplaced`], the create path's write of a new
//! Postgres `sandboxes` row: every column it writes, including the lifecycle
//! it starts in and the restore-lease columns that move with it, for each
//! combination of restore token and volume encryption. No lifecycle guards
//! the insert; an existing row is refused by the primary key alone, whatever
//! its lifecycle.

use std::collections::BTreeMap;

use pplx_space_common_core::types::{
    AutoLifecyclePolicy, AutoLifecycleWakePolicy, AutoLifecycleWakeSource, DiskIoLimits,
    SandboxLifecycle,
};
use sqlx::types::JsonValue;

use super::*;
use crate::{SandboxCreate, SandboxStoreError, insert_unplaced};

const LEASE_SECONDS: i32 = 120;

/// Every Postgres `sandboxes` column the insert writes, as stored.
#[derive(Debug, PartialEq, sqlx::FromRow)]
struct Row {
    id: SandboxId,
    name: Option<String>,
    user_id: Uuid,
    org_id: Uuid,
    managed_by: String,
    lifecycle: String,
    placement: String,
    template_id: Option<String>,
    content_hash: Option<String>,
    vcpu: Option<i32>,
    memory_mib: Option<i32>,
    disk_mib: Option<i32>,
    disk_io_max_operations_per_sec: i64,
    disk_io_max_bandwidth_mib_per_sec: i64,
    env: JsonValue,
    metadata: JsonValue,
    auto_lifecycle_json: JsonValue,
    network_policy_override_json: Option<JsonValue>,
    network_policy_generation: i64,
    volume_encryption: Option<bool>,
    restore_lease_token: Option<Uuid>,
    /// Seconds from now until `restore_lease_expires_at`, on the database
    /// clock.
    lease_remaining_secs: Option<f64>,
    preserve_local_artifact_on_restore_failure: bool,
    egress_profiles: Vec<String>,
}

async fn row(pool: &PgPool, id: SandboxId) -> TestResult<Row> {
    Ok(sqlx::query_as::<_, Row>(sql!(
        "SELECT id, name, user_id, org_id, managed_by, lifecycle, placement, template_id,
                content_hash, vcpu, memory_mib, disk_mib, disk_io_max_operations_per_sec,
                disk_io_max_bandwidth_mib_per_sec, env, metadata, auto_lifecycle_json,
                network_policy_override_json, network_policy_generation, volume_encryption,
                restore_lease_token,
                EXTRACT(EPOCH FROM (restore_lease_expires_at - NOW()))::float8
                    AS lease_remaining_secs,
                preserve_local_artifact_on_restore_failure, egress_profiles
           FROM sandboxes WHERE id = $1"
    ))
    .bind(id)
    .fetch_one(pool)
    .await?)
}

/// Create inputs that differ from the column defaults and, by `variant`,
/// from every other set of inputs here, so a value bound to the wrong column
/// or taken from another insert shows.
struct Inputs {
    name: String,
    managed_by: &'static str,
    template: String,
    content_hash: String,
    vcpu: i32,
    memory_mib: i32,
    disk_mib: i32,
    disk_io_limits: DiskIoLimits,
    env: BTreeMap<String, String>,
    metadata: SandboxMetadata,
    auto_lifecycle: AutoLifecyclePolicy,
    network_policy_override_json: JsonValue,
    egress_profiles: Vec<String>,
}

impl Inputs {
    fn new(variant: u8) -> TestResult<Self> {
        let n = i32::from(variant);
        Ok(Self {
            name: format!("store-insert-{variant}"),
            managed_by: if variant.is_multiple_of(2) {
                "k8s"
            } else {
                "space"
            },
            template: format!("tmpl-store-insert-{variant}"),
            content_hash: format!("sha256:synthetic-{variant}"),
            vcpu: 2 + n,
            memory_mib: 1024 + 256 * n,
            disk_mib: 8192 + n,
            disk_io_limits: DiskIoLimits::try_new(
                300 + u64::from(variant),
                40 + u64::from(variant),
            )?,
            env: BTreeMap::from([(format!("SYNTHETIC_{variant}"), format!("value-{variant}"))]),
            metadata: SandboxMetadata::from([
                (
                    "label".to_owned(),
                    SandboxMetadataValue::String(format!("synthetic-{variant}")),
                ),
                (
                    "ordinal".to_owned(),
                    SandboxMetadataValue::Number(i64::from(n)),
                ),
            ]),
            auto_lifecycle: AutoLifecyclePolicy {
                wake: AutoLifecycleWakePolicy {
                    on: vec![AutoLifecycleWakeSource::Command],
                },
                ..AutoLifecyclePolicy::default()
            },
            network_policy_override_json: serde_json::json!({ "synthetic_policy": variant }),
            egress_profiles: vec!["profile-a".to_owned(), format!("profile-{variant}")],
        })
    }

    fn create<'a>(&'a self, id: SandboxId, egress_profiles: &'a [&'a str]) -> SandboxCreate<'a> {
        SandboxCreate {
            id,
            name: Some(&self.name),
            user_id: UserId::from_uuid(SEED_USER),
            org_id: OrgId::from_uuid(SEED_ORG),
            managed_by: self.managed_by,
            template: &self.template,
            content_hash: &self.content_hash,
            vcpu: Some(self.vcpu),
            memory_mib: Some(self.memory_mib),
            disk_mib: Some(self.disk_mib),
            disk_io_limits: self.disk_io_limits,
            env: &self.env,
            metadata: &self.metadata,
            auto_lifecycle: &self.auto_lifecycle,
            network_policy_override_json: Some(&self.network_policy_override_json),
            egress_profiles,
        }
    }

    fn egress_profiles(&self) -> Vec<&str> {
        self.egress_profiles.iter().map(String::as_str).collect()
    }
}

/// Every combination of restore token and volume encryption. Without a
/// token the row starts `creating` with no restore lease; with one it starts
/// `restoring`, holding that token's lease for the given seconds on the
/// database clock and asking a failed restore to preserve the local artifact.
/// Either way the row is unplaced at network-policy generation 1, with every
/// create input stored as given.
#[tokio::test(flavor = "current_thread")]
async fn insert_unplaced_stores_every_input_and_starts_by_restore_token() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };

    let combinations = [
        (None, false),
        (None, true),
        (Some(Uuid::now_v7()), false),
        (Some(Uuid::now_v7()), true),
    ];
    for ((token, volume_encryption), variant) in combinations.into_iter().zip(0_u8..) {
        let inputs = Inputs::new(variant)?;
        let egress_profiles = inputs.egress_profiles();
        let id = SandboxId::new();
        insert_unplaced(
            &db.pool,
            &inputs.create(id, &egress_profiles),
            volume_encryption,
            token,
            i64::from(LEASE_SECONDS),
        )
        .await?;

        let mut actual = row(&db.pool, id).await?;
        let remaining = actual.lease_remaining_secs.take();
        let env = std::mem::take(&mut actual.env);
        let metadata = std::mem::take(&mut actual.metadata);
        let auto_lifecycle = std::mem::take(&mut actual.auto_lifecycle_json);
        let case = format!("token {token:?}, volume encryption {volume_encryption}");
        let restores = token.is_some();
        assert_eq!(
            actual,
            Row {
                id,
                name: Some(inputs.name.clone()),
                user_id: SEED_USER,
                org_id: SEED_ORG,
                managed_by: inputs.managed_by.to_owned(),
                lifecycle: if restores { "restoring" } else { "creating" }.to_owned(),
                placement: "unplaced".to_owned(),
                template_id: Some(inputs.template.clone()),
                content_hash: Some(inputs.content_hash.clone()),
                vcpu: Some(inputs.vcpu),
                memory_mib: Some(inputs.memory_mib),
                disk_mib: Some(inputs.disk_mib),
                disk_io_max_operations_per_sec: i64::try_from(
                    inputs.disk_io_limits.max_operations_per_sec(),
                )?,
                disk_io_max_bandwidth_mib_per_sec: i64::try_from(
                    inputs.disk_io_limits.max_bandwidth_mib_per_sec(),
                )?,
                env: JsonValue::Null,
                metadata: JsonValue::Null,
                auto_lifecycle_json: JsonValue::Null,
                network_policy_override_json: Some(inputs.network_policy_override_json.clone()),
                network_policy_generation: 1,
                volume_encryption: Some(volume_encryption),
                restore_lease_token: token,
                lease_remaining_secs: None,
                preserve_local_artifact_on_restore_failure: restores,
                egress_profiles: inputs.egress_profiles.clone(),
            },
            "{case}"
        );
        assert_eq!(
            serde_json::from_value::<BTreeMap<String, String>>(env)?,
            inputs.env,
            "{case}"
        );
        assert_eq!(
            serde_json::from_value::<SandboxMetadata>(metadata)?,
            inputs.metadata,
            "{case}"
        );
        assert_eq!(
            serde_json::from_value::<AutoLifecyclePolicy>(auto_lifecycle)?,
            inputs.auto_lifecycle,
            "{case}"
        );
        match remaining {
            None => assert!(
                !restores,
                "{case}: a restore token must set the lease expiry"
            ),
            Some(secs) => {
                assert!(
                    restores,
                    "{case}: no restore token, but a lease expiry was set"
                );
                let lease = f64::from(LEASE_SECONDS);
                assert!(
                    secs > lease - 30.0 && secs <= lease,
                    "{case}: lease expires {secs}s from now, not about {LEASE_SECONDS}s"
                );
            }
        }
    }
    Ok(())
}

/// An insert over an existing id, in every stored lifecycle, fails on the
/// primary key and leaves that row as it was.
#[tokio::test(flavor = "current_thread")]
async fn insert_unplaced_refuses_an_existing_id_in_every_lifecycle() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let inputs = Inputs::new(0)?;
    let egress_profiles = inputs.egress_profiles();
    // Every value Postgres `sandboxes.lifecycle` holds: the variants and the
    // two stored non-variants.
    let stored = SandboxLifecycle::all()
        .iter()
        .map(|lifecycle| lifecycle.as_str())
        .chain(["deleted", "cleanup_required"]);

    for lifecycle in stored {
        let id = SandboxId::from_uuid(insert_sandbox(&db.pool, None, lifecycle).await?);
        let before = row(&db.pool, id).await?;

        let error = insert_unplaced(
            &db.pool,
            &inputs.create(id, &egress_profiles),
            false,
            Some(Uuid::now_v7()),
            i64::from(LEASE_SECONDS),
        )
        .await
        .expect_err("a duplicate id must be refused");

        let SandboxStoreError::Database(sqlx::Error::Database(database)) = &error else {
            return Err(format!("{lifecycle}: not a database error: {error:?}").into());
        };
        assert!(database.is_unique_violation(), "{lifecycle}: {error:?}");
        assert_eq!(database.constraint(), Some("sandboxes_pkey"), "{lifecycle}");
        assert_eq!(row(&db.pool, id).await?, before, "{lifecycle}");
    }
    Ok(())
}

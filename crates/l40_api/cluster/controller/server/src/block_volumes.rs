//! Default-off, bounded cloud reconciler. It never infers device release from
//! a missing heartbeat or a sandbox lifecycle projection.

use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

use pplx_space_aws_client::{
    AwsClientError, QueryTransport,
    ec2::{Client, CreateEncryptedGp3Volume, Volume},
};
use pplx_space_sandbox_store::block_volume::{self as store, BlockVolume, VolumePhase};
use serde::Deserialize;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const POLICY_ENV: &str = "SPACE_EBS_EXPERIMENT_POLICY";
const TICK: Duration = Duration::from_secs(2);
const MAX_INVENTORY_PAGES: usize = 64;
const RESOURCE_LIMIT: usize = 2;
const LOGICAL_ID_TAG: &str = "space:volume-id";
const SANDBOX_ID_TAG: &str = "space:sandbox-id";
const RETAINED_TAG: &str = "space:retained";

mod independent;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Policy {
    account_id: String,
    region: String,
    availability_zone: String,
    cluster: String,
    experiment: String,
    instance_ids: BTreeSet<String>,
    #[serde(default = "default_resource_limit")]
    resource_limit: usize,
}

fn default_resource_limit() -> usize {
    RESOURCE_LIMIT
}

#[derive(Debug, thiserror::Error)]
pub enum PolicyError {
    #[error("invalid {POLICY_ENV}: {0}")]
    Json(#[from] serde_json::Error),
    #[error(
        "{POLICY_ENV} requires a valid account, region, AZ, ownership tags and one or two explicit EC2 instances"
    )]
    Scope,
    #[error("{POLICY_ENV} must be valid UTF-8")]
    Encoding,
}

impl Policy {
    pub(crate) fn from_environment(
        env: &dyn pplx_space_env_vars::Environment,
    ) -> Result<Option<Self>, PolicyError> {
        let Some(raw) = env.var_os(POLICY_ENV) else {
            return Ok(None);
        };
        let policy: Self = serde_json::from_str(raw.to_str().ok_or(PolicyError::Encoding)?)?;
        let identifier = |value: &str| {
            !value.is_empty()
                && value.len() <= 128
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"-_./:".contains(&byte))
        };
        if policy.account_id.len() != 12
            || !policy.account_id.bytes().all(|byte| byte.is_ascii_digit())
            || !identifier(&policy.region)
            || !policy.availability_zone.starts_with(&policy.region)
            || policy.availability_zone.len() != policy.region.len() + 1
            || !identifier(&policy.cluster)
            || !identifier(&policy.experiment)
            || policy.instance_ids.is_empty()
            || policy.instance_ids.len() > RESOURCE_LIMIT
            || !(1..=RESOURCE_LIMIT).contains(&policy.resource_limit)
            || policy.instance_ids.iter().any(|id| {
                !id.strip_prefix("i-").is_some_and(|suffix| {
                    suffix.len() == 17 && suffix.bytes().all(|byte| byte.is_ascii_hexdigit())
                })
            })
        {
            return Err(PolicyError::Scope);
        }
        Ok(Some(policy))
    }

    fn tags(&self) -> BTreeMap<String, String> {
        BTreeMap::from([
            ("space:cluster".into(), self.cluster.clone()),
            ("space:experiment".into(), self.experiment.clone()),
        ])
    }
}

#[derive(Debug, thiserror::Error)]
enum ReconcileError {
    #[error("cloud operation failed: {0}")]
    Aws(#[from] AwsClientError),
    #[error("block-volume storage failed: {0}")]
    Database(#[from] sqlx::Error),
    #[error("block-volume reconciliation refused: {0}")]
    Refused(&'static str),
}

async fn inventory<T: QueryTransport>(
    client: &Client<T>,
    tags: &BTreeMap<String, String>,
) -> Result<Vec<Volume>, ReconcileError> {
    let mut volumes = Vec::new();
    let mut next_token = None;
    let mut seen_tokens = BTreeSet::new();
    for _ in 0..MAX_INVENTORY_PAGES {
        let page = client
            .describe_volumes_page(tags, next_token.as_deref())
            .await?;
        volumes.extend(page.volumes);
        // AWS can return an empty page with a continuation token.
        let Some(token) = page.next_token else {
            let ids: BTreeSet<_> = volumes.iter().map(|volume| &volume.volume_id).collect();
            if ids.len() != volumes.len() {
                return Err(ReconcileError::Refused("duplicate cloud inventory entries"));
            }
            return Ok(volumes);
        };
        if !seen_tokens.insert(token.clone()) {
            return Err(ReconcileError::Refused("cloud inventory pagination cycled"));
        }
        next_token = Some(token);
    }
    Err(ReconcileError::Refused("cloud inventory was not exhausted"))
}

fn matches_resource(policy: &Policy, row: &BlockVolume, volume: &Volume) -> bool {
    let ownership = policy.tags();
    volume.availability_zone == row.availability_zone
        && volume.availability_zone == policy.availability_zone
        && volume.encrypted
        && volume.volume_type == "gp3"
        && volume.size_gib == 128
        && u32::from(row.mount_spec.size_gib) == volume.size_gib
        && ownership
            .iter()
            .all(|(key, value)| volume.tags.get(key) == Some(value))
        && volume.tags.get(LOGICAL_ID_TAG) == Some(&row.id.to_string())
        && if row.retained {
            volume.tags.get(RETAINED_TAG).map(String::as_str) == Some("true")
        } else {
            volume.tags.get(SANDBOX_ID_TAG) == Some(&row.sandbox_id.to_string())
        }
}

#[derive(Debug, PartialEq, Eq)]
enum Action {
    Wait,
    Create,
    Observe(VolumePhase),
    Attach,
    Detach,
    FinishDetach,
    Delete,
}

fn plan(
    policy: &Policy,
    row: &BlockVolume,
    cloud: Option<&Volume>,
) -> Result<Action, ReconcileError> {
    if row.availability_zone != policy.availability_zone
        || u32::from(row.mount_spec.size_gib) != 128
    {
        return Err(ReconcileError::Refused(
            "resource is outside the experiment envelope",
        ));
    }
    let Some(volume) = cloud else {
        return match row.phase {
            // Always settle an uncertain create with the original token, even
            // when deletion was requested meanwhile. Never discard its intent.
            VolumePhase::Requested | VolumePhase::Provisioning if row.aws_volume_id.is_none() => {
                Ok(Action::Create)
            }
            VolumePhase::Deleting => Ok(Action::Observe(VolumePhase::Deleted)),
            VolumePhase::Deleted => Ok(Action::Wait),
            _ => Err(ReconcileError::Refused(
                "durable disk is absent from cloud inventory",
            )),
        };
    };
    if !matches_resource(policy, row, volume)
        || row
            .aws_volume_id
            .as_ref()
            .is_some_and(|id| id != &volume.volume_id)
    {
        return Err(ReconcileError::Refused(
            "cloud identity or immutable disk properties changed",
        ));
    }
    if matches!(
        row.phase,
        VolumePhase::Requested | VolumePhase::Provisioning
    ) {
        return match (volume.state.as_str(), volume.attachments.is_empty()) {
            ("available", true) => Ok(Action::Observe(VolumePhase::Available)),
            ("creating", true) => Ok(Action::Observe(VolumePhase::Provisioning)),
            _ => Err(ReconcileError::Refused(
                "new disk has unexpected cloud state",
            )),
        };
    }
    if !volume.attachments.is_empty() {
        let [attachment] = volume.attachments.as_slice() else {
            return Err(ReconcileError::Refused("multiple cloud attachments"));
        };
        if Some(attachment.instance_id.as_str()) != row.instance_id.as_deref()
            || Some(attachment.device.as_str()) != row.device_name.as_deref()
            || !policy.instance_ids.contains(&attachment.instance_id)
        {
            return Err(ReconcileError::Refused(
                "attachment is not owned by the current execution",
            ));
        }
        return match row.phase {
            VolumePhase::Attaching if attachment.state == "attached" => {
                Ok(Action::Observe(VolumePhase::Attached))
            }
            VolumePhase::Attaching if attachment.state == "attaching" => Ok(Action::Wait),
            VolumePhase::Attached | VolumePhase::Releasing if attachment.state == "attached" => {
                Ok(Action::Wait)
            }
            VolumePhase::Detaching if row.released_epoch == Some(row.attachment_epoch) => {
                match attachment.state.as_str() {
                    "attached" => Ok(Action::Detach),
                    "detaching" => Ok(Action::Wait),
                    _ => Err(ReconcileError::Refused("unexpected detach state")),
                }
            }
            _ => Err(ReconcileError::Refused(
                "cloud attachment contradicts durable ownership",
            )),
        };
    }
    if volume.state != "available" {
        return Ok(Action::Wait);
    }
    match row.phase {
        VolumePhase::Available if row.delete_requested => Ok(Action::Delete),
        VolumePhase::Available => Ok(Action::Wait),
        VolumePhase::Attaching
            if !row.delete_requested
                && row
                    .instance_id
                    .as_ref()
                    .is_some_and(|id| policy.instance_ids.contains(id)) =>
        {
            Ok(Action::Attach)
        }
        VolumePhase::Detaching if row.released_epoch == Some(row.attachment_epoch) => {
            Ok(Action::FinishDetach)
        }
        VolumePhase::Deleting => Ok(Action::Delete),
        _ => Err(ReconcileError::Refused(
            "detached disk has unexpected durable state",
        )),
    }
}

async fn tick<T: QueryTransport>(
    pool: &PgPool,
    client: &Client<T>,
    policy: &Policy,
    managed_by: &str,
) -> Result<(), ReconcileError> {
    let mut tx = pool.begin().await?;
    let Some(mut row) = store::lock_next(&mut tx, managed_by).await? else {
        return Ok(());
    };
    if store::resource::release_deleted_owner(&mut *tx, row.id).await? {
        tx.commit().await?;
        return Ok(());
    }
    if store::request_orphan_deletion(&mut *tx, row.id).await? {
        row = store::get(&mut *tx, row.sandbox_id)
            .await?
            .ok_or(ReconcileError::Refused("locked volume disappeared"))?;
    }
    let result = async {
        let observed = inventory(client, &policy.tags()).await?;
        let candidates: Vec<_> = observed
            .iter()
            .filter(|volume| {
                row.aws_volume_id.as_ref() == Some(&volume.volume_id)
                    || volume.tags.get(LOGICAL_ID_TAG) == Some(&row.id.to_string())
            })
            .collect();
        if candidates.len() > 1 {
            return Err(ReconcileError::Refused(
                "multiple cloud disks claim the same logical owner",
            ));
        }
        // A tag-filtered inventory miss cannot prove deletion or authorize freeing
        // the budget. Look up every previously receipted disk by its exact ID.
        let exact = match row.aws_volume_id.as_deref() {
            Some(id) => client.describe_volume(id).await?,
            None => None,
        };
        if row.aws_volume_id.is_some() && exact.is_none() && !candidates.is_empty() {
            return Err(ReconcileError::Refused(
                "cloud inventory contradicts exact lookup",
            ));
        }
        if let (Some(candidate), Some(exact)) = (candidates.first(), &exact)
            && candidate.volume_id != exact.volume_id
        {
            return Err(ReconcileError::Refused(
                "cloud identity changed during inventory",
            ));
        }
        let cloud = if row.aws_volume_id.is_some() {
            exact.as_ref()
        } else {
            candidates.first().copied()
        };
        match plan(policy, &row, cloud)? {
            Action::Wait => {
                store::observe(
                    &mut *tx,
                    &row,
                    row.phase,
                    cloud.map(|v| v.volume_id.as_str()),
                    None,
                )
                .await?;
            }
            Action::Create => {
                let reservations: BTreeSet<_> = store::live_reservations(&mut *tx)
                    .await?
                    .into_iter()
                    .collect();
                if reservations.len() > policy.resource_limit
                    || observed.iter().any(|volume| {
                        !volume
                            .tags
                            .get(LOGICAL_ID_TAG)
                            .and_then(|id| Uuid::parse_str(id).ok())
                            .is_some_and(|id| reservations.contains(&id))
                    })
                {
                    return Err(ReconcileError::Refused(
                        "untracked disks or resource cap prevent creation",
                    ));
                }
                let mut tags = policy.tags();
                tags.insert(LOGICAL_ID_TAG.into(), row.id.to_string());
                if row.retained {
                    tags.insert(RETAINED_TAG.into(), "true".into());
                } else {
                    tags.insert(SANDBOX_ID_TAG.into(), row.sandbox_id.to_string());
                }
                let receipt = client
                    .create_encrypted_gp3_volume(&CreateEncryptedGp3Volume {
                        availability_zone: &row.availability_zone,
                        size_gib: 128,
                        client_token: &row.id.to_string(),
                        tags: &tags,
                    })
                    .await?;
                store::observe(
                    &mut *tx,
                    &row,
                    VolumePhase::Provisioning,
                    Some(&receipt.volume_id),
                    None,
                )
                .await?;
            }
            Action::Observe(phase) => {
                store::observe(
                    &mut *tx,
                    &row,
                    phase,
                    cloud.map(|v| v.volume_id.as_str()),
                    None,
                )
                .await?;
            }
            Action::Attach | Action::Detach => {
                let volume = cloud.ok_or(ReconcileError::Refused("cloud disk missing"))?;
                let instance = row
                    .instance_id
                    .as_deref()
                    .ok_or(ReconcileError::Refused("attachment target missing"))?;
                let device = row
                    .device_name
                    .as_deref()
                    .ok_or(ReconcileError::Refused("attachment device missing"))?;
                if row.phase == VolumePhase::Attaching {
                    client
                        .attach_volume(&volume.volume_id, instance, device)
                        .await?;
                } else {
                    client
                        .detach_volume(&volume.volume_id, instance, device)
                        .await?;
                }
                store::observe(&mut *tx, &row, row.phase, None, None).await?;
            }
            Action::FinishDetach => {
                store::finish_detach(&mut *tx, &row).await?;
            }
            Action::Delete => {
                let volume = cloud.ok_or(ReconcileError::Refused("cloud disk missing"))?;
                client.delete_volume(&volume.volume_id).await?;
                store::observe(&mut *tx, &row, VolumePhase::Deleting, None, None).await?;
            }
        }
        Ok::<_, ReconcileError>(())
    }
    .await;
    if let Err(error) = &result {
        store::observe(&mut *tx, &row, row.phase, None, Some(&error.to_string())).await?;
    }
    tx.commit().await?;
    result
}

pub(crate) async fn run(
    policy: Option<Policy>,
    pool: PgPool,
    managed_by: String,
    shutdown: CancellationToken,
) {
    let Some(policy) = policy else {
        shutdown.cancelled().await;
        return;
    };
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new(policy.region.clone()))
        .load()
        .await;
    let identity = pplx_space_aws_client::sts::Client::from_config(&config)
        .get_caller_identity()
        .await;
    match identity {
        Ok(identity) if identity.account == policy.account_id => {
            tracing::info!(account = %identity.account, caller = %identity.arn,
                experiment = %policy.experiment, "block-volume controller account verified");
        }
        result => {
            tracing::error!(
                ?result,
                "block-volume controller account fence refused startup"
            );
            return;
        }
    }
    let client = Client::from_config(&config);
    let mut interval = tokio::time::interval(TICK);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return,
            _ = interval.tick() => {
                // Two independent row locks bound concurrent work. A stalled
                // attachment never monopolizes all provisioning progress.
                let (a, b) = tokio::join!(
                    tick(&pool, &client, &policy, &managed_by),
                    tick(&pool, &client, &policy, &managed_by),
                );
                let scope = store::resource::controller_scope(&managed_by);
                let c = tick(&pool, &client, &policy, &scope).await;
                let d = independent::tick(&pool, &client, &policy, &managed_by).await;
                for result in [a, b, c, d] {
                    if let Err(error) = result {
                        tracing::warn!(%error, "block-volume reconciliation did not converge");
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;

//! Unbound resources have no invented sandbox or mount. Once reserved, the
//! existing node-plane state machine owns attachment and initialization.
use super::*;
use store::resource::{self, Resource};

pub(super) fn plan(
    policy: &Policy,
    row: &Resource,
    cloud: Option<&Volume>,
) -> Result<Action, ReconcileError> {
    if row.availability_zone != policy.availability_zone || row.size_gib != 128 {
        return Err(ReconcileError::Refused(
            "resource is outside the experiment envelope",
        ));
    }
    let Some(volume) = cloud else {
        return match row.phase {
            VolumePhase::Requested | VolumePhase::Provisioning if row.aws_volume_id.is_none() => {
                Ok(Action::Create)
            }
            _ if row.delete_requested && row.aws_volume_id.is_some() => {
                // The delete response can be lost before its phase is saved.
                // Exact-ID absence plus durable deletion intent is sufficient;
                // an inventory miss alone never reaches this branch.
                Ok(Action::Observe(VolumePhase::Deleted))
            }
            _ => Err(ReconcileError::Refused(
                "durable disk is absent from cloud inventory",
            )),
        };
    };
    if volume.availability_zone != row.availability_zone
        || !volume.encrypted
        || volume.volume_type != "gp3"
        || volume.size_gib != 128
        || volume.tags.get(LOGICAL_ID_TAG) != Some(&row.id.to_string())
        || volume.tags.get(RETAINED_TAG).map(String::as_str) != Some("true")
        || !policy
            .tags()
            .iter()
            .all(|(key, value)| volume.tags.get(key) == Some(value))
        || row
            .aws_volume_id
            .as_ref()
            .is_some_and(|id| id != &volume.volume_id)
        || !volume.attachments.is_empty()
    {
        return Err(ReconcileError::Refused(
            "independent disk identity or attachment changed",
        ));
    }
    match volume.state.as_str() {
        "creating" => Ok(Action::Observe(VolumePhase::Provisioning)),
        "available" if row.delete_requested => Ok(Action::Delete),
        "available" => Ok(Action::Observe(VolumePhase::Available)),
        "deleting" if row.delete_requested => Ok(Action::Observe(VolumePhase::Deleting)),
        _ => Err(ReconcileError::Refused(
            "independent disk has unexpected cloud state",
        )),
    }
}

pub(super) async fn tick<T: QueryTransport>(
    pool: &PgPool,
    client: &Client<T>,
    policy: &Policy,
    managed_by: &str,
) -> Result<(), ReconcileError> {
    let mut tx = pool.begin().await?;
    let Some(row) = resource::lock_next(&mut tx, managed_by).await? else {
        return Ok(());
    };
    let result = async {
        let observed = inventory(client, &policy.tags()).await?;
        let candidates: Vec<_> = observed
            .iter()
            .filter(|volume| {
                volume.tags.get(LOGICAL_ID_TAG) == Some(&row.id.to_string())
                    || row.aws_volume_id.as_ref() == Some(&volume.volume_id)
            })
            .collect();
        if candidates.len() > 1 {
            return Err(ReconcileError::Refused(
                "multiple disks claim an independent volume",
            ));
        }
        let exact = match row.aws_volume_id.as_deref() {
            Some(id) => client.describe_volume(id).await?,
            None => None,
        };
        if row.aws_volume_id.is_some()
            && candidates.first().is_some_and(|candidate| {
                exact
                    .as_ref()
                    .is_none_or(|disk| disk.volume_id != candidate.volume_id)
            })
        {
            return Err(ReconcileError::Refused(
                "cloud inventory contradicts exact lookup",
            ));
        }
        let cloud = if row.aws_volume_id.is_some() {
            exact.as_ref()
        } else {
            candidates.first().copied()
        };
        match plan(policy, &row, cloud)? {
            Action::Create => {
                let reservations: BTreeSet<_> = store::live_reservations(&mut *tx)
                    .await?
                    .into_iter()
                    .collect();
                if reservations.len() > policy.resource_limit
                    || observed.iter().any(|disk| {
                        !disk
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
                tags.insert(RETAINED_TAG.into(), "true".into());
                let receipt = client
                    .create_encrypted_gp3_volume(&CreateEncryptedGp3Volume {
                        availability_zone: &row.availability_zone,
                        size_gib: 128,
                        client_token: &row.id.to_string(),
                        tags: &tags,
                    })
                    .await?;
                resource::observe(
                    &mut *tx,
                    &row,
                    VolumePhase::Provisioning,
                    Some(&receipt.volume_id),
                    None,
                )
                .await?;
            }
            Action::Observe(phase) => {
                resource::observe(
                    &mut *tx,
                    &row,
                    phase,
                    cloud.map(|disk| disk.volume_id.as_str()),
                    None,
                )
                .await?;
            }
            Action::Delete => {
                let disk = cloud.ok_or(ReconcileError::Refused("cloud disk missing"))?;
                client.delete_volume(&disk.volume_id).await?;
                resource::observe(&mut *tx, &row, VolumePhase::Deleting, None, None).await?;
            }
            _ => return Err(ReconcileError::Refused("unbound volume cannot attach")),
        }
        Ok::<_, ReconcileError>(())
    }
    .await;
    if let Err(error) = &result {
        resource::observe(&mut *tx, &row, row.phase, None, Some(&error.to_string())).await?;
    }
    tx.commit().await?;
    result
}

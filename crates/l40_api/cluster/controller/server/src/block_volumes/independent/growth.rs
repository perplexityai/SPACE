//! Reconcile cloud capacity only. Attached disks still require VMM and guest
//! completion before their usable capacity can advance.
use super::*;
use pplx_space_aws_client::ec2::{VolumeModification, VolumeModificationState};
use std::num::NonZeroU32;

#[derive(Debug, PartialEq, Eq)]
enum GrowthAction {
    Submit,
    Wait,
    Complete,
}

fn plan_growth(
    row: &Resource,
    disk: &Volume,
    modification: Option<&VolumeModification>,
) -> Result<GrowthAction, ReconcileError> {
    if row.size_gib != 128
        || row.target_size_gib != Some(256)
        || row.resize_operation_id.is_none()
        || row.delete_requested
        || if row.sandbox_id.is_some() {
            row.phase != VolumePhase::Attached
                || disk.state != "in-use"
                || disk.attachments.len() != 1
                || !disk
                    .attachments
                    .first()
                    .is_some_and(|attachment| attachment.state == "attached")
        } else {
            row.phase != VolumePhase::Available
                || disk.state != "available"
                || !disk.attachments.is_empty()
        }
    {
        return Err(ReconcileError::Refused(
            "growth intent is no longer eligible",
        ));
    }
    let Some(modification) = modification else {
        return if disk.size_gib == 128 {
            Ok(GrowthAction::Submit)
        } else {
            Err(ReconcileError::Refused(
                "grown disk has no matching modification",
            ))
        };
    };
    if modification.volume_id != disk.volume_id
        || modification.original_size_gib.get() != 128
        || modification.target_size_gib.get() != 256
        || modification.original_volume_type != "gp3"
        || modification.target_volume_type != "gp3"
    {
        return Err(ReconcileError::Refused(
            "cloud modification contradicts growth intent",
        ));
    }
    match modification.state {
        VolumeModificationState::Failed => {
            Err(ReconcileError::Refused("cloud volume growth failed"))
        }
        VolumeModificationState::Modifying => Ok(GrowthAction::Wait),
        VolumeModificationState::Optimizing | VolumeModificationState::Completed => {
            if disk.size_gib == 256 {
                Ok(GrowthAction::Complete)
            } else {
                Ok(GrowthAction::Wait)
            }
        }
    }
}

pub(in crate::block_volumes) async fn reconcile<T: QueryTransport>(
    client: &Client<T>,
    row: &Resource,
    disk: &Volume,
) -> Result<bool, ReconcileError> {
    // Always observe before submitting: ModifyVolume has no idempotency token,
    // and the preceding request may have succeeded despite a lost response.
    let modification = client.describe_volume_modification(&disk.volume_id).await?;
    match plan_growth(row, disk, modification.as_ref())? {
        GrowthAction::Submit => {
            client
                .modify_volume_size(
                    &disk.volume_id,
                    NonZeroU32::new(256).expect("fixed nonzero target"),
                )
                .await?;
            Ok(false)
        }
        GrowthAction::Wait => Ok(false),
        GrowthAction::Complete => Ok(true),
    }
}

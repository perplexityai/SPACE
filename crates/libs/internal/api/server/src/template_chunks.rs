//! Optional chunk commitments follow the existing publication authorization
//! and transaction. Registering them grants no additional template access.

use pplx_space_common_core::{
    sql,
    types::{Digest, HashAlg},
};
use pplx_space_node_api_client::{
    chunk_representation::ChunkRepresentation, proto::TemplateChunkRepresentation,
};
use sqlx::types::Json;
use tonic::Status;
use uuid::Uuid;

use crate::{template_publication::storage_status, trusted_storage::TrustedTemplateStorageBuckets};

pub(super) fn validate(
    values: Vec<TemplateChunkRepresentation>,
    rootfs: &str,
    trusted: &TrustedTemplateStorageBuckets,
) -> Result<Vec<ChunkRepresentation>, Status> {
    if values.len() > 5 {
        return Err(Status::invalid_argument(
            "at most five chunk representations are supported",
        ));
    }
    let rootfs: Digest = rootfs
        .parse()
        .map_err(|_| Status::invalid_argument("invalid rootfs digest"))?;
    let mut result = Vec::with_capacity(values.len());
    for value in values {
        let representation = ChunkRepresentation::try_from(value)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        trusted
            .check(representation.storage_prefix().parsed())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        if rootfs.algorithm() == HashAlg::Blake3 && representation.image_blake3() != rootfs {
            return Err(Status::invalid_argument(
                "chunk representation image differs from release identity",
            ));
        }
        if result
            .iter()
            .any(|old: &ChunkRepresentation| old.chunk_size() == representation.chunk_size())
        {
            return Err(Status::invalid_argument("duplicate chunk size"));
        }
        if result
            .first()
            .is_some_and(|old| old.image_blake3() != representation.image_blake3())
        {
            return Err(Status::invalid_argument(
                "representations disagree on full-image BLAKE3",
            ));
        }
        result.push(representation);
    }
    result.sort_by_key(|value| value.chunk_size().bytes());
    Ok(result)
}

pub(super) async fn attach(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    release_id: Uuid,
    additions: &[ChunkRepresentation],
) -> Result<Vec<ChunkRepresentation>, Status> {
    // Serialize additions across publishers, including independent chunk sizes.
    let Json(mut existing): Json<Vec<ChunkRepresentation>> = sqlx::query_scalar(sql!(
        "SELECT chunk_representations FROM template_releases WHERE id = $1 FOR UPDATE"
    ))
    .bind(release_id)
    .fetch_one(&mut **transaction)
    .await
    .map_err(|error| storage_status("lock chunk representations", &error))?;
    let mut changed = false;
    for addition in additions {
        if existing
            .first()
            .is_some_and(|old| old.image_blake3() != addition.image_blake3())
        {
            return Err(Status::already_exists(
                "release has a different full-image BLAKE3",
            ));
        }
        if let Some(old) = existing
            .iter()
            .find(|old| old.chunk_size() == addition.chunk_size())
        {
            if old != addition {
                return Err(Status::already_exists(
                    "chunk size already has a different immutable representation",
                ));
            }
        } else {
            existing.push(addition.clone());
            changed = true;
        }
    }
    if changed {
        existing.sort_by_key(|value| value.chunk_size().bytes());
        super::template_releases_store::set_chunk_representations(
            &mut **transaction,
            release_id,
            &existing,
        )
        .await
        .map_err(|error| storage_status("attach chunk representations", &error))?;
    }
    Ok(existing)
}

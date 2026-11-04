//! Wire and bounded-sidecar counterparts of the storage inventory budget.

use pplx_space_common_core::inventory::{
    InventoryBudget, InventoryLimitExceeded, MAX_ENCODED_BYTES,
};
use tonic::{Response, Status};

pub(crate) fn exceeded(error: InventoryLimitExceeded) -> Status {
    Status::resource_exhausted(error.to_string())
}

pub(crate) fn response<T: prost::Message>(message: T) -> Result<Response<T>, Status> {
    InventoryBudget::new(1, MAX_ENCODED_BYTES)
        .charge(message.encoded_len())
        .map_err(exceeded)?;
    Ok(Response::new(message))
}

/// Unpaged inventory counterpart of [`response`] that reports, rather than
/// rejects, an oversized complete inventory. See `db::inventory_read`.
pub(crate) fn unary_list_response<T: prost::Message>(rpc: &'static str, message: T) -> Response<T> {
    let encoded_bytes = message.encoded_len();
    if encoded_bytes > MAX_ENCODED_BYTES {
        tracing::warn!(
            rpc,
            encoded_bytes,
            max_encoded_bytes = MAX_ENCODED_BYTES,
            "unary sandbox list response exceeds its encoded budget; serving the complete list"
        );
        pplx_space_metrics::sandbox::worker::unary_list_budget_exceeded(rpc, "encoded_bytes");
    }
    Response::new(message)
}

/// Read at most one bounded metadata document, even if it grows after stat.
pub(crate) fn read_sidecar(
    path: &std::path::Path,
    budget: &mut InventoryBudget,
) -> Result<Option<Vec<u8>>, Status> {
    use std::io::Read;
    let file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(Status::internal(format!("read snapshot metadata: {error}"))),
    };
    let mut bytes = Vec::new();
    file.take((MAX_ENCODED_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|error| Status::internal(format!("read snapshot metadata: {error}")))?;
    if bytes.len() > MAX_ENCODED_BYTES {
        return Err(exceeded(InventoryLimitExceeded));
    }
    budget.charge_bytes(bytes.len()).map_err(exceeded)?;
    Ok(Some(bytes))
}

#[cfg(test)]
mod tests;

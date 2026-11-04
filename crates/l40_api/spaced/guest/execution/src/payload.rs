//! Payload limits shared by native calls and the predecode transport adapter.
use std::sync::Arc;

use pplx_space_common_grpc::RequestPermit;
use pplx_space_metrics::payload::Budget;
use tokio::sync::Semaphore;
use tonic::{Request, Status};

pub(crate) fn check_message(message: &impl prost::Message, limit: usize) -> Result<(), Status> {
    if message.encoded_len() > limit {
        return Err(Status::out_of_range(
            "request exceeds the message size limit",
        ));
    }
    Ok(())
}

pub(crate) fn acquire(
    slots: &Arc<Semaphore>,
    weight: u32,
    budget: Budget,
    admitted: Option<RequestPermit>,
) -> Result<RequestPermit, Status> {
    RequestPermit::try_acquire_observed(slots.clone(), weight, admitted, budget).map_err(|_| {
        crate::refused(Status::resource_exhausted(
            "payload request capacity exhausted",
        ))
    })
}

pub(crate) fn admit<T: prost::Message>(
    request: &mut Request<T>,
    slots: &Arc<Semaphore>,
    weight: u32,
    budget: Budget,
    limit: usize,
) -> Result<RequestPermit, Status> {
    check_message(request.get_ref(), limit)?;
    acquire(
        slots,
        weight,
        budget,
        request.extensions_mut().remove::<RequestPermit>(),
    )
}

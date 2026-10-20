//! Payload limits shared by native calls and the predecode transport adapter.
use std::sync::Arc;

use pplx_space_common_grpc::RequestPermit;
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
    admitted: Option<RequestPermit>,
) -> Result<RequestPermit, Status> {
    RequestPermit::try_acquire(slots.clone(), weight, admitted).map_err(|_| {
        crate::refused(Status::resource_exhausted(
            "payload request capacity exhausted",
        ))
    })
}

pub(crate) fn admit<T: prost::Message>(
    request: &mut Request<T>,
    slots: &Arc<Semaphore>,
    weight: u32,
    limit: usize,
) -> Result<RequestPermit, Status> {
    check_message(request.get_ref(), limit)?;
    acquire(
        slots,
        weight,
        request.extensions_mut().remove::<RequestPermit>(),
    )
}

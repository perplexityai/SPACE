use futures_util::{Stream, StreamExt};
use tokio::sync::oneshot;
use tonic::Status;

/// Deliver buffered items before the producer's explicit completion. The
/// separate channel cannot block cleanup behind a full data queue. A producer
/// that exits without reporting completion must not turn partial output into EOF.
pub(super) fn with_completion<T>(
    items: impl Stream<Item = Result<T, Status>>,
    completion: oneshot::Receiver<Result<(), Status>>,
) -> impl Stream<Item = Result<T, Status>> {
    let terminal = futures_util::stream::once(completion).filter_map(|result| {
        std::future::ready(
            result
                .unwrap_or_else(|_| {
                    Err(Status::internal(
                        "filesystem stream producer stopped without completion",
                    ))
                })
                .err()
                .map(Err),
        )
    });
    items.chain(terminal)
}

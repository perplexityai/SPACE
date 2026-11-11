//! Bounded synchronous database and filesystem execution for service callers.
//!
//! Transactions stay inside one closure. An admitted RPC's drain lease follows
//! blocking work even if its waiter disappears. Callers must also move any
//! operation-specific exclusion guards into the closure when cancellation
//! could otherwise release them before the work finishes.
//! Bulk artifact work has a separate budget: a slow copy or directory removal
//! must not occupy the slots used by cleanup admission and completion.

use std::sync::Arc;

use tonic::Status;

use super::SandboxManagerService;

pub(super) const ARTIFACT_IO_CONCURRENCY: usize = 2;
pub(super) const EXPORT_IO_CONCURRENCY: usize = 2;

impl SandboxManagerService {
    pub(super) async fn blocking_export_io<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&Self) -> Result<T, Status> + Send + 'static,
    ) -> Result<T, Status> {
        self.run_blocking_io(Arc::clone(&self.export_io), operation)
            .await
    }

    pub(super) async fn blocking_artifact_io<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&Self) -> Result<T, Status> + Send + 'static,
    ) -> Result<T, Status> {
        self.run_blocking_io(Arc::clone(&self.artifact_io), operation)
            .await
    }

    pub(super) async fn storage<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&crate::db::Connection) -> Result<T, Status> + Send + 'static,
    ) -> Result<T, Status> {
        self.blocking_io(move |service| {
            let db = service.db_conn()?;
            operation(&db)
        })
        .await
    }

    pub(super) async fn blocking_io<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&Self) -> Result<T, Status> + Send + 'static,
    ) -> Result<T, Status> {
        self.run_blocking_io(Arc::clone(&self.cleanup_io), operation)
            .await
    }

    async fn run_blocking_io<T: Send + 'static>(
        &self,
        slots: Arc<tokio::sync::Semaphore>,
        operation: impl FnOnce(&Self) -> Result<T, Status> + Send + 'static,
    ) -> Result<T, Status> {
        let writer = super::drain::inherited_state_writer(&self.drain_state);
        let permit = slots
            .acquire_owned()
            .await
            .map_err(|_| Status::unavailable("blocking storage closed"))?;
        let service = self.clone();
        let span = tracing::Span::current();
        tokio::task::spawn_blocking(move || {
            span.in_scope(move || {
                let _writer = writer;
                let _permit = permit;
                operation(&service)
            })
        })
        .await
        .map_err(|error| Status::internal(format!("blocking storage task: {error}")))?
    }
}

#[cfg(test)]
mod tests;

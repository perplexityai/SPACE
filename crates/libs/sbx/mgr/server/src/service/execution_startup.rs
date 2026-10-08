use super::*;

impl SandboxManagerService {
    /// Validate local execution ownership under the process mutex and recovery
    /// work gate. Dependency availability is handled before recovery admission.
    pub(crate) async fn prepare_execution_authority(&self) -> Result<(), Status> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let db = db
                .lock()
                .map_err(|_| Status::unavailable("sandbox database mutex poisoned"))?;
            let unadopted = crate::db::unbound_node_executions(&db).map_err(|error| {
                Status::unavailable(format!("read execution preflight: {error}"))
            })?;
            pplx_space_node_mgr_client::execution_fence::execution_preflight_result(&unadopted)
        })
        .await
        .map_err(|error| Status::unavailable(format!("execution preflight task: {error}")))?
    }
}

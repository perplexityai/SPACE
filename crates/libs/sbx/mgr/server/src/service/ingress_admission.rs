//! Per-sandbox service-port ingress admission and permit release.

use super::*;

#[derive(Clone, Default)]
pub(super) struct ServicePortIngressAdmission {
    pub(super) slots: Arc<dashmap::DashMap<SandboxId, Arc<tokio::sync::Semaphore>>>,
}

impl ServicePortIngressAdmission {
    pub(super) fn try_acquire(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<ServicePortIngressAdmissionPermit, Status> {
        let slots = self
            .slots
            .entry(sandbox_id)
            .or_insert_with(|| {
                Arc::new(tokio::sync::Semaphore::new(
                    pplx_space_net_gateway_client::MAX_ACTIVE_SERVICE_PORT_INGRESS_STREAMS,
                ))
            })
            .clone();
        let permit = slots.clone().try_acquire_owned().map_err(|_| {
            pplx_space_metrics::sandbox::worker::service_port_ingress_rejected(
                pplx_space_metrics::sandbox::worker::ServicePortIngressRejectionReason::Capacity,
            );
            pplx_space_common_grpc::uds_per_sandbox_limit_exceeded_status(
                "ServicePortIngress per-sandbox concurrency limit exceeded",
            )
        })?;
        Ok(ServicePortIngressAdmissionPermit {
            permit: Some(permit),
            slots: Some(slots),
            admission: self.clone(),
            sandbox_id,
        })
    }

    #[cfg(test)]
    pub(super) fn available_permits(&self, sandbox_id: SandboxId) -> usize {
        self.slots.get(&sandbox_id).map_or(
            pplx_space_net_gateway_client::MAX_ACTIVE_SERVICE_PORT_INGRESS_STREAMS,
            |slots| slots.available_permits(),
        )
    }

    #[cfg(test)]
    pub(super) fn tracked_sandboxes(&self) -> usize {
        self.slots.len()
    }
}

pub(super) struct ServicePortIngressAdmissionPermit {
    pub(super) permit: Option<tokio::sync::OwnedSemaphorePermit>,
    pub(super) slots: Option<Arc<tokio::sync::Semaphore>>,
    pub(super) admission: ServicePortIngressAdmission,
    pub(super) sandbox_id: SandboxId,
}

impl Drop for ServicePortIngressAdmissionPermit {
    fn drop(&mut self) {
        drop(self.permit.take());
        let Some(slots_arc) = self.slots.take() else {
            return;
        };
        let slots = Arc::downgrade(&slots_arc);
        drop(slots_arc);
        self.admission
            .slots
            .remove_if(&self.sandbox_id, |_, current| {
                std::sync::Weak::ptr_eq(&Arc::downgrade(current), &slots)
                    && current.available_permits()
                        == pplx_space_net_gateway_client::MAX_ACTIVE_SERVICE_PORT_INGRESS_STREAMS
                    && Arc::strong_count(current) == 1
            });
    }
}

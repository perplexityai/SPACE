use super::*;

impl SandboxManagerService {
    pub(super) async fn list_auto_lifecycle_delete_receipts(
        &self,
        request: Request<ListAutoLifecycleDeleteReceiptsRequest>,
    ) -> Result<Response<ListAutoLifecycleDeleteReceiptsResponse>, Status> {
        self.authorize_peer(
            &request,
            "ListAutoLifecycleDeleteReceipts",
            ALLOW_AUTO_LIFECYCLE_DELETE_RECEIPTS,
        )?;
        let receipts = {
            let db = self
                .db
                .lock()
                .map_err(|_| Status::internal("database mutex poisoned"))?;
            crate::db::auto_lifecycle_delete_receipts::list_completed_receipts(&db)
                .map_err(|error| Status::internal(format!("list delete receipts: {error}")))?
        };
        Ok(Response::new(ListAutoLifecycleDeleteReceiptsResponse {
            receipts: receipts
                .into_iter()
                .map(|receipt| AutoLifecycleDeleteReceipt {
                    receipt_id: receipt.receipt_id.to_string(),
                    sandbox_id: receipt.sandbox_id.to_string(),
                    execution_generation: receipt.execution_generation.get(),
                    route_activation_id: receipt.route_activation_id.to_string(),
                    auto_lifecycle_policy_fingerprint: receipt.policy_fingerprint.to_string(),
                })
                .collect(),
        }))
    }

    pub(super) async fn acknowledge_auto_lifecycle_delete_receipts(
        &self,
        request: Request<AcknowledgeAutoLifecycleDeleteReceiptsRequest>,
    ) -> Result<Response<AcknowledgeAutoLifecycleDeleteReceiptsResponse>, Status> {
        self.authorize_peer(
            &request,
            "AcknowledgeAutoLifecycleDeleteReceipts",
            ALLOW_AUTO_LIFECYCLE_DELETE_RECEIPTS,
        )?;
        let receipt_ids = request
            .into_inner()
            .receipt_ids
            .into_iter()
            .map(|receipt_id| {
                receipt_id.parse().map_err(|error| {
                    Status::invalid_argument(format!("invalid receipt id: {error}"))
                })
            })
            .collect::<Result<std::collections::BTreeSet<_>, _>>()?
            .into_iter()
            .collect::<Vec<_>>();
        let acknowledged = {
            let db = self
                .db
                .lock()
                .map_err(|_| Status::internal("database mutex poisoned"))?;
            crate::db::auto_lifecycle_delete_receipts::acknowledge_completed_receipts(
                &db,
                &receipt_ids,
            )
            .map_err(|error| Status::internal(format!("acknowledge delete receipts: {error}")))?
        };
        Ok(Response::new(
            AcknowledgeAutoLifecycleDeleteReceiptsResponse {
                acknowledged_receipt_ids: acknowledged
                    .into_iter()
                    .map(|receipt_id| receipt_id.to_string())
                    .collect(),
            },
        ))
    }
}

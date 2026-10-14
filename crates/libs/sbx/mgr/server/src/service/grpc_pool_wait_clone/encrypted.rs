use super::*;

impl SandboxManagerService {
    pub(super) async fn adopt_claimed_pool_sandbox(
        &self,
        req: AdoptPoolSandboxRequest,
        owner: SandboxOwner,
    ) -> Result<Response<AdoptPoolSandboxResponse>, Status> {
        let claimed = req.claimed_sandbox.as_ref().ok_or_else(|| {
            Status::invalid_argument("encrypted adoption requires an exact sandbox and claim")
        })?;
        let sandbox_id = claimed
            .sandbox_id
            .parse::<SandboxId>()
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let claim_id = Uuid::parse_str(&claimed.claim_id)
            .map_err(|_| Status::invalid_argument("invalid claim ID"))?;
        let org_id = claimed
            .expected_org_id
            .parse::<pplx_space_common_core::types::OrgId>()
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let claim = crate::db::pool_adoption::PoolClaim {
            id: claim_id,
            org_id,
            volume_encryption: claimed.volume_encryption,
        };
        let id = sandbox_id.to_string();
        let _wake =
            self.auto_lifecycle.wake_lock(&id).try_lock().map_err(|_| {
                Status::unavailable("pool member has an active lifecycle operation")
            })?;
        let sandbox = self.lookup_sandbox(&id)?;
        if sandbox.template != req.template
            || req
                .content_hash
                .as_ref()
                .is_some_and(|hash| sandbox.template_hash.as_ref() != Some(hash))
        {
            return Err(Status::failed_precondition("pool member template changed"));
        }
        let env = sanitize_process_env(req.env.into_iter().collect(), "pool_adopt");
        {
            let db = self.db_conn()?;
            if !crate::db::pool_adoption::begin(&db, &id, claim, &owner, &env)
                .map_err(|e| Status::internal(format!("claim encryptable pool member: {e}")))?
            {
                return Err(Status::failed_precondition(
                    "pool member is not live, encryptable, or owned by this claim",
                ));
            }
        }
        let response = AdoptPoolSandboxResponse {
            sandbox_id: id.clone(),
            claim_id: Some(claim_id.to_string()),
            volume_encryption: claimed.volume_encryption,
            route_identity: space_api_route_identity_to_wire(&sandbox.space_api_route_identity),
        };
        if !crate::db::pool_adoption::guest_access_blocked(&*self.db_conn()?, &id)
            .map_err(|e| Status::internal(format!("read pool adoption completion: {e}")))?
        {
            return Ok(Response::new(response));
        }
        if sandbox.state == SandboxState::Started {
            self.pause(internal_request(crate::proto::PauseRequest {
                sandbox_id: id.clone(),
            }))
            .await?;
        }
        if claimed.volume_encryption {
            let fence = super::super::cleanup_support::read_node_execution_fence(&self.db, &id)?
                .ok_or_else(|| Status::failed_precondition("pool member has no node execution"))?;
            let mut node = self
                .node_manager
                .connect()
                .await
                .map_err(|e| Status::unavailable(format!("connect node manager: {e}")))?;
            let prepared = node
                .prepare_encrypted_rootfs(
                    pplx_space_node_mgr_client::proto::PrepareEncryptedRootfsRequest {
                        sandbox_id: id.clone(),
                        execution_fence: Some(fence.into()),
                        restore_cow_source: None,
                        pool_adoption: Some(
                            pplx_space_node_mgr_client::proto::PoolRootfsAdoption {
                                claim_id: claim_id.to_string(),
                                expected_org_id: org_id.to_string(),
                            },
                        ),
                    },
                )
                .await?
                .into_inner();
            if prepared.adopted_claim_id.as_deref() != Some(claimed.claim_id.as_str()) {
                return Err(Status::failed_precondition(
                    "node manager did not acknowledge pool adoption",
                ));
            }
        }
        {
            let db = self.db_conn()?;
            if !crate::db::pool_adoption::complete(&db, &id, claim)
                .map_err(|e| Status::internal(format!("complete encrypted adoption: {e}")))?
            {
                return Err(Status::failed_precondition(
                    "pool member changed during encryption",
                ));
            }
        }
        Ok(Response::new(response))
    }

    pub(in crate::service) fn require_completed_pool_adoption(
        &self,
        id: &str,
    ) -> Result<(), Status> {
        let db = self.db_conn()?;
        if crate::db::pool_adoption::incomplete(&db, id)
            .map_err(|e| Status::internal(format!("read pool adoption: {e}")))?
        {
            return Err(Status::failed_precondition(
                "pool encryption adoption is incomplete",
            ));
        }
        Ok(())
    }

    pub(in crate::service) fn is_encryptable_pool_member(&self, id: &str) -> Result<bool, Status> {
        let db = self.db_conn()?;
        crate::db::pool_adoption::guest_access_blocked(&db, id)
            .map_err(|e| Status::internal(format!("read encryptable pool membership: {e}")))
    }
}

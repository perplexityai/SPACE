use super::*;

impl SandboxManagerService {
    /// Only fresh initializations consult the flag. Existing host decisions,
    /// including explicit legacy, and inherited fork decisions always win.
    pub(super) async fn initial_trust_enrollment(
        &self,
        sandbox: &crate::db::SandboxRow,
    ) -> Result<crate::split_trust::Enrollment, Status> {
        let cache_root = std::path::Path::new(CACHE_DIR);
        #[cfg(test)]
        let cache_root = self.create_cache_dir.as_deref().unwrap_or(cache_root);
        if let Some(existing) = crate::split_trust::enrollment(cache_root, &sandbox.id)? {
            crate::split_trust::require_support(existing.is_scoped())?;
            return Ok(existing);
        }
        if crate::split_trust::initial_enrollment(cache_root, &sandbox.id, &self.feature_flags)? {
            self.fresh_scoped_enrollment(&sandbox.id).await
        } else {
            Ok(crate::split_trust::Enrollment::Legacy)
        }
    }

    pub(super) async fn fresh_scoped_enrollment(
        &self,
        sandbox_id: &str,
    ) -> Result<crate::split_trust::Enrollment, Status> {
        crate::split_trust::require_support(true)?;
        #[cfg(feature = "split-trust-prototype")]
        {
            let identity = pplx_space_credmgr_client::x509::parse_sandbox_id(sandbox_id)
                .map_err(|_| Status::invalid_argument("invalid sandbox identity"))?;
            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                let mut credentials = self
                    .credential_manager_endpoint
                    .connect()
                    .await
                    .map_err(|_| Status::unavailable("node credential agent is unavailable"))?;
                let anchor =
                    crate::split_trust::public_anchor(&mut credentials, identity, None).await?;
                Ok(crate::split_trust::Enrollment::Scoped(anchor.key_id))
            })
            .await
            .map_err(|_| Status::deadline_exceeded("sandbox enrollment timed out"))?
        }
        #[cfg(not(feature = "split-trust-prototype"))]
        {
            let _ = sandbox_id;
            Err(Status::failed_precondition(
                "scoped trust is not compiled in",
            ))
        }
    }
}

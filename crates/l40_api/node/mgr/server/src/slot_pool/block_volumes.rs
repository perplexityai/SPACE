use super::*;
use pplx_space_execution_common::proto::BlockVolumeBinding;
use prost::Message;

pub(crate) struct RetainedBlockBinding {
    pub execution_id: String,
    pub binding: BlockVolumeBinding,
}

impl NetworkPool {
    pub(crate) fn retained_block_binding(
        &self,
        sandbox: &str,
    ) -> Result<Option<RetainedBlockBinding>, SlotStoreError> {
        let db = self.db.lock().map_err(|_| SlotStoreError::LockPoisoned)?;
        pplx_space_node_mgr_store::retained_block_binding(&db, sandbox)?
            .map(|row| {
                let binding =
                    BlockVolumeBinding::decode(row.binding.as_slice()).map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            1,
                            rusqlite::types::Type::Blob,
                            Box::new(error),
                        )
                    })?;
                Ok(RetainedBlockBinding {
                    execution_id: row.execution_id,
                    binding,
                })
            })
            .transpose()
    }

    pub(crate) fn save_retained_block_binding(
        &self,
        sandbox: &str,
        execution: &str,
        binding: &BlockVolumeBinding,
    ) -> Result<bool, SlotStoreError> {
        let db = self.db.lock().map_err(|_| SlotStoreError::LockPoisoned)?;
        Ok(pplx_space_node_mgr_store::save_retained_block_binding(
            &db,
            sandbox,
            execution,
            &binding.encode_to_vec(),
        )?)
    }

    pub(crate) fn clear_retained_block_binding(
        &self,
        sandbox: &str,
        execution: &str,
    ) -> Result<(), SlotStoreError> {
        let db = self.db.lock().map_err(|_| SlotStoreError::LockPoisoned)?;
        pplx_space_node_mgr_store::clear_retained_block_binding(&db, sandbox, execution)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pplx_space_common_core::types::SandboxActivationAttemptId;
    use pplx_space_node_mgr_client::execution_fence::ExecutionFence;

    #[test]
    fn retained_block_volume_release_receipt_outlives_execution_and_fences_replacement() {
        let pool = NetworkPool::open_in_memory_for_test(NetworkPoolConfig::new(1));
        let sandbox = SandboxId::new().to_string();
        let old = SandboxActivationAttemptId::new();
        let new = SandboxActivationAttemptId::new();
        let binding = BlockVolumeBinding {
            aws_volume_id: "vol-00000000000000001".into(),
            ..Default::default()
        };
        assert!(
            !pool
                .save_retained_block_binding(&sandbox, &old.to_string(), &binding)
                .unwrap()
        );
        pool.bind_execution(&sandbox, ExecutionFence::new(old, None))
            .unwrap();
        assert!(
            pool.save_retained_block_binding(&sandbox, &old.to_string(), &binding)
                .unwrap()
        );
        pool.retire_execution(&sandbox, old).unwrap();
        assert_eq!(
            pool.retained_block_binding(&sandbox)
                .unwrap()
                .unwrap()
                .binding,
            binding
        );
        pool.bind_execution(&sandbox, ExecutionFence::new(new, Some(old)))
            .unwrap();
        assert!(
            !pool
                .save_retained_block_binding(&sandbox, &new.to_string(), &binding)
                .unwrap()
        );
        pool.clear_retained_block_binding(&sandbox, &new.to_string())
            .unwrap();
        assert_eq!(
            pool.retained_block_binding(&sandbox)
                .unwrap()
                .unwrap()
                .execution_id,
            old.to_string()
        );
        pool.clear_retained_block_binding(&sandbox, &old.to_string())
            .unwrap();
        assert!(
            pool.save_retained_block_binding(&sandbox, &new.to_string(), &binding)
                .unwrap()
        );
        assert!(
            !pool
                .save_retained_block_binding(&sandbox, &old.to_string(), &binding)
                .unwrap()
        );
    }
}

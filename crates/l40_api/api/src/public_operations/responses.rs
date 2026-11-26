use super::*;

#[derive(Debug, thiserror::Error)]
pub(crate) enum ResponseAdmissionError {
    #[error(transparent)]
    Store(#[from] PublicOperationStoreError),
    #[error(transparent)]
    Sql(#[from] sqlx::Error),
    #[error("idempotency key was already used with a different request")]
    Conflict,
}

impl From<pplx_space_postgres_pool::DbError> for ResponseAdmissionError {
    fn from(error: pplx_space_postgres_pool::DbError) -> Self {
        Self::from(sqlx::Error::from(error))
    }
}

pub(crate) struct PublicResponseAdmission {
    tx: Transaction<'static, Postgres>,
    org_id: OrgId,
    response: Option<serde_json::Value>,
}

impl PublicOperationStore {
    pub(crate) async fn begin_response_admission(
        &self,
        org_id: OrgId,
        idempotency: &CreatePublicOperationIdempotency<'_>,
    ) -> Result<PublicResponseAdmission, ResponseAdmissionError> {
        let mut tx = self.pool.begin().await?;
        lock_idempotency_key(&mut tx, org_id, ApiProduct::Native, idempotency).await?;
        let row = operation_store::idempotency::find_live_native_reply(
            &mut *tx,
            idempotency.store_key(org_id),
        )
        .await?
        .map(|row| (row.request_fingerprint, row.response_json));
        let response = match row {
            Some((fingerprint, Some(response)))
                if fingerprint == idempotency.request_fingerprint.as_str() =>
            {
                Some(response.0)
            }
            Some(_) => return Err(ResponseAdmissionError::Conflict),
            None => None,
        };
        Ok(PublicResponseAdmission {
            tx,
            org_id,
            response,
        })
    }
}

impl PublicResponseAdmission {
    pub(crate) fn response(&self) -> Option<&serde_json::Value> {
        self.response.as_ref()
    }

    pub(crate) async fn complete(
        mut self,
        idempotency: &CreatePublicOperationIdempotency<'_>,
        response: serde_json::Value,
    ) -> Result<(), PublicOperationStoreError> {
        delete_expired_idempotency_record_tx(
            &mut self.tx,
            self.org_id,
            ApiProduct::Native,
            idempotency.method,
            idempotency.path_template,
            idempotency.key,
        )
        .await?;
        operation_store::idempotency::insert_reply(
            &mut *self.tx,
            Uuid::now_v7(),
            ApiProduct::Native.as_str(),
            idempotency.store_key(self.org_id),
            idempotency.request_fingerprint.as_str(),
            &response,
        )
        .await?;
        self.tx.commit().await?;
        Ok(())
    }
}

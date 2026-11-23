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
        let row = sqlx::query_as::<_, (String, Option<Json<serde_json::Value>>)>(sql!(
            "SELECT request_fingerprint, response_json FROM public_idempotency_records
             WHERE org_id = $1 AND api_product = 'native' AND method = $2
               AND path_template = $3 AND idempotency_key = $4 AND expires_at > NOW()"
        ))
        .bind(org_id)
        .bind(idempotency.method.as_str())
        .bind(idempotency.path_template.as_str())
        .bind(idempotency.key.as_str())
        .fetch_optional(&mut *tx)
        .await?;
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

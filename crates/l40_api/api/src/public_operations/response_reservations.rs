use super::*;

/// A response-only idempotency key whose work cannot be held in a
/// transaction: the request commits a pending reservation before its side
/// effect, then records the response in its place.
///
/// Holding the key's lock and record in an open transaction across a slow
/// external call loses both, with nothing recorded, when the caller
/// disconnects, Postgres ends the idle transaction, or the final write fails;
/// the caller's retry then repeats the side effect. A committed reservation
/// survives all three, and its replays never repeat the work:
///
/// - a completed record answers with the recorded response;
/// - a pending one younger than the reservation lifetime may still be in
///   flight;
/// - a pending one older than that, or one its request noted as unresolved,
///   has an unknown outcome, which only the operator can settle, until the
///   record itself expires. The work may still be running: the lifetime is
///   the caller's estimate of how long the work takes, not a bound on it.
///
/// A request whose work may have happened without a result to record notes
/// its reservation as unresolved, with what it learned, such as a job ID, so
/// replays name it at once ([`PublicOperationStore::note_unresolved_response`]).
/// A panic, a process exit or a failed note leaves the reservation pending
/// without one, and replays report it unknown once the lifetime passes.
///
/// The record's `response_json` holds a [`StoredReservation`], so this API
/// owns every record under the path templates it is used for; those paths
/// must never meet [`PublicResponseAdmission`] or `create_operation`.
pub(crate) enum ResponseReservation {
    /// This request holds the key. Complete or release the reservation.
    Reserved(ReservationId),
    /// An earlier request recorded this response.
    Completed(serde_json::Value),
    /// An earlier request holds the key, is still within its lifetime, and
    /// has noted nothing, so it may still be in flight.
    InProgress { reserved_at: OffsetDateTime },
    /// An earlier request reserved the key and noted it unresolved, or
    /// recorded nothing within the lifetime. `known` is what it noted, if
    /// anything.
    OutcomeUnknown {
        reserved_at: OffsetDateTime,
        known: Option<serde_json::Value>,
    },
}

/// The pending record a [`ResponseReservation::Reserved`] request owns.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ReservationId(Uuid);

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "reservation", rename_all = "snake_case", deny_unknown_fields)]
enum StoredReservation {
    Pending {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        known: Option<serde_json::Value>,
    },
    Completed {
        response: serde_json::Value,
    },
}

impl StoredReservation {
    /// The record a reservation starts as, and the only one it can be
    /// completed or released from.
    const RESERVED: Self = Self::Pending { known: None };
}

impl PublicOperationStore {
    /// Reserve `idempotency` for a request whose work starts once this
    /// returns [`ResponseReservation::Reserved`], or report what an earlier
    /// request with the key recorded. A pending reservation older than
    /// `lifetime` is reported as of unknown outcome; pick one that outlasts
    /// the work as far as it can be bounded.
    pub(crate) async fn reserve_response(
        &self,
        org_id: OrgId,
        api_product: ApiProduct,
        idempotency: &CreatePublicOperationIdempotency<'_>,
        lifetime: Duration,
    ) -> Result<ResponseReservation, ResponseAdmissionError> {
        let mut tx = self.pool.begin().await?;
        lock_idempotency_key(&mut tx, org_id, api_product, idempotency).await?;
        let row = operation_store::idempotency::find_live_reservation(
            &mut *tx,
            api_product.as_str(),
            idempotency.store_key(org_id),
            lifetime.as_secs_f64(),
        )
        .await?;
        if let Some(row) = row {
            if row.request_fingerprint != idempotency.request_fingerprint.as_str() {
                return Err(ResponseAdmissionError::Conflict);
            }
            let stored = row
                .response_json
                .map(serde_json::from_value::<StoredReservation>);
            return match stored {
                Some(Ok(StoredReservation::Completed { response })) => {
                    Ok(ResponseReservation::Completed(response))
                }
                Some(Ok(StoredReservation::Pending { known: None }))
                    if !row.reservation_expired =>
                {
                    Ok(ResponseReservation::InProgress {
                        reserved_at: row.created_at,
                    })
                }
                Some(Ok(StoredReservation::Pending { known })) => {
                    Ok(ResponseReservation::OutcomeUnknown {
                        reserved_at: row.created_at,
                        known,
                    })
                }
                // Not a reservation record: another writer owns this key, so
                // nothing recorded here may be replayed or overwritten.
                Some(Err(_)) | None => Err(ResponseAdmissionError::Conflict),
            };
        }
        delete_expired_idempotency_record_tx(
            &mut tx,
            org_id,
            api_product,
            idempotency.method,
            idempotency.path_template,
            idempotency.key,
        )
        .await?;
        let id = Uuid::now_v7();
        operation_store::idempotency::insert_reply(
            &mut *tx,
            id,
            api_product.as_str(),
            idempotency.store_key(org_id),
            idempotency.request_fingerprint.as_str(),
            &StoredReservation::RESERVED,
        )
        .await?;
        tx.commit().await?;
        Ok(ResponseReservation::Reserved(ReservationId(id)))
    }

    /// Record `response` for a reservation this request holds. `false` means
    /// the reservation was no longer as reserved, so nothing was recorded.
    pub(crate) async fn complete_response_reservation(
        &self,
        reservation: ReservationId,
        response: serde_json::Value,
    ) -> Result<bool, sqlx::Error> {
        operation_store::idempotency::replace_recorded_reply(
            &self.pool,
            reservation.0,
            &StoredReservation::RESERVED,
            &StoredReservation::Completed { response },
        )
        .await
        .map_err(sqlx::Error::from)
    }

    /// Keep a reservation whose work may have happened pending for good,
    /// noting `known` for its replays, which report its outcome unknown from
    /// then on. It can no longer be completed or released, so no replay
    /// repeats the work while the record lives.
    /// `false` means it was no longer as reserved.
    pub(crate) async fn note_unresolved_response(
        &self,
        reservation: ReservationId,
        known: serde_json::Value,
    ) -> Result<bool, sqlx::Error> {
        operation_store::idempotency::replace_recorded_reply(
            &self.pool,
            reservation.0,
            &StoredReservation::RESERVED,
            &StoredReservation::Pending { known: Some(known) },
        )
        .await
        .map_err(sqlx::Error::from)
    }

    /// Withdraw a reservation whose work did not start, so the key can be
    /// retried. `false` means it was no longer as reserved.
    pub(crate) async fn release_response_reservation(
        &self,
        reservation: ReservationId,
    ) -> Result<bool, sqlx::Error> {
        operation_store::idempotency::delete_recorded_reply(
            &self.pool,
            reservation.0,
            &StoredReservation::RESERVED,
        )
        .await
        .map_err(sqlx::Error::from)
    }
}

//! Idempotent operator mutations. A receipt belongs to an actor and an
//! idempotency key, not to a route, and records the first result returned
//! for them.
//!
//! A mutation runs in one caller-owned transaction at `READ COMMITTED`,
//! Postgres's default:
//!
//! 1. The caller authorizes the actor, taken from trusted authentication and
//!    never from the request body, for this operation and resource.
//! 2. [`claim_receipt`] serializes the actor and key, then reads any receipt.
//!    A [`Claim::Replay`] or [`Claim::Conflict`] ends the request without
//!    mutating.
//! 3. On [`Claim::New`] only, the caller checks today's preconditions, such as
//!    a rollout's generation, and mutates.
//! 4. [`insert_receipt`] records the result, before or after the mutation's
//!    rows: a rollout's header references its creation receipt, so that one
//!    goes first. Both commit together or not at all.
//!
//! Lock order: the receipt key's lock comes before the firewall control row
//! and rollout header locks. No consumer may take them in reverse order, or
//! two mutations can deadlock.

use pplx_space_sql_locator::sql;
use sqlx::types::Json;
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::RequestFingerprint;

/// One request as its receipt names it.
#[derive(Clone, Copy, Debug)]
pub struct ReceiptRequest<'a> {
    /// From trusted authentication context: 1 to 1024 characters.
    pub actor: &'a str,
    /// 1 to 255 characters.
    pub idempotency_key: &'a str,
    pub fingerprint: &'a RequestFingerprint,
}

/// What [`claim_receipt`] found for a request's actor and key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Claim {
    /// No receipt: check preconditions, mutate and [`insert_receipt`].
    New,
    /// The same request already ran; return its stored result unchanged.
    Replay {
        receipt_id: Uuid,
        result: serde_json::Value,
    },
    /// The key already answered a different request.
    Conflict,
}

/// Serialize with every other claim of `request`'s actor and key until `tx`
/// ends, then read their receipt. Call it after authorization and before
/// reading any state a precondition checks, so an exact retry replays even
/// after that state moves on. The read is a separate statement after the
/// lock, so at `READ COMMITTED` a waiter sees what the holder committed.
pub async fn claim_receipt(
    tx: &mut Transaction<'_, Postgres>,
    request: &ReceiptRequest<'_>,
) -> Result<Claim, sqlx::Error> {
    // Requests that share a lock key, by a hash collision or because their
    // actor and key join alike, only wait for each other; the read still
    // tells them apart.
    sqlx::query(sql!(
        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))"
    ))
    .bind(format!(
        "firewall_rules_mutation_receipts/{}/{}",
        request.actor, request.idempotency_key
    ))
    .execute(&mut **tx)
    .await?;
    let receipt = sqlx::query_as::<_, (Uuid, String, Json<serde_json::Value>)>(sql!(
        "SELECT id, request_fingerprint, result
         FROM firewall_rules_mutation_receipts
         WHERE actor = $1 AND idempotency_key = $2"
    ))
    .bind(request.actor)
    .bind(request.idempotency_key)
    .fetch_optional(&mut **tx)
    .await?;
    Ok(match receipt {
        None => Claim::New,
        Some((receipt_id, fingerprint, Json(result)))
            if fingerprint == request.fingerprint.as_str() =>
        {
            Claim::Replay { receipt_id, result }
        }
        Some(_) => Claim::Conflict,
    })
}

/// Record `result` as the answer to `request`, in `tx` after its
/// [`claim_receipt`] returned [`Claim::New`]. Doesn't commit. The unique
/// actor and key is the last guard: a second receipt for them fails.
pub async fn insert_receipt(
    tx: &mut Transaction<'_, Postgres>,
    request: &ReceiptRequest<'_>,
    receipt_id: Uuid,
    result: &serde_json::Value,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "INSERT INTO firewall_rules_mutation_receipts
             (id, actor, idempotency_key, request_fingerprint, result)
         VALUES ($1, $2, $3, $4, $5)"
    ))
    .bind(receipt_id)
    .bind(request.actor)
    .bind(request.idempotency_key)
    .bind(request.fingerprint.as_str())
    .bind(Json(result))
    .execute(&mut **tx)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests;

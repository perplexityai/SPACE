/// The key [`fwd_retry_keyed!`](crate::fwd_retry_keyed) sends when its caller
/// supplied none: a UUIDv7, which is always a valid idempotency key.
pub fn generated_idempotency_key_string() -> String {
    uuid::Uuid::now_v7().to_string()
}

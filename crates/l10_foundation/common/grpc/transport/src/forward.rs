/// Forward a replay-safe unary tonic request with the local retry policy.
///
/// Use this only for read-only RPCs or commands that are idempotent by request
/// content. Effectful commands need server-side idempotency before they can be
/// safely replayed after an ambiguous transport failure; use `fwd_retry_keyed!`
/// for those once the server enforces `idempotency_key`.
#[macro_export]
macro_rules! fwd_retry {
    ($service:literal, $client:expr, $rpc:ident, $req:expr) => {{
        let req = $req;
        let client = $client;
        let metadata = req.metadata().clone();
        let message = req.into_inner();
        let context = $crate::GrpcRetryContext::none().with_grpc_timeout_metadata(&metadata);
        $crate::retry_grpc_status_with_context(
            $service,
            stringify!($rpc),
            $crate::DEFAULT_LOCAL_GRPC_RETRY_POLICY,
            context,
            || {
                let mut client = client.clone();
                let metadata = metadata.clone();
                let message = message.clone();
                async move {
                    let mut req = tonic::Request::new(message);
                    *req.metadata_mut() = metadata;
                    client.$rpc(req).await.map_err(Into::into)
                }
            },
        )
        .await
    }};
}

/// Forward an effectful unary tonic request with an idempotency key and the
/// local retry policy.
///
/// The request message must have an `idempotency_key: Option<String>` field.
/// If the caller already supplied a non-empty key, it is preserved. Otherwise a
/// UUIDv7 key is generated once before the retry loop and reused for every
/// attempt. Only use this for RPCs whose server persists and honors that key.
#[macro_export]
macro_rules! fwd_retry_keyed {
    ($service:literal, $client:expr, $rpc:ident, $req:expr) => {{
        let req = $req;
        let client = $client;
        let metadata = req.metadata().clone();
        let mut message = req.into_inner();
        let needs_idempotency_key = message
            .idempotency_key
            .as_deref()
            .map(str::is_empty)
            .unwrap_or(true);
        if needs_idempotency_key {
            message.idempotency_key = Some($crate::generated_idempotency_key().into());
        }
        let context = $crate::GrpcRetryContext::none().with_grpc_timeout_metadata(&metadata);
        $crate::retry_grpc_status_with_context(
            $service,
            stringify!($rpc),
            $crate::DEFAULT_LOCAL_GRPC_RETRY_POLICY,
            context,
            || {
                let mut client = client.clone();
                let metadata = metadata.clone();
                let message = message.clone();
                async move {
                    let mut req = tonic::Request::new(message);
                    *req.metadata_mut() = metadata;
                    client.$rpc(req).await.map_err(Into::into)
                }
            },
        )
        .await
    }};
}

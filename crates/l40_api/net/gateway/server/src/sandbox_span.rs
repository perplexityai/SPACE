//! The request-span layer that tags a single-sandbox server's RPCs with its sandbox.

/// Records one sandbox's id on every request span of a server that serves only
/// that sandbox, such as its net-gateway, so `{span.sandbox_id=...}` finds its
/// RPCs. Add it after [`pplx_space_grpc_server::grpc_trace_layer`], which makes the request span
/// current while it calls the services inside it.
#[derive(Clone, Copy, Debug)]
pub struct SandboxSpanLayer {
    sandbox_id: pplx_space_common_core::types::SandboxId,
}

impl SandboxSpanLayer {
    pub fn new(sandbox_id: pplx_space_common_core::types::SandboxId) -> Self {
        Self { sandbox_id }
    }
}

impl<S> tower::Layer<S> for SandboxSpanLayer {
    type Service = SandboxSpanService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        SandboxSpanService {
            inner,
            sandbox_id: self.sandbox_id,
        }
    }
}

#[derive(Clone, Debug)]
pub struct SandboxSpanService<S> {
    inner: S,
    sandbox_id: pplx_space_common_core::types::SandboxId,
}

impl<S, Request> tower::Service<Request> for SandboxSpanService<S>
where
    S: tower::Service<Request>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        tracing::Span::current().record("sandbox_id", tracing::field::display(self.sandbox_id));
        self.inner.call(request)
    }
}

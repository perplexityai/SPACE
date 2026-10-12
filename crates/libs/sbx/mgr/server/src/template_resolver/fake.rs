//! A scripted template resolver for tests.

use std::sync::{Mutex, PoisonError};

use pplx_space_template_mgr_client::proto::{GetTemplateRequest, GetTemplateResponse};

use super::{TemplateLookupError, TemplateResolver};

type Answer =
    dyn Fn() -> Result<tonic::Response<GetTemplateResponse>, TemplateLookupError> + Send + Sync;

/// Which [`TemplateResolver`] method a lookup used.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LookupChannel {
    /// [`TemplateResolver::get_template`]: a client dialed for the call.
    Dialed,
    /// [`TemplateResolver::get_template_on_shared_channel`].
    Shared,
}

/// One lookup as the resolver received it.
#[derive(Clone, Debug)]
pub(crate) struct Lookup {
    pub(crate) channel: LookupChannel,
    pub(crate) metadata: tonic::metadata::MetadataMap,
    pub(crate) request: GetTemplateRequest,
}

/// Answers every lookup with a scripted result and records each lookup in
/// arrival order. A scripted connect failure reaches a shared-channel lookup
/// as `Unavailable`, as a lazy channel reports it.
pub(crate) struct FakeTemplateResolver {
    answer: Box<Answer>,
    lookups: Mutex<Vec<Lookup>>,
}

impl FakeTemplateResolver {
    pub(crate) fn new(
        answer: impl Fn() -> Result<tonic::Response<GetTemplateResponse>, TemplateLookupError>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        Self {
            answer: Box::new(answer),
            lookups: Mutex::new(Vec::new()),
        }
    }

    /// Refuse every lookup with `status`.
    pub(crate) fn refusing(status: tonic::Status) -> Self {
        Self::new(move || Err(TemplateLookupError::Rpc(status.clone())))
    }

    pub(crate) fn lookups(&self) -> Vec<Lookup> {
        self.lookups
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn requests(&self) -> Vec<GetTemplateRequest> {
        self.lookups()
            .into_iter()
            .map(|lookup| lookup.request)
            .collect()
    }

    fn answer(
        &self,
        channel: LookupChannel,
        request: tonic::Request<GetTemplateRequest>,
    ) -> Result<tonic::Response<GetTemplateResponse>, TemplateLookupError> {
        let (metadata, _, request) = request.into_parts();
        self.lookups
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(Lookup {
                channel,
                metadata,
                request,
            });
        (self.answer)()
    }
}

#[async_trait::async_trait]
impl TemplateResolver for FakeTemplateResolver {
    async fn get_template(
        &self,
        request: tonic::Request<GetTemplateRequest>,
    ) -> Result<tonic::Response<GetTemplateResponse>, TemplateLookupError> {
        self.answer(LookupChannel::Dialed, request)
    }

    async fn get_template_on_shared_channel(
        &self,
        request: tonic::Request<GetTemplateRequest>,
    ) -> Result<tonic::Response<GetTemplateResponse>, tonic::Status> {
        self.answer(LookupChannel::Shared, request)
            .map_err(|error| match error {
                TemplateLookupError::Rpc(status) => status,
                TemplateLookupError::Connect(error) => {
                    tonic::Status::unavailable(error.to_string())
                }
            })
    }
}

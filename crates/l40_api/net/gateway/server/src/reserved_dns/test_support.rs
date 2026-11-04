use std::net::{IpAddr, Ipv4Addr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use hickory_resolver::proto::op::{Query, ResponseCode};
use hickory_resolver::proto::rr::rdata::A;
use hickory_resolver::proto::rr::{RData, Record};

use super::{
    CLOUDFLARE_DNS, GOOGLE_DNS, MAX_CONCURRENT_LOOKUPS, ReservedDnsCore, ReservedResolver,
    ResolverAnswer, ResolverBackend, ResolverFuture,
};

type AnswerFactory = dyn Fn(&Query) -> ResolverAnswer + Send + Sync;

#[derive(Clone)]
pub(crate) struct RecordedResolver {
    answer: Arc<AnswerFactory>,
    requests: Arc<Mutex<Vec<(IpAddr, Query)>>>,
}

impl RecordedResolver {
    pub(crate) fn new(answer: Ipv4Addr) -> Self {
        Self::with_answer(move |question| ResolverAnswer {
            question: question.clone(),
            code: ResponseCode::NoError,
            answers: vec![Record::from_rdata(
                question.name.clone(),
                60,
                RData::A(A(answer)),
            )],
            valid_until: Instant::now() + Duration::from_secs(60),
        })
    }

    pub(super) fn with_answer(
        answer: impl Fn(&Query) -> ResolverAnswer + Send + Sync + 'static,
    ) -> Self {
        Self {
            answer: Arc::new(answer),
            requests: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub(crate) fn core(&self) -> ReservedDnsCore {
        ReservedDnsCore::with_resolver(Arc::new(self.clone()), MAX_CONCURRENT_LOOKUPS)
    }

    pub(crate) fn requests(&self) -> Vec<(IpAddr, Query)> {
        self.requests.lock().unwrap().clone()
    }
}

pub(crate) fn test_core() -> ReservedDnsCore {
    RecordedResolver::new(Ipv4Addr::new(192, 0, 2, 1)).core()
}

impl ResolverBackend for RecordedResolver {
    fn lookup<'a>(&'a self, resolver: ReservedResolver, question: Query) -> ResolverFuture<'a> {
        Box::pin(async move {
            let resolver_ip = match resolver {
                ReservedResolver::Cloudflare => IpAddr::V4(CLOUDFLARE_DNS),
                ReservedResolver::Google => IpAddr::V4(GOOGLE_DNS),
            };
            self.requests
                .lock()
                .unwrap()
                .push((resolver_ip, question.clone()));
            Ok((self.answer)(&question))
        })
    }
}

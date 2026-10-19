use std::sync::Arc;
use std::time::Instant;

use hickory_resolver::config::ResolverOpts;
use hickory_resolver::net::NetError;
use hickory_resolver::proto::op::Query;
use hickory_resolver::{Resolver, config::ResolverConfig, lookup::Lookup};
use parking_lot::Mutex;

use super::upstream::LoggingConnectionProvider;
use super::{
    BuildReservedDnsCoreError, GatewayRuntimeProvider, ReservedResolver, TOTAL_LOOKUP_TIMEOUT,
};

pub(super) struct RecoveringResolver {
    config: ResolverConfig,
    provider: LoggingConnectionProvider,
    state: Mutex<ResolverState>,
}

struct ResolverState {
    resolver: Arc<Resolver<LoggingConnectionProvider>>,
    last_reset: Option<Instant>,
}

impl RecoveringResolver {
    pub(super) fn new(
        config: ResolverConfig,
        options: ResolverOpts,
        runtime: GatewayRuntimeProvider,
        resolver: ReservedResolver,
    ) -> Result<Self, BuildReservedDnsCoreError> {
        let provider = LoggingConnectionProvider::new(runtime, resolver);
        let builder =
            Resolver::builder_with_config(config.clone(), provider.clone()).with_options(options);
        let resolver =
            pplx_space_metrics::gateway::with_reserved_dns_hickory_metrics(|| builder.build())
                .map_err(BuildReservedDnsCoreError::Resolver)?;
        Ok(Self {
            config,
            provider,
            state: Mutex::new(ResolverState {
                resolver: Arc::new(resolver),
                last_reset: None,
            }),
        })
    }

    // The caller's deadline, cancellation, and lookup permit cover both attempts.
    pub(super) async fn lookup(
        &self,
        provider: ReservedResolver,
        question: &Query,
    ) -> Result<Lookup, NetError> {
        let original = self.state.lock().resolver.clone();
        let result = original
            .lookup(question.name.clone(), question.query_type)
            .await;
        if !matches!(result, Err(NetError::NoConnections)) {
            return result;
        }

        let replacement = {
            let mut state = self.state.lock();
            if Arc::ptr_eq(&state.resolver, &original) {
                // NoConnections can hide Busy from closed exchange queues.
                // Bound cache eviction and connection churn during real outages.
                if state
                    .last_reset
                    .is_some_and(|time| time.elapsed() < TOTAL_LOOKUP_TIMEOUT)
                {
                    return result;
                }
                state.last_reset = Some(Instant::now());
                let builder =
                    Resolver::builder_with_config(self.config.clone(), self.provider.clone())
                        .with_options(original.options().clone());
                match pplx_space_metrics::gateway::with_reserved_dns_hickory_metrics(|| {
                    builder.build()
                }) {
                    Ok(resolver) => state.resolver = Arc::new(resolver),
                    Err(error) => {
                        tracing::warn!(
                            resolver = provider.as_str(),
                            ?error,
                            "reserved DNS resolver replacement failed"
                        );
                        return result;
                    }
                }
                tracing::warn!(
                    resolver = provider.as_str(),
                    query_type = ?question.query_type,
                    "reserved DNS resolver replaced after NoConnections"
                );
            }
            // Concurrent failures reuse the replacement; existing lookups keep
            // their original resolver alive. Never hold the lock across I/O.
            state.resolver.clone()
        };
        let result = replacement
            .lookup(question.name.clone(), question.query_type)
            .await;
        tracing::info!(
            resolver = provider.as_str(),
            query_type = ?question.query_type,
            lookup_succeeded = result.is_ok(),
            "reserved DNS resolver retry completed"
        );
        result
    }
}

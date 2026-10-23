use pplx_space_common_core::types::SandboxId;

use std::error::Error as _;
use std::str::FromStr;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::time::{Duration as StdDuration, SystemTime, UNIX_EPOCH};

use pplx_space_common_core::types::SpaceApiRouteIdentity;
use pplx_space_common_svc::space_api_route::SpaceApiRoutePurpose as CommonSpaceApiRoutePurpose;
use pplx_space_common_svc::space_api_route_ed25519::{
    SpaceApiRouteEd25519Token, decode_unverified_space_api_route_ed25519_claims,
};
use pplx_space_node_api_client::proto::SpaceApiRouteToken as ProtoSpaceApiRouteToken;

use super::*;

pub(super) const SPACE_API_ROUTE_TOKEN_REFRESH_WINDOW: Duration = Duration::from_secs(60);
const BACKEND_CHANNEL_IDLE_SWEEP_INTERVAL: Duration = Duration::from_secs(60);
const BACKEND_CHANNEL_MAX_IDLE: Duration = Duration::from_secs(30 * 60);
const BACKEND_CHANNEL_IDLE_SWEEP_MAX_ENTRIES: usize = 16 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(super) struct BackendEndpoint(String);

#[derive(Debug, thiserror::Error)]
#[error("backend endpoint must not be empty")]
pub(super) struct BackendEndpointParseError;

impl BackendEndpoint {
    pub(super) fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for BackendEndpoint {
    type Err = BackendEndpointParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = value.trim();
        if value.is_empty() {
            return Err(BackendEndpointParseError);
        }
        Ok(Self(value.to_string()))
    }
}

impl TryFrom<String> for BackendEndpoint {
    type Error = BackendEndpointParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::from_str(&value)
    }
}

impl TryFrom<&str> for BackendEndpoint {
    type Error = BackendEndpointParseError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::from_str(value)
    }
}

impl std::fmt::Display for BackendEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, thiserror::Error)]
pub(super) enum ProtoSpaceApiRouteTokenError {
    #[error("route token is invalid")]
    InvalidToken,
    #[error("route token expiry is invalid")]
    InvalidExpiry,
    #[error("route token is expired")]
    Expired,
    #[error("route token purpose is invalid")]
    InvalidPurpose,
    #[error("route token sandbox id is invalid")]
    InvalidSandboxId,
}

impl ProtoSpaceApiRouteTokenError {
    pub(super) fn metric_reason(&self) -> SpaceApiRouteTokenDropReason {
        match self {
            Self::Expired => SpaceApiRouteTokenDropReason::Expired,
            Self::InvalidExpiry => SpaceApiRouteTokenDropReason::InvalidExpiry,
            Self::InvalidPurpose => SpaceApiRouteTokenDropReason::InvalidPurpose,
            Self::InvalidSandboxId => SpaceApiRouteTokenDropReason::InvalidSandboxId,
            Self::InvalidToken => SpaceApiRouteTokenDropReason::InvalidToken,
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct SpaceApiRouteTokenState {
    pub(super) token: SpaceApiRouteEd25519Token,
    pub(super) expires_at: tokio::time::Instant,
    pub(super) route_identity: SpaceApiRouteIdentity,
}

impl SpaceApiRouteTokenState {
    pub(super) fn token(&self) -> &SpaceApiRouteEd25519Token {
        &self.token
    }

    pub(super) fn needs_refresh(
        &self,
        now: tokio::time::Instant,
        refresh_window: Duration,
    ) -> bool {
        match now.checked_add(refresh_window) {
            Some(refresh_at) => refresh_at >= self.expires_at,
            None => true,
        }
    }

    pub(super) fn route_identity(&self) -> &SpaceApiRouteIdentity {
        &self.route_identity
    }
}

pub(super) fn space_api_route_token_needs_refresh(
    route_token: &Option<SpaceApiRouteTokenState>,
) -> bool {
    route_token.as_ref().is_some_and(|route_token| {
        route_token.needs_refresh(
            tokio::time::Instant::now(),
            SPACE_API_ROUTE_TOKEN_REFRESH_WINDOW,
        )
    })
}

pub(super) fn ecw_route_tokens_need_refresh(route: &EcwResolvedRoute) -> bool {
    space_api_route_token_needs_refresh(&route.space_api_route_token)
        || space_api_route_token_needs_refresh(&route.service_port_ingress_token)
}

pub(super) fn parse_proto_space_api_route_token(
    route_source: SpaceApiRouteTokenRouteSource,
    token: ProtoSpaceApiRouteToken,
) -> Option<SpaceApiRouteTokenState> {
    match decode_proto_space_api_route_token(route_source, token) {
        Ok(token) => Some(token),
        Err(error) => {
            pplx_space_metrics::data_plane::space_api_route_token_dropped(
                route_source,
                error.metric_reason(),
            );
            tracing::warn!(%error, "dropping unrefreshable SPACE API route token");
            None
        }
    }
}

fn decode_proto_space_api_route_token(
    route_source: SpaceApiRouteTokenRouteSource,
    token: ProtoSpaceApiRouteToken,
) -> Result<SpaceApiRouteTokenState, ProtoSpaceApiRouteTokenError> {
    if token.token.trim().is_empty() {
        return Err(ProtoSpaceApiRouteTokenError::InvalidToken);
    }
    let token = SpaceApiRouteEd25519Token::parse(token.token)
        .map_err(|_| ProtoSpaceApiRouteTokenError::InvalidToken)?;
    let claims = decode_unverified_space_api_route_ed25519_claims(&token)
        .map_err(|_| ProtoSpaceApiRouteTokenError::InvalidToken)?;
    validate_unverified_space_api_route_ed25519_claims(route_source, &claims)?;
    let expires_at = space_api_route_token_expiry_instant(claims.expires_at_unix_secs)?;
    let route_identity = claims.route_identity.into();
    Ok(SpaceApiRouteTokenState {
        token,
        expires_at,
        route_identity,
    })
}

fn space_api_route_token_expiry_instant(
    expires_at_unix_secs: u64,
) -> Result<tokio::time::Instant, ProtoSpaceApiRouteTokenError> {
    let expires_at_system = UNIX_EPOCH
        .checked_add(StdDuration::from_secs(expires_at_unix_secs))
        .ok_or(ProtoSpaceApiRouteTokenError::InvalidExpiry)?;
    let now_system = SystemTime::now();
    let remaining = expires_at_system
        .duration_since(now_system)
        .map_err(|_| ProtoSpaceApiRouteTokenError::Expired)?;
    tokio::time::Instant::now()
        .checked_add(remaining)
        .ok_or(ProtoSpaceApiRouteTokenError::InvalidExpiry)
}

fn validate_unverified_space_api_route_ed25519_claims(
    route_source: SpaceApiRouteTokenRouteSource,
    claims: &pplx_space_common_svc::space_api_route_ed25519::SpaceApiRouteEd25519Claims,
) -> Result<(), ProtoSpaceApiRouteTokenError> {
    let purpose_is_valid = match route_source {
        SpaceApiRouteTokenRouteSource::DataPlaneSession => matches!(
            claims.purpose,
            CommonSpaceApiRoutePurpose::SpaceApiBridge
                | CommonSpaceApiRoutePurpose::ServicePortIngress
        ),
        SpaceApiRouteTokenRouteSource::SandboxServiceRouteBridge => {
            claims.purpose == CommonSpaceApiRoutePurpose::SpaceApiBridge
        }
        SpaceApiRouteTokenRouteSource::SandboxServiceRoute => {
            claims.purpose == CommonSpaceApiRoutePurpose::ServicePortIngress
        }
    };
    if !purpose_is_valid {
        return Err(ProtoSpaceApiRouteTokenError::InvalidPurpose);
    }
    if claims.route_identity.sandbox_id().as_uuid().is_nil() {
        return Err(ProtoSpaceApiRouteTokenError::InvalidSandboxId);
    }
    Ok(())
}

pub(super) struct BackendChannelPool {
    pub(super) pool_size: usize,
    /// Endpoint entries are bounded by node-api endpoints, not sandbox route cache keys.
    /// Transport failures reset an endpoint; route-cache churn must not evict channels.
    pub(super) channels: DashMap<String, Arc<BackendChannelPoolEntry>>,
    last_idle_sweep: Mutex<tokio::time::Instant>,
}

pub(super) struct BackendChannelPoolEntry {
    channels: Vec<Channel>,
    last_used: Mutex<tokio::time::Instant>,
}

impl BackendChannelPoolEntry {
    fn new(channels: Vec<Channel>, now: tokio::time::Instant) -> Self {
        Self {
            channels,
            last_used: Mutex::new(now),
        }
    }

    fn touch(&self, now: tokio::time::Instant) {
        if let Ok(mut last_used) = self.last_used.lock() {
            *last_used = now;
        }
    }

    fn idle_for_at_least(&self, now: tokio::time::Instant, max_idle: Duration) -> bool {
        self.last_used
            .lock()
            .map(|last_used| now.saturating_duration_since(*last_used) >= max_idle)
            .unwrap_or(false)
    }
}

impl BackendChannelPool {
    pub(super) fn new(pool_size: usize) -> Self {
        let pool_size = pool_size.max(1);
        pplx_space_metrics::data_plane::backend_channel_pool_size(pool_size);
        Self {
            pool_size,
            channels: DashMap::new(),
            last_idle_sweep: Mutex::new(tokio::time::Instant::now()),
        }
    }

    pub(super) async fn channel(
        &self,
        endpoint: &BackendEndpoint,
        session_id: DataPlaneSessionId,
    ) -> Result<Channel, BackendChannelPoolError> {
        self.channel_for_key(endpoint, session_id.as_uuid()).await
    }

    pub(super) async fn channel_for_key(
        &self,
        endpoint: &BackendEndpoint,
        key: Uuid,
    ) -> Result<Channel, BackendChannelPoolError> {
        let cache_key = normalize_backend_endpoint(endpoint.as_str())?;
        self.channel_for_cache_key(cache_key, key).await
    }

    async fn channel_for_cache_key(
        &self,
        cache_key: String,
        key: Uuid,
    ) -> Result<Channel, BackendChannelPoolError> {
        let now = tokio::time::Instant::now();
        let entry = if let Some(entry) = self.channels.get(&cache_key) {
            let entry = entry.clone();
            entry.touch(now);
            entry
        } else {
            let entry = Arc::new(BackendChannelPoolEntry::new(
                build_backend_channels(&cache_key, self.pool_size).await?,
                now,
            ));
            let entry = self
                .channels
                .entry(cache_key)
                .or_insert_with(|| entry.clone())
                .clone();
            entry.touch(now);
            entry
        };
        let len = entry.channels.len();
        if len == 0 {
            return Err(BackendChannelPoolError::Empty);
        }
        let index = (key.as_u128() % len as u128) as usize;
        entry
            .channels
            .get(index)
            .cloned()
            .ok_or(BackendChannelPoolError::Empty)
            .inspect(|_| {
                if self
                    .sweep_idle_throttled(
                        BACKEND_CHANNEL_IDLE_SWEEP_INTERVAL,
                        BACKEND_CHANNEL_MAX_IDLE,
                        BACKEND_CHANNEL_IDLE_SWEEP_MAX_ENTRIES,
                    )
                    .is_some()
                {
                    pplx_space_metrics::data_plane::backend_channel_pool_entries(self.len());
                }
            })
    }

    pub(super) fn forget_endpoint(&self, endpoint: &BackendEndpoint) -> bool {
        let Ok(endpoint) = normalize_backend_endpoint(endpoint.as_str()) else {
            return false;
        };
        self.channels.remove(&endpoint).is_some()
    }

    pub(super) fn len(&self) -> usize {
        self.channels.len()
    }

    pub(super) fn sweep_idle_throttled(
        &self,
        interval: Duration,
        max_idle: Duration,
        max_entries: usize,
    ) -> Option<usize> {
        if max_entries == 0 {
            return None;
        }
        let now = tokio::time::Instant::now();
        let mut last_sweep = match self.last_idle_sweep.try_lock() {
            Ok(last_sweep) => last_sweep,
            Err(_) => return None,
        };
        if now.saturating_duration_since(*last_sweep) < interval {
            return None;
        }
        *last_sweep = now;
        drop(last_sweep);
        Some(self.sweep_idle(max_idle, max_entries))
    }

    pub(super) fn sweep_idle(&self, max_idle: Duration, max_entries: usize) -> usize {
        if max_entries == 0 {
            return 0;
        }
        let now = tokio::time::Instant::now();
        let keys = self
            .channels
            .iter()
            .filter_map(|entry| {
                entry
                    .idle_for_at_least(now, max_idle)
                    .then(|| entry.key().clone())
            })
            .take(max_entries)
            .collect::<Vec<_>>();
        keys.into_iter()
            .filter(|key| {
                self.channels
                    .remove_if(key, |_, entry| entry.idle_for_at_least(now, max_idle))
                    .is_some()
            })
            .count()
    }

    #[cfg(test)]
    pub(super) fn mark_endpoint_idle_for(&self, endpoint: &BackendEndpoint, idle_for: Duration) {
        let Ok(endpoint) = normalize_backend_endpoint(endpoint.as_str()) else {
            return;
        };
        if let Some(entry) = self.channels.get(&endpoint)
            && let Ok(mut last_used) = entry.last_used.lock()
        {
            *last_used = tokio::time::Instant::now() - idle_for;
        }
    }

    #[cfg(test)]
    pub(super) fn mark_idle_sweep_due(&self, interval: Duration) {
        let Ok(mut last_sweep) = self.last_idle_sweep.lock() else {
            return;
        };
        *last_sweep = tokio::time::Instant::now() - interval;
    }
}

pub(super) fn backend_transport_reset_reason(status: &tonic::Status) -> Option<ReconnectReason> {
    backend_status_has_transport_source(status).then_some(ReconnectReason::WorkerUnavailable)
}

pub(super) fn reset_ecw_backend_channel_on_transport_status(
    state: &DataPlaneState,
    route: &EcwResolvedRoute,
    status: &tonic::Status,
    operation: &'static str,
) -> bool {
    let Some(reason) = backend_transport_reset_reason(status) else {
        return false;
    };
    let removed = state
        .backend_channels
        .forget_endpoint(&route.backend_endpoint);
    pplx_space_metrics::data_plane::backend_channel_reset("ecw", reason.as_str());
    tracing::debug!(
        operation,
        sandbox_id = %route.sandbox_id,
        route_generation = route.route_generation,
        current_node_id = %route.current_node_id,
        backend_endpoint = %route.backend_endpoint,
        grpc_code = ?status.code(),
        grpc_message = %status.message(),
        removed_cached_endpoint = removed,
        "reset ECW backend channel after transport failure",
    );
    true
}

fn backend_status_has_transport_source(status: &tonic::Status) -> bool {
    let mut source = status.source();
    while let Some(error) = source {
        if error.is::<tonic::ConnectError>()
            || error.is::<tonic::transport::Error>()
            || error.is::<hyper::Error>()
        {
            return true;
        }
        source = error.source();
    }
    false
}

#[derive(Debug, thiserror::Error)]
pub(super) enum BackendChannelPoolError {
    #[error(transparent)]
    InternalTransport(#[from] pplx_space_common_grpc::internal_transport::TransportError),
    #[error("invalid endpoint: {0}")]
    InvalidEndpoint(#[from] tonic::transport::Error),
    #[error("channel pool is empty")]
    Empty,
}

pub(super) async fn build_backend_channels(
    endpoint: &str,
    pool_size: usize,
) -> Result<Vec<Channel>, pplx_space_common_grpc::internal_transport::TransportError> {
    // Each pooled channel carries calls for many sandboxes on one connection.
    let endpoint = pplx_space_node_api_client::configure_node_api_endpoint(
        Endpoint::from_shared(normalize_backend_endpoint(endpoint)?)?,
        pplx_space_node_api_client::NodeApiEndpointConfig::default(),
    )
    .initial_connection_window_size(
        pplx_space_node_api_client::MULTIPLEXED_CONNECTION_WINDOW_BYTES,
    );
    let endpoint = pplx_space_common_grpc::internal_transport::client_connect_endpoint(
        pplx_space_common_grpc::internal_transport::Service::NodeApi,
        endpoint,
    )
    .await?;
    Ok((0..pool_size)
        .map(|_| endpoint.clone().connect_lazy())
        .collect())
}

fn normalize_backend_endpoint(endpoint: &str) -> Result<String, tonic::transport::Error> {
    let endpoint = endpoint.trim();
    let endpoint = pplx_space_common_grpc::internal_transport::qualify_endpoint(endpoint);
    Endpoint::from_shared(endpoint.clone())?;
    Ok(endpoint)
}

#[derive(Debug, Default)]
pub(super) struct RevocationCache {
    pub(super) entries: DashMap<String, RevocationCacheEntry>,
    pub(super) orgs: DashMap<OrgId, OrgRevocationCacheEntry>,
}

impl RevocationCache {
    pub(super) fn remember_active(
        &self,
        session_id: DataPlaneSessionId,
        epoch: u64,
        org_id: OrgId,
        org_epoch: u64,
        read: RevocationRead,
    ) {
        self.remember_state(session_id, epoch, true, org_id, org_epoch, read);
    }

    pub(super) fn remember(
        &self,
        session_id: DataPlaneSessionId,
        org_id: OrgId,
        revocation: &DataPlaneRevocationState,
        read: RevocationRead,
    ) {
        self.remember_state(
            session_id,
            revocation.epoch,
            revocation_state_is_active(revocation),
            org_id,
            revocation.org_epoch,
            read,
        );
    }

    pub(super) fn remember_state(
        &self,
        session_id: DataPlaneSessionId,
        epoch: u64,
        active: bool,
        org_id: OrgId,
        org_epoch: u64,
        read: RevocationRead,
    ) {
        if self.remember_session(session_id, epoch, active, org_id, org_epoch) {
            self.remember_org_epoch(org_id, org_epoch, read);
        }
    }

    /// Record an active session from a route this replica resolved earlier
    /// and still caches. The cluster API was not asked, so the org's last
    /// confirmation and next refresh stay as the resolve left them.
    pub(super) fn remember_cached_active(
        &self,
        session_id: DataPlaneSessionId,
        epoch: u64,
        org_id: OrgId,
        org_epoch: u64,
    ) {
        self.remember_session(session_id, epoch, true, org_id, org_epoch);
    }

    /// Record the session's verdict unless a newer epoch is already cached;
    /// returns whether it was recorded.
    fn remember_session(
        &self,
        session_id: DataPlaneSessionId,
        epoch: u64,
        active: bool,
        org_id: OrgId,
        org_epoch: u64,
    ) -> bool {
        let key = session_id.to_string();
        if let Some(existing) = self.entries.get(&key)
            && epoch < existing.epoch
        {
            return false;
        }
        self.entries.insert(
            key,
            RevocationCacheEntry {
                epoch,
                active,
                org_id,
                org_epoch,
            },
        );
        true
    }

    /// Record the org epoch `read` returned. An older epoch than the cached
    /// one is ignored, and the same epoch only moves its confirmation and next
    /// refresh later, so an answer that arrives late cannot undo a newer one.
    pub(super) fn remember_org_epoch(&self, org_id: OrgId, epoch: u64, read: RevocationRead) {
        let refresh_after = read.started_at + read.fresh_for;
        match self.orgs.entry(org_id) {
            dashmap::mapref::entry::Entry::Occupied(mut current) => {
                let current = current.get_mut();
                if epoch < current.epoch
                    || (epoch == current.epoch && read.started_at < current.confirmed_at)
                {
                    return;
                }
                current.epoch = epoch;
                current.confirmed_at = read.started_at;
                current.refresh_after = refresh_after;
            }
            dashmap::mapref::entry::Entry::Vacant(slot) => {
                slot.insert(OrgRevocationCacheEntry {
                    epoch,
                    confirmed_at: read.started_at,
                    refresh_after,
                    refreshing: Arc::new(AtomicBool::new(false)),
                });
            }
        }
    }

    /// Probe the session's cached verdict. A refresh another stream has in
    /// flight answers from the cache only while that verdict is younger than
    /// `tolerance`; past it, this session is re-read instead.
    pub(super) fn probe(
        self: &Arc<Self>,
        session_id: DataPlaneSessionId,
        tolerance: Duration,
    ) -> RevocationProbe {
        let key = session_id.to_string();
        let Some(entry) = self.entries.get(&key) else {
            return RevocationProbe::RefreshSession;
        };
        let active = entry.active;
        if !active {
            return RevocationProbe::Fresh(active);
        }
        let org_id = entry.org_id;
        let session_org_epoch = entry.org_epoch;
        drop(entry);
        let Some(org) = self.orgs.get(&org_id) else {
            return RevocationProbe::RefreshSession;
        };
        // A revocation anywhere in the org advances the shared org epoch, and
        // a sibling session's refresh may record it before this session's
        // tick. A session row last read under an older epoch must be re-read
        // even while the org entry's TTL is fresh; otherwise this session's
        // own revocation stays hidden until an unrelated epoch bump.
        if session_org_epoch < org.epoch {
            return RevocationProbe::RefreshSession;
        }
        if tokio::time::Instant::now() < org.refresh_after {
            return RevocationProbe::Fresh(active);
        }
        let cached_org_epoch = org.epoch.max(session_org_epoch);
        let confirmed_recently = org.confirmed_at.elapsed() < tolerance;
        let refreshing = Arc::clone(&org.refreshing);
        drop(org);
        if refreshing
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            RevocationProbe::RefreshOrg(RevocationOrgRefresh {
                org_id,
                cached_org_epoch,
                active,
                guard: RevocationRefreshGuard {
                    refreshing: Some(refreshing),
                },
            })
        } else if confirmed_recently {
            RevocationProbe::RefreshInFlight(active)
        } else {
            RevocationProbe::RefreshSession
        }
    }

    /// Whether the session's last confirmed verdict is active and younger
    /// than `tolerance`. The verdict's age is its org entry's: every
    /// revocation advances the org epoch, so a check that confirms the org's
    /// epoch unchanged also confirms each session read under that epoch. A
    /// newer org epoch than the session was read under means a revocation in
    /// its org since, so the verdict no longer counts.
    pub(super) fn confirmed_active(
        &self,
        session_id: DataPlaneSessionId,
        tolerance: Duration,
    ) -> bool {
        let Some(entry) = self.entries.get(&session_id.to_string()) else {
            return false;
        };
        let Some(org) = self.orgs.get(&entry.org_id) else {
            return false;
        };
        entry.active && entry.org_epoch >= org.epoch && org.confirmed_at.elapsed() < tolerance
    }

    pub(super) fn forget(&self, session_id: DataPlaneSessionId) {
        self.entries.remove(&session_id.to_string());
    }

    #[cfg(test)]
    pub(super) fn cached_epoch(&self, session_id: DataPlaneSessionId) -> Option<u64> {
        self.entries
            .get(&session_id.to_string())
            .map(|entry| entry.epoch)
    }

    #[cfg(test)]
    pub(super) fn cached_org_epoch(&self, org_id: OrgId) -> Option<u64> {
        self.orgs.get(&org_id).map(|entry| entry.epoch)
    }
}

#[derive(Debug, Clone)]
pub(super) struct RevocationCacheEntry {
    pub(super) epoch: u64,
    pub(super) active: bool,
    pub(super) org_id: OrgId,
    pub(super) org_epoch: u64,
}

/// When a revocation read began, and how long its answer stays fresh. The
/// cluster API may have read its rows at any point before it answered, so an
/// answer is dated from when the read began, not from when it arrived.
#[derive(Debug, Clone, Copy)]
pub(super) struct RevocationRead {
    pub(super) started_at: tokio::time::Instant,
    pub(super) fresh_for: Duration,
}

impl RevocationRead {
    /// A read beginning now.
    pub(super) fn start(fresh_for: Duration) -> Self {
        Self {
            started_at: tokio::time::Instant::now(),
            fresh_for,
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct OrgRevocationCacheEntry {
    pub(super) epoch: u64,
    /// When the latest read that confirmed this epoch began.
    pub(super) confirmed_at: tokio::time::Instant,
    pub(super) refresh_after: tokio::time::Instant,
    pub(super) refreshing: Arc<AtomicBool>,
}

#[derive(Debug)]
pub(super) enum RevocationProbe {
    Fresh(bool),
    RefreshInFlight(bool),
    RefreshSession,
    RefreshOrg(RevocationOrgRefresh),
}

#[derive(Debug)]
pub(super) struct RevocationOrgRefresh {
    pub(super) org_id: OrgId,
    pub(super) cached_org_epoch: u64,
    pub(super) active: bool,
    pub(super) guard: RevocationRefreshGuard,
}

#[derive(Debug)]
pub(super) struct RevocationRefreshGuard {
    pub(super) refreshing: Option<Arc<AtomicBool>>,
}

impl Drop for RevocationRefreshGuard {
    fn drop(&mut self) {
        if let Some(refreshing) = self.refreshing.take() {
            refreshing.store(false, Ordering::Release);
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct AttachedSession {
    /// Monotonic insertion order used to retain the latest cached session route.
    pub(super) cache_epoch: u64,
    /// Authenticated cache slot that produced this handle. Refresh updates only
    /// this slot, avoiding a prefix scan and preserving credential isolation.
    pub(super) cache_key: Option<String>,
    pub(super) session_id: DataPlaneSessionId,
    pub(super) sandbox_id: SandboxId,
    pub(super) org_id: OrgId,
    pub(super) kind: SessionKind,
    pub(super) route_generation: u64,
    pub(super) revocation_epoch: u64,
    pub(super) org_revocation_epoch: u64,
    pub(super) current_node_id: NodeId,
    pub(super) backend_endpoint: BackendEndpoint,
    pub(super) target_port: Option<u32>,
    pub(super) target_protocol: Option<SessionTargetProtocol>,
    pub(super) adapter_state_json: String,
    pub(super) backend_process_id: Option<ProcessId>,
    pub(super) space_api_route_token: Option<SpaceApiRouteTokenState>,
}

impl AttachedSession {
    pub(super) fn route_identity(&self) -> Option<&SpaceApiRouteIdentity> {
        self.space_api_route_token
            .as_ref()
            .map(SpaceApiRouteTokenState::route_identity)
    }
}

#[derive(Clone, Debug, Default)]
pub(super) struct EcwHostConfig {
    pub(super) hosts: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct EcwHostEnv {
    pub(super) host: String,
}

impl EcwHostConfig {
    pub(super) fn from_env() -> Self {
        Self::from_env_value(std::env::var(SPACE_ECW_HOSTS_ENV).ok().as_deref())
    }

    pub(super) fn from_env_value(raw_hosts: Option<&str>) -> Self {
        let Some(raw_hosts) = raw_hosts.filter(|value| !value.trim().is_empty()) else {
            return Self::default();
        };

        match serde_json::from_str::<Vec<EcwHostEnv>>(raw_hosts) {
            Ok(hosts) => Self {
                hosts: hosts
                    .into_iter()
                    .filter_map(|host| normalize_ecw_host(&host.host))
                    .collect(),
            },
            Err(error) => {
                tracing::warn!(
                    %error,
                    env = SPACE_ECW_HOSTS_ENV,
                    "failed to parse ECW hosts",
                );
                Self::default()
            }
        }
    }

    pub(super) fn parse_sandbox_host(&self, headers: &HeaderMap) -> Option<EcwSandboxHost> {
        self.hosts
            .iter()
            .find_map(|host| parse_ecw_sandbox_host(headers, host))
    }
}

#[derive(Clone, Debug)]
pub(super) struct EcwSandboxHost {
    pub(super) port: u32,
    pub(super) sandbox_id: SandboxId,
}

/// Everything needed to resolve one ECW route again without re-reading the
/// request that first produced it.
///
/// A refresh triggered by a stale-identity rejection has to reach the same
/// cache slot and the same authoritative route as the original resolve.
/// Recomputing that from request headers at refresh time is how a refresh ends
/// up pointed at the wrong port or transport, so a resolved route carries its
/// own origin instead.
#[derive(Clone, Debug)]
pub(super) struct EcwRouteOrigin {
    pub(super) target: EcwSandboxHost,
    pub(super) transport: EcwRouteTransport,
    pub(super) envd_access_token_sha256: Option<String>,
    pub(super) traffic_access_token_sha256: Option<String>,
    pub(super) api_key_sha256: Option<String>,
}

impl EcwRouteOrigin {
    pub(super) fn cache_key(&self) -> String {
        ecw_route_cache_key(
            &self.target,
            self.transport,
            self.envd_access_token_sha256.as_deref(),
            self.traffic_access_token_sha256.as_deref(),
            self.api_key_sha256.as_deref(),
        )
    }
}

#[derive(Clone, Debug)]
pub(super) struct EcwResolvedRoute {
    pub(super) cache_epoch: u64,
    pub(super) origin: EcwRouteOrigin,
    pub(super) sandbox_id: SandboxId,
    /// Owning org: upload admission, transfer rates and upload attribution
    /// key on it. `None` when the resolve response carries no parseable org;
    /// nothing routes or authorizes on it.
    pub(super) org_id: Option<OrgId>,
    pub(super) template_id: Option<String>,
    pub(super) route_generation: u64,
    pub(super) current_node_id: NodeId,
    pub(super) backend_endpoint: BackendEndpoint,
    pub(super) space_api_route_token: Option<SpaceApiRouteTokenState>,
    pub(super) service_port_ingress_token: Option<SpaceApiRouteTokenState>,
}

impl EcwResolvedRoute {
    pub(super) fn same_ingress_execution(&self, other: &Self) -> bool {
        match (
            &self.service_port_ingress_token,
            &other.service_port_ingress_token,
        ) {
            (Some(current), Some(fresh)) => current.route_identity() == fresh.route_identity(),
            _ => false,
        }
    }
}

#[derive(Debug)]
pub(super) struct RouteCache {
    pub(super) native: DashMap<String, RouteCacheEntry<AttachedSession>>,
    native_latest: DashMap<DataPlaneSessionId, RouteCacheEntry<AttachedSession>>,
    pub(super) ecw: DashMap<String, RouteCacheEntry<EcwResolvedRoute>>,
    last_native_sweep: Mutex<tokio::time::Instant>,
    last_ecw_sweep: Mutex<tokio::time::Instant>,
    next_cache_epoch: AtomicU64,
    /// One refresh slot per cache key, so a burst of requests holding the same
    /// stale route performs one authoritative resolve between them instead of
    /// one each. Shared by the ECW and native surfaces, keyed by ECW cache key
    /// and native session prefix respectively.
    refresh_locks: DashMap<String, Arc<tokio::sync::Mutex<()>>>,
}

impl Default for RouteCache {
    fn default() -> Self {
        Self {
            native: DashMap::new(),
            native_latest: DashMap::new(),
            ecw: DashMap::new(),
            last_native_sweep: Mutex::new(tokio::time::Instant::now()),
            last_ecw_sweep: Mutex::new(tokio::time::Instant::now()),
            next_cache_epoch: AtomicU64::new(1),
            refresh_locks: DashMap::new(),
        }
    }
}

impl RouteCache {
    pub(super) fn lookup_native(&self, key: &str) -> RouteCacheLookup<AttachedSession> {
        let Some(entry) = self.native.get(key) else {
            return RouteCacheLookup::Miss;
        };
        let now = tokio::time::Instant::now();
        if now < entry.expires_at {
            return RouteCacheLookup::Hit(entry.value.clone());
        }

        let expired = entry.value.clone();
        let expired_cache_epoch = expired.cache_epoch;
        let session_id = expired.session_id;
        drop(entry);
        self.native.remove_if(key, |_, current| {
            current.value.cache_epoch == expired_cache_epoch && now >= current.expires_at
        });
        self.native_latest.remove_if(&session_id, |_, current| {
            current.value.cache_epoch == expired_cache_epoch && now >= current.expires_at
        });
        RouteCacheLookup::Expired(expired)
    }

    pub(super) fn sweep_expired_native_throttled(
        &self,
        interval: Duration,
        max_entries: usize,
    ) -> Option<(usize, usize)> {
        if max_entries == 0 {
            return None;
        }
        let now = tokio::time::Instant::now();
        let mut last_sweep = match self.last_native_sweep.try_lock() {
            Ok(last_sweep) => last_sweep,
            Err(_) => return None,
        };
        if now.saturating_duration_since(*last_sweep) < interval {
            return None;
        }
        *last_sweep = now;
        drop(last_sweep);
        Some(self.sweep_expired_native(max_entries))
    }

    pub(super) fn sweep_expired_native(&self, max_entries: usize) -> (usize, usize) {
        if max_entries == 0 {
            return (0, 0);
        }
        let now = tokio::time::Instant::now();
        let native_keys = self
            .native
            .iter()
            .filter_map(|entry| (now >= entry.expires_at).then(|| entry.key().clone()))
            .take(max_entries)
            .collect::<Vec<_>>();
        let removed_native = native_keys
            .into_iter()
            .filter(|key| {
                self.native
                    .remove_if(key, |_, entry| now >= entry.expires_at)
                    .is_some()
            })
            .count();
        let latest_keys = self
            .native_latest
            .iter()
            .filter_map(|entry| (now >= entry.expires_at).then(|| *entry.key()))
            .take(max_entries)
            .collect::<Vec<_>>();
        let removed_latest = latest_keys
            .into_iter()
            .filter(|session_id| {
                self.native_latest
                    .remove_if(session_id, |_, entry| now >= entry.expires_at)
                    .is_some()
            })
            .count();
        (removed_native, removed_latest)
    }

    pub(super) fn remember_native(&self, key: String, route: AttachedSession, ttl: Duration) {
        let expires_at = tokio::time::Instant::now() + ttl;
        self.remember_native_latest(route.clone(), expires_at);
        self.native.insert(
            key,
            RouteCacheEntry {
                value: route,
                expires_at,
            },
        );
    }

    fn remember_native_latest(&self, route: AttachedSession, expires_at: tokio::time::Instant) {
        let latest_entry = RouteCacheEntry {
            value: route.clone(),
            expires_at,
        };
        match self.native_latest.entry(route.session_id) {
            dashmap::mapref::entry::Entry::Occupied(mut current) => {
                if current.get().value.cache_epoch <= route.cache_epoch {
                    current.insert(latest_entry);
                }
            }
            dashmap::mapref::entry::Entry::Vacant(slot) => {
                slot.insert(latest_entry);
            }
        }
    }

    pub(super) fn remember_native_latest_route(&self, route: AttachedSession, ttl: Duration) {
        self.remember_native_latest(route, tokio::time::Instant::now() + ttl);
    }

    /// Replace the route in an authenticated native cache slot without
    /// extending the authorization lease established by ClusterApi.
    ///
    /// Route refreshes resolve by session identity and do not revalidate the
    /// client token embedded in the cache key. They may update a still-live
    /// slot, but must not recreate an expired slot or replace a route another
    /// caller already refreshed.
    pub(super) fn replace_native_route_if_live(
        &self,
        key: &str,
        stale_identity: Option<&SpaceApiRouteIdentity>,
        route: AttachedSession,
    ) -> bool {
        let now = tokio::time::Instant::now();
        match self.native.entry(key.to_string()) {
            dashmap::mapref::entry::Entry::Occupied(mut slot) => {
                if now >= slot.get().expires_at {
                    slot.remove();
                    false
                } else if slot.get().value.route_identity() != stale_identity {
                    false
                } else {
                    let expires_at = slot.get().expires_at;
                    slot.insert(RouteCacheEntry {
                        value: route.clone(),
                        expires_at,
                    });
                    true
                }
            }
            dashmap::mapref::entry::Entry::Vacant(_) => false,
        }
    }

    /// A live cached route for this session with a different execution identity.
    ///
    /// Lets a caller that waited for the refresh slot observe the route the
    /// winner installed rather than resolving a second time.
    pub(super) fn newer_native_session(
        &self,
        session_id: DataPlaneSessionId,
        stale_identity: Option<&SpaceApiRouteIdentity>,
    ) -> Option<AttachedSession> {
        let now = tokio::time::Instant::now();
        let entry = self.native_latest.get(&session_id)?;
        if now >= entry.expires_at {
            let expired_cache_epoch = entry.value.cache_epoch;
            drop(entry);
            self.native_latest.remove_if(&session_id, |_, current| {
                current.value.cache_epoch == expired_cache_epoch && now >= current.expires_at
            });
            return None;
        }
        if entry.value.route_identity() == stale_identity {
            return None;
        }
        Some(entry.value.clone())
    }

    pub(super) fn forget_native_session(&self, session_id: DataPlaneSessionId) {
        let prefix = native_route_cache_prefix(session_id);
        self.native.retain(|key, _| !key.starts_with(&prefix));
        self.native_latest.remove(&session_id);
    }

    pub(super) fn lookup_ecw(&self, key: &str) -> RouteCacheLookup<EcwResolvedRoute> {
        self.lookup(&self.ecw, key)
    }

    pub(super) fn sweep_expired_ecw_throttled(
        &self,
        interval: Duration,
        max_entries: usize,
    ) -> Option<usize> {
        if max_entries == 0 {
            return None;
        }
        let now = tokio::time::Instant::now();
        let mut last_sweep = match self.last_ecw_sweep.try_lock() {
            Ok(last_sweep) => last_sweep,
            Err(_) => return None,
        };
        if now.saturating_duration_since(*last_sweep) < interval {
            return None;
        }
        *last_sweep = now;
        drop(last_sweep);
        Some(self.sweep_expired_ecw(max_entries))
    }

    pub(super) fn sweep_expired_ecw(&self, max_entries: usize) -> usize {
        if max_entries == 0 {
            return 0;
        }
        let now = tokio::time::Instant::now();
        let keys = self
            .ecw
            .iter()
            .filter_map(|entry| (now >= entry.expires_at).then(|| entry.key().clone()))
            .take(max_entries)
            .collect::<Vec<_>>();
        keys.into_iter()
            .filter(|key| {
                self.ecw
                    .remove_if(key, |_, entry| now >= entry.expires_at)
                    .is_some()
            })
            .count()
    }

    pub(super) fn remember_ecw(&self, key: String, route: EcwResolvedRoute, ttl: Duration) {
        self.remember(&self.ecw, key, route, ttl);
    }

    /// Stamp for the next route installed into any cache slot.
    ///
    /// One counter across both surfaces keeps insertion ordering monotonic for
    /// the native latest-route index and makes cache state easy to inspect.
    pub(super) fn next_cache_epoch(&self) -> u64 {
        self.next_cache_epoch.fetch_add(1, Ordering::Relaxed)
    }

    /// Drop a cached ECW route only if the slot still holds the rejected install.
    ///
    /// Returns whether this call is the one that evicted it. A caller that lost
    /// the race — because another request already installed a fresher route —
    /// leaves that route in place rather than forcing everyone to resolve again.
    pub(super) fn forget_ecw_if_current(&self, key: &str, stale_cache_epoch: u64) -> bool {
        self.ecw
            .remove_if(key, |_, entry| entry.value.cache_epoch == stale_cache_epoch)
            .is_some()
    }

    pub(super) async fn acquire_refresh_slot(&self, key: &str) -> RouteRefreshSlot<'_> {
        let lock = self
            .refresh_locks
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        let mut slot = RouteRefreshSlot {
            cache: self,
            key: key.to_string(),
            lock: Some(lock),
            guard: None,
        };
        let guard = Arc::clone(slot.lock.as_ref().expect("refresh slot owns its lock"))
            .lock_owned()
            .await;
        slot.guard = Some(guard);
        slot
    }

    #[cfg(test)]
    pub(super) fn refresh_lock_len(&self) -> usize {
        self.refresh_locks.len()
    }

    pub(super) fn native_latest_len(&self) -> usize {
        self.native_latest.len()
    }

    pub(super) fn native_len(&self) -> usize {
        self.native.len()
    }

    pub(super) fn forget_ecw_target(&self, sandbox_id: SandboxId, port: u32) -> usize {
        let prefix = ecw_route_cache_prefix(sandbox_id, port);
        let keys = self
            .ecw
            .iter()
            .filter_map(|entry| {
                entry
                    .key()
                    .starts_with(&prefix)
                    .then(|| entry.key().clone())
            })
            .collect::<Vec<_>>();
        keys.into_iter()
            .filter(|key| self.ecw.remove(key).is_some())
            .count()
    }

    pub(super) fn ecw_len(&self) -> usize {
        self.ecw.len()
    }

    #[cfg(test)]
    pub(super) fn mark_ecw_sweep_due(&self, interval: Duration) {
        let Ok(mut last_sweep) = self.last_ecw_sweep.lock() else {
            return;
        };
        *last_sweep = tokio::time::Instant::now() - interval;
    }

    pub(super) fn lookup<T: Clone>(
        &self,
        map: &DashMap<String, RouteCacheEntry<T>>,
        key: &str,
    ) -> RouteCacheLookup<T> {
        let Some(entry) = map.get(key) else {
            return RouteCacheLookup::Miss;
        };
        if tokio::time::Instant::now() >= entry.expires_at {
            let expired = entry.value.clone();
            drop(entry);
            map.remove(key);
            return RouteCacheLookup::Expired(expired);
        }
        RouteCacheLookup::Hit(entry.value.clone())
    }

    pub(super) fn remember<T: Clone>(
        &self,
        map: &DashMap<String, RouteCacheEntry<T>>,
        key: String,
        value: T,
        ttl: Duration,
    ) -> Option<T> {
        map.insert(
            key,
            RouteCacheEntry {
                value,
                expires_at: tokio::time::Instant::now() + ttl,
            },
        )
        .map(|entry| entry.value)
    }
}

pub(super) struct RouteRefreshSlot<'a> {
    cache: &'a RouteCache,
    key: String,
    lock: Option<Arc<tokio::sync::Mutex<()>>>,
    guard: Option<tokio::sync::OwnedMutexGuard<()>>,
}

impl Drop for RouteRefreshSlot<'_> {
    fn drop(&mut self) {
        self.guard.take();
        self.lock.take();
        self.cache
            .refresh_locks
            .remove_if(&self.key, |_, lock| Arc::strong_count(lock) == 1);
    }
}

#[derive(Clone, Debug)]
pub(super) struct RouteCacheEntry<T> {
    pub(super) value: T,
    pub(super) expires_at: tokio::time::Instant,
}

#[derive(Clone, Debug)]
pub(super) enum RouteCacheLookup<T> {
    Hit(T),
    Miss,
    Expired(T),
}

impl<T> RouteCacheLookup<T> {
    pub(super) fn result_label(&self) -> &'static str {
        match self {
            Self::Hit(_) => "hit",
            Self::Miss => "miss",
            Self::Expired(_) => "expired",
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct EcwProcessInputLocks {
    pub(super) locks: DashMap<EcwProcessInputKey, Arc<tokio::sync::Mutex<()>>>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct EcwProcessInputKey {
    pub(super) sandbox_id: SandboxId,
    pub(super) process_id: ProcessId,
}

impl EcwProcessInputLocks {
    pub(super) fn lock_for(
        &self,
        sandbox_id: SandboxId,
        process_id: ProcessId,
    ) -> Arc<tokio::sync::Mutex<()>> {
        self.prune_idle();
        let key = EcwProcessInputKey {
            sandbox_id,
            process_id,
        };
        self.locks
            .entry(key)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    pub(super) fn prune_idle(&self) {
        self.locks.retain(|_, lock| Arc::strong_count(lock) > 1);
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.locks.len()
    }
}

pub(super) fn native_route_cache_prefix(session_id: DataPlaneSessionId) -> String {
    format!("native:{session_id}:")
}

pub(super) fn native_route_cache_key(
    session_id: DataPlaneSessionId,
    token_sha256: &str,
    required_scope: &str,
) -> String {
    format!(
        "{}{}:{}",
        native_route_cache_prefix(session_id),
        required_scope,
        token_sha256
    )
}

pub(super) fn ecw_route_cache_prefix(sandbox_id: SandboxId, port: u32) -> String {
    format!("ecw:{sandbox_id}:{port}:")
}

pub(super) fn ecw_route_cache_key(
    target: &EcwSandboxHost,
    transport: EcwRouteTransport,
    envd_access_token_sha256: Option<&str>,
    traffic_access_token_sha256: Option<&str>,
    api_key_sha256: Option<&str>,
) -> String {
    format!(
        "{}{}:{}:{}:{}",
        ecw_route_cache_prefix(target.sandbox_id, target.port),
        transport.as_str(),
        envd_access_token_sha256.unwrap_or("-"),
        traffic_access_token_sha256.unwrap_or("-"),
        api_key_sha256.unwrap_or("-")
    )
}

pub(super) fn parse_ecw_sandbox_host(
    headers: &HeaderMap,
    ecw_host: &str,
) -> Option<EcwSandboxHost> {
    let host = headers
        .get(header::HOST)
        .and_then(|host| host.to_str().ok())
        .map(host_without_port)?
        .to_ascii_lowercase();
    let sandbox_prefix = host
        .strip_suffix(ecw_host)
        .and_then(|prefix| prefix.strip_suffix('.'))?;
    let (port, sandbox_id) = sandbox_prefix.split_once('-')?;
    let port = port.parse::<u32>().ok().filter(|port| {
        matches!(
            u16::try_from(*port),
            Ok(valid_port) if valid_port != 0
        )
    })?;
    let sandbox_id = sandbox_id.parse::<SandboxId>().ok()?;

    Some(EcwSandboxHost { port, sandbox_id })
}

pub(super) fn normalize_ecw_host(host: &str) -> Option<String> {
    let host = host.trim().trim_end_matches('.').to_ascii_lowercase();
    if host.is_empty() { None } else { Some(host) }
}

pub(super) fn host_without_port(host: &str) -> &str {
    host.split_once(':').map_or(host, |(host, _)| host)
}

#[derive(Debug)]
pub(super) enum AttachError {
    BadRequest(&'static str),
}

impl IntoResponse for AttachError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            Self::BadRequest(message) => (StatusCode::BAD_REQUEST, message),
        };
        data_plane_http_error(
            status,
            DataPlaneHttpErrorCode::InvalidRequest,
            message,
            false,
            ResponseOrigin::Client,
            ResponseKind::Validation,
        )
    }
}

#[derive(Debug)]
pub(super) enum ProxyError {
    BadRequest(&'static str),
    Unauthorized(&'static str),
    Route(RouteResolutionError),
    Upstream(&'static str),
    Internal(&'static str),
}

impl IntoResponse for ProxyError {
    fn into_response(self) -> Response {
        let retryable = matches!(
            self,
            Self::Route(RouteResolutionError::SessionNotReady)
                | Self::Route(RouteResolutionError::ResolutionFailed)
                | Self::Upstream(_)
        );
        let (status, code, message, origin, kind) = match self {
            Self::BadRequest(message) => (
                StatusCode::BAD_REQUEST,
                DataPlaneHttpErrorCode::InvalidRequest,
                message,
                ResponseOrigin::Client,
                ResponseKind::Validation,
            ),
            Self::Unauthorized(message) => (
                StatusCode::UNAUTHORIZED,
                DataPlaneHttpErrorCode::Unauthorized,
                message,
                ResponseOrigin::Client,
                ResponseKind::Auth,
            ),
            Self::Route(error) => {
                let (origin, kind) = error.classification();
                match error {
                    RouteResolutionError::Unauthenticated => (
                        StatusCode::UNAUTHORIZED,
                        DataPlaneHttpErrorCode::Unauthorized,
                        "invalid or expired data-plane token",
                        origin,
                        kind,
                    ),
                    RouteResolutionError::Forbidden => (
                        StatusCode::FORBIDDEN,
                        DataPlaneHttpErrorCode::Forbidden,
                        "data-plane token does not allow this session",
                        origin,
                        kind,
                    ),
                    RouteResolutionError::SessionNotFound => (
                        StatusCode::NOT_FOUND,
                        DataPlaneHttpErrorCode::NotFound,
                        "session not found",
                        origin,
                        kind,
                    ),
                    RouteResolutionError::SessionNotReady => (
                        StatusCode::SERVICE_UNAVAILABLE,
                        DataPlaneHttpErrorCode::NotReady,
                        "session route is not ready",
                        origin,
                        kind,
                    ),
                    RouteResolutionError::ResolutionFailed => (
                        StatusCode::SERVICE_UNAVAILABLE,
                        DataPlaneHttpErrorCode::ServiceUnavailable,
                        "session route could not be resolved",
                        origin,
                        kind,
                    ),
                    RouteResolutionError::RouteStateInvalid => (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        DataPlaneHttpErrorCode::Internal,
                        "session route is invalid",
                        origin,
                        kind,
                    ),
                }
            }
            Self::Upstream(message) => (
                StatusCode::BAD_GATEWAY,
                DataPlaneHttpErrorCode::UpstreamError,
                message,
                ResponseOrigin::Node,
                ResponseKind::BackendTransport,
            ),
            Self::Internal(message) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                DataPlaneHttpErrorCode::Internal,
                message,
                ResponseOrigin::DataPlane,
                ResponseKind::Internal,
            ),
        };
        observe_data_plane_http_error(status, origin, kind);
        (
            status,
            Json(DataPlaneHttpError {
                code,
                message: message.to_string(),
                retryable,
            }),
        )
            .into_response()
    }
}

impl From<AttachError> for ProxyError {
    fn from(error: AttachError) -> Self {
        match error {
            AttachError::BadRequest(message) => Self::BadRequest(message),
        }
    }
}

#[derive(Debug)]
pub(super) struct EcwProxyError {
    pub(super) status: StatusCode,
    pub(super) message: String,
    pub(super) sandbox_id: Option<SandboxId>,
    pub(super) retry_after: Option<Duration>,
    /// Refused because the replica is draining; the response carries the
    /// draining headers.
    pub(super) draining: bool,
    pub(super) can_use_runtime_access: bool,
    pub(super) origin: ResponseOrigin,
    pub(super) kind: ResponseKind,
    backend_status: Option<tonic::Status>,
    too_many_requests_reason: Option<TooManyRequestsReason>,
}

impl EcwProxyError {
    pub(super) fn bad_request(message: impl Into<String>) -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            message,
            ResponseOrigin::Client,
            ResponseKind::Validation,
        )
    }

    pub(super) fn unauthorized() -> Self {
        Self::new(
            StatusCode::UNAUTHORIZED,
            "Invalid or missing API key",
            ResponseOrigin::Client,
            ResponseKind::Auth,
        )
    }

    pub(super) fn not_found(resource: &'static str) -> Self {
        Self::new(
            StatusCode::NOT_FOUND,
            format!("{resource} not found"),
            ResponseOrigin::Client,
            ResponseKind::Validation,
        )
    }

    pub(super) fn sandbox_data_plane_unavailable() -> Self {
        Self::new(
            StatusCode::BAD_GATEWAY,
            "Sandbox is not running",
            ResponseOrigin::Cluster,
            ResponseKind::RouteUnavailable,
        )
    }

    pub(super) fn sandbox_timeout() -> Self {
        Self::sandbox_data_plane_unavailable()
            .with_classification(ResponseOrigin::Cluster, ResponseKind::Timeout)
    }

    pub(super) fn sandbox_waking(retry_after: Duration) -> Self {
        Self::new(
            StatusCode::BAD_GATEWAY,
            "Sandbox is waking; retry",
            ResponseOrigin::Cluster,
            ResponseKind::RouteNotReady,
        )
        .with_retry_after(retry_after)
    }

    pub(super) fn sandbox_service_route_not_ready() -> Self {
        Self::new(
            StatusCode::BAD_GATEWAY,
            "Sandbox service route is not ready",
            ResponseOrigin::Cluster,
            ResponseKind::RouteNotReady,
        )
        .with_retry_after(ECW_SERVICE_ROUTE_NOT_READY_RETRY_AFTER)
    }

    pub(super) fn upstream(message: impl Into<String>) -> Self {
        Self::new(
            StatusCode::BAD_GATEWAY,
            message,
            ResponseOrigin::Node,
            ResponseKind::BackendTransport,
        )
    }

    pub(super) fn sandbox_service_unreachable() -> Self {
        Self::new(
            StatusCode::BAD_GATEWAY,
            "Sandbox service is not reachable",
            ResponseOrigin::SandboxTraffic,
            ResponseKind::BackendTransport,
        )
        .without_runtime_access_classification()
    }

    pub(super) fn too_many_attached_streams(reason: TooManyRequestsReason) -> Self {
        Self::new(
            StatusCode::TOO_MANY_REQUESTS,
            "Too many attached streams",
            ResponseOrigin::DataPlane,
            ResponseKind::Admission,
        )
        .with_too_many_requests_reason(reason)
    }

    /// Never 503: 503 invites immediate retries, so only a 429 with a
    /// jittered `Retry-After` spreads an upload burst out.
    pub(super) fn too_many_uploads(retry_after: Duration) -> Self {
        Self::new(
            StatusCode::TOO_MANY_REQUESTS,
            "Too many in-flight uploads; retry later",
            ResponseOrigin::DataPlane,
            ResponseKind::Admission,
        )
        .with_retry_after(retry_after)
    }

    /// A file transfer refused while its org owes its byte-rate bucket.
    pub(super) fn org_transfer_rate_exceeded(retry_after: Duration) -> Self {
        Self::new(
            StatusCode::TOO_MANY_REQUESTS,
            "Org file transfer rate exceeded; retry later",
            ResponseOrigin::DataPlane,
            ResponseKind::Admission,
        )
        .with_retry_after(retry_after)
    }

    /// An admitted upload whose body arrived below the minimum body rate. 408,
    /// since the client, not the pod, ran out of time.
    pub(super) fn upload_body_stalled() -> Self {
        Self::new(
            StatusCode::REQUEST_TIMEOUT,
            "Upload body arrived too slowly",
            ResponseOrigin::DataPlane,
            ResponseKind::Timeout,
        )
    }

    /// An admitted HTTP/2 upload that hadn't finished within its time limit.
    pub(super) fn upload_body_timed_out() -> Self {
        Self::new(
            StatusCode::REQUEST_TIMEOUT,
            "Upload didn't finish within its time limit",
            ResponseOrigin::DataPlane,
            ResponseKind::Timeout,
        )
    }

    /// A new stream refused because this replica is draining. A 503 rather
    /// than a 429: the refusal says nothing about the caller's load, and the
    /// retry lands on a replica that is not leaving.
    pub(super) fn draining() -> Self {
        let mut error = Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "Data plane is draining; retry",
            ResponseOrigin::DataPlane,
            ResponseKind::Admission,
        );
        error.draining = true;
        error
    }

    /// A streamed input cut at the drain deadline. A 409, which Connect clients
    /// map to a code they don't retry, rather than the retryable 503 of
    /// [`Self::draining`]: some of the input may already have reached the
    /// process, so resending the whole stream could duplicate it.
    pub(super) fn input_interrupted() -> Self {
        Self::new(
            StatusCode::CONFLICT,
            "Data plane shut down mid-stream; input may be partially delivered",
            ResponseOrigin::DataPlane,
            ResponseKind::Timeout,
        )
    }

    pub(super) fn internal(message: impl Into<String>) -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            message,
            ResponseOrigin::DataPlane,
            ResponseKind::Internal,
        )
    }

    pub(super) fn traffic_token_missing(sandbox_id: SandboxId) -> Self {
        Self::new(
            StatusCode::FORBIDDEN,
            format!(
                "Sandbox is secured with traffic access token. Token header '{ECW_TRAFFIC_ACCESS_TOKEN_HEADER}' is missing"
            ),
            ResponseOrigin::Client,
            ResponseKind::Auth,
        )
        .with_sandbox_id(sandbox_id)
    }

    pub(super) fn traffic_token_invalid(sandbox_id: SandboxId) -> Self {
        Self::new(
            StatusCode::FORBIDDEN,
            format!(
                "Sandbox is secured with traffic access token. Provided token in header '{ECW_TRAFFIC_ACCESS_TOKEN_HEADER}' is invalid"
            ),
            ResponseOrigin::Client,
            ResponseKind::Auth,
        )
        .with_sandbox_id(sandbox_id)
    }

    pub(super) fn new(
        status: StatusCode,
        message: impl Into<String>,
        origin: ResponseOrigin,
        kind: ResponseKind,
    ) -> Self {
        Self {
            status,
            message: message.into(),
            sandbox_id: None,
            retry_after: None,
            draining: false,
            can_use_runtime_access: true,
            origin,
            kind,
            backend_status: None,
            too_many_requests_reason: None,
        }
    }

    pub(super) fn with_sandbox_id(mut self, sandbox_id: SandboxId) -> Self {
        self.sandbox_id = Some(sandbox_id);
        self
    }

    pub(super) fn with_retry_after(mut self, retry_after: Duration) -> Self {
        self.retry_after = Some(retry_after);
        self
    }

    pub(super) fn without_runtime_access_classification(mut self) -> Self {
        self.can_use_runtime_access = false;
        self
    }

    pub(super) fn with_classification(
        mut self,
        origin: ResponseOrigin,
        kind: ResponseKind,
    ) -> Self {
        self.origin = origin;
        self.kind = kind;
        self
    }

    pub(super) fn with_backend_status(mut self, status: tonic::Status) -> Self {
        self.backend_status = Some(status);
        self
    }

    /// Names the limit behind a 429 for `data_plane.http.too_many_requests`.
    pub(super) fn with_too_many_requests_reason(mut self, reason: TooManyRequestsReason) -> Self {
        self.too_many_requests_reason = Some(reason);
        self
    }

    pub(super) fn into_tonic_status(self) -> tonic::Status {
        if let Some(status) = self.backend_status {
            return status;
        }
        let code = match self.status {
            StatusCode::BAD_REQUEST => tonic::Code::InvalidArgument,
            StatusCode::UNAUTHORIZED => tonic::Code::Unauthenticated,
            StatusCode::FORBIDDEN => tonic::Code::PermissionDenied,
            StatusCode::NOT_FOUND => tonic::Code::NotFound,
            StatusCode::CONFLICT => tonic::Code::FailedPrecondition,
            StatusCode::TOO_MANY_REQUESTS => tonic::Code::ResourceExhausted,
            StatusCode::GATEWAY_TIMEOUT => tonic::Code::DeadlineExceeded,
            StatusCode::NOT_IMPLEMENTED => tonic::Code::Unimplemented,
            StatusCode::BAD_GATEWAY | StatusCode::SERVICE_UNAVAILABLE => tonic::Code::Unavailable,
            _ => tonic::Code::Internal,
        };
        tonic::Status::new(code, self.message)
    }
}

impl IntoResponse for EcwProxyError {
    fn into_response(self) -> Response {
        pplx_space_metrics::data_plane::response(
            ResponseSurface::Ecw,
            self.origin,
            self.kind,
            self.status.as_u16(),
        );
        tracing::Span::current().record("space.response_origin", self.origin.as_str());
        tracing::Span::current().record("space.response_kind", self.kind.as_str());

        let mut body = serde_json::json!({
            "code": self.status.as_u16(),
            "message": self.message,
        });
        if let Some(sandbox_id) = self.sandbox_id
            && let Some(object) = body.as_object_mut()
        {
            object.insert(
                "sandboxId".to_string(),
                serde_json::json!(sandbox_id.to_string()),
            );
        }

        let mut response = (self.status, Json(body)).into_response();
        if let Some(retry_after) = self.retry_after
            && let Ok(value) =
                header::HeaderValue::from_str(&retry_after.as_secs().max(1).to_string())
        {
            response.headers_mut().insert(header::RETRY_AFTER, value);
        }
        if self.draining {
            mark_draining_response(&mut response);
        }
        if let Some(reason) = self.too_many_requests_reason {
            response.extensions_mut().insert(reason);
        }
        response
    }
}

pub(super) struct AttachedStreamLimiter {
    pub(super) max_attached: usize,
    pub(super) max_per_org: usize,
    pub(super) max_per_sandbox: usize,
    pub(super) total: AtomicUsize,
    pub(super) by_kind: DashMap<&'static str, Arc<AtomicUsize>>,
    pub(super) by_org: DashMap<OrgId, Arc<AtomicUsize>>,
    pub(super) by_sandbox: DashMap<SandboxId, Arc<AtomicUsize>>,
}

impl AttachedStreamLimiter {
    pub(super) fn new(max_attached: usize, max_per_org: usize, max_per_sandbox: usize) -> Self {
        Self {
            max_attached: max_attached.max(1),
            max_per_org: max_per_org.max(1),
            max_per_sandbox: max_per_sandbox.max(1),
            total: AtomicUsize::new(0),
            by_kind: DashMap::new(),
            by_org: DashMap::new(),
            by_sandbox: DashMap::new(),
        }
    }

    pub(super) fn try_acquire_attached(
        self: &Arc<Self>,
        attached: &AttachedSession,
    ) -> Result<AttachedStreamPermit, AttachedStreamLimit> {
        let kind = attached.kind.as_str();
        self.try_acquire_total()
            .map_err(|()| AttachedStreamLimit::Total)?;

        let kind_counter = self
            .by_kind
            .entry(kind)
            .or_insert_with(|| Arc::new(AtomicUsize::new(0)))
            .clone();
        let kind_count = kind_counter.fetch_add(1, Ordering::AcqRel) + 1;

        let org_key = attached.org_id;
        let org_counter = self
            .by_org
            .entry(org_key)
            .or_insert_with(|| Arc::new(AtomicUsize::new(0)))
            .clone();
        if !try_increment_bounded(&org_counter, self.max_per_org) {
            decrement_counter(&kind_counter);
            decrement_counter(&self.total);
            return Err(AttachedStreamLimit::Org);
        }

        let sandbox_key = attached.sandbox_id;
        let sandbox_counter = self
            .by_sandbox
            .entry(sandbox_key)
            .or_insert_with(|| Arc::new(AtomicUsize::new(0)))
            .clone();
        if !try_increment_bounded(&sandbox_counter, self.max_per_sandbox) {
            if decrement_counter(&org_counter) == 0 {
                remove_zero_counter(&self.by_org, &org_key, &org_counter);
            }
            decrement_counter(&kind_counter);
            decrement_counter(&self.total);
            return Err(AttachedStreamLimit::Sandbox);
        }

        pplx_space_metrics::data_plane::stream_active("all", self.total.load(Ordering::Acquire));
        pplx_space_metrics::data_plane::stream_active(kind, kind_count);

        Ok(AttachedStreamPermit {
            limiter: Arc::clone(self),
            kind,
            kind_counter,
            org_key: Some(org_key),
            org_counter: Some(org_counter),
            sandbox_key: Some(sandbox_key),
            sandbox_counter: Some(sandbox_counter),
        })
    }

    pub(super) fn try_acquire_label(
        self: &Arc<Self>,
        kind: &'static str,
    ) -> Result<AttachedStreamPermit, ()> {
        self.try_acquire_total()?;
        let kind_counter = self
            .by_kind
            .entry(kind)
            .or_insert_with(|| Arc::new(AtomicUsize::new(0)))
            .clone();
        let kind_count = kind_counter.fetch_add(1, Ordering::AcqRel) + 1;
        pplx_space_metrics::data_plane::stream_active("all", self.total.load(Ordering::Acquire));
        pplx_space_metrics::data_plane::stream_active(kind, kind_count);

        Ok(AttachedStreamPermit {
            limiter: Arc::clone(self),
            kind,
            kind_counter,
            org_key: None,
            org_counter: None,
            sandbox_key: None,
            sandbox_counter: None,
        })
    }

    pub(super) fn try_acquire_total(&self) -> Result<(), ()> {
        loop {
            let current = self.total.load(Ordering::Acquire);
            if current >= self.max_attached {
                return Err(());
            }
            if self
                .total
                .compare_exchange(current, current + 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break;
            }
        }
        Ok(())
    }

    pub(super) fn active_total(&self) -> usize {
        self.total.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(super) fn active_kind(&self, kind: SessionKind) -> usize {
        self.active_label(kind.as_str())
    }

    #[cfg(test)]
    pub(super) fn active_label(&self, kind: &'static str) -> usize {
        self.by_kind
            .get(kind)
            .map(|counter| counter.load(Ordering::Acquire))
            .unwrap_or(0)
    }

    #[cfg(test)]
    pub(super) fn active_org(&self, org_id: OrgId) -> usize {
        self.by_org
            .get(&org_id)
            .map(|counter| counter.load(Ordering::Acquire))
            .unwrap_or(0)
    }

    #[cfg(test)]
    pub(super) fn active_sandbox(&self, sandbox_id: SandboxId) -> usize {
        self.by_sandbox
            .get(&sandbox_id)
            .map(|counter| counter.load(Ordering::Acquire))
            .unwrap_or(0)
    }

    #[cfg(test)]
    pub(super) fn tracked_orgs(&self) -> usize {
        self.by_org.len()
    }

    #[cfg(test)]
    pub(super) fn tracked_sandboxes(&self) -> usize {
        self.by_sandbox.len()
    }
}

pub(super) struct AttachedStreamPermit {
    pub(super) limiter: Arc<AttachedStreamLimiter>,
    pub(super) kind: &'static str,
    pub(super) kind_counter: Arc<AtomicUsize>,
    pub(super) org_key: Option<OrgId>,
    pub(super) org_counter: Option<Arc<AtomicUsize>>,
    pub(super) sandbox_key: Option<SandboxId>,
    pub(super) sandbox_counter: Option<Arc<AtomicUsize>>,
}

impl Drop for AttachedStreamPermit {
    fn drop(&mut self) {
        let total = self.total_after_release();
        let kind_count = decrement_counter(&self.kind_counter);
        if let (Some(key), Some(counter)) = (&self.org_key, &self.org_counter)
            && decrement_counter(counter) == 0
        {
            remove_zero_counter(&self.limiter.by_org, key, counter);
        }
        if let (Some(key), Some(counter)) = (&self.sandbox_key, &self.sandbox_counter)
            && decrement_counter(counter) == 0
        {
            remove_zero_counter(&self.limiter.by_sandbox, key, counter);
        }
        pplx_space_metrics::data_plane::stream_active("all", total);
        pplx_space_metrics::data_plane::stream_active(self.kind, kind_count);
    }
}

impl AttachedStreamPermit {
    pub(super) fn total_after_release(&self) -> usize {
        decrement_counter(&self.limiter.total)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AttachedStreamLimit {
    Total,
    Org,
    Sandbox,
}

impl AttachedStreamLimit {
    pub(super) const fn metric_label(self) -> &'static str {
        match self {
            Self::Total => "too_many_attached_streams",
            Self::Org => "too_many_attached_streams_per_org",
            Self::Sandbox => "too_many_attached_streams_per_sandbox",
        }
    }

    pub(super) const fn too_many_requests_reason(self) -> TooManyRequestsReason {
        match self {
            Self::Total => TooManyRequestsReason::AttachedStreams,
            Self::Org => TooManyRequestsReason::AttachedStreamsPerOrg,
            Self::Sandbox => TooManyRequestsReason::AttachedStreamsPerSandbox,
        }
    }
}

pub(super) fn try_increment_bounded(counter: &AtomicUsize, max: usize) -> bool {
    loop {
        let current = counter.load(Ordering::Acquire);
        if current >= max {
            return false;
        }
        if counter
            .compare_exchange(current, current + 1, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            return true;
        }
    }
}

pub(super) fn decrement_counter(counter: &AtomicUsize) -> usize {
    counter
        .try_update(Ordering::AcqRel, Ordering::Acquire, |value| {
            Some(value.saturating_sub(1))
        })
        .map(|previous| previous.saturating_sub(1))
        .unwrap_or_else(std::convert::identity)
}

pub(super) fn remove_zero_counter<K>(
    counters: &DashMap<K, Arc<AtomicUsize>>,
    key: &K,
    counter: &Arc<AtomicUsize>,
) where
    K: Eq + std::hash::Hash,
{
    counters.remove_if(key, |_, current| {
        Arc::ptr_eq(current, counter) && current.load(Ordering::Acquire) == 0
    });
}

#[derive(Default)]
pub(super) struct NativeSessionAttachLimiter {
    pub(super) active: DashMap<String, ()>,
}

impl NativeSessionAttachLimiter {
    pub(super) fn try_acquire(
        self: &Arc<Self>,
        session_id: DataPlaneSessionId,
        kind: SessionKind,
    ) -> Result<Option<NativeSessionAttachPermit>, ()> {
        if !matches!(kind, SessionKind::Terminal | SessionKind::Process) {
            return Ok(None);
        }

        let key = session_id.to_string();
        match self.active.entry(key.clone()) {
            dashmap::mapref::entry::Entry::Occupied(_) => Err(()),
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(());
                Ok(Some(NativeSessionAttachPermit {
                    limiter: Arc::clone(self),
                    key,
                }))
            }
        }
    }

    #[cfg(test)]
    pub(super) fn is_active(&self, session_id: DataPlaneSessionId) -> bool {
        self.active.contains_key(&session_id.to_string())
    }
}

pub(super) struct NativeSessionAttachPermit {
    pub(super) limiter: Arc<NativeSessionAttachLimiter>,
    pub(super) key: String,
}

impl Drop for NativeSessionAttachPermit {
    fn drop(&mut self) {
        self.limiter.active.remove(&self.key);
    }
}

pub(super) struct UnauthenticatedUpgradeLimiter {
    pub(super) cap_per_ip: usize,
    pub(super) counts: DashMap<IpAddr, Arc<AtomicUsize>>,
}

impl UnauthenticatedUpgradeLimiter {
    pub(super) fn new(cap_per_ip: usize) -> Self {
        Self {
            cap_per_ip,
            counts: DashMap::new(),
        }
    }

    pub(super) fn try_register(
        self: &Arc<Self>,
        ip: IpAddr,
    ) -> Result<UnauthenticatedUpgradeGuard, ()> {
        let counter = self
            .counts
            .entry(ip)
            .or_insert_with(|| Arc::new(AtomicUsize::new(0)))
            .clone();
        let previous = counter.fetch_add(1, Ordering::AcqRel);
        if previous >= self.cap_per_ip {
            counter.fetch_sub(1, Ordering::AcqRel);
            return Err(());
        }
        Ok(UnauthenticatedUpgradeGuard {
            limiter: Arc::clone(self),
            ip,
            counter,
        })
    }
}

pub(super) struct UnauthenticatedUpgradeGuard {
    pub(super) limiter: Arc<UnauthenticatedUpgradeLimiter>,
    pub(super) ip: IpAddr,
    pub(super) counter: Arc<AtomicUsize>,
}

impl Drop for UnauthenticatedUpgradeGuard {
    fn drop(&mut self) {
        if decrement_counter(&self.counter) == 0 {
            self.limiter.counts.remove(&self.ip);
        }
    }
}

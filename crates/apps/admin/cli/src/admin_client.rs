use comfy_table::{Table, presets::UTF8_FULL_CONDENSED};
use pplx_space_common_core::types::{
    NodeAddress, TemplateReference,
    admin::{AdminNodeNetworkSlots, AdminNodeResponse, AdminNodeSort, DrainRequest},
};
use reqwest::{StatusCode, header};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use thiserror::Error;
use uuid::Uuid;

use crate::cli::{DrainPhaseFilter, ListNodesArgs, RoleFilter};

/// Path prefix for the admin API. The EKS admin surface mounts these routes at
/// both `/v1` (dedicated-host) and `/admin/api/v1` (shared-host); the local
/// kind/Tilt single-process api-server (`--surface=shared`) mounts them only at
/// `/admin/api/v1`. `/admin/api/v1` is therefore the one prefix that resolves on
/// every deployment, including through the prod ingress `/admin` path rule.
pub(crate) const ADMIN_API_PREFIX: &str = "/admin/api/v1";

/// HTTP client for the admin API surface. One per process — `reqwest::Client`
/// is already an `Arc`-backed pool, no extra wrapping needed.
pub(crate) struct AdminClient {
    pub(crate) base_url: String,
    pub(crate) http: reqwest::Client,
}

pub(crate) fn is_loopback(url: &reqwest::Url) -> bool {
    url.host_str().is_some_and(|host| {
        host == "localhost"
            || host
                .trim_matches(['[', ']'])
                .parse::<std::net::IpAddr>()
                .is_ok_and(|address| address.to_canonical().is_loopback())
    })
}

pub(crate) fn http_client(api_url: &str) -> reqwest::ClientBuilder {
    let builder = reqwest::Client::builder().redirect(reqwest::redirect::Policy::none());
    if reqwest::Url::parse(api_url).is_ok_and(|url| is_loopback(&url)) {
        // A proxy cannot reach the operator's local port forward.
        builder.no_proxy()
    } else {
        builder
    }
}

impl AdminClient {
    pub(crate) fn new(api_url: &str, api_token: &str) -> Result<Self, String> {
        let mut headers = header::HeaderMap::new();
        let mut auth = header::HeaderValue::from_str(&format!("Bearer {api_token}"))
            .map_err(|err| format!("invalid SPACE_API_TOKEN: {err}"))?;
        auth.set_sensitive(true);
        headers.insert(header::AUTHORIZATION, auth);
        headers.insert(
            header::ACCEPT,
            header::HeaderValue::from_static("application/json"),
        );

        let http = http_client(api_url)
            .default_headers(headers)
            .build()
            .map_err(|err| format!("failed to build HTTP client: {err}"))?;

        Ok(Self {
            base_url: api_url.trim_end_matches('/').to_string(),
            http,
        })
    }

    pub(crate) async fn post_node_action(
        &self,
        node_ref: &str,
        action: &str,
    ) -> Result<String, CliError> {
        let node = self.resolve_node_ref(node_ref).await?;
        let url = action_url(&self.base_url, &node, action);
        let response = self
            .http
            .post(&url)
            .send()
            .await
            .map_err(|err| CliError::Transport(err.to_string()))?;

        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|err| CliError::Transport(err.to_string()))?;
        interpret_response(status, &body)
    }

    /// Drain variant that carries a [`DrainRequest`] body. Kept separate
    /// from [`post_node_action`] so the bodiless POST path used by the other
    /// action verbs (`terminate`, `undrain`, `cordon`, `uncordon`) stays
    /// untouched — the API ignores bodies on those actions, but sending one
    /// would be a needless protocol surprise.
    pub(crate) async fn post_drain(
        &self,
        node_ref: &str,
        request: &DrainRequest,
    ) -> Result<String, CliError> {
        let node = self.resolve_node_ref(node_ref).await?;
        let url = action_url(&self.base_url, &node, "drain");
        let response = self
            .http
            .post(&url)
            .json(request)
            .send()
            .await
            .map_err(|err| CliError::Transport(err.to_string()))?;

        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|err| CliError::Transport(err.to_string()))?;
        interpret_response(status, &body)
    }

    /// Resolve a user-supplied node reference to a full UUID. Accepts a full
    /// UUID, a unique short id prefix (min 4 chars), or an exact
    /// `k8s_node_name` / `address`. Matches the `sbx-mgr` UX where operators
    /// paste the short `ID` column from the table back in, and lets them target
    /// a node by the k8s name they already have from other tooling.
    ///
    /// Full UUIDs short-circuit and never hit the API. Everything else triggers
    /// a `GET /admin/api/v1/nodes` and client-side match — ambiguous references
    /// surface every match so the operator can pick the right one.
    async fn resolve_node_ref(&self, node_ref: &str) -> Result<String, CliError> {
        let trimmed = node_ref.trim();
        if trimmed.is_empty() {
            return Err(CliError::Argument("node argument must not be empty".into()));
        }
        if Uuid::parse_str(trimmed).is_ok() {
            return Ok(trimmed.to_string());
        }
        let body = self
            .get(&list_url(&self.base_url, None, None, true, None, None))
            .await?;
        let nodes: Vec<AdminNodeResponse> = serde_json::from_str(&body).map_err(|err| {
            CliError::Decode(format!(
                "could not parse /admin/api/v1/nodes response while resolving prefix: {err}"
            ))
        })?;
        resolve_prefix(trimmed, &nodes)
    }

    pub(crate) async fn list_nodes(&self, args: &ListNodesArgs) -> Result<String, CliError> {
        let url = list_url(
            &self.base_url,
            args.drain_phase,
            args.role,
            args.include_deleted,
            args.sort,
            args.long_runner_age_hours,
        );
        let body = self.get(&url).await?;
        render_listing(&body, args)
    }

    pub(crate) async fn get_node(
        &self,
        node_ref: &str,
        json: bool,
        diagnostics: bool,
    ) -> Result<String, CliError> {
        let node = self.resolve_node_ref(node_ref).await?;
        let url = single_node_url(&self.base_url, &node, diagnostics);
        let body = self.get(&url).await?;
        if json {
            return Ok(pretty_json_or_raw(&body));
        }
        let node: AdminNodeResponse = serde_json::from_str(&body).map_err(|err| {
            CliError::Decode(format!(
                "could not parse /admin/api/v1/nodes/{{id}} response: {err}"
            ))
        })?;
        Ok(render_node_detail(&node))
    }

    pub(crate) async fn upsert_warm_pool(
        &self,
        request: &WarmPoolConfigRequest,
        json: bool,
    ) -> Result<String, CliError> {
        let url = warm_pools_url(&self.base_url);
        let response = self
            .http
            .put(&url)
            .json(request)
            .send()
            .await
            .map_err(|err| CliError::Transport(err.to_string()))?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|err| CliError::Transport(err.to_string()))?;
        let body = interpret_response(status, &body)?;
        if json {
            return Ok(body);
        }
        let config: WarmPoolConfigResponse = serde_json::from_str(&body).map_err(|err| {
            CliError::Decode(format!(
                "could not parse /admin/api/v1/warm-pools response: {err}"
            ))
        })?;
        Ok(render_warm_pool_config(&config))
    }

    pub(crate) async fn list_warm_pools(&self, json: bool) -> Result<String, CliError> {
        let body = self.get(&warm_pools_url(&self.base_url)).await?;
        if json {
            return Ok(pretty_json_or_raw(&body));
        }
        let configs: Vec<WarmPoolConfigResponse> = serde_json::from_str(&body).map_err(|err| {
            CliError::Decode(format!(
                "could not parse /admin/api/v1/warm-pools response: {err}"
            ))
        })?;
        Ok(render_warm_pool_table(&configs))
    }

    pub(crate) async fn get_warm_pool(
        &self,
        pool_ref: &str,
        json: bool,
    ) -> Result<String, CliError> {
        let body = self.get(&warm_pools_url(&self.base_url)).await?;
        let configs: Vec<WarmPoolConfigResponse> = serde_json::from_str(&body).map_err(|err| {
            CliError::Decode(format!(
                "could not parse /admin/api/v1/warm-pools response: {err}"
            ))
        })?;
        let config = resolve_warm_pool_ref(pool_ref, &configs)?;
        if json {
            return serde_json::to_string_pretty(config)
                .map_err(|err| CliError::Decode(format!("could not encode warm pool: {err}")));
        }
        Ok(render_warm_pool_config(config))
    }

    async fn get(&self, url: &str) -> Result<String, CliError> {
        let response = self
            .http
            .get(url)
            .send()
            .await
            .map_err(|err| CliError::Transport(err.to_string()))?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|err| CliError::Transport(err.to_string()))?;
        if status.is_success() {
            Ok(body)
        } else {
            Err(CliError::Api {
                status: status.as_u16(),
                body: pretty_json_or_raw(&body),
            })
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum CliError {
    /// The CLI itself rejected the argument before sending anything.
    #[error("{0}")]
    Argument(String),
    /// Could not reach the API at all (DNS, TLS, connection refused, …).
    #[error("transport error: {0}")]
    Transport(String),
    /// API returned a non-2xx with a body. Body is propagated verbatim so
    /// operators see the server's error JSON unchanged.
    #[error("API error (HTTP {status}): {body}")]
    Api { status: u16, body: String },
    /// API returned 2xx but the body was not in the expected shape.
    #[error("decode error: {0}")]
    Decode(String),
    /// API answered 2xx having silently ignored a parameter the command needs,
    /// so the response does not mean what the flags asked for. Distinct from
    /// `Decode`: the body parsed fine, it just isn't an answer to the question.
    #[error("{0}")]
    Unsupported(String),
    #[error("build {job_id} reached {state}: {error}; content_hash: {hash}",
        hash = .content_hash.as_ref().map_or_else(|| "unavailable".to_string(), ToString::to_string))]
    Build {
        job_id: String,
        state: pplx_space_common_core::types::TemplateBuildJobState,
        content_hash: Option<pplx_space_common_core::types::ContentHash>,
        error: String,
    },
}

/// Everything `list_nodes` does after the fetch: apply the skew guard, then
/// render. Separate from the request so the guard's four `--sort` / `--json`
/// combinations can be exercised without a live server.
///
/// The boundary *above* this function is still untested: nothing asserts that
/// `list_nodes` routes through here, so a refactor that inlines the
/// decode-and-render — adding paging, a retry, a `--limit` — would drop the
/// guard with CI green. Closing it needs a stub HTTP listener, which the
/// existing `tokio` dev-dependency (`macros`, `rt-multi-thread`) already
/// supports — a one-shot `TcpListener` serving a canned response — so it is
/// undone rather than blocked. Keep the call site a single expression so the
/// temptation stays visible.
///
/// The guard runs on both rendering paths, because the skew is invisible in
/// either: a server that ignores the unknown query parameters answers 200 with
/// name-ordered rows, which `--json` would otherwise hand straight back.
///
/// Plain `--json` never *typed*-decodes, so a body this CLI cannot deserialize
/// into [`AdminNodeResponse`] still reaches the operator — but not
/// byte-for-byte. [`pretty_json_or_raw`] round-trips through
/// `serde_json::Value` and re-serializes, so whitespace and indentation are the
/// re-serializer's, not the server's. A body that is not JSON, or that is
/// already in exactly this pretty form, survives unchanged.
///
/// Object key order follows the workspace-wide `serde_json` feature selection
/// and is not part of this output contract.
///
/// `--sort drainability` additionally typed-decodes, giving up even that: a
/// response carrying an unknown enum variant fails here rather than printing.
/// Ranking without the guard is the worse trade, but dropping `--sort` restores
/// the pretty-printed passthrough.
fn render_listing(body: &str, args: &ListNodesArgs) -> Result<String, CliError> {
    let needs_ranking = args.sort.is_some_and(AdminNodeSort::needs_drainability);
    if args.json {
        if needs_ranking {
            // Name the escape hatch in the error too. Only the `Unsupported`
            // message below mentions dropping `--sort`, and this is the branch
            // where an operator who wanted the raw body gets neither it nor a
            // way back to it.
            let nodes = decode_nodes(body).map_err(|err| match err {
                CliError::Decode(message) => CliError::Decode(format!(
                    "{message}. --sort drainability has to decode the \
                     response to check it is ranked; re-run without --sort to \
                     print the body as-is."
                )),
                other => other,
            })?;
            ensure_ranked(&nodes)?;
        }
        return Ok(pretty_json_or_raw(body));
    }
    let nodes = decode_nodes(body)?;
    if needs_ranking {
        ensure_ranked(&nodes)?;
    }
    Ok(render_nodes_table(&nodes, args.verbose))
}

/// The long-runner threshold every drainability block in this listing agrees
/// on, or `None` if they disagree or none carry one. One request yields one
/// threshold, so disagreement means a server that mixed sources — worth showing
/// nothing rather than a number that is right for only some rows.
fn unanimous_long_runner_age(nodes: &[AdminNodeResponse]) -> Option<u32> {
    let mut hours = nodes
        .iter()
        .filter_map(|node| node.drainability.map(|d| d.long_runner_age_hours));
    let first = hours.next()?;
    hours.all(|other| other == first).then_some(first)
}

fn decode_nodes(body: &str) -> Result<Vec<AdminNodeResponse>, CliError> {
    serde_json::from_str(body).map_err(|err| {
        CliError::Decode(format!(
            "could not parse /admin/api/v1/nodes response: {err}"
        ))
    })
}

/// Refuse a listing that was asked to rank but came back without ranking data.
///
/// An API server that predates `sort=drainability` ignores the unknown query
/// parameter rather than rejecting it, so the request succeeds and returns rows
/// in name order with no `drainability` block. Presenting that as a ranking is
/// the failure worth guarding: the operator drains whichever node happens to
/// sort first by name, believing it was chosen for having no long-runners.
///
/// This rests on one clause of the wire contract: a server that computed the
/// counters emits the block for **every** node it returns, all-zeros included,
/// precisely because all-zeros is what marks the best gentle-drain candidate.
/// Absent therefore means "not computed", never "computed and empty" — so a
/// listing with no block anywhere is a server that did not compute, and one
/// with a block anywhere is a server that did. A server that instead dropped
/// the block for nodes holding nothing would break this: a fleet of emptied
/// nodes, or `--role cluster`, would come back all-absent and be reported as
/// version skew at the exact moment the true answer was "all ideal".
///
/// Two responses deliberately pass:
/// - **Empty** — indistinguishable from skew, and the legitimate answer to a
///   narrow `--role` / `--drain-phase` filter.
/// - **Partially populated** — unproducible by the current route at all, not
///   merely by an old one: emission is decided once per request, and a node
///   absent from the occupancy map still gets an all-zeros block. Accepted
///   rather than rejected because "this server predates ranking" would be the
///   wrong diagnosis for a server bug, and the rows that do carry blocks are
///   still ranked. The table renders the gaps as `-`. Defensive by intent — the
///   server's own ranking guard anticipates a future per-node computation path,
///   which is when a mixed response would first become reachable.
///
/// "Partially populated" needs at least one row carrying a block, so it cannot
/// apply to a single-row listing: there, a missing block is indistinguishable
/// from skew and is rejected. `--role cluster` is the case that reaches this,
/// and it is why the contract clause above has to hold rather than merely
/// usually hold — today it holds structurally, since the decision is
/// request-scoped rather than per node.
fn ensure_ranked(nodes: &[AdminNodeResponse]) -> Result<(), CliError> {
    if !nodes.is_empty() && nodes.iter().all(|node| node.drainability.is_none()) {
        return Err(CliError::Unsupported(
            "the API returned no drainability data for --sort drainability, so \
             these rows are in name order and are not ranked. This server does \
             not support drain-candidate ranking — either it predates the \
             feature, or it has it and did not compute the counters. Re-run \
             without --sort to list the nodes unranked."
                .to_string(),
        ));
    }
    Ok(())
}

/// Resolve a user-supplied node reference against a slice of node rows. Free
/// function so the matching rules are testable without spinning up an HTTP
/// server.
///
/// Tries an exact match on the substrate-side `k8s_node_name` or `address`
/// first, then falls back to a hyphenated-`id` prefix match. Operators
/// routinely hold a k8s node name (e.g. from `get-nodes-ssd-space.py`) but
/// not the SPACE UUID; matching the name here saves a manual name→UUID lookup.
///
/// Rules:
/// - Exactly one row whose `k8s_node_name` or `address` equals `reference` →
///   return that full id. Exact (not prefix) match keeps names unambiguous.
/// - Otherwise, exactly one row whose hyphenated `id` starts with `reference`
///   → return that full id. The id-prefix path requires `reference` to be at
///   least 4 chars; exact name/address matches are exempt from that floor.
/// - Zero matches → `Argument` error.
/// - More than one match (at either stage) → `Argument` error listing every
///   matching id so the operator can disambiguate.
pub(crate) fn resolve_prefix(
    reference: &str,
    nodes: &[AdminNodeResponse],
) -> Result<String, CliError> {
    let reference_address = reference.parse::<NodeAddress>().ok();
    let exact: Vec<&AdminNodeResponse> = nodes
        .iter()
        .filter(|n| {
            n.k8s_node_name.as_deref() == Some(reference)
                || reference_address.is_some_and(|address| n.address == Some(address))
        })
        .collect();
    match exact.as_slice() {
        [only] => return Ok(only.id.to_string()),
        many @ [_, _, ..] => return Err(ambiguous_node_error(reference, many)),
        [] => {}
    }

    // Id-prefix fallback. A 1–3 char prefix would match a large swath of
    // unrelated ids, so require at least 4 chars here. Exact name/address
    // matches above are exempt — operators can still target a short-named node.
    if reference.len() < 4 {
        return Err(CliError::Argument(format!(
            "no node matches {reference:?}; id prefixes must be at least 4 characters"
        )));
    }

    let matches: Vec<&AdminNodeResponse> = nodes
        .iter()
        .filter(|n| n.id.to_string().starts_with(reference))
        .collect();
    match matches.as_slice() {
        [] => Err(CliError::Argument(format!("no node matches {reference:?}"))),
        [only] => Ok(only.id.to_string()),
        many => Err(ambiguous_node_error(reference, many)),
    }
}

/// Format the shared "ambiguous reference" error, listing every matching id so
/// the operator can resubmit with something more specific.
fn ambiguous_node_error(reference: &str, matches: &[&AdminNodeResponse]) -> CliError {
    let ids = matches
        .iter()
        .map(|n| n.id.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    CliError::Argument(format!(
        "node reference {reference:?} is ambiguous, matches {} nodes: {ids}",
        matches.len()
    ))
}

/// Build `{base}/admin/api/v1/nodes/{node}:{action}`.
///
/// The colon-action verb shape comes from the LLD; matching it exactly is
/// what makes this CLI a "thin wrapper" — there is no client-side route
/// translation to drift from.
pub(crate) fn action_url(base_url: &str, node: &str, action: &str) -> String {
    let base = base_url.trim_end_matches('/');
    format!("{base}{ADMIN_API_PREFIX}/nodes/{node}:{action}")
}

/// Build `{base}/admin/api/v1/nodes/{node}`.
pub(crate) fn single_node_url(base_url: &str, node: &str, diagnostics: bool) -> String {
    let base = base_url.trim_end_matches('/');
    format!("{base}{ADMIN_API_PREFIX}/nodes/{node}?include_occupancy={diagnostics}")
}

/// Build `{base}/admin/api/v1/warm-pools`.
pub(crate) fn warm_pools_url(base_url: &str) -> String {
    let base = base_url.trim_end_matches('/');
    format!("{base}{ADMIN_API_PREFIX}/warm-pools")
}

/// Build `{base}/admin/api/v1/nodes` with the optional list filters appended as
/// a query string. Filter values mirror those accepted by
/// `ListAdminNodesFilters` server-side. `include_deleted=false` is the
/// server default, so we omit the param in that case to keep URLs tidy.
pub(crate) fn list_url(
    base_url: &str,
    drain_phase: Option<DrainPhaseFilter>,
    role: Option<RoleFilter>,
    include_deleted: bool,
    sort: Option<AdminNodeSort>,
    long_runner_age_hours: Option<u32>,
) -> String {
    let base = base_url.trim_end_matches('/');
    let mut params: Vec<(&str, String)> = vec![("include_occupancy", "false".to_string())];
    if let Some(phase) = drain_phase {
        params.push(("drain_phase", phase.as_query_value().to_string()));
    }
    if let Some(role) = role {
        params.push(("role", role.as_query_value().to_string()));
    }
    if include_deleted {
        params.push(("include_deleted", "true".to_string()));
    }
    if let Some(sort) = sort {
        params.push(("sort", sort.as_str().to_string()));
    }
    // Forwarded whenever set, including without `sort`, rather than dropped
    // client-side: which thresholds are acceptable is the server's to define,
    // and a silently discarded flag is worse than one the server rejects. Clap's
    // `u32` has already excluded anything non-integral or negative, so what
    // reaches here is a plain hour count. It has an effect only when the
    // counters are computed.
    if let Some(hours) = long_runner_age_hours {
        params.push(("long_runner_age_hours", hours.to_string()));
    }
    if params.is_empty() {
        format!("{base}{ADMIN_API_PREFIX}/nodes")
    } else {
        let qs = params
            .into_iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");
        format!("{base}{ADMIN_API_PREFIX}/nodes?{qs}")
    }
}

/// Decide what to do with the API's HTTP status + body.
///
/// Pulled out as a free function so we can unit-test the success / failure
/// split without spinning up a server.
pub(crate) fn interpret_response(status: StatusCode, body: &str) -> Result<String, CliError> {
    if status.is_success() {
        Ok(pretty_json_or_raw(body))
    } else {
        Err(CliError::Api {
            status: status.as_u16(),
            body: pretty_json_or_raw(body),
        })
    }
}

fn pretty_json_or_raw(body: &str) -> String {
    match serde_json::from_str::<serde_json::Value>(body) {
        Ok(value) => serde_json::to_string_pretty(&value).unwrap_or_else(|_| body.to_string()),
        Err(_) => body.to_string(),
    }
}

/// Render a slice of node rows as a comfy-table. The default column set is
/// chosen for the "what's draining right now?" workflow: drain phase, when
/// the drain started, and optional occupancy counters that gate the
/// drained-→-terminating advance. `verbose=true` adds the substrate-side
/// k8s node name, the inventory state column, and any drain reconciler
/// error so operators can debug stuck drains without dropping to `--json`.
///
/// The five drainability columns — `VISIBLE`, `LIVE SBX`, `LONG RUNNERS`,
/// `GATE BLOCKERS`, `TERM BLOCKERS` — appear only when the server computed
/// them (`--sort drainability`). Rendering them as `0` otherwise would read as
/// "this node is empty and ready to drain" for every node in the fleet.
///
/// `LONG RUNNERS` carries the threshold the server reported using, so the count
/// is readable without knowing what `--long-runner-age-hours` resolved to.
pub(crate) fn render_nodes_table(nodes: &[AdminNodeResponse], verbose: bool) -> String {
    if nodes.is_empty() {
        return "no nodes match the requested filters".to_string();
    }
    let with_drainability = nodes.iter().any(|node| node.drainability.is_some());
    let mut table = Table::new();
    table.load_style(UTF8_FULL_CONDENSED);
    // Owned rather than `&str` because one header carries a runtime value: the
    // long-runner threshold the server actually used.
    let mut header: Vec<String> = [
        "ID",
        "NAME",
        "ROLE",
        "SCHED",
        "SLOTS",
        "DRAIN PHASE",
        "DRAIN MODE",
        "DRAIN STARTED",
        "ACTIVE SBX",
    ]
    .map(String::from)
    .to_vec();
    if with_drainability {
        // The threshold rides in the header rather than a column of its own: it
        // is one value per request, and a bare `LONG RUNNERS 3` does not say
        // "3 past what age". Rendered only when every block agrees, because the
        // field is per-node and a header derived from one row would be a claim
        // about all of them.
        let long_runners = match unanimous_long_runner_age(nodes) {
            // `> 0` because the block carries `#[serde(default)]`, so a payload
            // omitting the field deserializes to zero — and `>0h` would assert
            // a threshold the list route rejects with a 400 when an operator
            // asks for it. Absent is the honest rendering, same as disagreement.
            Some(hours) if hours > 0 => format!("LONG RUNNERS >{hours}h"),
            _ => "LONG RUNNERS".to_string(),
        };
        header.extend([
            "VISIBLE".to_string(),
            "LIVE SBX".to_string(),
            long_runners,
            "GATE BLOCKERS".to_string(),
            "TERM BLOCKERS".to_string(),
        ]);
    }
    header.extend(["LOCAL SNAPS", "LAST HEARTBEAT"].map(String::from));
    if verbose {
        header.extend(["K8S NODE", "STATE", "DRAIN ERROR"].map(String::from));
    }
    table.set_header(header);
    for node in nodes {
        let mut row = vec![
            short_id(node.id.as_uuid()),
            opt(node.name.as_deref()),
            node.role.to_string(),
            opt(node.scheduling_status.as_deref()),
            network_slots_summary(&node.network_slots),
            opt(node.drain_phase.as_deref()),
            opt_display(node.drain_mode.as_ref()),
            opt(node.drain_started_at.as_deref()),
            opt_num(node.sandboxes_active),
        ];
        if with_drainability {
            // `VISIBLE` is the ranking's first term, and it is not readable off
            // `LAST HEARTBEAT`: a poll that ran a minute ago but *failed* leaves
            // a recent-looking timestamp with `reachable = false`, which sends
            // the node to the bottom. Without this column the operator cannot
            // explain the order they are looking at.
            row.push(
                node.drainability
                    .map(|d| if d.heartbeat_fresh { "yes" } else { "no" })
                    .unwrap_or("-")
                    .to_string(),
            );
            // A node missing the block in a response that carries it for
            // others is a server-side inconsistency, not a zero.
            row.push(opt_num(node.drainability.map(|d| d.live_residents)));
            row.push(opt_num(node.drainability.map(|d| d.live_long_runners)));
            // Deliberately its own column rather than folded into LIVE SBX:
            // the gate's lifecycle arms match on `target_node_id`, so a row
            // whose `active_node_id` is NULL — an in-flight user restore of a
            // suspended sandbox is the common one — holds the gate open while
            // staying invisible to every residency count. A second arm matches
            // through a snapshot's `source_node_id` with no node predicate on
            // the sandbox row at all. So a non-zero here next to a zero LIVE SBX
            // is exactly the case an operator must not read as "ready to
            // drain".
            //
            // The two columns may also overlap rather than partition: a
            // `suspending` row carries both node ids, so whether it is counted
            // twice depends on the same `live_residents` scoping question the
            // TERM BLOCKERS comment below leaves open.
            row.push(opt_num(node.drainability.map(|d| d.drain_blockers)));
            // The final ranking term, so it has to be on screen too. Its
            // scope sits between LIVE SBX and ACTIVE SBX: wider than LIVE SBX
            // by the lifecycles that occupy the node without being evacuable
            // (`stopped`, `created`, `quarantined`, `deleting`, and — depending
            // how narrowly `live_residents` is scoped — the transient ones),
            // narrower than ACTIVE SBX by the phantom-row discount.
            //
            // That discount is the *only* thing separating it from ACTIVE SBX,
            // and the `lifecycle_only` default does not apply it — so on a
            // default-policy cluster this column equals ACTIVE SBX for every
            // node, and carries nothing ACTIVE SBX does not already say.
            row.push(opt_num(node.drainability.map(|d| d.terminate_blockers)));
        }
        row.push(opt_num(node.sandboxes_local_snapshots));
        row.push(opt(node.last_heartbeat.as_deref()));
        if verbose {
            row.push(opt(node.k8s_node_name.as_deref()));
            row.push(opt(node.state.as_deref()));
            row.push(opt(node.drain_last_error.as_deref()));
        }
        table.add_row(row);
    }
    table.to_string()
}

/// Render a single node as a vertical key:value list. The listing table sets no
/// content arrangement, so comfy-table never truncates a cell — instead a
/// row of 10 to 18 columns carrying `name=ip-192-0-2-125.ec2.internal` and full
/// timestamps wraps in the terminal and stops being readable. Vertical layout
/// puts one field per line regardless of width.
///
/// Not every field the admin API returns: `node_api_endpoint`, `updated_at`,
/// `deleted_at`, the `status` snapshot, the `metadata` blob, and the
/// drainability block's `local_snapshots` mirror are all omitted. Use `--json`
/// for those.
pub(crate) fn render_node_detail(node: &AdminNodeResponse) -> String {
    let mut fields: Vec<(&str, String)> = vec![
        ("ID", node.id.to_string()),
        ("Name", opt(node.name.as_deref())),
        ("Address", opt_display(node.address.as_ref())),
        ("Role", node.role.to_string()),
        ("Managed By", node.managed_by.to_string()),
        ("State", opt(node.state.as_deref())),
        ("Scheduling Status", opt(node.scheduling_status.as_deref())),
        ("Scheduling Reason", opt(node.scheduling_reason.as_deref())),
        ("Network Slots", network_slots_detail(&node.network_slots)),
        ("K8s Node", opt(node.k8s_node_name.as_deref())),
        ("Drain Phase", opt(node.drain_phase.as_deref())),
        ("Drain Mode", opt_display(node.drain_mode.as_ref())),
        ("Drain Started", opt(node.drain_started_at.as_deref())),
        ("Drain Last Error", opt(node.drain_last_error.as_deref())),
        (
            "Terminate Decrement Capacity",
            node.terminate_decrement_capacity.to_string(),
        ),
        ("Active Sandboxes", opt_num(node.sandboxes_active)),
        ("Local Snapshots", opt_num(node.sandboxes_local_snapshots)),
        ("Last Heartbeat", opt(node.last_heartbeat.as_deref())),
        ("Created At", node.created_at.clone()),
    ];
    // Appended as a group, policy line first, because it qualifies three of the
    // counts below it and has to be read before them: the discount is the
    // server's `present` flag, which gates `Terminate Blockers`,
    // `Live Residents` and `Live Long Runners`.
    //
    // `Drain Gate Blockers` is the exception — it comes from a separate query
    // that carries no residency term at all, so it is policy-independent.
    // Flipping the policy moves three of these four counts, not all four.
    //
    // That query is also the one place a count is not attributed by a single
    // column: it unions a `sandboxes.target_node_id` arm with a
    // `snapshots.source_node_id` arm, so a node can show a blocker for an
    // upload it is the source of even when no sandbox row points at it.
    // `SELECT … FROM sandboxes WHERE target_node_id = <node>` will not explain
    // the number on its own.
    //
    // `Active Sandboxes` minus `Live Residents` is not a phantom-row count on
    // its own: the difference also covers the `stopped` / `created` /
    // `quarantined` rows that occupy the node without being evacuable, and
    // under the `lifecycle_only` default that is the whole of it, because
    // nothing is discounted for census absence. Which of the two terms it is
    // depends on the policy line above.
    if let Some(evidence) = node.residency_evidence.as_deref() {
        fields.push(("Residency Policy", evidence.to_string()));
    }
    if let Some(drainability) = node.drainability {
        fields.push((
            "Control Plane Visible",
            if drainability.heartbeat_fresh {
                "yes".to_string()
            } else {
                "no — reachable and unexpired heartbeat required".to_string()
            },
        ));
        fields.push((
            "Drain Gate Blockers",
            drainability.drain_blockers.to_string(),
        ));
        fields.push((
            "Terminate Blockers",
            drainability.terminate_blockers.to_string(),
        ));
        fields.push(("Live Residents", drainability.live_residents.to_string()));
        fields.push((
            "Live Long Runners",
            drainability.live_long_runners.to_string(),
        ));
        // The count is uninterpretable without it, and it cannot be inferred:
        // this route uses a fixed default rather than whatever `list-nodes` was
        // last asked for, so the two views legitimately disagree.
        fields.push((
            "Long Runner Threshold",
            format!("{}h", drainability.long_runner_age_hours),
        ));
    }
    let width = fields
        .iter()
        .map(|(label, _)| label.len())
        .max()
        .unwrap_or(0);
    fields
        .iter()
        .map(|(label, value)| format!("{label:<width$}  {value}"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn network_slots_summary(slots: &AdminNodeNetworkSlots) -> String {
    if network_slots_empty(slots) {
        return "-".to_string();
    }
    format!(
        "{}/{} ready, {} leased",
        opt_num(slots.ready),
        opt_num(slots.total),
        opt_num(slots.leased),
    )
}

fn network_slots_detail(slots: &AdminNodeNetworkSlots) -> String {
    if network_slots_empty(slots) {
        return "-".to_string();
    }
    format!(
        "ready={} min_ready={} leased={} total={} degraded={} quarantined={} max={}",
        opt_num(slots.ready),
        opt_num(slots.min_ready),
        opt_num(slots.leased),
        opt_num(slots.total),
        opt_num(slots.degraded),
        opt_num(slots.quarantined),
        opt_num(slots.max_slots),
    )
}

fn network_slots_empty(slots: &AdminNodeNetworkSlots) -> bool {
    slots.ready.is_none()
        && slots.min_ready.is_none()
        && slots.degraded.is_none()
        && slots.quarantined.is_none()
        && slots.total.is_none()
        && slots.leased.is_none()
        && slots.max_slots.is_none()
}

/// Render an optional number, with `-` for absent. Absent is a real state
/// here — an uncomputed counter or a node-manager field the node never
/// reported — and must not print as `0`.
fn opt_num<T: std::fmt::Display>(value: Option<T>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_string())
}

fn opt(value: Option<&str>) -> String {
    value.unwrap_or("-").to_string()
}

fn opt_display<T: std::fmt::Display>(value: Option<&T>) -> String {
    value
        .map(ToString::to_string)
        .unwrap_or_else(|| "-".to_string())
}

#[derive(Debug, Serialize)]
pub(crate) struct WarmPoolConfigRequest {
    pub(crate) template_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) content_hash: Option<String>,
    pub(crate) vcpu: u32,
    pub(crate) memory_mib: u32,
    pub(crate) target_ready_slots: u32,
    pub(crate) refill_per_tick: u32,
    pub(crate) max_warming_slots: u32,
    pub(crate) enabled: bool,
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct WarmPoolConfigResponse {
    pub(crate) id: Uuid,
    pub(crate) template_release_id: Uuid,
    pub(crate) template_name: String,
    pub(crate) content_hash: String,
    pub(crate) vcpu: u32,
    pub(crate) memory_mib: u32,
    pub(crate) target_ready_slots: u32,
    pub(crate) refill_per_tick: u32,
    pub(crate) max_warming_slots: u32,
    pub(crate) namespace: String,
    pub(crate) enabled: bool,
    pub(crate) warming_slots: u32,
    pub(crate) ready_slots: u32,
    pub(crate) claimed_slots: u32,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
}

pub(crate) fn render_warm_pool_config(config: &WarmPoolConfigResponse) -> String {
    let fields: [(&str, String); 17] = [
        ("ID", config.id.to_string()),
        ("Template Release", config.template_release_id.to_string()),
        ("Template", config.template_name.clone()),
        ("Content Hash", config.content_hash.clone()),
        ("vCPU", config.vcpu.to_string()),
        ("Memory MiB", config.memory_mib.to_string()),
        ("Target Ready Slots", config.target_ready_slots.to_string()),
        ("Refill Per Tick", config.refill_per_tick.to_string()),
        ("Max Warming Slots", config.max_warming_slots.to_string()),
        ("Namespace", config.namespace.clone()),
        ("Enabled", config.enabled.to_string()),
        ("Warming Slots", config.warming_slots.to_string()),
        ("Ready Slots", config.ready_slots.to_string()),
        ("Claimed Slots", config.claimed_slots.to_string()),
        ("Created At", config.created_at.clone()),
        ("Updated At", config.updated_at.clone()),
        ("Endpoint", "PUT /admin/api/v1/warm-pools".to_string()),
    ];
    let width = fields
        .iter()
        .map(|(label, _)| label.len())
        .max()
        .unwrap_or(0);
    fields
        .iter()
        .map(|(label, value)| format!("{label:<width$}  {value}"))
        .collect::<Vec<_>>()
        .join("\n")
}

pub(crate) fn render_warm_pool_table(configs: &[WarmPoolConfigResponse]) -> String {
    if configs.is_empty() {
        return "no warm-pool configs found".to_string();
    }
    let mut table = Table::new();
    table.load_style(UTF8_FULL_CONDENSED);
    table.set_header([
        "ID", "TEMPLATE", "HASH", "VCPU", "MEM", "TARGET", "WARMING", "READY", "CLAIMED", "REFILL",
        "MAX WARM", "ENABLED",
    ]);
    for config in configs {
        table.add_row([
            short_id(config.id),
            config.template_name.clone(),
            short_hash(&config.content_hash),
            config.vcpu.to_string(),
            config.memory_mib.to_string(),
            config.target_ready_slots.to_string(),
            config.warming_slots.to_string(),
            config.ready_slots.to_string(),
            config.claimed_slots.to_string(),
            config.refill_per_tick.to_string(),
            config.max_warming_slots.to_string(),
            config.enabled.to_string(),
        ]);
    }
    table.to_string()
}

pub(crate) fn resolve_warm_pool_ref<'a>(
    pool_ref: &str,
    configs: &'a [WarmPoolConfigResponse],
) -> Result<&'a WarmPoolConfigResponse, CliError> {
    let trimmed = pool_ref.trim();
    if trimmed.is_empty() {
        return Err(CliError::Argument(
            "warm-pool reference must not be empty".into(),
        ));
    }

    let id_matches: Vec<&WarmPoolConfigResponse> = configs
        .iter()
        .filter(|config| config.id.to_string().starts_with(trimmed))
        .collect();
    match id_matches.as_slice() {
        [only] => return Ok(only),
        many @ [_, _, ..] => {
            let matches = render_warm_pool_matches(many);
            return Err(CliError::Argument(format!(
                "warm-pool id prefix {trimmed:?} is ambiguous, matches {} configs: {matches}",
                many.len()
            )));
        }
        [] => {}
    }

    let reference = TemplateReference::from_str(trimmed).map_err(|err| {
        CliError::Argument(format!("invalid template reference {trimmed:?}: {err}"))
    })?;

    let matches: Vec<&WarmPoolConfigResponse> = configs
        .iter()
        .filter(|config| template_reference_matches(&reference, config))
        .collect();
    match matches.as_slice() {
        [] => Err(CliError::Argument(format!(
            "no warm-pool config matches template reference {reference}"
        ))),
        [only] => Ok(only),
        many => {
            let matches = render_warm_pool_matches(many);
            Err(CliError::Argument(format!(
                "template reference {reference} is ambiguous, matches {} warm-pool configs: {matches}",
                many.len()
            )))
        }
    }
}

fn template_reference_matches(
    reference: &TemplateReference,
    config: &WarmPoolConfigResponse,
) -> bool {
    if config.template_name != reference.name().as_str() {
        return false;
    }
    if let Some(digest) = reference.artifact_digest() {
        return config.content_hash == digest.to_string();
    }
    if let Some(prefix) = reference.artifact_digest_prefix() {
        return config.content_hash.starts_with(&prefix.to_string());
    }
    reference.uses_default_selector()
}

fn render_warm_pool_matches(configs: &[&WarmPoolConfigResponse]) -> String {
    configs
        .iter()
        .map(|config| {
            format!(
                "{} {} {} vcpu={} memory_mib={}",
                config.id,
                config.template_name,
                config.content_hash,
                config.vcpu,
                config.memory_mib
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// First 8 chars of the hyphenated UUID. Operators copy/paste IDs all the
/// time; the full 36-char UUID makes the table too wide. `--json` keeps the
/// full ID.
fn short_id(id: Uuid) -> String {
    id.to_string().chars().take(8).collect()
}

fn short_hash(hash: &str) -> String {
    hash.chars().take(19).collect()
}

#[cfg(test)]
mod tests;

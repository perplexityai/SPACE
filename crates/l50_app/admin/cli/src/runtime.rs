//! `space-admin-cli runtime env ...`: the cluster's execution environment
//! policy, through `/admin/api/v1/runtime-environments`. The server validates
//! every write and enforces the policy rules; this module sends requests and
//! renders what comes back.

use std::collections::BTreeMap;

use comfy_table::{Table, presets::UTF8_FULL_CONDENSED};
use serde::Deserialize;
use uuid::Uuid;

use crate::{
    admin_client::{ADMIN_API_PREFIX, AdminClient, CliError},
    cli::{RuntimeCommands, RuntimeEnvCommands, RuntimeEnvironmentStateArg},
};

#[derive(Debug, Deserialize)]
struct RecordedChange {
    updated_at: String,
    updated_by_user_id: Option<Uuid>,
    reason: String,
}

#[derive(Debug, Deserialize)]
struct EnvironmentState {
    environment_ref: String,
    state: String,
    is_default: bool,
    last_change: Option<RecordedChange>,
}

#[derive(Debug, Deserialize)]
struct Policy {
    generation: u64,
    default_environment_ref: Option<String>,
    legacy_unbound_allowed: Option<bool>,
    last_change: Option<RecordedChange>,
    environments: Vec<EnvironmentState>,
}

#[derive(Debug, Deserialize)]
struct SetDefaultResponse {
    generation: u64,
    default_environment_ref: String,
}

#[derive(Debug, Deserialize)]
struct SetStateResponse {
    generation: u64,
    environment_ref: String,
    state: String,
}

#[derive(Debug, Deserialize)]
struct EnforcedPolicy {
    default_environment_reference: Option<String>,
    #[serde(default)]
    cluster_policy_unreadable: bool,
}

#[derive(Debug, Deserialize)]
struct NodeStatus {
    node_id: Uuid,
    name: Option<String>,
    role: String,
    substrate_node_id: Option<String>,
    status_fresh: Option<bool>,
    enforced: Option<EnforcedPolicy>,
    #[serde(default)]
    applied_generation: Option<u64>,
    #[serde(default)]
    applied_current: Option<bool>,
    conformance: String,
    #[serde(default)]
    default_differs: bool,
    #[serde(default)]
    executes_outside_policy: Vec<String>,
    #[serde(default)]
    allows_revoked_legacy_unbound: bool,
}

#[derive(Debug, Deserialize)]
struct NodePage {
    generation: u64,
    default_environment_ref: Option<String>,
    nodes: Vec<NodeStatus>,
    next_after: Option<Uuid>,
}

#[derive(Debug, Default, Deserialize)]
struct Counts {
    sandboxes: u64,
    sandboxes_by_lifecycle: BTreeMap<String, u64>,
    #[serde(default)]
    sandboxes_not_runnable: u64,
    template_releases: u64,
    #[serde(default)]
    template_releases_in_use: u64,
}

impl Counts {
    /// Sandboxes, noting how many can't run again.
    fn sandboxes_cell(&self) -> String {
        match self.sandboxes_not_runnable {
            0 => self.sandboxes.to_string(),
            dead => format!("{} ({dead} can't run)", self.sandboxes),
        }
    }

    /// Template releases, noting how many are in use: tagged or pooled.
    fn releases_cell(&self) -> String {
        match self.template_releases {
            0 => "0".to_string(),
            all => format!("{all} ({} in use)", self.template_releases_in_use),
        }
    }
}

#[derive(Debug, Deserialize)]
struct EnvironmentPopulation {
    environment_ref: String,
    state: Option<String>,
    is_default: bool,
    #[serde(flatten)]
    counts: Counts,
}

#[derive(Debug, Deserialize)]
struct Population {
    generation: u64,
    environments: Vec<EnvironmentPopulation>,
    unbound: Counts,
    unrecorded: Counts,
    invalid: Counts,
}

fn runtime_environments_url(base_url: &str, suffix: &str) -> String {
    format!("{base_url}{ADMIN_API_PREFIX}/runtime-environments{suffix}")
}

fn decode<T: serde::de::DeserializeOwned>(body: &str, what: &str) -> Result<T, CliError> {
    serde_json::from_str(body)
        .map_err(|err| CliError::Decode(format!("could not parse the {what} response: {err}")))
}

impl AdminClient {
    pub(crate) async fn runtime_command(
        &self,
        command: RuntimeCommands,
    ) -> Result<String, CliError> {
        let RuntimeCommands::Env { command } = command;
        match command {
            RuntimeEnvCommands::List { json } => {
                let body = self
                    .send(self.http.get(runtime_environments_url(&self.base_url, "")))
                    .await?;
                if json {
                    return Ok(body);
                }
                Ok(render_policy(&decode(&body, "runtime environment policy")?))
            }
            RuntimeEnvCommands::Default {
                environment,
                reason,
                strand,
                if_generation,
                json,
            } => {
                let request = self
                    .http
                    .put(runtime_environments_url(&self.base_url, "/default"))
                    .json(&serde_json::json!({
                        "environment_ref": environment.to_string(),
                        "reason": reason,
                        "strand": strand,
                    }));
                let body = self.send_write(request, if_generation).await?;
                if json {
                    return Ok(body);
                }
                let response: SetDefaultResponse = decode(&body, "set default")?;
                Ok(format!(
                    "Default is now {} (policy generation {}).",
                    response.default_environment_ref, response.generation
                ))
            }
            RuntimeEnvCommands::State {
                environment,
                state,
                reason,
                strand,
                if_generation,
                json,
            } => {
                if strand && state != RuntimeEnvironmentStateArg::Blocked {
                    return Err(CliError::Argument(
                        "--strand applies only to a block".to_string(),
                    ));
                }
                let request = self
                    .http
                    .put(runtime_environments_url(
                        &self.base_url,
                        &format!("/{environment}/state"),
                    ))
                    .json(&serde_json::json!({
                        "state": state.as_str(),
                        "reason": reason,
                        "strand": strand,
                    }));
                let body = self.send_write(request, if_generation).await?;
                if json {
                    return Ok(body);
                }
                let response: SetStateResponse = decode(&body, "set state")?;
                Ok(format!(
                    "{} is now {} (policy generation {}).",
                    response.environment_ref, response.state, response.generation
                ))
            }
            RuntimeEnvCommands::Nodes { after, all, json } => {
                self.runtime_nodes(after, all, json).await
            }
            RuntimeEnvCommands::Population { json } => {
                let body = self
                    .send(
                        self.http
                            .get(runtime_environments_url(&self.base_url, "/population")),
                    )
                    .await?;
                if json {
                    return Ok(body);
                }
                Ok(render_population(&decode(&body, "population")?))
            }
        }
    }

    /// [`AdminClient::send`] for a policy write, conditional on the policy
    /// generation when one is given. A transport failure may come after the
    /// server committed, so say so.
    async fn send_write(
        &self,
        request: reqwest::RequestBuilder,
        if_generation: Option<u64>,
    ) -> Result<String, CliError> {
        let request = match if_generation {
            Some(generation) => request.header(reqwest::header::IF_MATCH, generation.to_string()),
            None => request,
        };
        self.send(request).await.map_err(|error| match error {
            CliError::Transport(message) => CliError::Transport(format!(
                "{message}; the write may have landed, so check with `runtime env list` before \
                 retrying"
            )),
            other => other,
        })
    }

    async fn runtime_nodes(
        &self,
        mut after: Option<Uuid>,
        all: bool,
        json: bool,
    ) -> Result<String, CliError> {
        // Each page decoded once, kept as JSON for --json and typed to render.
        let mut pages = Vec::new();
        loop {
            // A UUID cursor needs no escaping.
            let suffix = after.map_or_else(
                || "/nodes".to_string(),
                |after| format!("/nodes?after={after}"),
            );
            let request = self
                .http
                .get(runtime_environments_url(&self.base_url, &suffix));
            let page: serde_json::Value = decode(&self.send(request).await?, "node status")?;
            let typed = NodePage::deserialize(&page).map_err(|err| {
                CliError::Decode(format!("could not parse the node status response: {err}"))
            })?;
            // Pages run in node id order, so a cursor that doesn't advance
            // means the server would serve the same page forever.
            if let (Some(previous), Some(next)) = (after, typed.next_after)
                && next <= previous
            {
                return Err(CliError::Decode(format!(
                    "node status paging didn't advance past {previous}"
                )));
            }
            after = typed.next_after;
            pages.push((page, typed));
            if !all || after.is_none() {
                break;
            }
        }
        if json {
            let raw: Vec<_> = pages.into_iter().map(|(page, _)| page).collect();
            // Without --all the loop ran once: one page, printed as itself.
            let out = match (all, raw.as_slice()) {
                (false, [page]) => serde_json::to_string_pretty(page),
                _ => serde_json::to_string_pretty(&raw),
            };
            return out.map_err(|err| CliError::Decode(err.to_string()));
        }
        let typed: Vec<_> = pages.into_iter().map(|(_, typed)| typed).collect();
        Ok(render_nodes(&typed))
    }
}

fn describe_change(change: Option<&RecordedChange>) -> (String, String, String) {
    match change {
        Some(change) => (
            change.updated_at.clone(),
            change
                .updated_by_user_id
                .map_or_else(|| "-".to_string(), |id| id.to_string()),
            change.reason.clone(),
        ),
        None => ("-".into(), "-".into(), "-".into()),
    }
}

fn render_policy(policy: &Policy) -> String {
    let legacy = match policy.legacy_unbound_allowed {
        None => "unset (nodes keep their configured permission)",
        Some(true) => "allowed",
        Some(false) => "refused",
    };
    let default = policy
        .default_environment_ref
        .as_deref()
        .unwrap_or("unset (nodes keep their configured default)");
    let mut out = format!(
        "Policy generation: {}\nDefault: {default}\nLegacy unbound work: {legacy}\n",
        policy.generation
    );
    if policy.last_change.is_some() {
        let (changed, by, reason) = describe_change(policy.last_change.as_ref());
        out.push_str(&format!("Last change: {changed} by {by}: {reason}\n"));
    }
    if policy.environments.is_empty() {
        out.push_str("No environment has a state.");
        return out;
    }
    let mut table = Table::new();
    table.load_style(UTF8_FULL_CONDENSED);
    table.set_header(vec![
        "ENVIRONMENT",
        "STATE",
        "DEFAULT",
        "CHANGED",
        "BY",
        "REASON",
    ]);
    for environment in &policy.environments {
        let (changed, by, reason) = describe_change(environment.last_change.as_ref());
        table.add_row(vec![
            environment.environment_ref.clone(),
            environment.state.clone(),
            if environment.is_default { "*" } else { "" }.to_string(),
            changed,
            by,
            reason,
        ]);
    }
    out.push_str(&table.to_string());
    out
}

/// The algorithm plus the first 12 hex digits: enough to tell environments
/// apart in a node table. Commands take the full reference.
fn short_ref(reference: &str) -> String {
    // `get` rather than slicing: a server value isn't guaranteed to be ASCII.
    let Some((algorithm, hex)) = reference.split_once(':') else {
        return reference.to_string();
    };
    match (hex.get(..12), hex.get(12..)) {
        (Some(prefix), Some(rest)) if !rest.is_empty() => format!("{algorithm}:{prefix}…"),
        _ => reference.to_string(),
    }
}

fn divergence(node: &NodeStatus) -> String {
    let mut parts = Vec::new();
    if node.default_differs {
        parts.push("default differs".to_string());
    }
    if !node.executes_outside_policy.is_empty() {
        let refs: Vec<_> = node
            .executes_outside_policy
            .iter()
            .map(|reference| short_ref(reference))
            .collect();
        parts.push(format!("executes {}", refs.join(", ")));
    }
    if node.allows_revoked_legacy_unbound {
        parts.push("allows revoked legacy work".to_string());
    }
    parts.join("; ")
}

fn render_nodes(pages: &[NodePage]) -> String {
    let Some(first) = pages.first() else {
        return String::new();
    };
    let mut out = format!(
        "Compared with policy generation {}, default {}.\n",
        first.generation,
        first
            .default_environment_ref
            .as_deref()
            .map_or_else(|| "unset".to_string(), short_ref)
    );
    if pages.iter().any(|page| page.generation != first.generation) {
        out.push_str(
            "The policy changed while paging; later pages compare with a newer generation.\n",
        );
    }
    let mut table = Table::new();
    table.load_style(UTF8_FULL_CONDENSED);
    table.set_header(vec![
        "NODE",
        "NAME",
        "ROLE",
        "CONFORMANCE",
        "FRESH",
        "APPLIED",
        "ENFORCED DEFAULT",
        "DIFFERENCES",
    ]);
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    for node in pages.iter().flat_map(|page| &page.nodes) {
        *counts.entry(node.conformance.as_str()).or_default() += 1;
        table.add_row(vec![
            node.node_id.to_string(),
            node.name
                .clone()
                .or_else(|| node.substrate_node_id.clone())
                .unwrap_or_else(|| "-".to_string()),
            node.role.clone(),
            node.conformance.clone(),
            match node.status_fresh {
                Some(true) => "yes",
                Some(false) => "stale",
                None => "never",
            }
            .to_string(),
            match (
                &node.enforced,
                node.applied_generation,
                node.applied_current,
            ) {
                (_, Some(generation), Some(true)) => format!("{generation}"),
                (_, Some(generation), _) => format!("{generation} (not current)"),
                (None, None, _) => "-".to_string(),
                (Some(policy), None, _) if policy.cluster_policy_unreadable => {
                    "unreadable".to_string()
                }
                (Some(_), None, _) => "startup".to_string(),
            },
            node.enforced
                .as_ref()
                .and_then(|policy| policy.default_environment_reference.as_deref())
                .map_or_else(|| "-".to_string(), short_ref),
            divergence(node),
        ]);
    }
    out.push_str(&table.to_string());
    let summary: Vec<_> = counts
        .iter()
        .map(|(conformance, count)| format!("{count} {conformance}"))
        .collect();
    out.push_str(&format!("\n{}", summary.join(", ")));
    if let Some(next) = pages.last().and_then(|page| page.next_after) {
        out.push_str(&format!(
            "\nMore nodes follow: pass --after {next}, or --all for every page."
        ));
    }
    out
}

fn lifecycles(counts: &Counts) -> String {
    counts
        .sandboxes_by_lifecycle
        .iter()
        .map(|(lifecycle, count)| format!("{lifecycle}={count}"))
        .collect::<Vec<_>>()
        .join(" ")
}

fn render_population(population: &Population) -> String {
    let mut table = Table::new();
    table.load_style(UTF8_FULL_CONDENSED);
    table.set_header(vec![
        "ENVIRONMENT",
        "STATE",
        "DEFAULT",
        "SANDBOXES",
        "BY LIFECYCLE",
        "TEMPLATE RELEASES",
    ]);
    for environment in &population.environments {
        table.add_row(vec![
            environment.environment_ref.clone(),
            environment
                .state
                .clone()
                .unwrap_or_else(|| "(none)".to_string()),
            if environment.is_default { "*" } else { "" }.to_string(),
            environment.counts.sandboxes_cell(),
            lifecycles(&environment.counts),
            environment.counts.releases_cell(),
        ]);
    }
    for (label, counts) in [
        ("(unbound)", &population.unbound),
        ("(unrecorded)", &population.unrecorded),
        ("(invalid binding)", &population.invalid),
    ] {
        table.add_row(vec![
            label.to_string(),
            String::new(),
            String::new(),
            counts.sandboxes_cell(),
            lifecycles(counts),
            counts.releases_cell(),
        ]);
    }
    format!(
        "Policy generation: {}\n{table}\n(unbound) is legacy work with no environment binding; it executes as the environment it matches.\n(unrecorded) sandboxes have no binding record: no observation from their node, no snapshot a restore could pick, or, if failed, no live observation and no repair snapshot.",
        population.generation
    )
}

#[cfg(test)]
mod tests;

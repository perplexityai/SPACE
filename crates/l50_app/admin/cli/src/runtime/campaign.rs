//! `space-admin-cli runtime campaign ...`: runtime upgrade campaigns, through
//! `/admin/api/v1/runtime-upgrade-campaigns`. The server validates every
//! write and owns the phase rules; this module sends requests and renders
//! what comes back.

use comfy_table::{Table, presets::UTF8_FULL_CONDENSED};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{decode, short_ref};
use crate::{
    admin_client::{ADMIN_API_PREFIX, AdminClient, CliError, opt_num},
    cli::RuntimeCampaignCommands,
};

/// `POST /runtime-upgrade-campaigns`'s body. An option left unset isn't
/// sent, so the server's default applies.
#[derive(Debug, Serialize)]
struct StartBody {
    kind: &'static str,
    from_environment_ref: String,
    to_environment_ref: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    canary_size: Option<u64>,
    max_in_flight_per_cluster: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_in_flight_per_node: Option<u64>,
    failure_budget: u64,
    include_running: bool,
    reason: String,
}

/// The command that shows whether a campaign write landed.
const CAMPAIGN_CHECK: &str = "runtime campaign list";

#[derive(Debug, Deserialize)]
struct ItemCounts {
    pending: u64,
    in_flight: u64,
    done: u64,
    failed: u64,
    skipped: u64,
}

#[derive(Debug, Deserialize)]
struct Campaign {
    id: Uuid,
    kind: String,
    from_environment_ref: String,
    to_environment_ref: String,
    phase: String,
    generation: u64,
    max_in_flight_per_node: u64,
    max_in_flight_per_cluster: u64,
    failure_budget: u64,
    include_running: bool,
    reason: String,
    created_at: String,
    created_by_user_id: Option<Uuid>,
    updated_at: String,
    updated_by_user_id: Option<Uuid>,
    finished_at: Option<String>,
    items: ItemCounts,
    stranded: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct CampaignPage {
    campaigns: Vec<Campaign>,
    next_after: Option<Uuid>,
}

#[derive(Debug, Deserialize)]
struct StartResponse {
    id: Uuid,
    phase: String,
    generation: u64,
}

#[derive(Debug, Deserialize)]
struct PhaseResponse {
    id: Uuid,
    phase: String,
    generation: u64,
    retried: u64,
    draining: u64,
}

fn campaigns_url(base_url: &str, suffix: &str) -> String {
    format!("{base_url}{ADMIN_API_PREFIX}/runtime-upgrade-campaigns{suffix}")
}

impl AdminClient {
    pub(super) async fn campaign_command(
        &self,
        command: RuntimeCampaignCommands,
    ) -> Result<String, CliError> {
        match command {
            RuntimeCampaignCommands::Start {
                kind,
                from,
                to,
                canary_size,
                wave_size,
                per_node,
                failure_budget,
                include_running,
                reason,
                json,
            } => {
                let body = StartBody {
                    kind: kind.as_str(),
                    from_environment_ref: from.to_string(),
                    to_environment_ref: to.to_string(),
                    canary_size,
                    max_in_flight_per_cluster: wave_size,
                    max_in_flight_per_node: per_node,
                    failure_budget,
                    include_running,
                    reason,
                };
                let request = self
                    .http
                    .post(campaigns_url(&self.base_url, ""))
                    .json(&body);
                let body = self.send_write(request, None, CAMPAIGN_CHECK).await?;
                if json {
                    return Ok(body);
                }
                let response: StartResponse = decode(&body, "campaign start")?;
                Ok(format!(
                    "Started campaign {} in {} (generation {}).",
                    response.id, response.phase, response.generation
                ))
            }
            RuntimeCampaignCommands::Pause {
                id,
                reason,
                if_generation,
                json,
            } => {
                let body = serde_json::json!({ "reason": reason });
                self.change_phase(id, "pause", &body, if_generation, json)
                    .await
            }
            RuntimeCampaignCommands::Resume {
                id,
                reason,
                retry_failed,
                if_generation,
                json,
            } => {
                let body = serde_json::json!({ "reason": reason, "retry_failed": retry_failed });
                self.change_phase(id, "resume", &body, if_generation, json)
                    .await
            }
            RuntimeCampaignCommands::Abort {
                id,
                reason,
                if_generation,
                json,
            } => {
                let body = serde_json::json!({ "reason": reason });
                self.change_phase(id, "abort", &body, if_generation, json)
                    .await
            }
            RuntimeCampaignCommands::Show { id, json } => {
                let body = self
                    .send(
                        self.http
                            .get(campaigns_url(&self.base_url, &format!("/{id}"))),
                    )
                    .await?;
                if json {
                    return Ok(body);
                }
                Ok(render_campaign(&decode(&body, "campaign")?))
            }
            RuntimeCampaignCommands::List { unfinished, json } => {
                self.list_campaigns(unfinished, json).await
            }
        }
    }

    /// `POST .../{id}:{action}`, conditional on the generation when given.
    async fn change_phase(
        &self,
        id: Uuid,
        action: &str,
        body: &serde_json::Value,
        if_generation: Option<u64>,
        json: bool,
    ) -> Result<String, CliError> {
        let request = self
            .http
            .post(campaigns_url(&self.base_url, &format!("/{id}:{action}")))
            .json(body);
        let body = self
            .send_write(request, if_generation, CAMPAIGN_CHECK)
            .await?;
        if json {
            return Ok(body);
        }
        let response: PhaseResponse = decode(&body, &format!("campaign {action}"))?;
        let mut out = format!(
            "Campaign {} is now {} (generation {}).",
            response.id, response.phase, response.generation
        );
        if response.retried > 0 {
            out.push_str(&format!(
                "\n{} failed items returned to pending.",
                response.retried
            ));
        }
        if response.draining > 0 {
            out.push_str(&format!(
                "\n{} in-flight items stop at their next lease renewal and return to pending \
                 when their leases lapse.",
                response.draining
            ));
        }
        Ok(out)
    }

    /// Every page, following `next_after` to the end.
    async fn list_campaigns(&self, unfinished: bool, json: bool) -> Result<String, CliError> {
        let mut after: Option<Uuid> = None;
        // Each page decoded once: its raw campaigns kept for --json, typed to
        // render.
        let mut raw = Vec::new();
        let mut campaigns = Vec::new();
        loop {
            // A UUID cursor needs no escaping.
            let mut query = vec![];
            if unfinished {
                query.push("unfinished=true".to_string());
            }
            if let Some(after) = after {
                query.push(format!("after={after}"));
            }
            let suffix = if query.is_empty() {
                String::new()
            } else {
                format!("?{}", query.join("&"))
            };
            let request = self.http.get(campaigns_url(&self.base_url, &suffix));
            let mut page: serde_json::Value = decode(&self.send(request).await?, "campaign list")?;
            let typed = CampaignPage::deserialize(&page).map_err(|err| {
                CliError::Decode(format!("could not parse the campaign list response: {err}"))
            })?;
            // Pages run in id order, so a cursor that doesn't advance means
            // the server would serve the same page forever.
            if let (Some(previous), Some(next)) = (after, typed.next_after)
                && next <= previous
            {
                return Err(CliError::Decode(format!(
                    "campaign list paging didn't advance past {previous}"
                )));
            }
            if let Some(serde_json::Value::Array(page_campaigns)) = page.get_mut("campaigns") {
                raw.append(page_campaigns);
            }
            campaigns.extend(typed.campaigns);
            after = typed.next_after;
            if after.is_none() {
                break;
            }
        }
        if json {
            // The shape of a last page, whatever the number of pages.
            return serde_json::to_string_pretty(&serde_json::json!({
                "campaigns": raw,
                "next_after": null,
            }))
            .map_err(|err| CliError::Decode(err.to_string()));
        }
        Ok(render_campaigns(&campaigns, unfinished))
    }
}

fn user_or(user: Option<Uuid>, otherwise: &str) -> String {
    user.map_or_else(|| otherwise.to_string(), |id| id.to_string())
}

fn items_summary(items: &ItemCounts) -> String {
    format!(
        "{} pending, {} in flight, {} done, {} failed, {} skipped",
        items.pending, items.in_flight, items.done, items.failed, items.skipped
    )
}

fn render_campaign(campaign: &Campaign) -> String {
    let mut out = format!(
        "Campaign: {}\nKind: {}\nFrom: {}\nTo: {}\nPhase: {} (generation {})\n\
         Wave size: {} per cluster, {} per node\nFailure budget: {}\nInclude running: {}\n\
         Items: {}\n",
        campaign.id,
        campaign.kind,
        campaign.from_environment_ref,
        campaign.to_environment_ref,
        campaign.phase,
        campaign.generation,
        campaign.max_in_flight_per_cluster,
        campaign.max_in_flight_per_node,
        campaign.failure_budget,
        if campaign.include_running {
            "yes"
        } else {
            "no"
        },
        items_summary(&campaign.items),
    );
    // Null for a kind that doesn't define stranded items.
    if let Some(stranded) = campaign.stranded {
        out.push_str(&format!(
            "Stranded: {stranded} (pending items with no certified upgrade edge)\n"
        ));
    }
    out.push_str(&format!(
        "Reason: {}\nCreated: {} by {}\nUpdated: {} by {}",
        campaign.reason,
        campaign.created_at,
        user_or(campaign.created_by_user_id, "-"),
        campaign.updated_at,
        user_or(campaign.updated_by_user_id, "the reconciler"),
    ));
    if let Some(finished_at) = &campaign.finished_at {
        out.push_str(&format!("\nFinished: {finished_at}"));
    }
    out
}

fn render_campaigns(campaigns: &[Campaign], unfinished: bool) -> String {
    if campaigns.is_empty() {
        return if unfinished {
            "No unfinished campaigns.".to_string()
        } else {
            "No campaigns.".to_string()
        };
    }
    let mut table = Table::new();
    table.load_style(UTF8_FULL_CONDENSED);
    table.set_header(vec![
        "CAMPAIGN",
        "KIND",
        "FROM",
        "TO",
        "PHASE",
        "GENERATION",
        "PENDING",
        "IN FLIGHT",
        "DONE",
        "FAILED",
        "SKIPPED",
        "STRANDED",
    ]);
    for campaign in campaigns {
        table.add_row(vec![
            campaign.id.to_string(),
            campaign.kind.clone(),
            short_ref(&campaign.from_environment_ref),
            short_ref(&campaign.to_environment_ref),
            campaign.phase.clone(),
            campaign.generation.to_string(),
            campaign.items.pending.to_string(),
            campaign.items.in_flight.to_string(),
            campaign.items.done.to_string(),
            campaign.items.failed.to_string(),
            campaign.items.skipped.to_string(),
            opt_num(campaign.stranded),
        ]);
    }
    table.to_string()
}

#[cfg(test)]
mod tests;

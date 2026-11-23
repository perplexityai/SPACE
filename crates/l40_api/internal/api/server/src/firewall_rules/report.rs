//! `ReportFirewallRules`: the report is decoded in full before any database
//! work, the worker is resolved as for a fetch, and the store accepts it at
//! most once. The request's node ID is only compared with the resolved one.

use std::time::Duration;

use pplx_space_accounts::NodeId;
use pplx_space_firewall_rules_store::{
    Acceptance, Census, CensusGap, Failure, InvalidReport, Report, ReportRefusal, RevisionId,
    SlotEvidence, SlotOutcome, UnknownReason, accept_report,
};
use pplx_space_rpc_auth::{FirewallRulesReportAuthority, NodeApiAuthenticatedCaller};
use tonic::{Request, Response, Status};

use super::{Call, resolve_worker, reviewed_node};
use crate::ClusterApiService;
use crate::proto::{
    FirewallRulesCensus, FirewallRulesCensusGap, FirewallRulesCensusGapKind,
    FirewallRulesReportOutcome, FirewallRulesSlotEvidence, FirewallRulesSlotOutcome,
    FirewallRulesSlotReason, ReportFirewallRulesRequest, ReportFirewallRulesResponse,
};

/// How long after its round was issued a report may be accepted. It covers
/// wake delivery, fetch, observation and report; the round scheduler must
/// reopen a round less often than this.
pub(crate) const REPORT_MAX_AGE: Duration = Duration::from_secs(300);

fn malformed(what: impl std::fmt::Display) -> Status {
    Status::invalid_argument(format!("malformed firewall rules report: {what}"))
}

fn required<T>(value: Option<T>, field: &str) -> Result<T, Status> {
    value.ok_or_else(|| malformed(format_args!("{field} is unset")))
}

/// The node `request` names and its report. Unset fields, unknown enum
/// values and every [`InvalidReport`] are malformed.
fn decode(request: ReportFirewallRulesRequest) -> Result<(NodeId, Report), Status> {
    let named = request
        .node_id
        .parse::<NodeId>()
        .map_err(|_| malformed("node_id isn't a node ID"))?;
    let revision = request
        .revision_id
        .parse::<uuid::Uuid>()
        .map_err(|_| malformed("revision_id isn't a UUID"))?;
    let census = match FirewallRulesCensus::try_from(request.census) {
        Ok(FirewallRulesCensus::Complete) if request.census_gaps.is_empty() => Census::Complete,
        Ok(FirewallRulesCensus::Complete) => {
            return Err(malformed("a complete census lists gaps"));
        }
        Ok(FirewallRulesCensus::Incomplete) => Census::Incomplete(
            request
                .census_gaps
                .iter()
                .map(gap)
                .collect::<Result<_, _>>()?,
        ),
        Ok(FirewallRulesCensus::Unspecified) | Err(_) => {
            return Err(malformed("census is unset or unknown"));
        }
    };
    let slots = request
        .slots
        .iter()
        .map(slot)
        .collect::<Result<Vec<_>, Status>>()?;
    let report = Report::new(
        required(request.round, "round")?,
        required(request.assignment_generation, "assignment_generation")?,
        RevisionId(revision),
        required(request.manager_epoch, "manager_epoch")?,
        census,
        slots,
    )
    .map_err(|error: InvalidReport| malformed(error))?;
    Ok((named, report))
}

fn gap(gap: &FirewallRulesCensusGap) -> Result<CensusGap, Status> {
    use FirewallRulesCensusGapKind as K;
    match (K::try_from(gap.kind), gap.slot_id, gap.omitted) {
        (Ok(K::InventoryFailed), None, None) => Ok(CensusGap::InventoryFailed),
        (Ok(K::Unowned), None, None) => Ok(CensusGap::Unowned),
        (Ok(K::ListingFailed), None, None) => Ok(CensusGap::ListingFailed),
        (Ok(K::Missing), Some(slot), None) => Ok(CensusGap::Missing(slot)),
        (Ok(K::Unread), Some(slot), None) => Ok(CensusGap::Unread(slot)),
        (Ok(K::Extra), None, None) => Ok(CensusGap::Extra),
        (Ok(K::Truncated), None, Some(omitted)) => Ok(CensusGap::Truncated(omitted)),
        _ => Err(malformed(
            "a census gap is unset or unknown, or its slot_id or omitted doesn't fit it",
        )),
    }
}

fn slot(slot: &FirewallRulesSlotEvidence) -> Result<SlotEvidence, Status> {
    use FirewallRulesSlotOutcome as O;
    use FirewallRulesSlotReason as R;
    let outcome = match (O::try_from(slot.outcome), R::try_from(slot.reason)) {
        (Ok(O::Verified), Ok(R::Unspecified)) => SlotOutcome::Verified,
        (Ok(O::Applying), Ok(R::Unspecified)) => SlotOutcome::Applying,
        (Ok(O::Failed), Ok(R::RulesMismatch)) => SlotOutcome::Failed(Failure::RulesMismatch),
        (Ok(O::Unknown), Ok(R::Ineligible)) => SlotOutcome::Unknown(UnknownReason::Ineligible),
        (Ok(O::Unknown), Ok(R::NamespaceMissing)) => {
            SlotOutcome::Unknown(UnknownReason::NamespaceMissing)
        }
        (Ok(O::Unknown), Ok(R::NamespaceUnreadable)) => {
            SlotOutcome::Unknown(UnknownReason::NamespaceUnreadable)
        }
        (Ok(O::Unknown), Ok(R::TableUnreadable)) => {
            SlotOutcome::Unknown(UnknownReason::TableUnreadable)
        }
        (Ok(O::Unknown), Ok(R::Unconfirmed)) => SlotOutcome::Unknown(UnknownReason::Unconfirmed),
        _ => {
            return Err(malformed(
                "a slot outcome is unset or unknown, or its reason doesn't fit it",
            ));
        }
    };
    Ok(SlotEvidence {
        slot_id: required(slot.slot_id, "slot_id")?,
        slot_generation: required(slot.slot_generation, "slot_generation")?,
        outcome,
    })
}

fn outcome(acceptance: Acceptance) -> FirewallRulesReportOutcome {
    match acceptance {
        Acceptance::Accepted => FirewallRulesReportOutcome::Accepted,
        Acceptance::Duplicate => FirewallRulesReportOutcome::Duplicate,
        Acceptance::Refused(refusal) => match refusal {
            ReportRefusal::NotPending => FirewallRulesReportOutcome::NotPending,
            ReportRefusal::Conflict => FirewallRulesReportOutcome::Conflict,
            ReportRefusal::EpochMismatch => FirewallRulesReportOutcome::EpochMismatch,
            ReportRefusal::GenerationMismatch => FirewallRulesReportOutcome::GenerationMismatch,
            ReportRefusal::Superseded => FirewallRulesReportOutcome::Superseded,
            ReportRefusal::Expired => FirewallRulesReportOutcome::Expired,
        },
    }
}

impl ClusterApiService {
    pub(crate) async fn report_firewall_rules(
        &self,
        request: Request<ReportFirewallRulesRequest>,
    ) -> Result<Response<ReportFirewallRulesResponse>, Status> {
        let call = Call::Report;
        let caller = request
            .extensions()
            .get::<NodeApiAuthenticatedCaller>()
            .ok_or_else(|| call.denied("no_report_authority"))?;
        // Never implied by the fetch authority.
        if caller.firewall_rules_report_authority() != FirewallRulesReportAuthority::Granted {
            return Err(call.denied("no_report_authority"));
        }
        let identity = reviewed_node(caller, call)?.clone();
        let (named, report) = decode(request.into_inner())?;
        let mut conn =
            self.pool.acquire().await.map_err(|error| {
                Status::internal(format!("begin firewall rules report: {error}"))
            })?;
        // Not locked: the write is still fenced by the node's own round,
        // generation and epoch, as a fetch's binding is by its snapshot alone.
        let node = resolve_worker(&mut conn, &identity, call).await?;
        if named != node {
            return Err(call.denied("other_node"));
        }
        self.firewall_enrollment.admit(node, call)?;
        let acceptance = accept_report(&mut conn, node, &report, REPORT_MAX_AGE)
            .await
            .map_err(|error| Status::internal(format!("accept firewall rules report: {error}")))?;
        Ok(Response::new(ReportFirewallRulesResponse {
            outcome: outcome(acceptance).into(),
        }))
    }
}

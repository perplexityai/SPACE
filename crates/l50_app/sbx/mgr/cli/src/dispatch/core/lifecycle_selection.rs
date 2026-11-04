//! Human selection is read-only; each mutation targets one revalidated identity.

use super::*;
use pplx_space_sbx_mgr_client::proto::{CleanupTargetObservation, ScanCleanupTargetsRequest};

const FANOUT: usize = 4;

#[derive(Clone, Copy, Debug)]
pub(super) enum Mutation {
    Stop { immediate_kill: bool },
    Delete,
}

enum MutationRequest {
    Stop(StopRequest),
    Delete(DeleteRequest),
}

impl Mutation {
    fn request(
        self,
        id: String,
        template_filter: Option<String>,
        blocking: bool,
    ) -> MutationRequest {
        match self {
            Self::Stop { immediate_kill } => MutationRequest::Stop(StopRequest {
                sandbox_id: id,
                template_filter,
                blocking,
                immediate_kill,
            }),
            Self::Delete => MutationRequest::Delete(DeleteRequest {
                sandbox_id: id,
                template_filter,
                blocking,
            }),
        }
    }

    async fn apply(
        self,
        client: &mut SandboxClient,
        id: String,
        template_filter: Option<String>,
        blocking: bool,
    ) -> Result<Vec<String>, tonic::Status> {
        match self.request(id, template_filter, blocking) {
            MutationRequest::Stop(request) => {
                Ok(client.stop(request).await?.into_inner().stopped_ids)
            }
            MutationRequest::Delete(request) => {
                Ok(client.delete(request).await?.into_inner().deleted_ids)
            }
        }
    }

    fn report(self, ids: Vec<String>) -> usize {
        for id in &ids {
            let verb = match self {
                Self::Stop { .. } => "Stopped",
                Self::Delete => "Deleted",
            };
            println!("{verb}: {}", sandbox_label(id));
        }
        ids.len()
    }
}

fn matching_id(
    target: &CleanupTargetObservation,
    selector: Option<&SandboxSelector>,
) -> Result<Option<SandboxId>, String> {
    if selector.is_some_and(|selector| {
        !pplx_space_common_core::ids::sandbox_id_matches(&target.sandbox_id, selector.as_str())
    }) {
        return Ok(None);
    }
    let id = target.sandbox_id.parse::<SandboxId>().map_err(|error| {
        format!(
            "invalid inventory identity {:?}: {error}",
            target.sandbox_id
        )
    })?;
    if id.to_string() != target.sandbox_id {
        return Err(format!(
            "noncanonical inventory identity {:?}; operator repair required",
            target.sandbox_id
        ));
    }
    Ok(Some(id))
}

fn eligible(target: &CleanupTargetObservation, template: Option<&str>) -> Result<bool, String> {
    if !target.metadata_valid {
        return Err(format!(
            "{}: unreadable cleanup metadata; operator repair required",
            target.sandbox_id
        ));
    }
    Ok(template.is_none_or(|filter| target.template.as_deref() == Some(filter)))
}

fn unique_target(
    selected: &mut Option<CleanupTargetObservation>,
    target: CleanupTargetObservation,
) -> Result<(), String> {
    match selected {
        Some(previous) if previous.sandbox_id != target.sandbox_id => Err(format!(
            "ambiguous sandbox selector, matches: {}, {}",
            previous.sandbox_id, target.sandbox_id,
        )),
        Some(_) => Ok(()),
        None => {
            *selected = Some(target);
            Ok(())
        }
    }
}

pub(super) async fn run(
    client: &mut SandboxClient,
    selector: String,
    template: Option<String>,
    blocking: bool,
    mutation: Mutation,
) -> Result<usize, String> {
    let template = template.filter(|value| !value.is_empty());
    if template.is_none()
        && let Ok(id) = selector.parse::<SandboxId>()
    {
        return mutation
            .apply(client, id.to_string(), None, blocking)
            .await
            .map(|ids| mutation.report(ids))
            .map_err(|error| error.to_string());
    }
    let mut stream = client
        .scan_cleanup_targets(ScanCleanupTargetsRequest {})
        .await
        .map_err(|error| error.to_string())?
        .into_inner();
    if selector != "all" {
        let selector = SandboxSelector::parse(selector).map_err(|error| error.to_string())?;
        let mut selected = None;
        while let Some(batch) = stream
            .message()
            .await
            .map_err(|error| format!("selection incomplete: {error}"))?
        {
            for target in batch.targets {
                if matching_id(&target, Some(&selector))?.is_some() {
                    // Resolve ambiguity before applying the template filter,
                    // matching the existing mutation selector's contract.
                    unique_target(&mut selected, target)?;
                }
            }
        }
        let Some(target) = selected else { return Ok(0) };
        if !eligible(&target, template.as_deref())? {
            return Ok(0);
        }
        return mutation
            .apply(client, target.sandbox_id, template, blocking)
            .await
            .map(|ids| mutation.report(ids))
            .map_err(|error| error.to_string());
    }

    let mut tasks = tokio::task::JoinSet::new();
    let mut changed = 0;
    let mut failures = 0;
    let mut selection_error = None;
    loop {
        let batch = match stream.message().await {
            Ok(Some(batch)) => batch,
            Ok(None) => break,
            Err(error) => {
                selection_error = Some(error.to_string());
                break;
            }
        };
        for target in batch.targets {
            match matching_id(&target, None).and_then(|id| {
                eligible(&target, template.as_deref()).map(|eligible| id.filter(|_| eligible))
            }) {
                Ok(Some(id)) => {
                    if tasks.len() >= FANOUT {
                        collect_one(&mut tasks, mutation, &mut changed, &mut failures).await;
                    }
                    let mut client = client.clone();
                    let template = template.clone();
                    tasks.spawn(async move {
                        let result = mutation
                            .apply(&mut client, id.to_string(), template, blocking)
                            .await;
                        (id, result)
                    });
                }
                Ok(None) => {}
                Err(error) => {
                    eprintln!("Skipped: {error}");
                    failures += 1;
                }
            }
        }
    }
    // An incomplete selection cannot cancel mutations already admitted by the
    // daemon. Wait for their outcomes before returning a nonzero exit status.
    while !tasks.is_empty() {
        collect_one(&mut tasks, mutation, &mut changed, &mut failures).await;
    }
    if let Some(error) = selection_error {
        return Err(format!(
            "selection incomplete after {changed} changes and {failures} failures: {error}; rerun for remaining targets"
        ));
    }
    if failures != 0 {
        return Err(format!("{changed} changes; {failures} targets failed"));
    }
    Ok(changed)
}

async fn collect_one(
    tasks: &mut tokio::task::JoinSet<(SandboxId, Result<Vec<String>, tonic::Status>)>,
    mutation: Mutation,
    changed: &mut usize,
    failures: &mut usize,
) {
    match tasks.join_next().await {
        Some(Ok((_, Ok(ids)))) => *changed += mutation.report(ids),
        Some(Ok((id, Err(error)))) => {
            eprintln!("Failed {id}: {error}");
            *failures += 1;
        }
        Some(Err(error)) => {
            eprintln!("Mutation task failed: {error}");
            *failures += 1;
        }
        None => {}
    }
}

#[cfg(test)]
mod tests;

use super::*;
use pplx_space_sbx_mgr_client::proto::{
    ApplyGuestUpdateRequest, GuestUpdateOperation, GuestUpdateStatusRequest, RestartGuestRequest,
};

pub(super) async fn handle(
    command: Commands,
    client: &mut pplx_space_sbx_mgr_client::SandboxManagerGrpcClient,
) {
    let (args, policy) = match command {
        Commands::GuestUpdate {
            command:
                GuestUpdateCommand::Status {
                    id,
                    operation,
                    json,
                },
        } => {
            let result = client
                .guest_update_status(GuestUpdateStatusRequest {
                    sandbox_id: id.to_string(),
                    operation_id: operation.map(|id| id.to_string()),
                })
                .await
                .unwrap_or_else(|e| exit_error(e));
            display(result.into_inner(), json);
            return;
        }
        Commands::GuestUpdate {
            command: GuestUpdateCommand::Apply { args, policy },
        } => (args, Some(policy)),
        Commands::GuestRestart { args } => (args, None),
        _ => unreachable!("guest maintenance dispatch"),
    };
    let status = client
        .guest_update_status(GuestUpdateStatusRequest {
            sandbox_id: args.id.to_string(),
            operation_id: None,
        })
        .await
        .unwrap_or_else(|e| exit_error(e))
        .into_inner();
    let execution = args
        .expected_execution
        .map(|v| v.to_string())
        .unwrap_or(status.execution_id);
    let operation = args
        .operation
        .unwrap_or_else(uuid::Uuid::now_v7)
        .to_string();
    // Keep stdout machine-readable while making a lost mutation reply safely
    // retryable without requiring operators to allocate identifiers themselves.
    eprintln!("Guest maintenance retry: --operation {operation} --expected-execution {execution}");
    let result = if let Some(path) = policy {
        let bytes = std::fs::File::open(&path)
            .and_then(read_policy_bytes)
            .unwrap_or_else(|error| exit_error(error));
        let catalog = pplx_space_spaced_update::catalog::Catalog::parse(&bytes)
            .unwrap_or_else(|e| exit_error(e));
        let contract = status
            .guest
            .as_ref()
            .map(|guest| guest.seed_contract.as_str())
            .unwrap_or_default();
        let platform = pplx_space_spaced_update::RecoveryPlatform::from_contract(contract)
            .unwrap_or_else(|| exit_error("guest has no supported update recovery contract"));
        let architecture = std::env::consts::ARCH
            .parse()
            .unwrap_or_else(|e| exit_error(e));
        let p = catalog
            .select(platform, architecture)
            .unwrap_or_else(|| exit_error("node policy has no target for this guest platform"))
            .promotion
            .clone();
        client
            .apply_guest_update(ApplyGuestUpdateRequest {
                sandbox_id: status.sandbox_id,
                operation_id: operation,
                expected_execution_id: execution,
                force: args.force,
                promotion: Some(pplx_space_spaced_common::proto::update::Promotion {
                    channel: p.channel,
                    target_version: p.target_version.to_string(),
                    guest_update_required_since: p.guest_update_required_since.to_string(),
                    intent: match p.intent {
                        pplx_space_spaced_update::policy::SelectionIntent::Upgrade => {
                            pplx_space_spaced_common::proto::update::SelectionIntent::Upgrade
                        }
                        pplx_space_spaced_update::policy::SelectionIntent::Rollback => {
                            pplx_space_spaced_common::proto::update::SelectionIntent::Rollback
                        }
                    } as i32,
                    target_digest: p.target.to_string(),
                    minimum_host: p.minimum_host.to_string(),
                    maximum_guest: p.maximum_guest.to_string(),
                }),
            })
            .await
    } else {
        client
            .restart_guest(RestartGuestRequest {
                sandbox_id: status.sandbox_id,
                operation_id: operation,
                expected_execution_id: execution,
                force: args.force,
            })
            .await
    };
    display(
        result.unwrap_or_else(|e| exit_error(e)).into_inner(),
        args.json,
    );
}

fn display(result: GuestUpdateOperation, json: bool) {
    if json {
        let guest = result.guest.as_ref().map(|guest| serde_json::json!({
            "phase": guest.phase().as_str_name(), "boot_id": guest.boot_id,
            "version": guest.committed_version, "incarnation": guest.incarnation, "admission_held": guest.customer_admission_held,
            "committed_digest": guest.committed.as_ref().map(|value| &value.digest),
            "running_digest": guest.running.as_ref().map(|value| &value.digest),
            "defer_reasons": guest.defer_reasons.iter().filter_map(|reason| pplx_space_spaced_common::proto::update::DeferReason::try_from(*reason).ok()).map(|reason| reason.as_str_name()).collect::<Vec<_>>(),
        }));
        println!(
            "{}",
            serde_json::json!({"sandbox_id": result.sandbox_id,
            "execution_id": result.execution_id, "operation_id": result.operation_id,
            "phase": result.phase().as_str_name(), "outcome": result.outcome, "automatic_deferral": result.automatic_deferral, "guest": guest})
        );
        return;
    }
    println!(
        "Sandbox: {}\nExecution: {}\nOperation: {}\nPhase: {:?}",
        result.sandbox_id,
        result.execution_id,
        result.operation_id.as_deref().unwrap_or("none"),
        result.phase()
    );
    if let Some(reason) = result.automatic_deferral {
        println!("Automatic deferral: {reason}");
    }
    if let Some(outcome) = result.outcome {
        println!("Outcome: {outcome}");
    }
    if let Some(guest) = result.guest {
        println!(
            "Guest: {:?}\nVersion: {}\nIncarnation: {}\nAdmission held: {}",
            guest.phase(),
            guest.committed_version,
            guest.incarnation,
            guest.customer_admission_held
        );
        if let Some(committed) = guest.committed {
            println!("Committed: {}", committed.digest);
        }
    }
}

fn read_policy_bytes(source: impl std::io::Read) -> std::io::Result<Vec<u8>> {
    use std::io::Read as _;
    let limit = pplx_space_spaced_update::MAX_METADATA_BYTES;
    let mut bytes = Vec::new();
    source.take(limit as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > limit {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "promotion document exceeds size limit",
        ));
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests;

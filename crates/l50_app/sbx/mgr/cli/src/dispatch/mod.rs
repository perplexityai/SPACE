use super::*;

mod admin;
mod core;
mod diagnostics;
mod lifecycle;

pub(super) async fn dispatch(
    command: Commands,
    client: &mut pplx_space_sbx_mgr_client::SandboxManagerGrpcClient,
) {
    match command {
        Commands::Create { .. }
        | Commands::Start { .. }
        | Commands::Stop { .. }
        | Commands::Delete { .. }
        | Commands::List
        | Commands::Stats { .. }
        | Commands::Health { .. }
        | Commands::DiskUsage { .. }
        | Commands::Exec { .. }
        | Commands::Update { .. } => core::handle(command, client).await,
        Commands::Templates
        | Commands::TemplateSetAutoLifecycle { .. }
        | Commands::NetworkPolicy { .. }
        | Commands::NetworkState { .. }
        | Commands::NetworkDebug { .. }
        | Commands::CycleNetGateway { .. }
        | Commands::Cleanup => admin::handle(command, client).await,
        Commands::Pause { .. }
        | Commands::Resume { .. }
        | Commands::Suspend { .. }
        | Commands::Wake { .. }
        | Commands::Kill { .. }
        | Commands::Checkpoint { .. }
        | Commands::Restore { .. }
        | Commands::EnableSnapshots { .. }
        | Commands::DisableSnapshots { .. }
        | Commands::Snapshots { .. } => lifecycle::handle(command, client).await,
        Commands::Doctor { .. }
        | Commands::Rollback { .. }
        | Commands::Snapshot { .. }
        | Commands::Wait { .. } => diagnostics::handle(command, client).await,
    }
}

use super::*;
use pplx_space_common_core::types::{
    LOCAL_OPERATOR_SANDBOX_OWNER_KINDS, ROOT_SANDBOX_OWNER, SandboxOwner, SandboxOwnerKind,
    format_reserved_sandbox_owner_kind_labels,
};

mod exec;
mod inventory;
mod lifecycle_selection;
mod sandbox_commands;
mod update;

type SandboxClient = pplx_space_sbx_mgr_client::SandboxManagerGrpcClient;

fn username_for_effective_uid() -> Option<String> {
    let uid = nix::unistd::Uid::effective();
    nix::unistd::User::from_uid(uid)
        .ok()
        .flatten()
        .map(|user| user.name)
}

fn username_from_env() -> Option<String> {
    ["USER", "LOGNAME"].into_iter().find_map(|name| {
        std::env::var(name)
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
    })
}

fn accepted_cli_owner_help() -> String {
    format_reserved_sandbox_owner_kind_labels(LOCAL_OPERATOR_SANDBOX_OWNER_KINDS)
}

fn is_supported_cli_owner_kind(kind: &SandboxOwnerKind) -> bool {
    LOCAL_OPERATOR_SANDBOX_OWNER_KINDS.contains(kind)
}

fn parse_cli_local_sandbox_owner(value: &str) -> Result<SandboxOwner, String> {
    let owner = SandboxOwner::parse(value.to_string())
        .map_err(|error| format!("invalid sandbox owner '{value}': {error}"))?;
    if is_supported_cli_owner_kind(&owner.kind()) {
        Ok(owner)
    } else {
        let accepted = accepted_cli_owner_help();
        Err(format!(
            "unsupported sandbox owner '{value}'; space-sbxmgr create only accepts {accepted}. Platform users must create sandboxes through the SPACE API"
        ))
    }
}

fn local_sandbox_owner_for_effective_identity(
    uid: u32,
    username: Option<&str>,
) -> Result<SandboxOwner, String> {
    if uid == 0 {
        return Ok(ROOT_SANDBOX_OWNER.clone());
    }
    match username {
        Some(username) => {
            let owner_label = format!("{username}@local");
            let owner = SandboxOwner::parse(owner_label).map_err(|error| {
                format!(
                    "{}: {error}",
                    default_local_sandbox_owner_error(uid, Some(username))
                )
            })?;
            let kind = owner.kind();
            if is_supported_cli_owner_kind(&kind) && kind != SandboxOwnerKind::Root {
                Ok(owner)
            } else {
                Err(default_local_sandbox_owner_error(uid, Some(username)))
            }
        }
        None => Err(default_local_sandbox_owner_error(uid, None)),
    }
}

fn default_local_sandbox_owner_error(uid: u32, username: Option<&str>) -> String {
    let accepted = accepted_cli_owner_help();
    match username {
        Some(username) => format!(
            "cannot choose default sandbox owner for local user '{username}' (uid {uid}); pass --user {accepted}"
        ),
        None => {
            format!("cannot choose default sandbox owner for uid {uid}; pass --user {accepted}")
        }
    }
}

fn default_local_sandbox_owner() -> Result<SandboxOwner, String> {
    let uid = nix::unistd::Uid::effective().as_raw();
    let username = username_for_effective_uid().or_else(username_from_env);
    local_sandbox_owner_for_effective_identity(uid, username.as_deref())
}

pub(super) async fn handle(command: Commands, client: &mut SandboxClient) {
    match command {
        Commands::Create { .. }
        | Commands::Start { .. }
        | Commands::Stop { .. }
        | Commands::Delete { .. } => sandbox_commands::handle(command, client).await,
        Commands::List
        | Commands::Stats { .. }
        | Commands::Health { .. }
        | Commands::DiskUsage { .. } => {
            inventory::handle(command, client).await;
        }
        Commands::Exec {
            sandbox_id,
            user,
            command,
        } => exec::handle(sandbox_id, user, command, client).await,
        Commands::Update {
            sandbox_id,
            vcpu,
            memory,
            pids,
            inherit_template_auto_lifecycle,
            auto_lifecycle,
        } => {
            update::handle(
                sandbox_id,
                vcpu,
                memory,
                pids,
                inherit_template_auto_lifecycle,
                auto_lifecycle,
                client,
            )
            .await;
        }
        _ => unreachable!("command routed to wrong dispatch group"),
    }
}

#[cfg(test)]
mod tests;

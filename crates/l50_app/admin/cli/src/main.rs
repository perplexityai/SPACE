use clap::Parser;
use pplx_space_common_cli::exit_error;

mod admin_client;
mod auth;
mod cli;
mod runtime;
mod templates;

use admin_client::{AdminClient, CliError, WarmPoolConfigRequest};
use cli::{Cli, Commands, EnvConfig, WarmPoolCommands, WarmPoolConfigArgs, WarmPoolUpdateArgs};
use pplx_space_common_core::types::{NodeDrainMode, TemplateReference, admin::DrainRequest};
use std::str::FromStr;

#[tokio::main]
async fn main() {
    pplx_space_common_cli::init_logging("space-admin-cli");
    let cli = Cli::parse();

    let command = match cli.command {
        Commands::Login(args) => {
            let print_token = args.print_token;
            match auth::login(args).await {
                Ok(token) if print_token => println!("{token}"),
                Ok(_) => eprintln!(
                    "Admin access verified. No session saved; use --print-token to capture a token."
                ),
                Err(error) => exit_error(format!("{error:#}")),
            }
            return;
        }
        command => command,
    };

    let env = match EnvConfig::from_env() {
        Ok(env) => env,
        Err(err) => exit_error(err),
    };

    let client = match AdminClient::new(&env.api_url, &env.api_token) {
        Ok(client) => client,
        Err(err) => exit_error(err),
    };

    let result = match command {
        Commands::Login(_) => {
            unreachable!("login handled above")
        }
        Commands::Drain {
            node,
            no_replace_after_terminate,
            forced,
        } => {
            let request = DrainRequest {
                // `Some(false)` only when the operator asked; otherwise no
                // opinion, so an escalation leaves the in-flight drain's
                // intent alone rather than resetting it to the default.
                replace_after_terminate: no_replace_after_terminate.then_some(false),
                mode: if forced {
                    NodeDrainMode::Forced
                } else {
                    NodeDrainMode::Gentle
                },
                // This command is how an operator escalates a drain in place,
                // so it never asks for the start-only precondition.
                require_undrained: false,
            };
            client.post_drain(&node, &request).await
        }
        Commands::Terminate { node } => client.post_node_action(&node, "terminate").await,
        Commands::Undrain { node } => client.post_node_action(&node, "undrain").await,
        Commands::ListNodes(args) => client.list_nodes(&args).await,
        Commands::GetNode {
            node,
            json,
            diagnostics,
        } => client.get_node(&node, json, diagnostics).await,
        Commands::Template { command } => client.template_command(command).await,
        Commands::Runtime { command } => client.runtime_command(command).await,
        Commands::WarmPool { command } => match command {
            WarmPoolCommands::Create(args) => {
                let json = args.json;
                let request = warm_pool_config_request(args);
                client.upsert_warm_pool(&request, json).await
            }
            WarmPoolCommands::Update(args) => {
                let json = args.json;
                let request = match warm_pool_update_request(&args) {
                    Ok(request) => request,
                    Err(err) => exit_error(err),
                };
                client.upsert_warm_pool(&request, json).await
            }
            WarmPoolCommands::List { json } => client.list_warm_pools(json).await,
            WarmPoolCommands::Get { pool, json } => client.get_warm_pool(&pool, json).await,
        },
    };

    match result {
        Ok(body) => {
            println!("{body}");
        }
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(1);
        }
    }
}

fn warm_pool_config_request(args: WarmPoolConfigArgs) -> WarmPoolConfigRequest {
    WarmPoolConfigRequest {
        template_name: args.template,
        content_hash: args.content_hash,
        vcpu: args.vcpu,
        memory_mib: args.memory_mib,
        target_ready_slots: args.target_ready_slots,
        refill_per_tick: args.refill_per_tick,
        max_warming_slots: args.max_warming_slots,
        enabled: !args.disabled,
    }
}

fn warm_pool_update_request(args: &WarmPoolUpdateArgs) -> Result<WarmPoolConfigRequest, CliError> {
    let reference = TemplateReference::from_str(&args.template).map_err(|err| {
        CliError::Argument(format!(
            "invalid warm-pool update template reference {:?}: {err}",
            args.template
        ))
    })?;
    let digest = reference.artifact_digest().ok_or_else(|| {
        CliError::Argument(
            "warm-pool update requires an exact content-hash template reference like my-template@sha256:..."
                .to_string(),
        )
    })?;
    Ok(WarmPoolConfigRequest {
        template_name: reference.name().as_str().to_string(),
        content_hash: Some(digest.to_string()),
        vcpu: args.vcpu,
        memory_mib: args.memory_mib,
        target_ready_slots: args.target_ready_slots,
        refill_per_tick: args.refill_per_tick,
        max_warming_slots: args.max_warming_slots,
        enabled: !args.disabled,
    })
}

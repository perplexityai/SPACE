//! The node's activation seal. Output is one `key=value` per line, then one
//! `in_flight=<kind> <sandbox_id> <age_ms>` line per piece of activation
//! work in flight, so scripts can read it.

use std::time::Duration;

use super::*;
use pplx_space_sbx_mgr_client::proto::{
    ActivationSealStateRequest, ActivationSealStatus, InFlightActivation, InFlightActivationKind,
    SealActivationsRequest, UnsealActivationsRequest,
};

/// How long past its wait a seal's answer may take to arrive.
const SEAL_ANSWER_MARGIN: Duration = Duration::from_secs(60);

pub(super) async fn handle(
    command: Commands,
    client: &mut pplx_space_sbx_mgr_client::SandboxManagerGrpcClient,
) {
    match command {
        Commands::SealActivations { epoch, wait_secs } => {
            // The shared client's request timeout is shorter than the
            // longest wait a seal may ask for.
            let mut client = pplx_space_sbx_mgr_client::connect_with_request_timeout(
                Duration::from_secs(wait_secs).saturating_add(SEAL_ANSWER_MARGIN),
            )
            .await
            .unwrap_or_else(|e| {
                exit_error(format!(
                    "cannot connect to sbx-manager: {e}\nIs space-sbx-manager running?"
                ))
            });
            let response = client
                .seal_activations(SealActivationsRequest {
                    epoch,
                    wait_timeout: Some(prost_types::Duration {
                        seconds: i64::try_from(wait_secs).unwrap_or(i64::MAX),
                        nanos: 0,
                    }),
                })
                .await
                .unwrap_or_else(|e| exit_error(e))
                .into_inner();
            println!("quiesced={}", response.quiesced);
            print_status(response.status.unwrap_or_default());
        }
        Commands::UnsealActivations { epoch } => {
            let response = client
                .unseal_activations(UnsealActivationsRequest { epoch })
                .await
                .unwrap_or_else(|e| exit_error(e))
                .into_inner();
            print_status(response.status.unwrap_or_default());
        }
        Commands::ActivationSeal => {
            let response = client
                .activation_seal_state(ActivationSealStateRequest {})
                .await
                .unwrap_or_else(|e| exit_error(e))
                .into_inner();
            print_status(response.status.unwrap_or_default());
        }
        _ => unreachable!("command routed to wrong dispatch group"),
    }
}

fn print_status(status: ActivationSealStatus) {
    match status.sealed_epoch {
        Some(epoch) => println!("sealed_epoch={epoch}"),
        None => println!("sealed_epoch=none"),
    }
    print_sealed_since(status.sealed_since);
    println!("activations_admitted={}", status.activations_admitted);
    print_in_flight(&status.in_flight);
}

fn print_sealed_since(sealed_since: Option<Timestamp>) {
    let rendered = sealed_since
        .and_then(|since| jiff::Timestamp::new(since.seconds, since.nanos).ok())
        .map_or_else(|| "none".to_string(), |since| since.to_string());
    println!("sealed_since={rendered}");
}

fn print_in_flight(in_flight: &[InFlightActivation]) {
    for activation in in_flight {
        let kind = InFlightActivationKind::try_from(activation.kind)
            .map_or("unknown", |kind| kind.as_str_name());
        let age_ms = activation.age.map_or(0, |age| {
            age.seconds
                .saturating_mul(1_000)
                .saturating_add(i64::from(age.nanos / 1_000_000))
        });
        println!("in_flight={kind} {} {age_ms}", activation.sandbox_id);
    }
}

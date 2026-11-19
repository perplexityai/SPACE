//! Atomic installation of the owned table in one pinned network namespace.
//! A committed transaction is not verification; read the table back for that.

use std::fs::File;
use std::process::Output;
use std::time::Duration;

use serde_json::json;

use super::{FAMILY, TABLE, TableConstruction};

/// What one submission is known to have done.
#[derive(Debug)]
pub enum InstallOutcome {
    /// nft committed the whole transaction.
    Committed,
    /// nft or the kernel refused the transaction, so the previous state stands.
    Rejected { stderr: String },
    /// The transaction may or may not have committed, as after a timeout.
    Unknown { error: std::io::Error },
}

/// Replaces `ip space_firewall_rules` in the namespace `netns` pins with
/// `construction`, in one transaction. Never retries.
pub fn install(construction: &TableConstruction, netns: &File) -> InstallOutcome {
    submit(
        construction,
        netns,
        "nft",
        crate::host_command::MUTATION_BUDGET,
    )
}

fn submit(
    construction: &TableConstruction,
    netns: &File,
    program: &str,
    budget: Duration,
) -> InstallOutcome {
    let table = json!({"table": {"family": FAMILY, "name": TABLE}});
    // The add makes the delete valid on first install; nft commits the batch
    // whole or not at all, so the old table never disappears on its own.
    let commands: Vec<_> = [json!({"add": table}), json!({"delete": table})]
        .into_iter()
        .chain(construction.commands().iter().cloned())
        .collect();
    let transaction = json!({"nftables": commands}).to_string();
    let result = crate::net::command_output_in_pinned_ns(
        netns,
        program,
        &["-j", "-f", "-"],
        transaction.as_bytes(),
        budget,
    );
    outcome(result)
}

fn outcome(result: std::io::Result<Output>) -> InstallOutcome {
    match result {
        Ok(output) if output.status.success() => InstallOutcome::Committed,
        // nft exits 1 when it or the kernel refuses the batch; any other
        // exit, such as a signal, says nothing about what committed.
        Ok(output) if output.status.code() == Some(1) => InstallOutcome::Rejected {
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
        },
        Ok(output) => InstallOutcome::Unknown {
            error: std::io::Error::other(format!("nft ended with {}", output.status)),
        },
        // The error does not say whether nft received the batch.
        Err(error) => InstallOutcome::Unknown { error },
    }
}

#[cfg(test)]
mod tests;

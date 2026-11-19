//! Atomic installation of the owned table in one pinned network namespace.
//! No outcome is verification: read the table back after every one.

use std::fs::File;
use std::process::Output;
use std::time::Duration;

use serde_json::json;

use super::{FAMILY, TABLE, TableConstruction};

/// What nft reported for one submission, which is not what Linux holds.
#[derive(Debug)]
#[must_use = "read the table back to verify"]
pub enum InstallOutcome {
    /// nft reported that the whole transaction committed.
    Committed,
    /// nft reported failure. The state is unknown until verified: nft can
    /// exit 1 after the kernel committed, as on an ACK receive overflow.
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
    // The add makes the delete valid on first install. One batch, so the
    // kernel never commits the delete without the rebuild.
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
        // nft exits 1 when it reports a failure; any other exit, such as a
        // signal, is no report at all.
        Ok(output) if output.status.code() == Some(1) => InstallOutcome::Rejected {
            stderr: stderr(&output),
        },
        Ok(output) => InstallOutcome::Unknown {
            error: std::io::Error::other(format!(
                "nft ended with {}: {}",
                output.status,
                stderr(&output)
            )),
        },
        // The error does not say whether nft received the batch.
        Err(error) => InstallOutcome::Unknown { error },
    }
}

/// nft's trimmed stderr, cut to a size an error message can carry.
fn stderr(output: &Output) -> String {
    const LIMIT: usize = 4096;
    let mut text = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if text.len() > LIMIT {
        let mut end = LIMIT;
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        text.truncate(end);
        text.push('…');
    }
    text
}

#[cfg(test)]
mod tests;

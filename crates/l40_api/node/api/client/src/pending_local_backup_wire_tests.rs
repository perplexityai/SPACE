//! Wire-shape facts about `PublishNodeStateRequest.pending_local_backups`
//! and the cost of the field on the one un-paged census RPC that carries it.

use prost::Message;

use crate::proto::{PendingLocalBackupReport, PublishNodeStateRequest};
use pplx_space_sbx_mgr_client::proto::{ArtifactKind, PendingLocalBackup};

fn pending(index: u32) -> PendingLocalBackup {
    PendingLocalBackup {
        // Both ids are 36-character UUID text on the wire; the byte cost
        // measured below is only meaningful at that length.
        sandbox_id: format!("019e6370-cfc9-7a43-9337-{index:012}"),
        local_artifact_id: format!("019e6371-0000-7a43-9337-{index:012}"),
        artifact_kind: ArtifactKind::Full as i32,
    }
}

fn census(pending_count: u32) -> PublishNodeStateRequest {
    PublishNodeStateRequest {
        node_id: "019e6370-cfc9-7a43-9337-399c7b51a1d9".to_string(),
        sandbox_inventory_complete: true,
        pending_local_backups: Some(PendingLocalBackupReport {
            pending: (0..pending_count).map(pending).collect(),
        }),
        ..PublishNodeStateRequest::default()
    }
}

/// "Nothing is pending" and "the node did not report" are different
/// answers, and the point of the submessage is that they survive a round
/// trip as different values rather than collapsing onto one empty list.
/// Without it a node with a clean disk is indistinguishable from a node
/// whose enumeration failed, and only one of those is safe to act on.
#[test]
fn an_empty_report_is_distinguishable_from_no_report() {
    let nothing_pending = census(0);
    let no_report = PublishNodeStateRequest {
        pending_local_backups: None,
        ..census(0)
    };

    let decoded_empty =
        PublishNodeStateRequest::decode(nothing_pending.encode_to_vec().as_slice()).unwrap();
    let decoded_absent =
        PublishNodeStateRequest::decode(no_report.encode_to_vec().as_slice()).unwrap();

    assert_eq!(
        decoded_empty.pending_local_backups,
        Some(PendingLocalBackupReport { pending: vec![] })
    );
    assert_eq!(decoded_absent.pending_local_backups, None);
}

/// The full census is one unpaged RPC. ClusterApi accepts up to 32 MiB, but
/// the pending-backup field should still add only a small amount per entry.
#[test]
fn the_pending_list_costs_eighty_bytes_per_entry() {
    let empty = census(0).encoded_len();
    let per_entry = census(1).encoded_len() - empty;

    assert_eq!(
        per_entry, 80,
        "one entry is two 36-byte UUID strings, an enum, and their tags"
    );
}

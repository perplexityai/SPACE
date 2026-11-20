//! The admin API's drain v2 rules: which `:drain`, `:undrain` and
//! `:terminate` requests a v2 drain takes and what they write, and the
//! preview call the admin API makes of a node.

use super::*;

/// [`drain_v2_write`]'s refusal of a node a v2 drain can't run on by its
/// substrate: it drains Kubernetes nodes only.
pub(super) fn drain_v2_drains_substrate(
    managed_by: NodeManagedBy,
) -> Result<(), (StatusCode, &'static str)> {
    if managed_by == NodeManagedBy::Kubernetes {
        Ok(())
    } else {
        Err((
            StatusCode::CONFLICT,
            "drain engine v2 drains Kubernetes nodes only",
        ))
    }
}

/// [`drain_v2_write`]'s refusal to start a v2 drain on a node whose EC2
/// instance isn't recorded.
pub(super) fn drain_v2_needs_instance(
    recorded_instance: bool,
) -> Result<(), (StatusCode, &'static str)> {
    if recorded_instance {
        Ok(())
    } else {
        // The drain terminates exactly the recorded instance, and asks EC2
        // about it to tell a node that died.
        Err((
            StatusCode::CONFLICT,
            "drain engine v2 needs the node's EC2 instance recorded: the drain terminates that \
             instance, and asks EC2 about it when the node stops answering",
        ))
    }
}

/// Why `:drain` would refuse to start a v2 drain on a node whatever the
/// request asks, from the rules [`drain_v2_write`] applies: the node's
/// substrate and its recorded instance. Whether v2 is enabled is left out:
/// that is the cluster's, not the node's.
pub(super) fn drain_v2_node_refusals(
    managed_by: NodeManagedBy,
    recorded_instance: bool,
) -> Vec<&'static str> {
    [
        drain_v2_drains_substrate(managed_by),
        drain_v2_needs_instance(recorded_instance),
    ]
    .into_iter()
    .filter_map(Result::err)
    .map(|(_, reason)| reason)
    .collect()
}

/// A node's answer to [`V2DrainProbe::preview`].
pub type V2DrainPreview<'a> = std::pin::Pin<
    Box<
        dyn std::future::Future<
                Output = Result<
                    pplx_space_sbx_mgr_client::proto::PreviewDrainNodeResponse,
                    DrainPreviewError,
                >,
            > + Send
            + 'a,
    >,
>;

/// The `PreviewDrainNode` call the node page asks for. Production calls the
/// node through its node-api ([`NodeApiV2DrainProbe`]); tests script it.
/// The admin API makes no `DrainNode` call: only the cluster controller
/// drives a drain.
pub trait V2DrainProbe: Send + Sync {
    /// The node's answer to `PreviewDrainNode`: what an evacuating v2 drain
    /// would find on it now, with nothing changed on the node. The preview
    /// handler bounds it (`DRAIN_PREVIEW_DEADLINE` in
    /// `nodes/drain_preview.rs`).
    fn preview<'a>(
        &'a self,
        state: &'a AppState,
        target: &'a crate::controllers::substrate::OwnedNodeApiTarget,
    ) -> V2DrainPreview<'a>;
}

/// [`V2DrainProbe`] through the node's internal node-api.
pub struct NodeApiV2DrainProbe;

/// How long the `PreviewDrainNode` call waits for the node's answer, once
/// connected.
pub(super) const DRAIN_NODE_PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

impl V2DrainProbe for NodeApiV2DrainProbe {
    fn preview<'a>(
        &'a self,
        state: &'a AppState,
        target: &'a crate::controllers::substrate::OwnedNodeApiTarget,
    ) -> V2DrainPreview<'a> {
        Box::pin(async move {
            let mut client =
                crate::controllers::substrate::connect_internal_node_api_client_for_target(
                    state.sandbox_controller.substrate(),
                    &target.as_target(),
                    DRAIN_NODE_PROBE_TIMEOUT,
                )
                .await
                .map_err(|error| DrainPreviewError::Unreachable(error.to_string()))?;
            client
                .preview_drain_node(pplx_space_sbx_mgr_client::proto::PreviewDrainNodeRequest {})
                .await
                .map(tonic::Response::into_inner)
                .map_err(DrainPreviewError::from_status)
        })
    }
}

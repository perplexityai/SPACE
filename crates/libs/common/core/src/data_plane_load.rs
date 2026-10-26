//! Wire contract for the data plane's internal stream-load report.
//!
//! The data plane serves this on an internal listener that is not part of its
//! public Service; the cluster controller reads it to order data-plane scale-in
//! (see `controller.kubernetes.io/pod-deletion-cost`). Both sides take the
//! path and the body shape from here so they cannot drift; the port is
//! whatever the pod declares under [`DATA_PLANE_INTERNAL_PORT_NAME`].

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Path of the stream-load report on the data plane's internal listener.
pub const DATA_PLANE_STREAM_LOAD_PATH: &str = "/internal/v1/stream-load";

/// Name of the container port that declares the internal listener. The cluster
/// controller probes the pod on whichever port carries this name, so the
/// deployment that sets `--internal-listen` also tells the reader where it is.
pub const DATA_PLANE_INTERNAL_PORT_NAME: &str = "internal";

/// One data-plane replica's current stream load.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DataPlaneStreamLoad {
    /// Streams attached to this replica right now, whatever their age: every
    /// connection holding an attached-stream permit (native sessions, ECW
    /// process and file-watch streams, upgraded ECW service-port connections).
    /// Plain HTTP requests (through a sandbox service port, or an ECW file
    /// transfer or streamed process input) hold no permit and are counted in
    /// [`Self::active_ingress_requests`] instead.
    pub active_streams: u64,
    /// Plain HTTP requests still in flight: through a sandbox service port (ECW
    /// or native) from the request until its response body finishes, the
    /// client goes away, or the transfer stalls past its progress timeout; and
    /// ECW file transfers and streamed process input while they're served.
    /// Long ones, such as server-sent events or large downloads, are cut by a
    /// scale-in just like a stream. Absent from a report by a data plane that
    /// predates the field, which reads as 0.
    #[cfg_attr(feature = "serde", serde(default))]
    pub active_ingress_requests: u64,
}

impl DataPlaneStreamLoad {
    /// Everything a scale-in that removes this replica would cut: attached
    /// streams plus in-flight plain requests.
    pub fn connections_at_risk(&self) -> u64 {
        self.active_streams
            .saturating_add(self.active_ingress_requests)
    }
}

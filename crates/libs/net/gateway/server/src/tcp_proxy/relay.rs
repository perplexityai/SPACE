//! Relay established streams. Dialing and HTTP upgrade handoff happen before entry.
use std::{io, time::Duration};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::sync::CancellationToken;

mod leg;
use super::observation::{Close, Connection, Phase, Side};
pub(super) use leg::RelayFault;
use leg::RelayWitness;

#[derive(Debug, thiserror::Error)]
pub(super) enum Error {
    #[error("relay cancelled")]
    Cancelled,
    #[error("relay idle deadline elapsed")]
    Idle,
    #[error("relay I/O failed ({kind:?})", kind = .0.kind())]
    Io(#[source] io::Error),
}

pub(super) struct Report {
    pub result: Result<(), Error>,
    /// Bytes read from each peer, including data still buffered at cancellation.
    pub read: (u64, u64),
    /// Bytes successfully written, downstream-to-upstream first.
    pub written: (u64, u64),
    pub fault: Option<RelayFault>,
    /// First observed EOF, not the initiator of a connection close.
    pub first_eof: Option<leg::Side>,
}

impl From<leg::Side> for Side {
    fn from(side: leg::Side) -> Self {
        match side {
            leg::Side::Guest => Self::Downstream,
            leg::Side::Origin => Self::Upstream,
        }
    }
}

impl Report {
    pub(super) fn observe(&self, observation: &Connection) {
        if let Err(Error::Io(error)) = &self.result {
            let side = self.fault.map(|fault| fault.side().into());
            observation.failure(Phase::Relay, side, error);
        } else if matches!(&self.result, Err(Error::Cancelled | Error::Idle)) {
            observation.closed(Side::Downstream, Close::Local);
        } else if let Some(side) = self.first_eof {
            observation.closed(side.into(), Close::Remote);
        }
    }
}

pub(super) async fn run<D, U>(
    downstream: D,
    upstream: U,
    shutdown: &CancellationToken,
    idle_timeout: Option<Duration>,
) -> Report
where
    D: AsyncRead + AsyncWrite + Unpin,
    U: AsyncRead + AsyncWrite + Unpin,
{
    let witness = RelayWitness::default();
    let result = {
        let mut downstream = witness.guest(downstream);
        let mut upstream = witness.origin(upstream);
        let idle = async {
            let Some(timeout) = idle_timeout else {
                return std::future::pending().await;
            };
            let mut bytes = (0, 0);
            let mut last_progress = tokio::time::Instant::now();
            loop {
                tokio::time::sleep(Duration::from_secs(10)).await;
                let current = witness.bytes();
                if current != bytes {
                    bytes = current;
                    last_progress = tokio::time::Instant::now();
                }
                if last_progress.elapsed() >= timeout {
                    return;
                }
            }
        };
        tokio::select! {
            biased;
            () = shutdown.cancelled() => Err(Error::Cancelled),
            () = idle => Err(Error::Idle),
            result = tokio::io::copy_bidirectional(&mut downstream, &mut upstream) => result.map(|_| ()).map_err(Error::Io),
        }
    };
    Report {
        result,
        read: witness.bytes(),
        written: witness.written(),
        fault: witness.fault(),
        first_eof: witness.closed_by(),
    }
}

#[cfg(test)]
mod tests;

//! Component qualification probe, run against an explicitly named VMM process.
//! This does not admit a Windows image for SPACE lifecycle or tenant operations.
use pplx_space_spaced_common::{
    BRIDGE_GUEST_VSOCK_PORT, ExpectedUdsPeer, connect_firecracker_vsock_channel_with_expected_peer,
    proto::{
        bridge_guest::{FlushFilesystemsRequest, WaitReadyRequest, meta_client::MetaClient},
        platform::GuestOs,
    },
};
use std::{path::PathBuf, time::Duration};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let path = PathBuf::from(
        args.next()
            .ok_or_else(|| anyhow::anyhow!("expected socket path, VMM PID and UID"))?,
    );
    let pid = args
        .next()
        .ok_or_else(|| anyhow::anyhow!("missing VMM PID"))?
        .parse()?;
    let uid = args
        .next()
        .ok_or_else(|| anyhow::anyhow!("missing VMM UID"))?
        .parse()?;
    anyhow::ensure!(args.next().is_none(), "unexpected arguments");
    tokio::time::timeout(Duration::from_secs(30), async {
        for _ in 0..3 {
            let (channel, _connection) = connect_firecracker_vsock_channel_with_expected_peer(
                "Windows readiness probe",
                &path,
                BRIDGE_GUEST_VSOCK_PORT,
                ExpectedUdsPeer::new(pid, uid),
            )
            .await?;
            let mut client = MetaClient::new(channel);
            let old = client
                .wait_ready(WaitReadyRequest {
                    timeout: None,
                    host_contract_revision: 0,
                })
                .await
                .err()
                .ok_or_else(|| anyhow::anyhow!("unsupported operation unexpectedly succeeded"))?;
            anyhow::ensure!(
                old.code() == tonic::Code::FailedPrecondition,
                "legacy host was not refused: {old}"
            );
            for host_contract_revision in [1, 2] {
                let ready = client
                    .wait_ready(WaitReadyRequest {
                        timeout: None,
                        host_contract_revision,
                    })
                    .await?
                    .into_inner();
                anyhow::ensure!(ready.ready, "guest not ready");
                let platform = ready
                    .platform
                    .ok_or_else(|| anyhow::anyhow!("missing descriptor"))?;
                anyhow::ensure!(platform.os == GuestOs::Windows as i32, "incorrect guest OS");
                anyhow::ensure!(
                    platform.operations.is_empty(),
                    "unqualified guest advertised operations"
                );
            }
            let flush = client
                .flush_filesystems(FlushFilesystemsRequest {})
                .await
                .err()
                .ok_or_else(|| anyhow::anyhow!("unsupported operation unexpectedly succeeded"))?;
            anyhow::ensure!(
                flush.code() == tonic::Code::FailedPrecondition,
                "unqualified flush did not refuse"
            );
        }
        Ok::<(), anyhow::Error>(())
    })
    .await??;
    println!("Windows readiness, legacy-host refusal, empty capabilities and reconnect: passed");
    Ok(())
}

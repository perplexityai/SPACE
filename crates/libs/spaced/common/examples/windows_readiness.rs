//! Component qualification probe, run against an explicitly named VMM process.
//! This does not admit a Windows image for SPACE lifecycle or tenant operations.
use pplx_space_spaced_common::{
    BRIDGE_GUEST_VSOCK_PORT, ExpectedUdsPeer, connect_firecracker_vsock_channel_with_expected_peer,
    proto::{
        bridge_guest::{
            ClockSyncStatus, FlushFilesystemsRequest, NotifyResumeRequest, ResumeReason,
            WaitReadyRequest, meta_client::MetaClient,
        },
        platform::{GuestOperation, GuestOs},
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
    let lifecycle = match args.next().as_deref() {
        None => false,
        Some("--lifecycle") => true,
        Some(_) => anyhow::bail!("expected optional --lifecycle"),
    };
    anyhow::ensure!(args.next().is_none(), "unexpected arguments");
    tokio::time::timeout(Duration::from_secs(30), async {
        for attempt in 0..3 {
            println!("connection {attempt}: connecting");
            let (channel, _connection) = connect_firecracker_vsock_channel_with_expected_peer(
                "Windows readiness probe",
                &path,
                BRIDGE_GUEST_VSOCK_PORT,
                ExpectedUdsPeer::new(pid, uid),
            )
            .await?;
            let mut client = MetaClient::new(channel);
            println!("connection {attempt}: checking host revisions and readiness");
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
                // --lifecycle selects destructive component checks; it does not
                // change which methods the candidate binary implements.
                let expected = vec![GuestOperation::Flush as i32, GuestOperation::Resume as i32];
                anyhow::ensure!(
                    platform.operations == expected,
                    "unexpected advertised operations"
                );
            }
            if lifecycle {
                println!("connection {attempt}: flushing volumes");
                client.flush_filesystems(FlushFilesystemsRequest {}).await?;
                println!("connection {attempt}: synchronizing resume clock");
                let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?;
                let resumed = client
                    .notify_resume(NotifyResumeRequest {
                        reason: ResumeReason::SnapshotRestore as i32,
                        host_time: Some(prost_types::Timestamp {
                            seconds: i64::try_from(now.as_secs())?,
                            nanos: i32::try_from(now.subsec_nanos())?,
                        }),
                    })
                    .await?
                    .into_inner();
                anyhow::ensure!(
                    resumed.ready && resumed.clock_sync_status == ClockSyncStatus::Synced as i32,
                    "Windows resume did not synchronize UTC"
                );
                continue;
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
    println!("Windows readiness, host revision refusal, selected operations and reconnect: passed");
    Ok(())
}

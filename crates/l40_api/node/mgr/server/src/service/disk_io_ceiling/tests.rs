use super::*;

fn configuration(operations: Value) -> Value {
    json!({"drives":[{"drive_id":"rootfs","is_root_device":true,"rate_limiter":{
        "ops":operations,"bandwidth":{"size":3145728000_u64,"refill_time":30000}
    }},{"drive_id":"data","is_root_device":false}]})
}

#[test]
fn operator_ceiling_preserves_lower_assignments_bandwidth_and_other_drives() {
    let ceiling = DiskIoCeiling::parse(Some("1000")).unwrap();
    for (ops, expected, refill) in [
        (json!({"size":300000,"refill_time":30000}), 30000, 30000),
        (json!({"size":15000,"refill_time":30000}), 15000, 30000),
        (json!({"size":10000,"refill_time":1000}), 1000, 1000),
        (json!({"size":0,"refill_time":0}), 30000, 30000),
        (Value::Null, 30000, 30000),
    ] {
        if let (Some(size), Some(refill_time_ms)) =
            (ops["size"].as_u64(), ops["refill_time"].as_u64())
        {
            let assigned = RateLimiter {
                bandwidth: TokenBucket {
                    size: 3145728000,
                    refill_time_ms: 30000,
                },
                operations: TokenBucket {
                    size,
                    refill_time_ms,
                },
            };
            let applied = ceiling.clamp_limiter(assigned).unwrap();
            assert_eq!(applied.bandwidth, assigned.bandwidth);
            assert_eq!(
                applied.operations,
                TokenBucket {
                    size: expected,
                    refill_time_ms: refill
                }
            );
        }
        let mut config = configuration(ops);
        let bandwidth = config["drives"][0]["rate_limiter"]["bandwidth"].clone();
        let extra = config["drives"][1].clone();
        ceiling.clamp_config(&mut config).unwrap();
        assert_eq!(config["drives"][0]["rate_limiter"]["ops"]["size"], expected);
        assert_eq!(
            config["drives"][0]["rate_limiter"]["ops"]["refill_time"],
            refill
        );
        assert_eq!(config["drives"][0]["rate_limiter"]["bandwidth"], bandwidth);
        assert_eq!(config["drives"][1], extra);
        let already_limited = config.clone();
        assert!(!ceiling.clamp_config(&mut config).unwrap());
        assert_eq!(config, already_limited);
        assert!(!DiskIoCeiling::default().clamp_config(&mut config).unwrap());
        assert_eq!(config, already_limited);
    }
    let mut builder = json!({"drives":[{"drive_id":"rootfs","is_root_device":true}]});
    ceiling.clamp_config(&mut builder).unwrap();
    assert_eq!(builder["drives"][0]["rate_limiter"]["ops"]["size"], 30000);
    assert!(
        builder["drives"][0]["rate_limiter"]
            .get("bandwidth")
            .is_none()
    );
}

#[test]
fn configuration_is_strict_and_extreme_bucket_arithmetic_stays_bounded() {
    for value in [
        "",
        "0",
        "-1",
        "invalid",
        "1.5",
        "1000 ",
        "18446744073709551615",
    ] {
        DiskIoCeiling::parse(Some(value)).expect_err(value);
    }
    assert!(DiskIoCeiling::parse(None).unwrap().configured().is_none());
    let ceiling = DiskIoCeiling::parse(Some("1")).unwrap();
    let mut short = configuration(json!({"size":1,"refill_time":1}));
    ceiling.clamp_config(&mut short).unwrap();
    assert_eq!(short["drives"][0]["rate_limiter"]["ops"]["size"], 1);
    assert_eq!(
        short["drives"][0]["rate_limiter"]["ops"]["refill_time"],
        1000
    );
    let mut huge = configuration(json!({"size":u64::MAX,"refill_time":u64::MAX}));
    ceiling.clamp_config(&mut huge).unwrap();
    let bucket = &huge["drives"][0]["rate_limiter"]["ops"];
    assert!(
        u128::from(bucket["size"].as_u64().unwrap()) * 1000
            <= u128::from(bucket["refill_time"].as_u64().unwrap())
    );
    for bad in [
        json!({}),
        json!({"drives":[]}),
        configuration(json!({"size":"invalid","refill_time":30000})),
    ] {
        ceiling.update(&bad).unwrap_err();
    }
}

#[tokio::test]
async fn live_enforcement_verifies_the_update_and_does_not_replenish_each_sweep() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    for applies_patch in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("fc.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let server = tokio::spawn(async move {
            let mut config = configuration(json!({"size":300000,"refill_time":30000}));
            let mut patches = 0;
            for _ in 0..if applies_patch { 4 } else { 3 } {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut received = Vec::new();
                let (header_end, content_length) = loop {
                    let mut bytes = [0_u8; 4096];
                    let count = stream.read(&mut bytes).await.unwrap();
                    assert!(count > 0);
                    received.extend_from_slice(&bytes[..count]);
                    if let Some(pos) = received.windows(4).position(|v| v == b"\r\n\r\n") {
                        let header = String::from_utf8_lossy(&received[..pos]).to_ascii_lowercase();
                        let length = header
                            .lines()
                            .find_map(|l| {
                                l.strip_prefix("content-length:")
                                    .map(|v| v.trim().parse::<usize>().unwrap())
                            })
                            .unwrap_or(0);
                        if received.len() >= pos + 4 + length {
                            break (pos + 4, length);
                        }
                    }
                };
                let patch = received.starts_with(b"PATCH /drives/rootfs ");
                let body = if patch {
                    patches += 1;
                    let request: Value =
                        serde_json::from_slice(&received[header_end..header_end + content_length])
                            .unwrap();
                    assert_eq!(request["rate_limiter"]["ops"]["size"], 30000);
                    assert_eq!(request["rate_limiter"]["ops"]["refill_time"], 30000);
                    assert_eq!(request["rate_limiter"]["ops"]["one_time_burst"], 0);
                    assert!(request["rate_limiter"].get("bandwidth").is_none());
                    if applies_patch {
                        config["drives"][0]["rate_limiter"]["ops"] =
                            request["rate_limiter"]["ops"].clone();
                    }
                    "{}".to_owned()
                } else {
                    assert!(received.starts_with(b"GET /vm/config "));
                    config.to_string()
                };
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
            }
            patches
        });
        let client = crate::fc_api::FcApiClient::new(&socket);
        let ceiling = DiskIoCeiling::parse(Some("1000")).unwrap();
        ceiling
            .enforce_launch_adoption(
                &crate::fc_api::FcApiClient::new(directory.path().join("absent.sock")),
                true,
            )
            .await
            .unwrap();
        let result = tokio::time::timeout(
            Duration::from_secs(3),
            ceiling.enforce_launch_adoption(&client, false),
        )
        .await
        .unwrap();
        if applies_patch {
            result.unwrap();
            assert_eq!(
                ceiling.enforce(&client).await.unwrap(),
                Enforcement::Verified { updated: false }
            );
        } else {
            result.unwrap_err();
        }
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), server)
                .await
                .unwrap()
                .unwrap(),
            1
        );
    }
}

#[test]
fn cold_boot_config_is_capped_without_following_destination_aliases() {
    use std::os::unix::fs::symlink;
    for (maximum, opaque) in [(None, false), (None, true), (Some("1000"), false)] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let source = root.join("source.json");
        let destination = root.join("vm-config.json");
        let victim = root.join("victim");
        let original = if opaque {
            b"opaque configuration\n".to_vec()
        } else {
            serde_json::to_vec_pretty(&configuration(json!({"size":300000,"refill_time":30000})))
                .unwrap()
        };
        std::fs::write(&source, &original).unwrap();
        for alias in ["regular", "symlink", "hardlink"] {
            std::fs::write(&victim, b"untouched").unwrap();
            match alias {
                "regular" => std::fs::write(&destination, b"old config").unwrap(),
                "symlink" => symlink(&victim, &destination).unwrap(),
                "hardlink" => std::fs::hard_link(&victim, &destination).unwrap(),
                _ => unreachable!(),
            }
            DiskIoCeiling::parse(maximum)
                .unwrap()
                .stage_cold_boot_config(&source, &destination)
                .unwrap();
            let written = std::fs::read(&destination).unwrap();
            if maximum.is_some() {
                let actual: Value = serde_json::from_slice(&written).unwrap();
                assert_eq!(actual["drives"][0]["rate_limiter"]["ops"]["size"], 30000);
            } else {
                assert_eq!(written, original);
            }
            assert_eq!(std::fs::read(&source).unwrap(), original);
            assert_eq!(std::fs::read(&victim).unwrap(), b"untouched");
            std::fs::remove_file(&destination).unwrap();
        }
        if maximum.is_some() {
            std::fs::write(&source, b"malformed").unwrap();
            std::fs::write(&destination, b"previous").unwrap();
            DiskIoCeiling::parse(maximum)
                .unwrap()
                .stage_cold_boot_config(&source, &destination)
                .unwrap_err();
            assert_eq!(std::fs::read(&destination).unwrap(), b"previous");
            assert_eq!(std::fs::read_dir(&root).unwrap().count(), 3);
        }
    }
}

#[tokio::test]
async fn sweep_rechecks_changed_vms_and_rejects_stale_targets_before_api_io() {
    let service = crate::service::tests::service_tests::service_for_unit_tests()
        .await
        .with_disk_io_ceiling(DiskIoCeiling::parse(Some("1000")).unwrap());
    service.pool.set_all_slots_ready_for_test();
    let id = pplx_space_common_core::types::SandboxId::new().to_string();
    service
        .pool
        .allocate(
            &id,
            None,
            Some(crate::slot_pool::NewLeaseMemoryReservation {
                memory_reservation_mib: 1024,
            }),
        )
        .await
        .unwrap();
    service.pool.set_pid(&id, 424242).unwrap();
    let record = service.pool.get_record(&id).unwrap().unwrap();
    let mut verified = VerifiedVms::default();
    let records = [record.clone()];
    assert_eq!(verified.pending(&records).len(), 1);
    for outcome in [
        Err(Status::unavailable("API unavailable")),
        Err(Status::deadline_exceeded("busy")),
        Ok(Enforcement::Superseded),
        Ok(Enforcement::AwaitingSnapshot),
    ] {
        assert!(!verified.observe(&record, &outcome));
        assert_eq!(verified.pending(&records).len(), 1);
    }
    assert!(verified.observe(&record, &Ok(Enforcement::Verified { updated: false })));
    assert!(verified.pending(&records).is_empty());
    for field in ["generation", "slot", "pid"] {
        let mut stale = record.clone();
        match field {
            "generation" => stale.info.generation += 1,
            "slot" => stale.info.slot += 1,
            "pid" => stale.info.pid = Some(424243),
            _ => unreachable!(),
        }
        assert_eq!(
            service
                .enforce_disk_io_ceiling_on_record(&stale)
                .await
                .unwrap(),
            Enforcement::Superseded
        );
        let mut cache = VerifiedVms::default();
        cache.0.insert(VerifiedVm::from_record(&record).unwrap());
        assert_eq!(cache.pending(&[stale]).len(), 1);
        assert!(cache.0.is_empty());
    }
    assert!(verified.pending(&[]).is_empty());
    assert!(verified.0.is_empty());
}

#[tokio::test]
async fn restore_enforcement_rolls_back_errors_and_timeouts_before_adoption() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    for mode in ["compliant", "invalid", "idle", "timeout"] {
        let temp = tempfile::tempdir().unwrap();
        let socket = temp.path().join("fc.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let mut accepted_tx = Some(accepted_tx);
            for _ in 0..if mode == "idle" { 2 } else { 1 } {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = [0; 4096];
                assert!(stream.read(&mut request).await.unwrap() > 0);
                if let Some(accepted_tx) = accepted_tx.take() {
                    accepted_tx.send(()).unwrap();
                }
                if mode == "timeout" {
                    std::future::pending::<()>().await;
                }
                let body = if mode == "compliant" {
                    configuration(json!({"size":30000,"refill_time":30000})).to_string()
                } else if mode == "idle" {
                    json!({"drives":[]}).to_string()
                } else {
                    "{}".to_owned()
                };
                stream
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            }
        });
        let rollbacks = AtomicUsize::new(0);
        let client = crate::fc_api::FcApiClient::new(&socket);
        let ceiling = DiskIoCeiling::parse(Some("1000")).unwrap();
        if mode == "idle" {
            assert_eq!(
                ceiling.enforce(&client).await.unwrap(),
                Enforcement::AwaitingSnapshot
            );
        }
        let rollback_count = &rollbacks;
        let work = ceiling.enforce_before_adoption(&client, |status| async move {
            assert_eq!(
                status.code(),
                if mode == "timeout" {
                    tonic::Code::DeadlineExceeded
                } else {
                    tonic::Code::FailedPrecondition
                }
            );
            rollback_count.fetch_add(1, Ordering::SeqCst);
        });
        tokio::pin!(work);
        tokio::select! {
            biased;
            accepted = accepted_rx => accepted.unwrap(),
            _ = &mut work => panic!("enforcement finished before receiving the API request"),
        }
        if mode == "timeout" {
            tokio::time::pause();
            tokio::time::advance(TARGET_TIMEOUT + Duration::from_millis(1)).await;
        }
        let result = work.await;
        if mode == "compliant" {
            result.unwrap();
        } else {
            result.unwrap_err();
        }
        assert_eq!(
            rollbacks.load(Ordering::SeqCst),
            usize::from(mode != "compliant")
        );
        if mode == "timeout" {
            tokio::time::resume();
            server.abort();
            assert!(server.await.unwrap_err().is_cancelled());
        } else {
            server.await.unwrap();
        }
    }
}

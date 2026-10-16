use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::time::Duration;

use k8s_openapi::api::core::v1::{Container, ContainerPort, Pod, PodCondition, PodSpec, PodStatus};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, Time};
use pplx_space_common_core::data_plane_load::DataPlaneStreamLoad;

use super::backend::{BackendError, ProbeError, candidate_pod, deletion_cost_patch, live_uid};
use super::*;

const INTERVAL: Duration = Duration::from_secs(30);

/// What a fake pod's stream-load read does.
#[derive(Debug, Clone, Copy)]
enum FakeLoad {
    Active(u64),
    Fail,
    Hang,
}

#[derive(Default)]
struct FakeBackend {
    pods: Mutex<Option<Vec<DataPlanePod>>>,
    /// Pod names that are live but NotReady: absent from the candidates.
    not_ready: Mutex<std::collections::HashSet<String>>,
    loads: Mutex<HashMap<String, FakeLoad>>,
    fail_writes: Mutex<bool>,
    write_delay: Mutex<Duration>,
    probes: Mutex<Vec<String>>,
    writes: Mutex<Vec<(String, String, DeletionCost)>>,
}

impl FakeBackend {
    fn with_pods(pods: Vec<(DataPlanePod, FakeLoad)>) -> Self {
        let backend = Self::default();
        backend.set_pods(pods);
        backend
    }

    fn set_pods(&self, pods: Vec<(DataPlanePod, FakeLoad)>) {
        let mut loads = self.loads.lock().unwrap();
        loads.clear();
        for (pod, load) in &pods {
            loads.insert(pod.name.clone(), *load);
        }
        *self.pods.lock().unwrap() = Some(pods.into_iter().map(|(pod, _)| pod).collect());
    }

    fn set_ready(&self, name: &str, ready: bool) {
        let mut not_ready = self.not_ready.lock().unwrap();
        if ready {
            not_ready.remove(name);
        } else {
            not_ready.insert(name.to_string());
        }
    }

    fn set_load(&self, name: &str, load: FakeLoad) {
        self.loads.lock().unwrap().insert(name.to_string(), load);
    }

    /// Writes as `(pod name, uid, cost)`, sorted, since they fan out.
    fn take_writes(&self) -> Vec<(String, String, DeletionCost)> {
        let mut writes = std::mem::take(&mut *self.writes.lock().unwrap());
        writes.sort();
        writes
    }

    fn probe_count(&self) -> usize {
        self.probes.lock().unwrap().len()
    }
}

impl DeletionCostBackend for FakeBackend {
    fn pods(&self) -> Option<DataPlanePods> {
        let pods = self.pods.lock().unwrap().clone()?;
        let not_ready = self.not_ready.lock().unwrap();
        Some(DataPlanePods {
            live_uids: pods.iter().map(|pod| pod.uid.clone()).collect(),
            candidates: pods
                .into_iter()
                .filter(|pod| !not_ready.contains(&pod.name))
                .collect(),
        })
    }

    async fn probe(&self, pod: &DataPlanePod) -> Result<DataPlaneStreamLoad, ProbeError> {
        self.probes.lock().unwrap().push(pod.name.clone());
        let load = self.loads.lock().unwrap().get(&pod.name).copied();
        match load.expect("every fake pod has a load") {
            FakeLoad::Active(active_streams) => Ok(DataPlaneStreamLoad { active_streams }),
            FakeLoad::Fail => Err(ProbeError::Status(503)),
            FakeLoad::Hang => std::future::pending().await,
        }
    }

    async fn set_cost(&self, pod: &DataPlanePod, cost: DeletionCost) -> Result<(), BackendError> {
        let delay = *self.write_delay.lock().unwrap();
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        self.writes
            .lock()
            .unwrap()
            .push((pod.name.clone(), pod.uid.clone(), cost));
        if *self.fail_writes.lock().unwrap() {
            // Any kube error will do; the reconciler only classifies it.
            return Err(BackendError::Kube(
                kube::Error::LinesCodecMaxLineLengthExceeded,
            ));
        }
        // Mirror the apiserver: the next reflector read sees the new value.
        if let Some(pods) = self.pods.lock().unwrap().as_mut()
            && let Some(stored) = pods.iter_mut().find(|stored| stored.uid == pod.uid)
        {
            stored.current = Some(cost.as_str().to_string());
        }
        Ok(())
    }
}

fn pod(name: &str, current: Option<&str>) -> DataPlanePod {
    DataPlanePod {
        name: name.to_string(),
        uid: format!("uid-{name}"),
        pod_ip: "10.0.0.1".to_string(),
        port: 8091,
        current: current.map(str::to_string),
    }
}

fn write(name: &str, cost: DeletionCost) -> (String, String, DeletionCost) {
    (name.to_string(), format!("uid-{name}"), cost)
}

fn config() -> ReconcilerConfig {
    ReconcilerConfig {
        enabled: true,
        ..ReconcilerConfig::default()
    }
}

#[test]
fn buckets_are_orders_of_magnitude_with_idle_only_at_zero() {
    let cases = [
        (0, DeletionCost::Idle, "0"),
        (1, DeletionCost::Low, "10"),
        (9, DeletionCost::Low, "10"),
        (10, DeletionCost::Medium, "100"),
        (99, DeletionCost::Medium, "100"),
        (100, DeletionCost::High, "1000"),
        (u64::MAX, DeletionCost::High, "1000"),
    ];
    for (active, bucket, annotation) in cases {
        let cost = DeletionCost::from_active_streams(active);
        assert_eq!(cost, bucket, "{active} attached streams");
        assert_eq!(cost.as_str(), annotation, "{active} attached streams");
    }
}

#[test]
fn decide_skips_writes_that_would_not_change_the_ranking() {
    let now = Instant::now();
    assert_eq!(
        decide(None, DeletionCost::Idle, None, now, INTERVAL),
        WriteDecision::Unchanged,
        "an absent annotation already means cost 0"
    );
    assert_eq!(
        decide(Some("100"), DeletionCost::Medium, None, now, INTERVAL),
        WriteDecision::Unchanged
    );
    assert_eq!(
        decide(None, DeletionCost::Low, None, now, INTERVAL),
        WriteDecision::Write(DeletionCost::Low)
    );
    assert_eq!(
        decide(Some("not-a-cost"), DeletionCost::Idle, None, now, INTERVAL),
        WriteDecision::Write(DeletionCost::Idle),
        "a foreign value is rewritten"
    );
}

#[test]
fn decide_rate_limits_lowering_a_pod_to_one_write_per_interval() {
    let start = Instant::now();
    let lowered = |elapsed| {
        decide(
            Some("1000"),
            DeletionCost::Low,
            Some(start),
            start + elapsed,
            INTERVAL,
        )
    };
    assert_eq!(lowered(Duration::from_secs(29)), WriteDecision::RateLimited);
    assert_eq!(lowered(INTERVAL), WriteDecision::Write(DeletionCost::Low));
}

#[test]
fn decide_raises_a_pod_at_once_whenever_it_was_last_written() {
    let start = Instant::now();
    let just_after = start + Duration::from_secs(1);
    for (current, desired) in [
        (Some("0"), DeletionCost::High),
        (None, DeletionCost::Low),
        (Some("10"), DeletionCost::Medium),
        (Some("not-a-cost"), DeletionCost::Idle),
    ] {
        assert_eq!(
            decide(current, desired, Some(start), just_after, INTERVAL),
            WriteDecision::Write(desired),
            "{current:?} -> {desired}: a busy pod must not keep reading low, and a \
             foreign value is replaced at once"
        );
    }
}

#[tokio::test]
async fn cold_reflector_probes_and_writes_nothing() {
    let backend = FakeBackend::default();
    let mut history = WriteHistory::default();

    reconcile_once(&backend, &config(), &mut history).await;

    assert_eq!(backend.probe_count(), 0);
    assert!(backend.take_writes().is_empty());
}

#[tokio::test]
async fn writes_only_pods_whose_bucket_changed_and_whose_load_is_known() {
    let backend = FakeBackend::with_pods(vec![
        (pod("idle-unannotated", None), FakeLoad::Active(0)),
        (pod("idle-stale", Some("1000")), FakeLoad::Active(0)),
        (pod("busy", None), FakeLoad::Active(42)),
        (pod("steady", Some("10")), FakeLoad::Active(3)),
        (pod("unreachable", Some("1000")), FakeLoad::Fail),
    ]);
    let mut history = WriteHistory::default();

    reconcile_once(&backend, &config(), &mut history).await;

    assert_eq!(backend.probe_count(), 5);
    assert_eq!(
        backend.take_writes(),
        vec![
            write("busy", DeletionCost::Medium),
            write("idle-stale", DeletionCost::Idle),
        ],
        "a failed probe must not lower a busy pod's cost"
    );
}

#[tokio::test(start_paused = true)]
async fn a_pod_whose_load_drops_is_lowered_at_most_once_per_interval() {
    let backend = FakeBackend::with_pods(vec![(pod("dp", None), FakeLoad::Active(500))]);
    let config = config();
    let mut history = WriteHistory::default();

    reconcile_once(&backend, &config, &mut history).await;
    assert_eq!(backend.take_writes(), vec![write("dp", DeletionCost::High)]);

    backend.set_load("dp", FakeLoad::Active(5));
    tokio::time::advance(Duration::from_secs(10)).await;
    reconcile_once(&backend, &config, &mut history).await;
    tokio::time::advance(Duration::from_secs(10)).await;
    reconcile_once(&backend, &config, &mut history).await;
    assert!(
        backend.take_writes().is_empty(),
        "the bucket dropped 10s and 20s after the last write"
    );

    tokio::time::advance(Duration::from_secs(10)).await;
    reconcile_once(&backend, &config, &mut history).await;
    assert_eq!(backend.take_writes(), vec![write("dp", DeletionCost::Low)]);
}

#[tokio::test(start_paused = true)]
async fn a_pod_that_takes_streams_right_after_a_lowering_is_raised_on_the_next_tick() {
    let backend = FakeBackend::with_pods(vec![(pod("dp", Some("1000")), FakeLoad::Active(0))]);
    let config = config();
    let mut history = WriteHistory::default();

    reconcile_once(&backend, &config, &mut history).await;
    assert_eq!(backend.take_writes(), vec![write("dp", DeletionCost::Idle)]);

    backend.set_load("dp", FakeLoad::Active(150));
    tokio::time::advance(config.tick).await;
    reconcile_once(&backend, &config, &mut history).await;
    assert_eq!(
        backend.take_writes(),
        vec![write("dp", DeletionCost::High)],
        "a pod that reads 0 while holding 150 streams is the first a scale-in removes"
    );
}

#[tokio::test(start_paused = true)]
async fn a_failed_lowering_still_counts_against_the_interval() {
    let backend = FakeBackend::with_pods(vec![(pod("dp", Some("1000")), FakeLoad::Active(5))]);
    *backend.fail_writes.lock().unwrap() = true;
    let config = config();
    let mut history = WriteHistory::default();

    reconcile_once(&backend, &config, &mut history).await;
    assert_eq!(backend.take_writes().len(), 1);

    tokio::time::advance(Duration::from_secs(10)).await;
    reconcile_once(&backend, &config, &mut history).await;
    assert!(backend.take_writes().is_empty());

    tokio::time::advance(Duration::from_secs(20)).await;
    reconcile_once(&backend, &config, &mut history).await;
    assert_eq!(backend.take_writes().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_hung_probe_times_out_and_leaves_the_annotation_alone() {
    let backend = FakeBackend::with_pods(vec![
        (pod("hung", Some("1000")), FakeLoad::Hang),
        (pod("idle", Some("100")), FakeLoad::Active(0)),
    ]);
    let mut history = WriteHistory::default();

    reconcile_once(&backend, &config(), &mut history).await;

    assert_eq!(
        backend.take_writes(),
        vec![write("idle", DeletionCost::Idle)]
    );
}

#[tokio::test(start_paused = true)]
async fn an_unreadable_pod_ranks_as_busy_after_a_run_of_failures() {
    let backend = FakeBackend::with_pods(vec![
        (pod("was-idle", Some("0")), FakeLoad::Fail),
        (pod("was-busy", Some("1000")), FakeLoad::Fail),
    ]);
    let config = config();
    let mut history = WriteHistory::default();

    for _ in 1..config.unknown_as_busy_after {
        reconcile_once(&backend, &config, &mut history).await;
        tokio::time::advance(config.tick).await;
    }
    assert!(
        backend.take_writes().is_empty(),
        "a short run of failed probes changes nothing"
    );

    reconcile_once(&backend, &config, &mut history).await;
    assert_eq!(
        backend.take_writes(),
        vec![write("was-idle", DeletionCost::High)],
        "a stale low cost must not put a pod of unknown load first in line; \
         a stale high one is already where unknown belongs"
    );

    for _ in 0..(24 * 60 * 6) {
        tokio::time::advance(config.tick).await;
        reconcile_once(&backend, &config, &mut history).await;
    }
    assert!(
        backend.take_writes().is_empty(),
        "a day more of failures keeps both pods ranked busy"
    );
}

#[tokio::test(start_paused = true)]
async fn a_recovered_probe_ranks_the_pod_by_its_real_load_again() {
    let backend = FakeBackend::with_pods(vec![(pod("dp", Some("0")), FakeLoad::Fail)]);
    let config = config();
    let mut history = WriteHistory::default();

    for _ in 0..config.unknown_as_busy_after {
        reconcile_once(&backend, &config, &mut history).await;
        tokio::time::advance(config.tick).await;
    }
    assert_eq!(backend.take_writes(), vec![write("dp", DeletionCost::High)]);
    assert!(history.failing.contains_key("uid-dp"));

    backend.set_load("dp", FakeLoad::Active(0));
    tokio::time::advance(config.min_write_interval).await;
    reconcile_once(&backend, &config, &mut history).await;
    assert_eq!(backend.take_writes(), vec![write("dp", DeletionCost::Idle)]);
    assert!(
        history.failing.is_empty(),
        "a successful probe ends the run"
    );
}

#[tokio::test(start_paused = true)]
async fn a_write_queued_behind_the_concurrency_limit_is_stamped_when_it_starts() {
    let pods: Vec<_> = (0..9)
        .map(|i| (pod(&format!("dp{i}"), None), FakeLoad::Active(5)))
        .collect();
    let backend = FakeBackend::with_pods(pods);
    *backend.write_delay.lock().unwrap() = Duration::from_secs(5);
    let config = config();
    assert_eq!(config.concurrency, 8);
    let mut history = WriteHistory::default();

    let tick_start = Instant::now();
    reconcile_once(&backend, &config, &mut history).await;

    let mut stamps: Vec<Duration> = history
        .last_write
        .values()
        .map(|at| at.saturating_duration_since(tick_start))
        .collect();
    stamps.sort();
    assert_eq!(stamps.len(), 9);
    assert_eq!(
        stamps.last().copied(),
        Some(Duration::from_secs(5)),
        "the ninth write started after the first eight finished"
    );
}

#[tokio::test(start_paused = true)]
async fn a_long_lived_stream_keeps_its_pod_ranked_busy() {
    let backend = FakeBackend::with_pods(vec![
        (pod("old-streams", None), FakeLoad::Active(500)),
        (pod("new-stream", None), FakeLoad::Active(1)),
        (pod("empty", None), FakeLoad::Active(0)),
    ]);
    let config = config();
    let mut history = WriteHistory::default();

    for _ in 0..(3 * 60 * 6) {
        reconcile_once(&backend, &config, &mut history).await;
        tokio::time::advance(config.tick).await;
    }

    assert_eq!(
        backend.take_writes(),
        vec![
            write("new-stream", DeletionCost::Low),
            write("old-streams", DeletionCost::High),
        ],
        "after three hours the pod holding 500 old streams still ranks last to go"
    );
}

#[tokio::test(start_paused = true)]
async fn a_readiness_flap_keeps_the_write_interval() {
    let backend = FakeBackend::with_pods(vec![(pod("dp", None), FakeLoad::Active(500))]);
    let config = config();
    let mut history = WriteHistory::default();

    reconcile_once(&backend, &config, &mut history).await;
    assert_eq!(backend.take_writes(), vec![write("dp", DeletionCost::High)]);

    backend.set_ready("dp", false);
    tokio::time::advance(Duration::from_secs(10)).await;
    reconcile_once(&backend, &config, &mut history).await;
    backend.set_ready("dp", true);
    backend.set_load("dp", FakeLoad::Active(5));
    tokio::time::advance(Duration::from_secs(10)).await;
    reconcile_once(&backend, &config, &mut history).await;

    assert!(
        backend.take_writes().is_empty(),
        "the pod was written 20s ago; a NotReady spell must not clear that"
    );
}

#[tokio::test(start_paused = true)]
async fn a_replacement_pod_is_not_rate_limited_by_its_predecessor() {
    let backend = FakeBackend::with_pods(vec![(pod("dp", None), FakeLoad::Active(5))]);
    let config = config();
    let mut history = WriteHistory::default();

    reconcile_once(&backend, &config, &mut history).await;
    assert_eq!(backend.take_writes().len(), 1);

    let mut replacement = pod("dp", None);
    replacement.uid = "uid-replacement".to_string();
    backend.set_pods(vec![(replacement, FakeLoad::Active(5))]);
    tokio::time::advance(Duration::from_secs(10)).await;
    reconcile_once(&backend, &config, &mut history).await;

    assert_eq!(
        backend.take_writes(),
        vec![(
            "dp".to_string(),
            "uid-replacement".to_string(),
            DeletionCost::Low
        )]
    );
    assert_eq!(
        history.last_write.keys().collect::<Vec<_>>(),
        vec!["uid-replacement"],
        "the departed pod's entry is pruned"
    );
}

#[tokio::test(start_paused = true)]
async fn run_with_backend_ticks_until_shutdown() {
    let backend = FakeBackend::with_pods(vec![(pod("dp", None), FakeLoad::Active(5))]);
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(run_with_backend(backend, config(), shutdown.clone()));
    tokio::time::advance(Duration::from_secs(1)).await;
    shutdown.cancel();
    task.await.expect("reconciler task");
}

fn kube_pod(ready: bool, terminating: bool, ip: Option<&str>, cost: Option<&str>) -> Pod {
    Pod {
        metadata: ObjectMeta {
            name: Some("space-data-plane-abc".to_string()),
            uid: Some("uid-abc".to_string()),
            deletion_timestamp: terminating
                .then_some(Time(k8s_openapi::jiff::Timestamp::UNIX_EPOCH)),
            annotations: cost.map(|cost| {
                BTreeMap::from([(POD_DELETION_COST_ANNOTATION.to_string(), cost.to_string())])
            }),
            ..ObjectMeta::default()
        },
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "data-plane".to_string(),
                ports: Some(vec![
                    ContainerPort {
                        name: Some("http".to_string()),
                        container_port: 8090,
                        ..ContainerPort::default()
                    },
                    ContainerPort {
                        name: Some("internal".to_string()),
                        container_port: 9191,
                        ..ContainerPort::default()
                    },
                ]),
                ..Container::default()
            }],
            ..PodSpec::default()
        }),
        status: Some(PodStatus {
            pod_ip: ip.map(str::to_string),
            conditions: Some(vec![PodCondition {
                type_: "Ready".to_string(),
                status: if ready { "True" } else { "False" }.to_string(),
                ..PodCondition::default()
            }]),
            ..PodStatus::default()
        }),
    }
}

#[test]
fn candidates_are_ready_live_addressable_pods_with_an_internal_port() {
    assert_eq!(
        candidate_pod(&kube_pod(true, false, Some("10.1.2.3"), Some("100"))),
        Some(DataPlanePod {
            name: "space-data-plane-abc".to_string(),
            uid: "uid-abc".to_string(),
            pod_ip: "10.1.2.3".to_string(),
            port: 9191,
            current: Some("100".to_string()),
        }),
        "the probe port comes from the named container port"
    );
    assert_eq!(
        candidate_pod(&kube_pod(false, false, Some("10.1.2.3"), None)),
        None,
        "not Ready"
    );
    assert_eq!(
        candidate_pod(&kube_pod(true, true, Some("10.1.2.3"), None)),
        None,
        "terminating"
    );
    assert_eq!(
        candidate_pod(&kube_pod(true, false, None, None)),
        None,
        "no pod IP"
    );

    let mut no_listener = kube_pod(true, false, Some("10.1.2.3"), None);
    no_listener.spec.as_mut().unwrap().containers[0]
        .ports
        .as_mut()
        .unwrap()
        .retain(|port| port.name.as_deref() != Some("internal"));
    assert_eq!(
        candidate_pod(&no_listener),
        None,
        "a pod that does not declare the internal listener is not probed"
    );
}

#[test]
fn live_pods_include_not_ready_ones_but_not_terminating_ones() {
    assert_eq!(
        live_uid(&kube_pod(false, false, None, None)).as_deref(),
        Some("uid-abc"),
        "NotReady pods keep their history"
    );
    assert_eq!(
        live_uid(&kube_pod(true, true, Some("10.1.2.3"), None)),
        None
    );
}

#[test]
fn patch_sets_only_the_annotation_and_is_conditional_on_the_uid() {
    assert_eq!(
        deletion_cost_patch("uid-abc", DeletionCost::High),
        serde_json::json!({
            "metadata": {
                "uid": "uid-abc",
                "annotations": {
                    "controller.kubernetes.io/pod-deletion-cost": "1000",
                },
            }
        })
    );
}

#[test]
fn config_defaults_to_disabled_against_the_space_data_plane_pods() {
    let config = ReconcilerConfig::from_environment(&pplx_space_test_env::TestEnv::default());
    assert_eq!(config, ReconcilerConfig::default());
    assert!(!config.enabled);
    assert_eq!(config.namespace, "space");
    assert_eq!(
        config.pod_selector,
        "app.kubernetes.io/name=space-data-plane"
    );
    assert_eq!(config.min_write_interval, Duration::from_secs(30));
}

#[test]
fn config_reads_overrides_and_ignores_blank_values() {
    let mut env = pplx_space_test_env::TestEnv::default();
    env.set(ENABLED_ENV, "true");
    env.set("POD_NAMESPACE", "controller-ns");
    env.set(POD_SELECTOR_ENV, "app=dp");
    let config = ReconcilerConfig::from_environment(&env);
    assert!(config.enabled);
    assert_eq!(
        config.namespace, "controller-ns",
        "POD_NAMESPACE is the fallback"
    );
    assert_eq!(config.pod_selector, "app=dp");

    env.set(NAMESPACE_ENV, "dp-ns");
    let config = ReconcilerConfig::from_environment(&env);
    assert_eq!(config.namespace, "dp-ns", "an explicit namespace wins");

    env.set(NAMESPACE_ENV, "  ");
    let config = ReconcilerConfig::from_environment(&env);
    assert_eq!(
        config.namespace, "controller-ns",
        "a blank override is unset"
    );
}

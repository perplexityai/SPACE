# Configuration reference

[Documentation](../README.md)

Every `SPACE_*` environment variable listed here is a supported operator
setting. Variables that are not listed are not read by release binaries.
"Unset" means the built-in default applies. Variables marked **required** make
the service fail at startup when missing.

Settings that exist only so tests can shorten a timer or flip a scenario are
test hooks. They are read only by binaries built with the `test-hooks` Cargo
feature (`pplx-space-common-core/test-hooks`), which the local kind image
enables and release images never do. They are intentionally undocumented, and
`scripts/source_guards/env_test_hook_boundary.py` fails CI if code names a
variable listed here in a test-hook read, directly or through a string constant
from the same crate.

Secrets are passed as files: set the `*_PATH` variable to a mounted file. The
inline forms (`SPACE_ECW_CREDENTIAL_KEY`, `SPACE_ECW_CREDENTIAL_PREVIOUS_KEY`,
`SPACE_API_ROUTE_ED25519_SIGNING_KEY`, `SPACE_API_ROUTE_ED25519_VERIFICATION_BUNDLE`)
and `SPACE_CLUSTER_NODE_API_ENDPOINT` are deprecated and still read for one
release so directly provisioned hosts can upgrade in place.

## Reconcilers and kill switches

Each mutating loop has one `*_ENABLED` switch. Loops that are on by default
treat any value other than a truthy one as off, so a typo stops the loop
instead of leaving it running. The one exception is
`SPACE_PUBLIC_OPERATION_DISPATCHER_ENABLED`, which keeps the lenient parser
because disabling it by mistake would leave every create queued forever.

| Variable | Default | Notes |
| --- | --- | --- |
| `SPACE_FAILED_RESTORABLE_REPAIR_ENABLED` | on | Reverts failed restorable sandboxes. |
| `SPACE_STARTED_SANDBOX_REPAIR_ENABLED` | on | |
| `SPACE_FAILED_SANDBOX_CLEANUP_ENABLED` | on | Part of the transient lifecycle sweeper. |
| `SPACE_INTERNAL_API_DELETED_PARTITION_ENABLED` | on | |
| `SPACE_INTERNAL_API_DELETED_SUPPRESS_ABSOLUTE` | 500 | |
| `SPACE_KUBE_NODE_SYNC_ENABLED` | on | Also records the Kubernetes UID of each node running a node-api pod, builders included. Off, only an admin node listing (node-status refresh, reconcile or template listing) records a new or replaced builder's UID; until then the credential-manager authority refuses its callers and tenant template publication from it waits. Turning it off doesn't erase a recorded UID. |
| `SPACE_OBSERVED_PROJECTION_GC_ENABLED` | on | |
| `SPACE_PUBLIC_OPERATION_DISPATCHER_ENABLED` | on | Without it, create requests are accepted and never complete. |
| `SPACE_SNAPSHOT_STORAGE_SWEEPER_ENABLED` | on | Does nothing without `SPACE_SNAPSHOT_BUCKET`. |
| `SPACE_ORPHANED_SANDBOX_RECONCILER_ENABLED` | on | |
| `SPACE_API_RECONCILER_DRY_RUN` | true | The orphaned-sandbox reconciler classifies and logs only until set to false. |
| `SPACE_API_RECONCILER_MAX_CLASSIFICATIONS_PER_MINUTE` | 60 | A larger value clamps to 300; `0` or a non-number keeps the default. |
| `SPACE_API_RECONCILER_MAX_ACTIVE_QUARANTINES`, `SPACE_API_RECONCILER_MAX_NEW_QUARANTINES_PER_SWEEP`, `SPACE_API_RECONCILER_MAX_QUARANTINE_PERCENTAGE` | 50, 20, 25 | Safety ceilings; `0` stops new quarantines. A larger value clamps to 500, 200 and 75 respectively. |
| `SPACE_GONE_NODE_QUEUE_GC_ENABLED` | on | |
| `SPACE_GONE_NODE_QUEUE_GC_DRY_RUN` | true | |
| `SPACE_TRANSIENT_LIFECYCLE_SWEEPER_ENABLED` | off | |
| `SPACE_TRANSIENT_LIFECYCLE_SWEEPER_TICK_SECS` | 60 | |
| `SPACE_TRANSIENT_LIFECYCLE_SWEEPER_DEFAULT_AGE_SECS` | 600 | Age before a row in a transient lifecycle state is retired, unless a per-state age below applies. |
| `SPACE_TRANSIENT_LIFECYCLE_SWEEPER_CREATING_AGE_SECS`, `SPACE_TRANSIENT_LIFECYCLE_SWEEPER_RESTORING_AGE_SECS`, `SPACE_TRANSIENT_LIFECYCLE_SWEEPER_SUSPENDING_AGE_SECS`, `SPACE_TRANSIENT_LIFECYCLE_SWEEPER_CHECKPOINTING_AGE_SECS` | default age | Per-state override; `0` disables that state. A `creating` row is left alone while its create's 300 s activation lease is live and for one more `creating` age after it expires; a create still running past both is failed, unless its VM is alive and its pod became routable, which its node's activation-completion receipt finishes. The default 600 s is comfortably past a live create's remaining work. A longer age only delays the cleanup: the sweep acts by the lease plus the age plus a sweep tick after the crash. A `restoring` row likewise waits one `restoring` age past the later of its restore lease and the activation lease of the wake driving it (300 s). |
| `SPACE_ABANDONED_RESTORE_RETIREMENT_ENABLED` | off | API and cluster controller: whether a failed restore's rollback (API) and the transient-lifecycle sweeper's revert of an aged `restoring` row retire the abandoned restore's execution and delete its pod. Off, both revert only the lifecycle and the sweeper's cleanup refuses a restore pod that came up after its check. Turn it on only once every API and internal-API replica refuses to route to a `restoring` sandbox, and turn it off before rolling back to replicas that don't. `1`, `true`, `yes`, `on` or `enabled` turns it on; anything else keeps it off. |
| `SPACE_STUCK_SNAPSHOT_REAPER_ENABLED` | off | |
| `SPACE_SUSPENDED_EXECUTION_REAPER_ENABLED` | off | Runs dry by default. |
| `SPACE_STUCK_CONTAINER_CREATING_GC_ENABLED` | off | |
| `SPACE_STUCK_CONTAINER_CREATING_GC_MAX_DELETES_PER_MINUTE` | built-in | |
| `SPACE_WARM_POOL_ORPHAN_GC_ENABLED` | off | Runs dry by default. |
| `SPACE_ORPHANED_RESTORE_POD_GC_ENABLED` | off | |
| `SPACE_ORPHANED_RESTORE_POD_GC_DRY_RUN` | true | Classifies and reports orphaned restore pods without deleting them. Only `0`, `false`, `False` or `FALSE` turns it off; any other value keeps it on. |
| `SPACE_ORPHANED_RESTORE_POD_GC_INTERVAL_SECS`, `SPACE_ORPHANED_RESTORE_POD_GC_TICK_BUDGET_SECS` | 60, 45 | Tick cadence and per-tick time budget. A budget at or above the interval lets ticks run back to back. Seconds; a value above one day clamps to it, and `0` or a non-number keeps the default. |
| `SPACE_ORPHANED_RESTORE_POD_GC_THRESHOLD_SECS` | 300 | How long a pod must stay superseded before it's deleted. Seconds; a value above one day clamps to it, and `0` or a non-number keeps the default. |
| `SPACE_ORPHANED_RESTORE_POD_GC_MAX_DELETES_PER_MINUTE` | 60 | Orphans acted on per rolling minute. A larger value clamps to 600; `0` or a non-number keeps the default. To stop deletions, use `DRY_RUN` or `ENABLED`. |
| `SPACE_ORPHANED_RESTORE_POD_GC_BATCH_SIZE` | 500 | Sandbox rows looked up per query. A larger value clamps to 5000; `0` or a non-number keeps the default. |
| `SPACE_ORPHANED_RESTORE_POD_GC_TEARDOWN_STUCK_AFTER_SECS` | 300 | How long a pod may stay terminating before it's reported as a stuck teardown. Seconds; a value above one day clamps to it, and `0` or a non-number keeps the default. |
| `SPACE_RECONCILE_QUEUE_GC_ENABLED` | off | Needs AWS: it calls EC2 `DescribeInstances`, at `AWS_ENDPOINT_URL_EC2` when that is set. |
| `SPACE_ORPHANED_NODES_ENABLED` | off | Needs AWS. |
| `SPACE_LOST_STARTED_SANDBOXES_ENABLED` | off | Marks a `started` sandbox `failed`, with `sandbox_lost` and `sandbox_failed` events, once its started recovery failed with no snapshot that restores it without a rollback and its node is gone. Gone means the node row is deleted, or all three of these agree: the Kubernetes Node has been absent from the controller's reflector for at least 10 minutes, EC2 has no live instance under the Node's name, and a direct Node GET returns NotFound. EC2 is looked up by private DNS name, so on a cluster whose Node names aren't EC2 private DNS names it finds nothing and only the two Kubernetes checks protect a live node. Wakes on a sandbox marked lost get the terminal-lifecycle rejection instead of retrying recovery. Needs AWS for the EC2 check. A registered node in any drain phase is left to the drain. |
| `SPACE_NODE_DRAIN_ENABLED` | off | |
| `SPACE_NODE_DRAIN_NODE_CENSUS_RESIDENCY` | off | |
| `SPACE_NODE_DRAIN_TERMINATION_BLOCKING_GATE` | off | Parks nodes that still hold stopped, created, deleting or quarantined residents in `draining` instead of advancing them to `drained`. |
| `SPACE_NODE_DRAIN_EVACUATE_STOPPED_RESIDENTS` | off | On a forced drain, publishes a resident `stopped` sandbox's disk to S3, waits for it to be durable, then releases the node (the node-local copy goes when the node terminates) and hands the sandbox to the restore sweep. Has no effect unless `SPACE_NODE_DRAIN_TERMINATION_BLOCKING_GATE` is also on. Turning it off stops new evacuations; ones already claimed, including a claim an `:undrain` left behind, still proceed when a forced drain next reaches them. The restore sweep cannot yet re-place a `stopped` sandbox, so with this on an evacuated sandbox answers `start` with a retryable 503 until it can. |
| `SPACE_NODE_DRAIN_TERMINATOR_KIND` | `aws` | `aws`, `kube-delete` or `noop`. |
| `SPACE_DRAINED_NODE_TERMINATOR_ENABLED` | off | |
| `SPACE_CLUSTER_CONTROLLER_DRAIN_EVACUATION_COHORT_VALUE` | unset | Unset disables the rollout-cohort feature. |
| `SPACE_NODE_GROUP_LABELER_ENABLED`, `SPACE_NODE_GROUP_MIRROR_ENABLED` | off | |
| `SPACE_NODE_SERVICE_READINESS_ENABLED` | off | |
| `SPACE_SNAPSHOT_RETENTION_SWEEPER_ENABLED` | off | |
| `SPACE_SNAPSHOT_RETENTION_SWEEPER_DRY_RUN` | off | When on, the sweeper scans and reports what it would prune, but deletes nothing. Set it with `ENABLED` to observe a new environment before pruning. Accepts `true`/`1`/`yes`/`on` and their negations, ignoring case and surrounding spaces; any other value, empty included, keeps dry run on and logs a warning. |
| `SPACE_SNAPSHOT_RETENTION_SWEEPER_KEEP_N`, `SPACE_SNAPSHOT_RETENTION_SWEEPER_MAX_PER_TICK` | built-in | |
| `SPACE_ADMIN_AUDIT_RETENTION_GRACE_SECS` | 30 days | The retention loops themselves always run. |
| `SPACE_ADMIN_AUDIT_RETENTION_MAX_RECORDS` | 100,000 | Row cap applied regardless of age; raise it to keep more history (ceiling 10,000,000). |
| `SPACE_TEMPLATE_PUBLISH_RESERVED_NAMES` | empty | Extra template names reserved for operator publication, in addition to `seed` and `code-runtime`. |

## Sandbox placement and nodes

| Variable | Default | Notes |
| --- | --- | --- |
| `SPACE_NODE_ROLE` | `worker` | |
| `SPACE_NODE_ID` | file, then generated | |
| `SPACE_NODE_MANAGED_BY` | unset | |
| `SPACE_K8S_NODE_NAME`, `SPACE_K8S_CONTROL_PLANE` | unset / off | |
| `SPACE_ALLOW_SANDBOX_PLACEMENT` | off | |
| `SPACE_SANDBOX_NODE_GROUP_PLACEMENT` | off | |
| `SPACE_SANDBOX_RESTORE_SOURCE_NODE_HARD_PIN` | off | Pin a restore to the snapshot's source node with a required node-affinity term. |
| `SPACE_SANDBOX_NODE_SERVICES_READY_AFFINITY` | off | Prefer nodes whose node-local services are ready; useful only where `SPACE_NODE_SERVICE_READINESS_ENABLED` is on. |
| `SPACE_SANDBOX_NODE_SELECTOR_KEY`, `SPACE_SANDBOX_NODE_SELECTOR_VALUE` | `node.kubernetes.io/space-enabled`, `true` | Base node selector for sandbox pods and for the node-sync, node-group mirror and warm-pool loops; set both together. |
| `SPACE_SANDBOX_NODE_SELECTOR_LABELS` | empty | |
| `SPACE_SANDBOX_NAMESPACE` | `POD_NAMESPACE`, then `default` | Namespace of the sandbox pods. When it is not the node services' namespace, also set `SPACE_NODE_API_NAMESPACE`: without it and `POD_NAMESPACE`, the API looks for node-api pods here. |
| `SPACE_SANDBOX_POD_IMAGE` | `registry.k8s.io/pause:3.10` | |
| `SPACE_SANDBOX_FORK_ENABLED` | on | |
| `SPACE_SANDBOX_FORK_HARD_DEADLINE_SECS` | 240 | Positive integer seconds. |
| `SPACE_NODEMGR_NETWORK_POOL_MAX_SLOTS` | 300 | |
| `SPACE_NODEMGR_NETWORK_POOL_TARGET`, `SPACE_NODEMGR_NETWORK_POOL_RETAIN_READY`, `SPACE_NODEMGR_NETWORK_POOL_MIN_READY` | `min(max_slots, 100)`, `max_slots`, `min(max_slots, 32)` | Warm network slot pool sizes; set target and retained to `0` to keep the pool off. The pool only serves allocations that use the pool's own network namespace. |
| `SPACE_NODEMGR_SERVICE_PORT_INGRESS_ISOLATION` | auto | `shared_host` on Kubernetes, else `exclusive_host`. |
| `SPACE_NODEMGR_ACTIVATE_RUNTIME_SUPERVISOR_ON_START` | off | |
| `SPACE_RTMGR_INSTALL_KERNELS_DIR` | unset | Enables runtime-manager kernel installs in this versioned kernel root, which must exist on the state directory's filesystem. Unset, installs and uninstalls are refused. |
| `SPACE_NODEMGR_BTRFS_BALANCE_ENABLED` | on | |
| `SPACE_NODEMGR_DISK_IO_RESERVATION_ENABLED` | off | |
| `SPACE_NODEMGR_DISK_IO_MAX_OPERATIONS_PER_SEC` | unset | Operator ceiling for each Firecracker root drive. Set a positive integer (for example `1000`) on node-manager and restart it. Applies to new boots, restores and running VMs, independently of saved assignments. See [guest disk I/O](space/design/guest-io.md#operator-ceiling). |
| `SPACE_DISK_IO_SAFETY_MAX_OPERATIONS_PER_SEC`, `SPACE_DISK_IO_SAFETY_MAX_BANDWIDTH_MIB_PER_SEC` | unset | Set both or neither. Caps explicit warm-pool policy limits; unset, the cap is the platform default of 10,000 operations/s and 100 MiB/s. A value below the default leaves that dimension at the default. |
| `SPACE_NODE_DISK_ADMISSION_MIN_AVAILABLE_BYTES` | 50 GiB | Hard free-space floor. Lower it on small disks. |
| `SPACE_BUILDER_BLOB_MIN_AVAILABLE_BYTES` | 50 GiB | |
| `SPACE_NODE_API_UDS_PER_SANDBOX` | 64 | Per-sandbox node-api permits shared by streams and unary calls, `1..=2048` (the node-wide permit count). Process kill, signal and listing have 16 further permits of their own, which this setting does not change. Sandbox stop, kill and delete take no per-sandbox permit. |
| `SPACE_INTERNAL_API_VISIBILITY_CACHE_TTL_SECS`, `SPACE_INTERNAL_API_VISIBILITY_CACHE_STALE_CEILING_SECS` | built-in | Template visibility cache freshness and stale ceiling. |
| `SPACE_INTERNAL_API_RUNTIME_DESIRED_STATE_ENABLED` | off | Whether `GetNodeRuntimeDesiredState` serves the cluster execution environment policy. Off, it returns no policy, so nodes have no cluster restrictions to apply on top of `SPACE_ALLOWED_EXECUTION_ENVIRONMENTS` and `SPACE_DEFAULT_EXECUTION_ENVIRONMENT`. Read per replica at startup, which logs the value it read and its decision; an unrecognized value reads as off. See "Cluster desired policy" in [execution-environments.md](execution-environments.md). |
| `SPACE_SBXMGR_ACTIVATION_ADMISSION_MAX_CONCURRENT` | 5 | |
| `SPACE_SBX_EVICT_ENABLED`, `SPACE_SBX_EVICT_ENGAGE_FRACTION`, `SPACE_SBX_EVICT_TARGET_FRACTION`, `SPACE_SBX_EVICT_MIN_IDLE_SECS` | off, 0.75, 0.70, 25 h | |
| `SPACE_SBX_EVICT_INTERVAL_SECS`, `SPACE_SBX_EVICT_MAX_PER_TICK`, `SPACE_SBX_EVICT_REUPLOAD_MAX_PER_TICK` | 600, 100, 5 | |
| `SPACE_SBXMGR_NETWORK_CREDENTIALS_ENABLED` | off | |
| `SPACE_SNAPSHOT_KEY_PREFIX` | empty | Object key prefix for suspend snapshots; keeps clusters apart in a shared bucket. |
| `SPACE_CREDMGR_TOKEN_PATH` | `/var/run/space-auth/credmgr.token` | When set, the file is authoritative and an unreadable file is an error. |
| `SPACE_SBX_GC_ENABLED`, `SPACE_SBX_GC_ENGAGE_FRACTION` | off, 0.50 | |
| `SPACE_UDS_PEER_PROCESS_CHECK` | off | The peer uid check always applies. |
| `SPACE_ENABLE_GUEST_CONSOLE_LOGS` | off | Debug: attach the guest serial console and write `guest-console.log` per sandbox (node manager and sandbox manager). |
| `SPACE_FIRECRACKER_VENDOR_DIR`, `SPACE_FIRECRACKER_ASSET_DIR`, `SPACE_FIRECRACKER_KERNEL_ASSET_DIR` | built-in paths | Node-manager runtime asset locations. |
| `SPACE_SUSPEND_WAKE_ARBITRATION` | on | Kill switch: off lets a suspend that queued behind a concurrent wake through instead of rejecting it as superseded. |
| `SPACE_SUSPEND_DEFAULT_FORMAT` | `btrfs-send` | Set `full` to roll the suspend codec default back. |
| `SPACE_CLUSTER_CONTROLLER_LEADER_ELECTION_ENABLED` | on | Kill switch; off runs the reconcilers without a lease, which is only safe with one replica. |

## API, admission and limits

| Variable | Default | Notes |
| --- | --- | --- |
| `SPACE_API_CLIENT_CONCURRENCY_MODE` | `observe` | `off`, `observe` or `enforce`. |
| `SPACE_API_CLIENT_CONCURRENCY_MAX_PER_CLIENT` | 32 | |
| `SPACE_API_CLIENT_CONCURRENCY_KNOWN_CLIENTS` | empty | |
| `SPACE_API_CLIENT_CLASSES` | empty | JSON map; invalid JSON fails startup. |
| `SPACE_CLUSTER_ACTIVATION_ADMISSION_ENABLED` | on | Fails open (with a warning) when `SPACE_REDIS_URL` is unset. |
| `SPACE_CLUSTER_ACTIVATION_ADMISSION_REFILL_RPS` | 60 | |
| `SPACE_PUBLIC_OPERATION_DISPATCHER_EXECUTION_CONCURRENCY` | 16 | Operations the dispatcher runs at once. A larger value clamps to 128; `0` or a non-number keeps the default without a warning. An execution holds its slot while it works. A wake whose restore claim stays fenced by a stale owner retries in place for 2 s, then hands the slot back and is requeued instead (see the retry deferral switch below), so the cap counts active work. Among the operations the dispatcher claims, at most one per sandbox executes under a live claim at a time, across every controller replica, in request order; a stale claim on a sandbox is reclaimed before anything else there starts, and a delete waits for one running under a live claim, an earlier create, or the sandbox's earliest operation while that one is due, but not for other earlier work waiting out a retry backoff. Requesting a delete cancels the sandbox's earlier creates that are queued or waiting out a backoff, so the delete waits only for a create it could not cancel: one whose attempt was running, or whose claim had gone stale, when the delete arrived. That attempt finishes, and if it would retry, or the dispatcher hands the create back unrun, the create ends canceled instead, so a create never runs after its sandbox's delete. A create reclaimed while its earlier execution still runs in the same dispatcher is the exception: it is handed back unchanged, and the delete waits for its next attempt. Lifecycle paths outside the dispatcher (ECW pause, resume and connect, and reconcilers) are not serialized by this. Freed slots refill on the next 25 ms claim tick, at most 64 per tick. So throughput is roughly this divided by mean execution time, up to 2,560 operations a second. Executions draw on the cluster controller's `space_user` Postgres pool, which most other reconcilers share (`POSTGRES_POOL_MAX_CONNECTIONS`, default 25). Raising this adds contention for that pool, and an execution whose claim renewal or final status write can't get a connection within the pool's acquire timeout is abandoned and runs again once its claim goes stale, repeating any work it had already done. |
| `SPACE_PUBLIC_OPERATION_DISPATCHER_TEARDOWN_RESERVED_SLOTS` | 2 | Of the execution slots above, how many only teardowns may use: sandbox deletes, including ECW kills, and Native `:kill`. Deletes and kills share this one reservation; neither has slots of its own. A teardown can then start while other work holds every other slot. Other work never takes a reserved slot, even while no teardown is waiting, because a teardown arriving later would otherwise wait for long-running work to finish. A teardown may also use any other free slot. At least one slot always stays open to other work, so the reservation is at most the slot count minus one. A value above 127 clamps to it, `0` turns the reservation off, and a non-number keeps the default. A delete still waits for an operation running on its sandbox and for an earlier create it could not cancel. While only reserved slots are free, a claim that finds no teardown to start makes the dispatcher wait 250 ms before claiming again, unless an execution finishes first, so a teardown requested then waits up to that long. |
| `SPACE_PUBLIC_OPERATION_DISPATCHER_OTHER_RESERVED_SLOTS` | 2 | While other work is waiting, teardowns leave free the last this-many free slots that other work may use, however many operations of either class are already running, so other work can start while teardowns hold every other slot. The slots kept are free ones, not a share of the pool: the teardown reservation is never among them. While no other work is waiting, teardowns may use these too; they finish quickly, so other work that arrives later waits only briefly. It never takes the slots owed to teardowns: what is left of their reservation or, with the reservation off, one free slot while no teardown is running, so other work cannot shut teardowns out. So it is at most the slot count minus the teardown reservation, or minus one with the reservation off. A value above 128 clamps to it, `0` turns it off, and a non-number keeps the default. |
| `SPACE_PUBLIC_OPERATION_DISPATCHER_ORG_SLOT_LIMIT` | 8 | The most execution slots one organization's operations other than teardowns may hold at once while another organization has such work waiting, counted across every controller replica. An organization is waiting while it has such work due and holds fewer slots than this, so organizations already at the limit do not hold each other to it, so one organization's slow or stuck work leaves slots for the others. Teardowns are not limited and do not count. Neither do operations the API runs itself as it creates them, such as a native create that claims a warm-pool sandbox at admission: they hold no dispatcher slot, so the limit bounds the slots an organization holds, not all of its concurrent work. While no other organization is waiting, the organization may go past it, up to the headroom below. A value above 128 clamps to it, `0` turns the limit off, and a non-number keeps the default. Operations past the limit wait in the queue. After a claim takes nothing because this limit holds back every due operation other than a teardown, which with organizations past their limit happens only while no more than the headroom below is free, the dispatcher waits 250 ms before claiming again, unless an execution finishes first, so work another organization submits meanwhile waits up to that long. Work held back only by earlier work on its sandbox, or by a row another transaction holds locked, does not slow the claims. |
| `SPACE_PUBLIC_OPERATION_DISPATCHER_ORG_SLOT_HEADROOM` | 2 | How many of the slots open to other work organizations past their limit leave free while no other organization is waiting, so another organization's work that arrives later can start without waiting for theirs to end. The slots left are free ones, counted at each claim, so slots other organizations already hold do not count toward it, and organizations past their limit at once share what is left: alone on an idle dispatcher, an organization still gets every slot open to other work but these, and two organizations both at their limit together get every free slot but these. `0` lets one organization use every such slot; a value at least the slot count minus the limit keeps the limit strict. A value above 128 clamps to it, and a non-number keeps the default. |
| `SPACE_PUBLIC_OPERATION_DISPATCHER_KEYED_CLAIM_ENABLED` | on | Kill switch for per-sandbox serialization. `0`, `false`, `False` or `FALSE` turns it off; `1`, `true`, `True` or `TRUE` keeps it on; any other value, including `off`, keeps the default (on). Off restores the unkeyed claim, in which nothing holds a delete back behind a create's running attempt: the delete can finish first, and the create, finding no active delete when its attempt ends, retries. A delete that finds no sandbox row is the exception: while a create of the sandbox is queued or running, the delete's execution waits for it instead of finishing, then deletes the sandbox if that create recorded one. Independent of retry deferral. |
| `SPACE_PUBLIC_OPERATION_DISPATCHER_RETRY_DEFERRAL_ENABLED` | on | Kill switch for retry deferral, with the same spellings as the keyed switch. Off keeps every retry wait inside the execution's slot. On, an execution blocked on a fenced restore claim keeps retrying in its slot for 2 s, then is requeued without spending an attempt, after the executor's hint (5 s) plus an equal-jittered eighth of the time since its first claim; that jittered term is capped at 60 s, so the longest delay is 65 s. Kinds with an age deadline keep it. Every deferring operation is also bounded by a 36-minute window of wall-clock time from its first claim, so dispatcher downtime, backpressure requeues and earlier counted retries count toward it; past it, the operation fails with its `retry_attempts_exhausted` error, which then also carries `deferred_retry_window_ms` and `elapsed_since_first_claim_ms`. Independent of the keyed claim. |
| `SPACE_NATIVE_ORG_SANDBOX_LIMIT` | unset | Most live sandboxes one organization may hold when `POST /v1/sandboxes` admits a new create; unset or empty means no limit, and `0` fails startup. Counts every sandbox of the org, whoever owns it and whatever API created it, that is not `failed`, `quarantined`, `deleted` or `cleanup_required`, plus the org's native creates still in flight without such a live row. A create at the limit gets `429` `quota_exceeded` with `retryable: false`, no `Retry-After`, and the message `organization sandbox limit of N reached`. A delay does not lift it: it lifts once a delete completes (a `deleting` sandbox still counts) or an in-flight create ends. Only native creates are checked; ECW, fork and admin creates are not limited, though their sandboxes count. Creates are serialized per org within twice a headroom below the limit (the headroom is a tenth of the limit, at least 64): a limit of 128 or less is exact, and above that the count exceeds the limit only if more than the headroom's worth of creates are admitted at the same moment, by at most that excess. A serialized create waits at most 2 s (or the application role's shorter `lock_timeout`) for each lock it takes, holding a database connection meanwhile, so a federated caller with both an org and a per-user limit can wait about 4 s; a longer wait, or a create whose count moves into the headroom while it waits, answers the retryable `503` `service_unavailable`. A non-integer or negative value fails startup. |
| `SPACE_NATIVE_ORG_SANDBOX_LIMIT_OVERRIDES` | unset | JSON object mapping organization IDs to their own limit, which takes precedence over `SPACE_NATIVE_ORG_SANDBOX_LIMIT` (for example `{"0190d5a4-2b4e-7c3e-8f00-000000000001": 50}`). Applies even when that variable is unset. An override of `0` refuses every native create of that org. Invalid JSON, a key that is not a UUID, a negative or non-integer value, or an organization listed twice fails startup. |
| `SPACE_NATIVE_SUBJECT_SANDBOX_LIMIT` | unset | Most live sandboxes each federated-identity subject may hold, counted the same way over the sandboxes it owns and the creates it requested, in addition to its organization's limit, and serialized the same way per subject. Local callers are not limited by it. A create at the limit gets the same `429` `quota_exceeded` with the message `sandbox limit of N per user reached`. `0`, a negative or a non-integer value fails startup. |
| `SPACE_NATIVE_CREATE_POOL_CLAIM_ENABLED` | off | Env master for claiming a ready warm-pool slot when `POST /v1/sandboxes` admits a create, under the `space-native-create-pool-claim` flag (falls back off). Every `space-cluster-controller` replica must understand claim-carrying create payloads before this is turned on. |
| `SPACE_IMPLICIT_WAKE_ACTIVITY_ENABLED` | off | API: whether the wake the system queues for a session records the session's activity (`command`, or `access` for a reattach to a process that already started or a file watch), so the cluster controller judges the sandbox's `auto_lifecycle.wake.on` by that activity when the wake runs. Off, every such wake runs by the command rule, as before this switch. Turn it on only once every `space-cluster-controller` replica runs a release that reads the activity, since older ones reject the wake's payload. Before rolling a controller back, turn it off and let the wakes queued with it finish. `1`, `true`, `yes`, `on` or `enabled` turns it on; anything else keeps it off. |
| `SPACE_NATIVE_HIDDEN_OPERATIONS_ORGS` | empty | Orgs that may use the native operations hidden by default, the ones `/openapi-full.json` marks `x-space-m1-status: incubating-hidden`: `*` for every org, or comma-separated SPACE org ids. `*` must stand alone, and an invalid entry or a value that is not valid Unicode fails startup. The org is the request's effective org, the one that owns the data. For an integration key that asserts a federated owner, that is the org SPACE materialized for the owner: its own id, listed by the admin API's `GET /admin/api/v1/orgs` as `Federated <issuer> org <upstream org id>` once the owner has made a request, not the upstream org id the integrator sends, and not the key's own org. For every other org a hidden route answers `404 not_found` and a hidden lifecycle action answers `400 invalid sandbox action`, exactly as a route or action that does not exist, unless the `space-native-hidden-operations` flag opens them. `/openapi.json` omits hidden operations for every caller; `/openapi-full.json` documents them and requires a native credential, which need not resolve an owner. |
| `SPACE_NATIVE_HIDDEN_OPERATIONS_FLAG_ENABLED` | on | Env master for the `space-native-hidden-operations` flag (subject: SPACE org id, attribute `operation_id`, falls back off). `off` (or `false`, `0`, `no`, `disabled`) closes every org the flag opened; orgs listed in `SPACE_NATIVE_HIDDEN_OPERATIONS_ORGS` stay open. Empty, `on`, `true`, `1`, `yes` or `enabled` keeps it on; any other value fails startup. |
| `SPACE_NATIVE_CREATE_INLINE_ENABLED` | on | Kill switch for running a native create that won an admission-time warm-pool claim inside the `POST /v1/sandboxes` request instead of leaving it queued for the dispatcher. Read only while admission-time claiming is on; off keeps those creates queued. The API then runs the create executor itself, so it needs the same create settings as `space-cluster-controller` (for example `SPACE_BYOK_VOLUME_ENCRYPTION`). |
| `SPACE_NATIVE_CREATE_INLINE_WAIT_MS` | `2000` | How long the create request waits for that inline execution before answering. A create that finishes within the wait answers with its terminal record in the response's `operation` field, so the client need not poll. Past the wait the request answers without it and the execution continues in the background; a malformed value uses the default, and values above 30000 are capped. |
| `SPACE_REDIS_URL` | unset | |
| `SPACE_SHARED_VALKEY_URL` | unset | Unset disables the consumer. |
| `SPACE_GRPC_CLIENT_MAX_CONNECTION_AGE_SECS` | off | |
| `SPACE_CLUSTER_API_ENDPOINT_DISCOVERY` | `off` | `off` or `kubernetes` (EndpointSlice per-RPC balancing for cluster-api clients; rollout gate). |
| `SPACE_PUBLIC_CURSOR_SECRET` | **required** | |
| `SPACE_DATA_PLANE_PUBLIC_URL` | empty | Empty yields relative URLs. |
| `SPACE_DATA_PLANE_INTERNAL_URL` | unset | `http://` origin of the data plane as the API reaches it. Enables streaming command creation (`Accept: text/event-stream`); unset answers those requests with the JSON handle. Streams relayed this way end when the API shuts down. |
| `SPACE_FEDERATED_GRANT_AUDIENCE`, `SPACE_FEDERATED_IDENTITY_ISSUER`, `SPACE_FEDERATED_REJECTED_SUBJECTS` | `credential-provider`, `space`, `[]` | |
| `SPACE_E2B_HOME_WORKDIR_TEMPLATES`, `SPACE_ECW_HOSTS` | `code-runtime`, none | E2B routes return 404 without hosts. |
| `SPACE_CLUSTER_ID` | unset | Required for route signing. |
| `SPACE_CREATE_COMMAND_READY_TIMEOUT_SECS`, `SPACE_CREATE_DATA_PLANE_READY_TIMEOUT_SECS` | built-in | Create-path waits for command and data-plane readiness. |
| `SPACE_DATA_PLANE_UPLOAD_ADMISSION` | off | Data plane: admit ECW `POST /files` uploads under the per-replica, per-org and per-sandbox limits below; rejected uploads get 429 with a jittered `Retry-After`. Accepts `true`/`1`/`yes`/`on` and their negations. Uploads to a sandbox whose route has no org id are refused with 500 while it's on, since admission is charged to the org. |
| `SPACE_DATA_PLANE_MAX_CONCURRENT_UPLOADS`, `SPACE_DATA_PLANE_MAX_IN_FLIGHT_UPLOAD_BYTES` | 64, 1 GiB | Per-replica upload slots and byte budget. Each upload reserves its declared `Content-Length` rounded up to a KiB, or its path's body limit (256 MiB, or 1 MiB for JSON) when none is declared, so the 1 GiB default fits four undeclared-length uploads at a time, from four different orgs at the default per-org share. A reservation is capped at the whole budget: a budget smaller than one upload admits uploads one at a time and is exceeded by that upload. Both must be non-zero. |
| `SPACE_DATA_PLANE_UPLOAD_ADMISSION_WAIT_MS` | 2000 | How long an HTTP/1.x upload waits for every slot and reservation, its sandbox's, its org's and the replica's, before the 429. HTTP/2 uploads don't wait: an HTTP/2 connection's streams share its flow control, so an upload left unread would hold back the others. They take what's free at once or get the 429. |
| `SPACE_DATA_PLANE_MAX_CONCURRENT_UPLOADS_PER_ORG`, `SPACE_DATA_PLANE_MAX_IN_FLIGHT_UPLOAD_BYTES_PER_ORG` | 16, 256 MiB | Each org's share of a replica's upload slots and byte budget, a quarter of the defaults above, so one org can't take the replica. Keyed by the sandbox's org, not the client. Reservations are made and capped as for the replica's budget, so an undeclared-length upload fills the default byte share, and every other upload from the org on the replica that reserves any bytes waits for it and gets 429 if the admission wait runs out first, or at once over HTTP/2. Uploads that declare `Content-Length` reserve only that, so several fit at once. Both must be non-zero. |
| `SPACE_DATA_PLANE_MAX_CONCURRENT_UPLOADS_PER_SANDBOX`, `SPACE_DATA_PLANE_MAX_IN_FLIGHT_UPLOAD_BYTES_PER_SANDBOX` | 4, 256 MiB | Each sandbox's share. The byte share fits one full-size upload, which an undeclared-length upload fills. Both must be non-zero. |
| `SPACE_DATA_PLANE_ORG_TRANSFER_BYTES_PER_SECOND`, `SPACE_DATA_PLANE_ORG_TRANSFER_BURST_BYTES` | 100000000, 1000000000 | Per-org token bucket on each replica for `POST /files` request bodies and `GET /files` response bodies together. An HTTP/1.x upload over the rate slows through TCP backpressure rather than failing, but one that arrives while its org owes more than the admission wait could sleep off gets 429 before it takes any permit, and an admitted one that meets more than 15 s of debt, which the org's other transfers can run up meanwhile, ends with 429 and frees its permits rather than hold them through the sleep, unless that frame is the last of a body that declared its length; bytes are counted as the data plane reads them, before the throttle holds a frame, so what crosses the wire can run ahead by about a frame per upload plus socket buffering. HTTP/2 uploads and downloads are never slowed, since their streams share an HTTP/2 connection's flow control with other requests (a download's with other sandboxes' calls on a pooled backend connection). Their bytes are charged to the bucket as they move, and a new one gets 429 while the org owes the bucket, which is most of the time while the org's HTTP/1.x uploads are being slowed. So the rate binds a download only as it starts: downloads started while the org owes nothing all run as fast as their clients read them, however many there are. An org's HTTP/2 uploads can run ahead of the rate by at most its in-flight byte share, or one upload's 256 MiB body limit if the share is smaller. Starting values for testing, not measured ones. The per-org buckets are a bounded cache, swept in the background at most once a minute, started by a transfer: unreferenced buckets that are full or have sat idle for 10 minutes are dropped, and when a new org would take a replica past 65,536 tracked orgs the fullest unreferenced ones go, at most once a second. The org's next transfer then starts from a full burst, whatever the dropped bucket still had or owed. The rate accepts `off`; every other value, the burst included, must be non-zero. Applies only with upload admission on. |
| `SPACE_DATA_PLANE_UPLOAD_MIN_BODY_BYTES_PER_SECOND`, `SPACE_DATA_PLANE_UPLOAD_MIN_BODY_RATE_WINDOW_MS` | 65536, 15000 | An admitted upload must deliver rate × window bytes in each consecutive window of time spent waiting on the client, or it ends with 408 and frees its permits. Time in the byte-rate bucket or spent spooling and writing to the guest doesn't count. At the defaults that's 960 KiB per 15 s, so an upload smaller than 960 KiB must finish before the pod has waited 15 s on its client in total. It applies to HTTP/1.x uploads only: an HTTP/2 upload shares its connection's flow control with other requests, so it isn't held to the floor; the time limit below bounds how long it holds its permits instead. The rate accepts `off`; every other value, the window included, must be non-zero. Applies only with upload admission on. |
| `SPACE_DATA_PLANE_UPLOAD_HTTP2_MAX_DURATION_MS` | 600000 | How long after admission an HTTP/2 upload may take to finish before it ends with 408 and frees its permits; a body whose declared length has all arrived gets 5 s more for its end, which an HTTP/2 client may send in a frame of its own. A time rather than a rate, so another stream's slowness on the connection can't trip it early: at the default a full-size 256 MiB upload has it at about 450 KB/s. Accepts `off`, which lets a stalled HTTP/2 upload hold its permits until its client gives up; otherwise it must be non-zero. Applies only with upload admission on. |
| `SPACE_DATA_PLANE_SHUTDOWN_GRACE_SECS` | unset | Data plane: how long live streams keep running after SIGTERM before they're cancelled. Unset, they run until they end or the termination grace period kills the process; a second stop signal ends the drain either way, without waiting for a `Process.Start` that's still opening. Keep it plus 5 seconds within the pod's termination grace period minus its preStop delay. Values under 8 seconds, the stream-open budget, are rejected, so an ECW `Process.Start` already opening at SIGTERM can report itself. See [architecture](architecture.md) for what each stream kind sees. |

## Templates and builds

Prefetch requires the catalog to confirm generic-only release resolution from the same database snapshot as its storage locations. Cached visibility alone does not authorize a download. Upgrade the catalog before template-manager; an older catalog cannot acknowledge this restriction, so prefetch pauses until it is upgraded. On-demand pulls retain their existing authorization.

| Variable | Default | Notes |
| --- | --- | --- |
| `SPACE_TEMPLATE_PREFETCH_ENABLED` | on | |
| `SPACE_TEMPLATE_PREFETCH_TAGS` | `seed:latest`, `code-runtime:latest` | |
| `SPACE_TEMPLATE_PREFETCH_VISIBILITY_ENFORCEMENT` | retired | Ignored; prefetch requires confirmed generic visibility. |
| `SPACE_TEMPLATE_CACHE_BUDGET_BYTES` | 100 GiB | 0 or an invalid value disables cache GC. |
| `SPACE_NODECTL_UNANSWERED_TEMPLATE_EVICT_AFTER_SECS` | 3600 | node-ctl on workers: how long the cluster must keep releasing a local template (present, held by the node, but with no sandbox, generic visibility or warm pool to justify it) before the worker deletes its copy. 0 or an invalid value turns these deletes off. |
| `SPACE_TEMPLATE_BUILD_DISK_MIB` | build-manager default | JSON. |
| `SPACE_TRUSTED_TEMPLATE_STORAGE_BUCKETS` | empty = any bucket | |
| `SPACE_TEMPLATE_PUBLISH_SYSTEM_SERVICE_ACCOUNTS`, `SPACE_TEMPLATE_PUBLISH_TENANT_SERVICE_ACCOUNTS` | empty | Startup error when the allowed set is empty. An account in both lists (build-manager) must present the grant issued for that system build by the API or a system-only operator; each grant authorizes one release. A tenant-list account must likewise present the grant the API issued before dispatching that organization's build of that template, from the builder node the grant is bound to; it authorizes one release. |
| `SPACE_BUILD_AUTH_SYSTEM_SERVICE_ACCOUNTS`, `SPACE_BUILD_AUTH_TENANT_SERVICE_ACCOUNTS` | empty | |
| `SPACE_TEMPLATE_PUBLICATION_TOKEN_PATH` | unset | Direct deployments only: dedicated publisher credential file shared by ClusterApi and build-manager. Never reuse the shared node token; incompatible with Kubernetes authentication. See below. |
| `SPACE_BUILD_MANAGER_ENDPOINT` | `127.0.0.1:9900` | |
| `SPACE_BUILD_MANAGER_TLS_SERVER_NAME` | from endpoint | |
| `SPACE_BUILD_MANAGER_AUTH_ALLOWED_SERVICE_ACCOUNTS` | empty | |
| `SPACE_BUILD_EXPORT_<NAME>` | unset = not exported | Opt a value into recipe `env_refs` as guest variable `NAME`; see [build environment exports](../templates/firecracker/seed/README.md#build-environment-exports). |
| `SPACE_BUILDER_ECR_REPOSITORIES` | unset | JSON. |
| `SPACE_UBUNTU_APT_MIRROR` | preserve base sources when building from an image or template; `https://archive.ubuntu.com/ubuntu` when creating a seed | An explicit value rewrites Ubuntu archive/security sources during the build. Without it, inherited sources keep their transport and trust requirements, including in builds derived from older or custom templates. |

Direct/systemd deployments can publish API-submitted template builds without
Kubernetes projected credentials. Generate a separate random publisher secret
(for example, 32 random bytes encoded as hex), install it in a protected file
readable only by the appropriate service identity, and set
`SPACE_TEMPLATE_PUBLICATION_TOKEN_PATH` on **ClusterApi and build-manager only**.
The file must contain 32–4096 visible ASCII bytes; surrounding whitespace is
trimmed. Both processes still require their normal shared node bearer and
internal RPC TLS configuration. Other node services must not receive the
publisher secret.

Build-manager attaches the dedicated proof only to template and release
registration. The shared node bearer alone cannot publish. Both tenant and
system publication require the corresponding **per-build grant** issued by the
API. A tenant grant must match the template, organization, and registered builder
at the connection’s source address; no Kubernetes identity is required. The
publisher credential cannot issue system grants itself, and
does not enable ungranted system builds from the operator CLI. Kubernetes
publication continues using projected tokens; combining Kubernetes auth with
the direct receiver credential fails startup.

Upgrade and configure ClusterApi first, then build-manager, before submitting
builds. Unconfigured receivers retain their existing rejection of static
publication; a sender upgrade alone cannot enable publication. Missing or
malformed configured files fail closed. The receiver loads its credential at
startup; the sender reads it on each registration attempt. To rotate it, pause
build submissions and drain publishing jobs, replace both files atomically,
restart ClusterApi, and resume. Removing the setting from both services disables
direct publication without changing previously published templates. Failed
builds must be resubmitted after configuration is corrected.

For direct `space-buildmgr build` calls without `--org-id`, configure
`SPACE_INTERNAL_API_ENDPOINT` and `SPACE_CLUSTER_API_TOKEN_PATH` in the CLI's
process. The projected token must identify an account in the system publication
list only. The CLI obtains a grant before submitting the build and sends it with
the job; build-manager's dual-authority credential cannot issue grants. Configure
`SPACE_BUILD_MANAGER_TOKEN_PATH` separately for permission to submit builds.
Direct operator builds may still use custom system template names. API system
builds remain limited to `seed` and `code-runtime` at the node gateway.

The grant follows the persisted build job through template and release
registration, expires after seven days, and is consumed only when the release
transaction commits. A failed API dispatch revokes only its own grant. Grants
left by failed direct submissions expire and are swept when a new grant is issued.
With no ClusterApi endpoint configured, the CLI retains local-only build behavior;
cluster publication requires the operator configuration above.

Pause system build submissions and drain older system jobs before rolling out
ClusterApi's grant enforcement. Apply the grant-table migration and role
bootstrap first, upgrade build-manager and the node-api gateway, then upgrade
the API and ClusterApi before resuming submissions. The gateway must understand
the new field to forward each build's grant. Resubmit any older queued system
jobs that could not be drained: jobs without a grant cannot publish through a
dual-authority account after the rollout. System-only deployment publishers
retain their existing publication authority.

Tenant builds follow the same pattern. Before dispatching an organization's
build, the API records a tenant publication grant naming the target template
and that organization, and sends it with `StartBuild`; it withdraws the grant if
dispatch fails and pins it to the accepting builder node from the response.
Build-manager forwards the grant to template and release registration.
ClusterApi accepts a tenant-list account's publication only with a grant that
names the requested template and organization and that covers the builder node
identified by freshly reviewed pod-bound node name and UID, matched against
live Kubernetes builder inventory. A builder whose registration has no
inventory UID yet cannot publish; build-manager retries until inventory
synchronization completes, within its bounded registration backoff. Updated build-managers also
retry publication failures explicitly marked as uncommitted database
contention; transport failures with an unknown commit outcome remain terminal.
Both build-manager and ClusterApi must be upgraded for these retries.
Standalone trusted deployments retain transport-address identity. A grant
covers any registered builder for one hour while unpinned, then only the pinned
builder for seven days. Release registration
consumes the grant in its transaction. Direct `space-buildmgr build --org-id`
submissions carry no grant, so their builds cannot publish to a cluster through
a tenant-list account; submit organization builds through the API.

Deploy tenant grant enforcement only after the preparation release has fully
rolled out to every API, ClusterApi, node-api gateway, and build-manager. That
release produces and forwards grants while still accepting grantless tenant
publication; the grant-table migration and role bootstrap must already be
applied. Do not skip directly from an older release to enforcement.

Drain or resubmit every tenant job accepted before preparation finished; an
older gateway or builder may have dropped its grant. Verify a newly submitted
tenant build retains the grant through dispatch and publication, and that
TokenReview returns the pod-bound node identity matched by builder inventory.
Direct tenant CLI submissions must move to the API before enforcement.

Once these prerequisites hold, enforcement may roll out through the normal
service order: existing preparation builders already carry grants and understand
safe retry responses. Roll application components back to the preparation
release if needed, retaining the additive schema. Remove enforcement from all
ClusterApi replicas before rolling grant producers or forwarders back further.

### Template balloon device

A template can opt its sandboxes into a Firecracker
[balloon device](https://github.com/firecracker-microvm/firecracker/blob/main/docs/ballooning.md)
through `balloon` in its recipe's `[metadata]`, which the build records in the
template's `metadata.json`:

```toml
[metadata]
memory_mib = 8192
# One of:
balloon = { mode = "inflate", amount_mib = 1024, deflate_on_oom = true }
balloon = { mode = "report" }  # free-page reporting
balloon = { mode = "hint" }    # free-page hinting
```

`stats_polling_interval_s` (default 0, off) enables balloon statistics in any
mode. `amount_mib` and `deflate_on_oom` are required for `inflate` and rejected
otherwise; `amount_mib` must be below the sandbox's memory. Unknown modes and
fields are rejected. Without `balloon`, the VM configuration is unchanged.

The device is rendered only when a sandbox cold-boots from the template build,
which includes creating the template's base snapshot. Sandboxes restored from a
snapshot keep the device that snapshot captured, so changing the setting takes
effect only after rebuilding the template and its base snapshot. The REST build
API does not accept the setting yet.

Hint mode also needs `balloon_hint_timeout_ms` (at most 30000) in the node's
`/space/etc/sandbox.toml`. Before each suspend snapshot, a VM whose balloon has
free-page hinting then runs one hinting pass bounded by this time; timeouts and
errors are logged and the snapshot proceeds. Unset, suspend makes no balloon
calls.


## Object storage and snapshots

| Variable | Default | Notes |
| --- | --- | --- |
| `SPACE_OBJECT_STORE_BUCKET`, `SPACE_OBJECT_STORE_REGION` | unset | Set together. |
| `SPACE_OBJECT_STORE_ENDPOINT` | AWS | A custom endpoint also turns on path-style addressing. |
| `SPACE_OBJECT_STORE_STATIC_ACCESS_KEY_ID`, `SPACE_OBJECT_STORE_STATIC_SECRET_ACCESS_KEY` | unset | Set both or neither. |
| `SPACE_OBJECT_STORE_MULTIPART_PART_SIZE_BYTES` | built-in | 8 MiB to 5 GiB. |
| `SPACE_OBJECT_STORE_MULTIPART_CONCURRENCY` | built-in | 1 to 64 in-flight parts. |
| `SPACE_OBJECT_STORE_DOWNLOAD_READER_CONCURRENCY`, `SPACE_OBJECT_STORE_DOWNLOAD_CHUNK_SIZE_BYTES`, `SPACE_OBJECT_STORE_DOWNLOAD_BUFFER_BUDGET_BYTES` | 8, 32 MiB, 2 GiB | Peak buffered memory is bounded by the budget; size it below the service's memory limit. |
| `SPACE_OBJECT_STORE_REQUEST_ATTEMPT_TIMEOUT_SECS`, `SPACE_OBJECT_STORE_BODY_UPLOAD_ATTEMPT_TIMEOUT_SECS`, `SPACE_OBJECT_STORE_SERVER_SIDE_ATTEMPT_TIMEOUT_SECS` | 10, 60, 30 | 5 to 21600. Bound one attempt of a get, head, or create/abort multipart call; of a put or part upload; and of a complete or copy. Raise the body-upload value with the part size. |
| `SPACE_OBJECT_STORE_REQUEST_OPERATION_TIMEOUT_SECS`, `SPACE_OBJECT_STORE_BODY_UPLOAD_OPERATION_TIMEOUT_SECS`, `SPACE_OBJECT_STORE_SERVER_SIDE_OPERATION_TIMEOUT_SECS` | 3 × attempt + 5 s | 5 to 86400. Bound the same calls across all three SDK attempts. |
| `SPACE_OBJECT_STORE_POOL_IDLE_TIMEOUT_SECS` | 3 | 1 to 90. How long an idle pooled connection stays eligible for reuse. Keep it below the endpoint's idle close; AWS S3 closes idle HTTPS connections after about 6 s. Applies to object-store transfers; the snapshot-storage sweepers and the kernel fetcher use the 3 s default. |
| `SPACE_OBJECT_STORE_STATIC_SESSION_TOKEN` | unset | Session token for temporary static credentials. |
| `SPACE_SNAPSHOT_BUCKET`, `SPACE_SNAPSHOT_REGION`, `SPACE_SNAPSHOT_ENDPOINT` | unset | Unset disables snapshot upload. Give the bucket an abort-incomplete-multipart lifecycle rule; uploads interrupted by process death otherwise keep their parts. |
| `SPACE_SNAPSHOT_STREAM_BTRFS_SEND` | on | Sandbox-manager retains a read-only source for each btrfs-send suspend and lets volume-manager stream the upload from it instead of writing `rootfs.btrfs-send`; `false` writes the send file. Defers only while volume-manager streams uploads under its own identity. See [snapshot upload streaming](snapshot-upload-streaming.md). |
| `SPACE_VOLUME_STREAM_PUSH_BUFFER_BUDGET_BYTES` | 1 GiB | Volume-manager streams eligible snapshot uploads with this shared part budget; `0` uploads through temporary files. Whole MiB, at least 64 MiB. See [snapshot upload streaming](snapshot-upload-streaming.md). |

## Authentication, TLS and credentials

| Variable | Default | Notes |
| --- | --- | --- |
| `SPACE_INTERNAL_TOKEN`, `SPACE_INTERNAL_CONTROL_TOKEN` | **required** where used | Node-to-node bearer tokens. |
| `SPACE_INTERNAL_SECRET_NAME` | `space-internal` | |
| `SPACE_INTERNAL_API_ENDPOINT` | unset | |
| `SPACE_INTERNAL_RPC_INSECURE_DEV` | off | TLS is required unless set. |
| `SPACE_INTERNAL_RPC_TLS_CA`, `SPACE_INTERNAL_RPC_TLS_CERT`, `SPACE_INTERNAL_RPC_TLS_KEY` | unset | Cert and key are set together. |
| `SPACE_K8S_TOKEN_AUTH_ENABLED` | off | |
| `SPACE_K8S_TOKEN_AUTH_STATIC_FALLBACK_ENABLED` | on | Keeps the shared static token accepted next to ServiceAccount auth. |
| `SPACE_K8S_TOKEN_AUTH_REVIEW_RATE_PER_SEC` | 50 | |
| `SPACE_CLUSTER_API_AUTH_ALLOWED_SERVICE_ACCOUNTS`, `SPACE_NODE_API_AUTH_ALLOWED_SERVICE_ACCOUNTS`, `SPACE_NODE_API_INTERNAL_AUTH_ALLOWED_SERVICE_ACCOUNTS` | empty | With Kubernetes auth on, an empty list rejects every call. |
| `SPACE_NODE_API_AUTH_GUEST_BRIDGE_SERVICE_ACCOUNTS`, `SPACE_NODE_API_AUTH_TEMPLATE_BUILD_SERVICE_ACCOUNTS`, `SPACE_NODE_API_AUTH_TRUSTED_POLICY_SERVICE_ACCOUNTS`, `SPACE_NODE_API_AUTH_TRUSTED_WAIT_READY_SERVICE_ACCOUNTS` | empty | |
| `SPACE_NODE_API_AUTH_TEMPLATE_TRANSFER_SERVICE_ACCOUNTS` | empty | Explicit artifact-transfer accounts for `NodeExportTemplate` and `PullTemplate`; also require normal surface admission. Guest-bridge accounts are always excluded. |
| `SPACE_NODE_API_AUTH_PEER_NODE_SERVICE_ACCOUNTS` | empty | Accounts another node's node-api presents. They may call only `NodeExportTemplate` and `GetNodeInfo`, after admission and the transfer grant. |
| `SPACE_NODE_API_NAMESPACE`, `SPACE_NODE_API_TLS_SERVER_NAME`, `SPACE_CLUSTER_API_TLS_SERVER_NAME` | namespace chain, from endpoint | |
| `SPACE_API_ROUTE_ED25519_ISSUER`, `SPACE_API_ROUTE_ED25519_SIGNING_KEY_ID`, `SPACE_API_ROUTE_ED25519_SIGNING_KEY_PATH` | unset = no signing | Set all three. |
| `SPACE_API_ROUTE_ED25519_VERIFICATION_BUNDLE_PATH` | required on sandbox managers | |
| `SPACE_API_ROUTE_GENERATION_FENCE_ENABLED` | on | |
| `SPACE_ECW_CREDENTIAL_KEY_PATH` | **required** on api and internal-api | |
| `SPACE_ECW_CREDENTIAL_PREVIOUS_KEY_PATH` | unset | Set only while rotating. |
| `SPACE_CREDMGR_ENDPOINT`, `SPACE_CREDMGR_TLS_CA` | local Unix socket | `https` needs the CA. |
| `SPACE_CREDMGR_K8S_AUTH_ENABLED` | off | |
| `SPACE_CREDMGR_K8S_AUTH_NAMESPACE` | pod namespace | Namespace of the ServiceAccounts credmgr accepts. |
| `SPACE_CREDMGR_AUTH_AUDIENCE` | `space-credential-manager` | Token audience credmgr validates. |
| `SPACE_CREDMGR_AUTH_ALLOWED_SERVICE_ACCOUNTS` | every credmgr client service | Comma-separated allowlist; narrows which ServiceAccounts are admitted. |
| `SPACE_CREDMGR_FAIL_ON_KEYLESS_AUTHORITY` | off | Crash instead of serving Ready-but-broken when a node-local authority has no key. |
| `SPACE_CREDMGR_S3_VEND_ROLE_ARN` | unset = no storage exchange | Requires `DATABASE_URL`; a malformed ARN fails startup. See `docs/space/design/credential-token-exchange.md`. |
| `SPACE_CREDMGR_SECRETS_MANAGER_ROLE_ARN` | unset = credential references are not resolvable | IAM role assumed per organization for every Secrets Manager read, with session tag `space-org-id=<organization>` and a session policy that allows `GetSecretValue` only on secrets tagged with that organization and `kms:Decrypt` only through Secrets Manager; a malformed ARN fails startup. The role's trust policy admits only the authority's identity, with `sts:AssumeRole` and `sts:TagSession`, and requires the `space-org-id` request tag. Its policy allows `GetSecretValue` only when `secretsmanager:ResourceTag/space-org-id` equals `${aws:PrincipalTag/space-org-id}`, allows `kms:Decrypt` only with `kms:ViaService` `secretsmanager.*.amazonaws.com`, and explicitly denies secrets whose `space-org-id` tag is missing or differs from the session's, sessions without the tag, and any KMS use outside Secrets Manager; the explicit denials also hold against same-account resource and key policies. Tag each referenced secret `space-org-id=<owning organization ID>`. A secret in another account also needs a resource policy allowing the role, and a secret under a customer-managed KMS key (required across accounts) needs the key policy to allow the role `kms:Decrypt` with `kms:ViaService` `secretsmanager.<region>.amazonaws.com`. |
| `SPACE_CREDMGR_INTERMEDIATE_CA_ARN` | unset | |
| `SPACE_CREDMGR_INTERMEDIATE_KEY_BROKER_URL`, `SPACE_CREDMGR_INTERMEDIATE_KEY_BROKER_TOKEN_FILE`, `SPACE_CREDMGR_INTERMEDIATE_KEY_BROKER_TLS_CA`, `SPACE_CREDMGR_INTERMEDIATE_KEY_BROKER_GRANT_SCHEME`, `SPACE_CREDMGR_INTERMEDIATE_KEY_BROKER_TENANT_NAMESPACE` | unset | Configure as a group. |
| `SPACE_PLATFORM_KEK_PROVIDER` | none | |
| `SPACE_BYOK_VOLUME_ENCRYPTION` | off | |
| `SPACE_EGRESS_PROFILES`, `SPACE_NET_GATEWAY_KNOWN_EGRESS_DOMAINS` | unset / empty | |

### External console sign-in

By default the console signs in with a SPACE username and password. A
deployment that puts the console behind an identity-aware proxy can instead
install an `ExternalConsoleAuthenticator` through
`ApiExtensions::with_external_console_authenticator` and start the API with
`pplx_space_api::run_with`. The stock binaries install none, so these variables
have no effect on them. The authenticator verifies the proxy's signed
assertion and applies the deployment's admission policy; SPACE holds no
provider-specific policy and never reads an unsigned identity header. Install
the same authenticator on every API surface the console calls, including the
admin API.

| Variable | Default | Notes |
| --- | --- | --- |
| `SPACE_EXTERNAL_CONSOLE_TRUSTED_ORIGINS` | **required** with an authenticator | Comma-separated exact origins (`scheme://host[:port]`, at most 16) the console is served from. State-changing console requests must send a matching `Origin`. Invalid or empty fails startup. |
| `SPACE_EXTERNAL_CONSOLE_PROVIDER_NAME` | `Single sign-on` | Name the console shows on its sign-in card. At most 64 characters, no control characters. |
| `SPACE_EXTERNAL_CONSOLE_LOGOUT_URL` | unset | Where the console sends the browser to end the provider session: a same-origin absolute path or an `https` URL. |

With an authenticator installed:

- `GET /auth/v1/config` returns `{"mode":"external","provider_name":…,"logout_url":…}`
  (otherwise `{"mode":"password"}`), unauthenticated and `no-store`, so the
  console can choose its sign-in flow. A console that gets 404 here assumes
  password mode.
- A console request is one that carries `X-Space-Console-Request: 1` exactly
  once; the console sends it on every request in this mode. Console requests
  authenticate only with the assertion: none is a 401, and a rejected one is a
  final 401, 403 or 503. A bearer token sent alongside is never used: console
  routes ignore it and the native API refuses the combination. State-changing
  methods also need a trusted `Origin`.
- The verified email maps to an existing local user by exact match. Nothing is
  provisioned and no role is granted: the user's own role and organization
  authorize the request, so a member still cannot reach admin-only routes.
  Federated owners and legacy disabled service accounts are not eligible.
- Password login (`POST /auth/v1/sessions`) is refused with 403 and existing
  password session tokens no longer authenticate, so an old session cannot
  bypass the mode. `DELETE /auth/v1/session` revokes nothing; the console
  ends the provider session through the logout URL instead.
- Requests without the console header are machine requests. The native API
  authenticates them with API keys exactly as before and never consults an
  assertion the proxy attached; console-gated routes refuse them. Session
  tokens are not accepted as native API credentials in this mode.
- Each console decision increments `auth.external_console.decision` with
  `result` `accepted`, `missing_assertion`, `unauthenticated`, `forbidden`,
  `unavailable`, `csrf_rejected`, `no_user` or `lookup_failed`.
- The authenticator runs inline on every console request with no timeout
  applied by SPACE; it must bound its own latency.
- Cross-origin requests never carry credentials: the API's CORS policy sets no
  `Access-Control-Allow-Credentials`. The origin and header checks above rely
  on that; do not relax it for the API host.

Removing the authenticator and restarting restores password sign-in;
password sessions that have not expired become valid again.

### Template mutation authority

On worker and builder node-api, `DeleteTemplate`, `DeleteTemplateBuild`,
`CreatePool`, and `DeletePool` require the trusted policy service-account
capability. Template-manager's local roles and deletion dependency guards still
apply. Pool target counts have no capacity-based upper bound yet; operators
must budget pool sizes even after caller authorization succeeds.

With Kubernetes authentication disabled, the shared internal token remains the
operator trust boundary and retains mutation authority. When Kubernetes
authentication is enabled, static fallback has no mutation authority. Missing
authentication context is denied.

### Template transfer authority

`SPACE_NODE_API_AUTH_TEMPLATE_TRANSFER_SERVICE_ACCOUNTS` grants only artifact
transfer, independently of mutation. Configure the controller and node accounts
that perform transfers before upgrading. This is a cluster infrastructure role:
it permits exporting cached private releases from any organization as well as
system templates. Do not grant it to tenant-facing or general admitted services.
Node-api replaces forwarded authority metadata, and template-manager requires
that grant from its authenticated node-api Unix-socket peer. Upgrade node-api
before template-manager; an old forwarder is denied by the new manager.

With Kubernetes authentication disabled, the shared internal token remains the
operator trust boundary and retains transfer authority. When Kubernetes
authentication is enabled, static fallback has no transfer authority.
Missing authentication context is denied. Local root/operator exports remain
available through template-manager's Unix socket.

A peer transfer has two callers, and each needs the grant and admission by
`SPACE_NODE_API_AUTH_ALLOWED_SERVICE_ACCOUNTS` on the node it calls. The caller
of `PullTemplate` on the target node starts it: the admin API's node
template-pull route, which only the direct substrate serves, or an operator
calling node-api directly. The target's node-api then calls
`NodeExportTemplate` on the source node with its own `SPACE_NODE_API_TOKEN_PATH`
token. List the account that starts pulls and the node-api account of every
role that can be a target. An empty
`SPACE_NODE_API_AUTH_TEMPLATE_TRANSFER_SERVICE_ACCOUNTS` disables peer
transfers and node-api logs a startup warning. A deployment without peer
transfers may leave it empty. Also list each node-api account in
`SPACE_NODE_API_AUTH_PEER_NODE_SERVICE_ACCOUNTS`, which limits that account to
the export call and `GetNodeInfo`.

Exact-digest export can use the local cache offline. An omitted digest resolves
the requested catalog tag (default `latest`) and exports only that digest; a
missing tag, unavailable catalog, or uncached resolved digest fails without
falling back to a newer local build.

### Policy authorization

Before enabling Kubernetes authentication or upgrading an authenticated worker
or builder, set `SPACE_NODE_API_AUTH_TRUSTED_POLICY_SERVICE_ACCOUNTS` to the
service accounts that own policy intent. It must be nonempty on both roles or
node-api refuses to start. These accounts must also be admitted by the normal
and internal surface allowlists for the RPCs they call. Policy trust is
independent of the `SpacedWaitReady` trust set. Policy-bearing pool adoption
uses the internal surface, so its caller needs an internal-audience token.

Shared static-token fallback cannot mutate policy when Kubernetes
authentication is enabled; static-only deployments retain that authority.
Policy-bearing cold creation, internal pool adoption, and worker and builder
template updates require a trusted caller. Cold creation and pool adoption also
require policy authority for a nonzero sandbox-policy generation, even without
an override. Requests with no overrides and generation zero remain available to
otherwise admitted callers and inherit the template's policy. Guest-bridge
accounts already have separate method restrictions.

`Local` policy is node-administrator-owned, including normal standalone use and
operator overrides on cluster-managed nodes. It requires a root or Operator Unix
socket peer. Forwarders such as node-api and node-ctl must run under their
dedicated non-root identities: UID 0 is classified as Root before the
service-account lookup.

Forwarded imports carrying `Local` network or auto-lifecycle policy are
rejected, and retrying does not help. To retain `Local` policy, an operator must
review its ownership and perform the import through an authorized local peer;
forwarding never relabels it.

## Database

| Variable | Notes |
| --- | --- |
| `SPACE_DB_USER_PASSWORD`, `SPACE_DB_CREDS_PASSWORD` | **required** by services. |
| `SPACE_DB_BOOTSTRAP_USER_PASSWORD`, `SPACE_DB_BOOTSTRAP_CREDS_PASSWORD`, `SPACE_DB_BOOTSTRAP_MIGRATOR_PASSWORD`, `SPACE_DB_BOOTSTRAP_RO_PASSWORD`, `SPACE_DB_BOOTSTRAP_SUPERADMIN_PASSWORD` | Required by `bootstrap-roles` unless `--allow-missing-passwords`. |
| `SPACE_USER_PASSWORD` | Migrator command. |
| `SPACE_SQLX_QUERY_LOG_LEVEL` | Off by default. |
| `SPACE_DB_MIGRATE_JOB_DEADLINE_SECONDS` | The migrate Job's `activeDeadlineSeconds`, passed in so retries stop before Kubernetes kills the Job. |
| `SPACE_DB_APP_LOCK_TIMEOUT` | `bootstrap-roles` application-role `lock_timeout`; same as `--app-lock-timeout`. `0` resets it, unset leaves it unchanged. |
| `SPACE_API_URL`, `SPACE_API_TOKEN` | Admin CLI. |

## Execution eligibility

| Variable | Default | Notes |
| --- | --- | --- |
| `SPACE_ALLOWED_EXECUTION_ENVIRONMENTS` | `e0,legacy` on x86_64; `legacy` on aarch64 | Comma-separated compiled environment names and optional historical-unbound permission. Surrounding whitespace is ignored; empty, duplicate and unknown entries fail startup. |
| `SPACE_DEFAULT_EXECUTION_ENVIRONMENT` | `e0` on x86_64; `legacy` on aarch64 | Must be allowed. Used for unpinned scratch builds, not to replace a bound source. |

The two node settings replace the former `SPACE_ENV` admission rule. Before upgrading
a node that runs non-E0 work, explicitly allow every environment that must remain
usable. Leaving settings absent revokes those bindings even on development or
Kind nodes. See [execution policy and rollout](execution-environments.md) for
cohort constraints and the separate permission for historical unbound work.

## Observability and feature flags

| Variable | Default | Notes |
| --- | --- | --- |
| `SPACE_ENV` | unset | Deployment identity for telemetry; it does not authorize execution environments. |
| `SPACE_SERVICE_NAME`, `SPACE_VERSION` | unset | SPACE identity tier: outrank `OTEL_SERVICE_NAME`/`OTEL_RESOURCE_ATTRIBUTES` and `DD_*`. |
| `SPACE_K8S_POD_NAME` | unset | Reported as `k8s.pod.name` on the metrics resource. |
| `SPACE_LOG_FORMAT` | auto | `json`, `compact`, `pretty` or `text`. |
| `SPACE_TRACE_FILTER` | follows `RUST_LOG` | Span filter, independent of the log filter. |
| `SPACE_GIT_SHA` | build identity file | Overrides the build git SHA reported in telemetry. |
| `SPACE_METRICS_SINK`, `SPACE_TRACE_EXPORTER` | auto | `datadog` when compiled in, else `otlp` when an endpoint is set, else off. |
| `SPACE_FLAGD_ENABLED` | off | |

Identity precedence and the full telemetry contract are in
[observability](observability.md).

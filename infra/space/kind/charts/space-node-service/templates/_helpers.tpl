{{/*
Shared pod shape for the Kind node-service charts. Each chart renders one
DaemonSet for one node service and calls these with
`(dict "ctx" . "component" "<component>")`, where the component is the
`app.kubernetes.io/component` value production's node DaemonSets carry
(crates/libs/cluster/controller/server/src/node_service_readiness/mod.rs
names node-ctl, volume-manager, sandbox-manager, node-manager, node-api and
template-manager).
*/}}

{{- define "space-node-service.role" -}}
{{- $role := required "node.role is required" .Values.node.role -}}
{{- if not (has $role (list "worker" "builder")) -}}
{{- fail (printf "node.role must be worker or builder, got %q" $role) -}}
{{- end -}}
{{- $role -}}
{{- end -}}

{{/* DaemonSet and ServiceAccount name: space-node-manager, space-builder-node-manager, ... */}}
{{- define "space-node-service.name" -}}
{{- printf "%s-%s" .ctx.Release.Name .component -}}
{{- end -}}

{{- define "space-node-service.selectorLabels" -}}
app.kubernetes.io/name: space-{{ include "space-node-service.role" .ctx }}
app.kubernetes.io/instance: {{ .ctx.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{/*
`app.kubernetes.io/name` is the node role (space-worker / space-builder) on
every node-service pod. The API's pod reflector maps that label to a SPACE
node role but watches only `app.kubernetes.io/component=node-api`
(crates/libs/api/src/controllers/substrate/kubernetes/caches.rs), so only the
node-api pods become node endpoints. `space.pplx.net/node-service` lets the
harness and diagnostics select every node-service pod at once.
*/}}
{{- define "space-node-service.labels" -}}
{{ include "space-node-service.selectorLabels" . }}
app.kubernetes.io/part-of: space-node
space.pplx.net/node-service: {{ .component }}
team: {{ .ctx.Values.node.datadogTeam | quote }}
{{- end -}}

{{- define "space-node-service.nodeSelector" -}}
{{- $role := include "space-node-service.role" .ctx -}}
nodeSelector:
  space.pplx.net/node-pool: {{ index .ctx.Values.node.pools $role | quote }}
{{- end -}}

{{- define "space-node-service.image" -}}
image: "{{ .Values.image.repository }}:{{ .Values.image.tag }}"
imagePullPolicy: {{ .Values.image.pullPolicy }}
{{- end -}}

{{/*
Identity every node service reports. SPACE_NODE_ID is the raw kube node name
(fieldRef spec.nodeName); services and the cluster API derive the node UUID
from (name, role) via space_k8s_node_id, so nothing hand-rolls an id. The
node and pod names are the observability identity
(crates/libs/observability/src/identity.rs). SPACE_INTERNAL_RPC_INSECURE_DEV
is Kind's opt-out of authenticated internal RPC TLS (docs/internal-rpc-tls.md)
for the node services' TCP clients and listeners.
*/}}
{{- define "space-node-service.env.identity" -}}
- name: SPACE_NODE_ID
  valueFrom:
    fieldRef:
      fieldPath: spec.nodeName
- name: SPACE_NODE_ROLE
  value: {{ include "space-node-service.role" . | quote }}
- name: SPACE_CLUSTER_ID
  value: {{ .Values.clusterId | quote }}
- name: KUBE_NODE_NAME
  valueFrom:
    fieldRef:
      fieldPath: spec.nodeName
- name: SPACE_K8S_NODE_NAME
  valueFrom:
    fieldRef:
      fieldPath: spec.nodeName
- name: SPACE_K8S_POD_NAME
  valueFrom:
    fieldRef:
      fieldPath: metadata.name
- name: SPACE_INTERNAL_RPC_INSECURE_DEV
  value: "true"
{{ include "space-kind-telemetry.env" . }}
{{- end -}}

{{/*
ClusterApi client wiring: the endpoint plus the pod's own projected
ServiceAccount token (audience space-cluster-api), which internal-api
authenticates by TokenReview against its allowlist
(SPACE_CLUSTER_API_AUTH_ALLOWED_SERVICE_ACCOUNTS in space-internal-api-local).
Pair with the cluster-api-token volume below.
*/}}
{{- define "space-node-service.env.clusterApi" -}}
- name: SPACE_INTERNAL_API_ENDPOINT
  value: {{ .Values.node.internalApiEndpoint | quote }}
- name: SPACE_CLUSTER_API_TOKEN_PATH
  value: /var/run/space-publication/cluster-api.token
{{- end -}}

{{/*
The control-plane credential-manager authority, for the node credential
agent and node-manager's direct sandbox volume DEK calls: its endpoint
(SPACE_CREDMGR_ENDPOINT, crates/libs/credential/mgr/client/src/endpoint.rs)
and the pod's projected ServiceAccount token at the client's default path
(/var/run/space-auth/credmgr.token, client/src/token.rs) with the
authority's audience. The authority maps the ServiceAccount to its client
service by TokenReview. Pair with the credmgr-token volume below.
*/}}
{{- define "space-node-service.env.credmgrAuthority" -}}
- name: SPACE_CREDMGR_ENDPOINT
  value: {{ .Values.node.credmgrAuthorityEndpoint | quote }}
{{- end -}}

{{- define "space-node-service.env.internalToken" -}}
- name: SPACE_INTERNAL_TOKEN
  valueFrom:
    secretKeyRef:
      name: {{ .Values.node.internalSecretName }}
      key: SPACE_INTERNAL_TOKEN
{{- end -}}

{{- define "space-node-service.env.diskAdmission" -}}
- name: SPACE_NODE_DISK_ADMISSION_MIN_AVAILABLE_BYTES
  value: {{ .Values.node.diskAdmission.minAvailableBytes | quote }}
- name: SPACE_NODE_DISK_ADMISSION_ALLOCATION_RESERVATION_BYTES
  value: {{ .Values.node.diskAdmission.allocationReservationBytes | quote }}
{{- end -}}

{{- define "space-node-service.env.objectStoreCredentials" -}}
- name: SPACE_OBJECT_STORE_STATIC_ACCESS_KEY_ID
  valueFrom:
    secretKeyRef:
      name: {{ .Values.node.objectStore.credentialsSecretName }}
      key: SPACE_OBJECT_STORE_STATIC_ACCESS_KEY_ID
- name: SPACE_OBJECT_STORE_STATIC_SECRET_ACCESS_KEY
  valueFrom:
    secretKeyRef:
      name: {{ .Values.node.objectStore.credentialsSecretName }}
      key: SPACE_OBJECT_STORE_STATIC_SECRET_ACCESS_KEY
{{- end -}}

{{- define "space-node-service.env.routeVerification" -}}
- name: SPACE_API_ROUTE_ED25519_VERIFICATION_BUNDLE_PATH
  value: /var/run/space-route-verification/verification-bundle.json
- name: SPACE_API_ROUTE_GENERATION_FENCE_ENABLED
  value: {{ .Values.node.routeVerification.generationFenceEnabled | quote }}
{{- end -}}

{{/*
Instance-local health socket. Each binary binds it before waiting for its
service mutex and answers `readiness` (SERVING while passive, NOT_SERVING
while taking over, then the production socket's verdict) and `liveness`
(crates/libs/common/svc/src/local_health.rs). It must be on a pod-private
mount, so a surging successor cannot replace its predecessor's socket.
*/}}
{{- define "space-node-service.healthSocket" -}}
/run/health/{{ .component }}.sock
{{- end -}}

{{- define "space-node-service.probes" -}}
{{- $socket := include "space-node-service.healthSocket" . -}}
{{- $startupSeconds := .startupSeconds | default 300 | int -}}
# Liveness answers as soon as the health socket is bound, before the service
# mutex and startup recovery, so the startup budget only covers reaching the
# bind; readiness gates traffic and the rollout.
startupProbe:
  exec:
    command: ["/opt/space/bin/grpc_health_probe", "-addr=unix://{{ $socket }}", "-service=liveness", "-connect-timeout=2s", "-rpc-timeout=2s"]
  periodSeconds: 5
  failureThreshold: {{ div $startupSeconds 5 }}
readinessProbe:
  exec:
    command: ["/opt/space/bin/grpc_health_probe", "-addr=unix://{{ $socket }}", "-service=readiness", "-connect-timeout=2s", "-rpc-timeout=2s"]
  periodSeconds: 5
  failureThreshold: 3
livenessProbe:
  exec:
    command: ["/opt/space/bin/grpc_health_probe", "-addr=unix://{{ $socket }}", "-service=liveness", "-connect-timeout=3s", "-rpc-timeout=3s"]
  periodSeconds: 20
  failureThreshold: 3
{{- end -}}

{{/*
Volume mounts. /space/run is the node's hostPath: the services' Unix
sockets, service mutexes and dependency-drain gates live there, and the
containerd shim and the host runtime-supervisor reach the same paths from the
node. /space/cache is the node's btrfs cache; HostToContainer receives the
node's mount (and a remount by space-cache.service) without sending this
pod's own mounts back to the node.
*/}}
{{- define "space-node-service.mount.run" -}}
- name: run
  mountPath: /space/run
{{- end -}}

{{- define "space-node-service.mount.cache" -}}
- name: cache
  mountPath: /space/cache
  mountPropagation: {{ .propagation | default "HostToContainer" }}
  {{- if .readOnly }}
  readOnly: true
  {{- end }}
{{- end -}}

{{- define "space-node-service.mount.private" -}}
- name: health
  mountPath: /run/health
- name: tmp
  mountPath: /tmp
{{- end -}}

{{- define "space-node-service.mount.clusterApiToken" -}}
- name: cluster-api-token
  mountPath: /var/run/space-publication
  readOnly: true
{{- end -}}

{{- define "space-node-service.mount.credmgrToken" -}}
- name: credmgr-token
  mountPath: /var/run/space-auth
  readOnly: true
{{- end -}}

{{- define "space-node-service.mount.routeVerification" -}}
- name: route-verification-bundle
  mountPath: /var/run/space-route-verification
  readOnly: true
{{- end -}}

{{- define "space-node-service.volume.run" -}}
# DirectoryOrCreate: the node's space-layout.service creates the skeleton
# before kubelet starts; a pod scheduled first still starts and its service
# fails, and restarts, until the directory it needs exists.
- name: run
  hostPath:
    path: /space/run
    type: DirectoryOrCreate
{{- end -}}

{{- define "space-node-service.volume.cache" -}}
# Mounted on the node by space-cache.service before kubelet starts.
- name: cache
  hostPath:
    path: /space/cache
    type: Directory
{{- end -}}

{{- define "space-node-service.volume.private" -}}
- name: health
  emptyDir:
    medium: Memory
    sizeLimit: 1Mi
- name: tmp
  emptyDir: {}
{{- end -}}

{{- define "space-node-service.volume.credmgrToken" -}}
- name: credmgr-token
  projected:
    defaultMode: 0444
    sources:
      - serviceAccountToken:
          path: credmgr.token
          audience: space-credential-manager
          expirationSeconds: 3600
{{- end -}}

{{- define "space-node-service.volume.clusterApiToken" -}}
- name: cluster-api-token
  projected:
    defaultMode: 0444
    sources:
      - serviceAccountToken:
          path: cluster-api.token
          audience: space-cluster-api
          expirationSeconds: 3600
{{- end -}}

{{- define "space-node-service.volume.routeVerification" -}}
- name: route-verification-bundle
  secret:
    secretName: {{ .Values.node.routeVerification.secretName }}
    defaultMode: 0444
    items:
      - key: {{ .Values.node.routeVerification.bundleKey }}
        path: verification-bundle.json
{{- end -}}

{{/*
An unprivileged, read-only container: no capabilities, no escalation. Every
service that only needs file access and Unix sockets runs this way, as the
service crates describe their production shape (e.g. "runAsNonRoot with no
added capabilities" in crates/libs/template/mgr/src/btrfs_via_nodemgr.rs and
crates/libs/sbx/mgr/server/src/btrfs_via_nodemgr.rs).
*/}}
{{- define "space-node-service.restrictedContainer" -}}
securityContext:
  allowPrivilegeEscalation: false
  readOnlyRootFilesystem: true
  capabilities:
    drop: ["ALL"]
{{- end -}}

{{/*
Init container that creates the service's own /space/run directory with the
ownership its socket peers expect, as production's per-service chart inits
do for the directories the node image does not bake (packer 07-sysctl.sh
creates node-manager's, sandbox-manager's, credential-manager's and
template-manager's). Root with only CHOWN and FOWNER: `install -d -o -g -m`
on a root-owned parent.
*/}}
{{- define "space-node-service.prepareRunDir" -}}
- name: prepare-run-dir
  {{- include "space-node-service.image" .ctx | nindent 2 }}
  command:
    - install
    - -d
    - -m
    - {{ .mode | quote }}
    - -o
    - {{ .uid | quote }}
    - -g
    - {{ .gid | quote }}
    - {{ .path }}
  securityContext:
    runAsNonRoot: false
    runAsUser: 0
    runAsGroup: 0
    allowPrivilegeEscalation: false
    readOnlyRootFilesystem: true
    capabilities:
      drop: ["ALL"]
      add: ["CHOWN", "FOWNER"]
  volumeMounts:
    {{- include "space-node-service.mount.run" .ctx | nindent 4 }}
{{- end -}}

{{/*
Rolling-update strategy. `surge` starts the replacement beside the old pod:
for services that hold a node-wide service mutex (/space/run/<svc>/service.mutex)
the successor waits passively, reports ready while passive, and takes over
when the predecessor drains and releases the lock, with its sockets replaced
by rename (crates/libs/common/svc/src/service_mutex.rs, bootstrap.rs).
`deleteFirst` stops the old pod before the new one starts, for services that
cannot run twice on a node.
*/}}
{{- define "space-node-service.updateStrategy" -}}
updateStrategy:
  type: RollingUpdate
  rollingUpdate:
    {{- if eq .strategy "surge" }}
    maxSurge: 1
    maxUnavailable: 0
    {{- else if eq .strategy "deleteFirst" }}
    maxSurge: 0
    maxUnavailable: 1
    {{- else }}
    {{- fail (printf "unknown update strategy %q" .strategy) }}
    {{- end }}
{{- end -}}

{{/*
Host network for a service that makes node-bound ClusterApi calls.
internal-api binds every node-bound RPC (PublishNodeState and the other
node_id-carrying calls, quarantine recovery plans, artifact backups) to the
node's registered address — the Kubernetes Node's InternalIP, which
cluster-controller's node sync writes — and refuses a call from any other
source address whatever the credential or transport
(validate_origin_remote_addr in crates/libs/internal/api/server/src/lib.rs;
internal RPC TLS is server-auth only). So each such caller must reach it from
the node's own IP. ClusterFirstWithHostNet keeps in-cluster DNS.
*/}}
{{- define "space-node-service.nodeBoundNetwork" -}}
hostNetwork: true
dnsPolicy: ClusterFirstWithHostNet
{{- end -}}

{{- define "space-node-service.podAnnotations" -}}
ad.datadoghq.com/tags: '{"team":"{{ .ctx.Values.node.datadogTeam }}"}'
{{- end -}}

{{- define "space-node-service.serviceAccount" -}}
apiVersion: v1
kind: ServiceAccount
metadata:
  name: {{ include "space-node-service.name" . }}
  namespace: {{ .ctx.Release.Namespace }}
  labels:
    {{- include "space-node-service.labels" . | nindent 4 }}
{{- end -}}

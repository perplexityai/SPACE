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

{{/*
The Rust build the bring-up deploys (`rustBuild`, infra/space/kind/lib/rust-build.sh):
`test-hooks`, the default, or `release`, whose binaries read no test hook
(`pplx_space_common_core::env::test_hook`), so no chart renders one for it.
*/}}
{{- define "space-node-service.rustBuild" -}}
{{- $build := .Values.rustBuild | default "test-hooks" -}}
{{- if not (has $build (list "test-hooks" "release")) -}}
{{- fail (printf "rustBuild must be test-hooks or release, got %q" $build) -}}
{{- end -}}
{{- $build -}}
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
`space.pplx.net/kind-rust-build`, set when a Kind bring-up names `rustBuild`,
records the build the pod was rendered for, so a change of build rolls it;
infra/space/kind/scripts/deployed-rust-build.py checks it against the image.
*/}}
{{- define "space-node-service.labels" -}}
{{ include "space-node-service.selectorLabels" . }}
app.kubernetes.io/part-of: space-node
space.pplx.net/node-service: {{ .component }}
{{- if .ctx.Values.rustBuild }}
space.pplx.net/kind-rust-build: {{ include "space-node-service.rustBuild" .ctx }}
{{- end }}
team: {{ .ctx.Values.node.datadogTeam | quote }}
{{- end -}}

{{- define "space-node-service.nodeSelector" -}}
{{- $role := include "space-node-service.role" .ctx -}}
nodeSelector:
  space.pplx.net/node-pool: {{ index .ctx.Values.node.pools $role | quote }}
{{- end -}}

{{/*
Node services carry the built-in system-node-critical class, as deployments
give their node-service DaemonSets (infra/space/kind/manifests/priorityclasses.yaml
has the cluster's other classes): a node-service pod that cannot fit preempts
lower-priority pods rather than leaving the node without it, and the kubelet's
node-pressure eviction ranks it behind lower-priority pods that exceed their
requests.
*/}}
{{- define "space-node-service.priorityClass" -}}
priorityClassName: system-node-critical
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
(crates/libs/observability/src/identity.rs).
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
{{ include "space-kind-telemetry.env" . }}
{{- end -}}

{{/*
ClusterApi client wiring: the HTTPS endpoint plus the pod's own projected
ServiceAccount token (audience space-cluster-api), which internal-api
authenticates by TokenReview against its allowlist
(SPACE_CLUSTER_API_AUTH_ALLOWED_SERVICE_ACCOUNTS in space-internal-api-local),
and the internal RPC trust (env.rpcTrust). Pair with the cluster-api-token and
rpc-ca volumes below.
*/}}
{{- define "space-node-service.env.clusterApi" -}}
- name: SPACE_INTERNAL_API_ENDPOINT
  value: {{ .Values.node.internalApiEndpoint | quote }}
- name: SPACE_CLUSTER_API_TOKEN_PATH
  value: /var/run/space-publication/cluster-api.token
{{ include "space-node-service.env.rpcTrust" . }}
{{- end -}}

{{/*
Internal RPC TLS trust for a ClusterApi, NodeApi or BuildManager TCP caller
(docs/internal-rpc-tls.md): the cluster's CA bundle
(internal-rpc-tls.sh's space-internal-rpc-ca, mounted where the EKS module
mounts it), and the shared NodeApi identity every node-api presents, since
callers dial node-api by node IP and its leaf carries no IP SANs. ClusterApi
needs no override: the endpoint host is the leaf's own name.
*/}}
{{- define "space-node-service.env.rpcTrust" -}}
- name: SPACE_INTERNAL_RPC_TLS_CA
  value: /etc/space-rpc-ca/ca.crt
- name: SPACE_NODE_API_TLS_SERVER_NAME
  value: space-node-api
{{- end -}}

{{/*
A TLS listener's own identity (internal-rpc-tls.sh's space-<listener>-tls
Secret, mounted where the EKS module mounts it). Pair with volume.rpcIdentity.
*/}}
{{- define "space-node-service.env.rpcIdentity" -}}
- name: SPACE_INTERNAL_RPC_TLS_CERT
  value: /etc/space-rpc-tls/tls.crt
- name: SPACE_INTERNAL_RPC_TLS_KEY
  value: /etc/space-rpc-tls/tls.key
{{- end -}}

{{/*
The control-plane credential-manager authority, for the node credential
agent and node-manager's direct sandbox volume DEK calls: its HTTPS endpoint
(SPACE_CREDMGR_ENDPOINT, crates/libs/credential/mgr/client/src/endpoint.rs),
whose host must be one of the certificate's names (the client has no
server-name override); the authority's self-signed certificate as the trust
anchor (SPACE_CREDMGR_TLS_CA, as on EKS: credmgr_authority_tls.tf); and the
pod's projected ServiceAccount token at the client's default path
(/var/run/space-auth/credmgr.token, client/src/token.rs) with the
authority's audience. The authority maps the ServiceAccount to its client
service by TokenReview. Pair with the credmgr-token volume below.
*/}}
{{- define "space-node-service.env.credmgrAuthority" -}}
- name: SPACE_CREDMGR_ENDPOINT
  value: {{ .Values.node.credmgrAuthorityEndpoint | quote }}
- name: SPACE_CREDMGR_TLS_CA
  value: /var/run/space-auth/credmgr-ca.crt
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
{{- if eq (include "space-node-service.rustBuild" .) "test-hooks" }}
- name: SPACE_NODE_DISK_ADMISSION_ALLOCATION_RESERVATION_BYTES
  value: {{ .Values.node.diskAdmission.allocationReservationBytes | quote }}
{{- end }}
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

{{- define "space-node-service.mount.rpcCa" -}}
- name: rpc-ca
  mountPath: /etc/space-rpc-ca
  readOnly: true
{{- end -}}

{{- define "space-node-service.mount.rpcIdentity" -}}
- name: rpc-identity
  mountPath: /etc/space-rpc-tls
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
          # The node services' deployed lifetime, as for the ClusterApi
          # token below.
          expirationSeconds: 600
      - secret:
          name: space-credmgr-authority-trust
          items:
            - key: ca.crt
              path: credmgr-ca.crt
{{- end -}}

{{- define "space-node-service.volume.rpcCa" -}}
- name: rpc-ca
  secret:
    secretName: space-internal-rpc-ca
    defaultMode: 0444
{{- end -}}

{{/*
`secret` is the listener's Secret. The key is readable only by the pod's
group: a non-root listener sets fsGroup to its own GID (pod securityContext),
a root listener reads it as owner.
*/}}
{{- define "space-node-service.volume.rpcIdentity" -}}
- name: rpc-identity
  secret:
    secretName: {{ .secret }}
    defaultMode: 0440
{{- end -}}

{{/*
The 600 s lifetime is the node services' deployed one; the kubelet refreshes
the file at 80% of it and the client rereads it (TOKEN_FILE_CACHE_TTL,
crates/libs/bearer-token/src/lib.rs).
*/}}
{{- define "space-node-service.volume.clusterApiToken" -}}
- name: cluster-api-token
  projected:
    defaultMode: 0444
    sources:
      - serviceAccountToken:
          path: cluster-api.token
          audience: space-cluster-api
          expirationSeconds: 600
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
by rename (crates/libs/service-mutex/src/lib.rs,
crates/libs/unix-socket-bind/src/unix_socket.rs).
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
{{ include "space-kind-telemetry.logAnnotations" .ctx }}
{{- end -}}

{{/*
Lets the service's ServiceAccount create TokenReviews, which a listener that
authenticates its callers' projected tokens issues for each one
(crates/libs/common/svc/src/auth/k8s.rs, auth/build/k8s.rs). Nothing else.
*/}}
{{- define "space-node-service.tokenReviewRbac" -}}
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: {{ include "space-node-service.name" . }}-token-review
  labels:
    {{- include "space-node-service.labels" . | nindent 4 }}
rules:
  - apiGroups: ["authentication.k8s.io"]
    resources: ["tokenreviews"]
    verbs: ["create"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: {{ include "space-node-service.name" . }}-token-review
  labels:
    {{- include "space-node-service.labels" . | nindent 4 }}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: {{ include "space-node-service.name" . }}-token-review
subjects:
  - kind: ServiceAccount
    name: {{ include "space-node-service.name" . }}
    namespace: {{ .ctx.Release.Namespace }}
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

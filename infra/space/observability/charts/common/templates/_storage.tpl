{{/*
A backend's data claim. Skipped entirely when the operator supplied an
existing claim, which the chart then never owns, or chose ephemeral storage. New shared claims carry
Helm's keep policy so an uninstall cannot discard collected data.
Call with a dict: "name" and "storage".
*/}}
{{- define "common.pvc" -}}
{{- $storage := .storage -}}
{{- if not (or $storage.existingClaim $storage.ephemeral) }}
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: {{ .name }}
  {{- if $storage.retain }}
  annotations: {helm.sh/resource-policy: keep}
  {{- end }}
spec:
  {{- if ne $storage.storageClassName nil }}
  storageClassName: {{ $storage.storageClassName | quote }}
  {{- end }}
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: {{ $storage.size | quote }}
{{- end }}
{{- end -}}

{{/* The claim a pod mounts: the operator's, or the one this chart created. */}}
{{- define "common.claimName" -}}
{{- default .name .storage.existingClaim -}}
{{- end -}}


{{/*
The data volume source a backend pod mounts: its claim, or an emptyDir when
storage.ephemeral is set. An emptyDir loses all data whenever the pod leaves
its node; it exists for clusters without any usable StorageClass.
Call with a dict: "name" and "storage".
*/}}
{{- define "common.dataVolumeSource" -}}
{{- if .storage.ephemeral -}}
emptyDir: {sizeLimit: {{ .storage.size | quote }}}
{{- else -}}
persistentVolumeClaim: {claimName: {{ include "common.claimName" . | quote }}}
{{- end -}}
{{- end -}}

{{/*
Keep optional ephemeral budgets exact without reimplementing Kubernetes' full
quantity grammar. This bound keeps conversion and Gi-to-Mi arithmetic in range.
*/}}
{{- define "common.wholeMi" -}}
{{- $raw := toString .value -}}
{{- if not (regexMatch "^[1-9][0-9]{0,8}(Mi|Gi)$" $raw) -}}
{{- fail (printf "%s must be a positive whole Mi/Gi quantity (at most nine digits) for ephemeral storage" .key) -}}
{{- end -}}
{{- if hasSuffix "Gi" $raw -}}
{{- mul (atoi (trimSuffix "Gi" $raw)) 1024 -}}
{{- else -}}
{{- atoi (trimSuffix "Mi" $raw) -}}
{{- end -}}
{{- end -}}

{{/*
Disk-backed emptyDir data shares the pod budget with logs and writable layers.
Retain 256Mi for that overhead rather than silently increasing a caller's limit.
*/}}
{{- define "common.ephemeralStorageGuard" -}}
{{- if .storage.ephemeral -}}
{{- $size := include "common.wholeMi" (dict "value" .storage.size "key" (printf "%s.size" .key)) | int64 -}}
{{- $limits := .resources.limits | default dict -}}
{{- $limitKey := printf "%s.limits.ephemeral-storage" .resourcesKey -}}
{{- $limit := include "common.wholeMi" (dict "value" (get $limits "ephemeral-storage") "key" $limitKey) | int64 -}}
{{- if lt $limit (add $size 256) -}}
{{- fail (printf "%s must cover %s.size plus 256Mi for logs and writable layers (at least %dMi)" $limitKey .key (add $size 256)) -}}
{{- end -}}
{{- end -}}
{{- end -}}

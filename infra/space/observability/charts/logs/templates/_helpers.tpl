{{/*
Fallback cluster stream label: clusterLabel, else global.observability.cluster
when installed under the space-observability umbrella.
*/}}
{{- define "logs.clusterLabel" -}}
{{- .Values.clusterLabel | default (include "common.observabilityCluster" .) -}}
{{- end -}}

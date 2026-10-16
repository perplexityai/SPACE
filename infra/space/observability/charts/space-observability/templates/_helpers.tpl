{{/* The validated mode: off, datadog or otlp. */}}
{{- define "space-observability.mode" -}}
{{- include "common.observabilityMode" . | required "global.observability.mode must be off, datadog or otlp" -}}
{{- end -}}

{{- define "space-observability.port" -}}
{{- $raw := toString .value -}}
{{- $port := atoi $raw -}}
{{- if not (and (regexMatch "^[0-9]{1,5}$" $raw) (ge $port 1) (le $port 65535)) -}}
{{- fail (printf "%s must be a port number between 1 and 65535, got %q" .key $raw) -}}
{{- end -}}
{{- $port -}}
{{- end -}}

{{- define "space-observability.labels" -}}
app.kubernetes.io/part-of: space-observability
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/* The name of the Grafana admin Secret this chart generates by default. */}}
{{- define "space-observability.grafanaAdminSecret" -}}
space-observability-grafana-admin
{{- end -}}

{{/* A namespaceSelector matching every SPACE namespace. */}}
{{- define "space-observability.spaceNamespaceSelector" -}}
namespaceSelector:
  matchExpressions:
    - key: kubernetes.io/metadata.name
      operator: In
      values: {{ toJson .Values.spaceNamespaces }}
{{- end -}}

{{/* Datadog container filter matching exactly the SPACE namespaces. */}}
{{- define "space-observability.datadogNamespaceFilter" -}}
{{- $filters := list -}}
{{- range .Values.spaceNamespaces }}{{ $filters = append $filters (printf "kube_namespace:^%s$" (regexQuoteMeta .)) }}{{ end -}}
{{- join " " $filters -}}
{{- end -}}

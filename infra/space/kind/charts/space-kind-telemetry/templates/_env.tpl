{{/* Workload identity and exporter inputs. Experiment tooling only supplies values. */}}
{{- define "space-kind-telemetry.env" -}}
- name: DD_ENV
  value: {{ .Values.datadogEnvironment | default "kind-local" | quote }}
- name: DD_VERSION
  value: {{ .Values.telemetry.version | default .Values.image.tag | quote }}
- name: SPACE_ENV
  value: {{ .Values.datadogEnvironment | default "kind-local" | quote }}
- name: SPACE_VERSION
  value: {{ .Values.telemetry.version | default .Values.image.tag | quote }}
- name: DD_TAGS
  value: {{ cat (printf "kind_cluster:%s" .Values.telemetry.cluster) (ternary (printf ",kind_owner:%s" .Values.telemetry.owner) "" (ne .Values.telemetry.owner "")) (ternary (printf ",kind_phase:%s" .Values.telemetry.phase) "" (ne .Values.telemetry.phase "")) | nospace | quote }}
- name: OTEL_RESOURCE_ATTRIBUTES
  value: {{ cat (printf "deployment.environment.name=%s,service.version=%s,k8s.cluster.name=%s" (.Values.datadogEnvironment | default "kind-local") (.Values.telemetry.version | default .Values.image.tag) .Values.telemetry.cluster) (ternary (printf ",kind.owner=%s" .Values.telemetry.owner) "" (ne .Values.telemetry.owner "")) (ternary (printf ",kind.phase=%s" .Values.telemetry.phase) "" (ne .Values.telemetry.phase "")) | nospace | quote }}
- name: DD_TRACE_ENABLED
  value: "true"
- name: DD_AGENT_HOST
  valueFrom:
    fieldRef:
      fieldPath: status.hostIP
- name: DD_DOGSTATSD_PORT
  value: "8125"
- name: SPACE_METRICS_SINK
  value: {{ .Values.telemetry.metricsSink | quote }}
- name: SPACE_TRACE_EXPORTER
  value: {{ .Values.telemetry.traceExporter | quote }}
{{- if eq .Values.telemetry.metricsSink "otlp" }}
- name: OTEL_EXPORTER_OTLP_METRICS_ENDPOINT
  value: {{ .Values.telemetry.otlpMetricsEndpoint | quote }}
- name: OTEL_EXPORTER_OTLP_METRICS_PROTOCOL
  value: {{ .Values.telemetry.otlpProtocol | quote }}
{{- with .Values.telemetry.otlpMetricsTemporality }}
- name: OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE
  value: {{ . | quote }}
{{- end }}
{{- end }}
{{- if eq .Values.telemetry.traceExporter "otlp" }}
- name: OTEL_EXPORTER_OTLP_TRACES_ENDPOINT
  value: {{ .Values.telemetry.otlpTracesEndpoint | quote }}
- name: OTEL_EXPORTER_OTLP_TRACES_PROTOCOL
  value: {{ .Values.telemetry.otlpProtocol | quote }}
{{- end }}
{{- with .Values.telemetry.traceSampler }}
- name: OTEL_TRACES_SAMPLER
  value: {{ . | quote }}
{{- end }}
{{- end -}}

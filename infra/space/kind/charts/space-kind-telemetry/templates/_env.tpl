{{/*
Workload identity and exporter inputs for one SPACE container.

Two input paths, chosen by whether `global.observability` is set:

- Absent or null (every Kind bring-up): the Kind profile's `telemetry` values,
  below in `space-kind-telemetry.env.kind`, exactly as before.
- Present: the cluster path, `space-kind-telemetry.env.cluster`, driven only by
  `global.observability` (README). Of the `telemetry` values only `version`
  and a trace-exporter veto (`traceExporter: off`) apply there.

Context: the chart root, or any dict whose `Values` carries `telemetry`,
`image` and optionally `global`.
*/}}
{{- define "space-kind-telemetry.env" -}}
{{- if (include "space-kind-telemetry.observability" . | fromJson).enabled -}}
{{ include "space-kind-telemetry.env.cluster" . }}
{{- else -}}
{{ include "space-kind-telemetry.env.kind" . }}
{{- end -}}
{{- end -}}

{{/* Kind profile inputs. Experiment tooling only supplies values. */}}
{{- define "space-kind-telemetry.env.kind" -}}
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
{{- with .Values.telemetry.otlpMetricExportIntervalMillis }}
- name: OTEL_METRIC_EXPORT_INTERVAL
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

{{/*
`global.observability` validated, with defaults applied, as JSON. `enabled` is
false when the key is absent or null. Fails the render on any value a producer
would reject or misroute, so a bad setting never reaches an install.
*/}}
{{- define "space-kind-telemetry.observability" -}}
{{- $global := .Values.global | default dict -}}
{{- if or (not (hasKey $global "observability")) (kindIs "invalid" $global.observability) -}}
{{- dict "enabled" false | toJson -}}
{{- else -}}
{{- $in := $global.observability -}}
{{- if not (kindIs "map" $in) -}}
{{- fail "global.observability must be a map with at least mode: off, datadog or otlp" -}}
{{- end -}}
{{- $o := mergeOverwrite (dict
    "mode" ""
    "environment" ""
    "cluster" ""
    "datadog" (dict "dogstatsdPort" 8125 "apmPort" 8126)
    "otlp" (dict "protocol" "grpc" "nodeLocalPort" 4317 "traceSampler" "")) (deepCopy $in) -}}
{{- /* YAML 1.1 reads a bare `off` as false. */ -}}
{{- $mode := ternary "off" (toString $o.mode) (and (kindIs "bool" $o.mode) (not $o.mode)) -}}
{{- if not (has $mode (list "off" "datadog" "otlp")) -}}
{{- fail (printf "global.observability.mode must be off, datadog or otlp (got %q)" $mode) -}}
{{- end -}}
{{- range $key := list "environment" "cluster" -}}
{{- $value := get $o $key -}}
{{- if not (kindIs "string" $value) -}}
{{- fail (printf "global.observability.%s must be a string" $key) -}}
{{- end -}}
{{- /* Both are joined into the comma-separated OTEL_RESOURCE_ATTRIBUTES and DD_TAGS. */ -}}
{{- if not (regexMatch "^[A-Za-z0-9._:/-]*$" $value) -}}
{{- fail (printf "global.observability.%s may contain only letters, digits and . _ : / - (got %q)" $key $value) -}}
{{- end -}}
{{- if and (ne $mode "off") (eq $value "") -}}
{{- fail (printf "global.observability.%s is required when mode is %s" $key $mode) -}}
{{- end -}}
{{- end -}}
{{- $ports := dict -}}
{{- range $path := list "datadog.dogstatsdPort" "datadog.apmPort" "otlp.nodeLocalPort" -}}
{{- $parts := splitList "." $path -}}
{{- $raw := get (get $o (first $parts)) (last $parts) -}}
{{- $text := toString $raw -}}
{{- if or (not (regexMatch "^[0-9]{1,5}$" $text)) (lt (atoi $text) 1) (gt (atoi $text) 65535) -}}
{{- fail (printf "global.observability.%s must be a port number from 1 to 65535 (got %v)" $path $raw) -}}
{{- end -}}
{{- $_ := set $ports $path (atoi $text) -}}
{{- end -}}
{{- /* The shim and host sidecars in the current producer images use 8126.
       Do not advertise a port override that only reaches pod exporters. */ -}}
{{- if and (eq $mode "datadog") (ne (get $ports "datadog.apmPort") 8126) -}}
{{- fail "global.observability.datadog.apmPort must be 8126 until all host producers support a configurable APM port" -}}
{{- end -}}
{{- /* The Rust metrics sink and the shim export OTLP over gRPC only
       (crates/libs/observability/src/sink/otlp.rs, shim/otlp.go). */ -}}
{{- if ne (toString $o.otlp.protocol) "grpc" -}}
{{- fail (printf "global.observability.otlp.protocol must be grpc (got %q)" (toString $o.otlp.protocol)) -}}
{{- end -}}
{{- if not (kindIs "string" $o.otlp.traceSampler) -}}
{{- fail "global.observability.otlp.traceSampler must be a string" -}}
{{- end -}}
{{- dict
    "enabled" true
    "mode" $mode
    "environment" $o.environment
    "cluster" $o.cluster
    "dogstatsdPort" (get $ports "datadog.dogstatsdPort")
    "apmPort" (get $ports "datadog.apmPort")
    "nodeLocalPort" (get $ports "otlp.nodeLocalPort")
    "protocol" $o.otlp.protocol
    "traceSampler" $o.otlp.traceSampler | toJson -}}
{{- end -}}
{{- end -}}

{{/*
Cluster path. One mode for every signal:

  off      SPACE_METRICS_SINK and SPACE_TRACE_EXPORTER off, DD_TRACE_ENABLED
           false, no Agent address and no endpoint.
  datadog  The Datadog sinks, to the Agent on the pod's node (status.hostIP)
           at the configured DogStatsD port and APM port 8126.
  otlp     OTLP/gRPC to a Collector on the pod's node,
           http://$(NODE_IP):<nodeLocalPort>, with cumulative metric
           temporality for the Rust services. NODE_IP precedes the endpoints
           because the kubelet expands $(NODE_IP) only from earlier entries.

A chart whose telemetry.traceExporter is off (build-manager) keeps traces off
in every mode: SPACE_TRACE_EXPORTER off and no trace endpoint, sampler or APM
port (DD_TRACE_ENABLED false where it is emitted, in off and datadog). Its
metrics follow the mode.
*/}}
{{- define "space-kind-telemetry.env.cluster" -}}
{{- $o := include "space-kind-telemetry.observability" . | fromJson -}}
{{- $version := toString (.Values.telemetry.version | default .Values.image.tag) -}}
{{- /* A chart's own telemetry.traceExporter=off still vetoes traces (build-manager
       keeps them off because build errors can carry guest output); any other
       value defers to the mode. Bare YAML off reads as false. */ -}}
{{- $chartTrace := .Values.telemetry.traceExporter -}}
{{- $traceOff := or (eq $o.mode "off") (eq (toString $chartTrace) "off") (and (kindIs "bool" $chartTrace) (not $chartTrace)) -}}
{{- $traces := ternary "off" $o.mode $traceOff -}}
{{- $attributes := list -}}
{{- with $o.environment -}}{{- $attributes = append $attributes (printf "deployment.environment.name=%s" .) -}}{{- end -}}
{{- with $version -}}{{- $attributes = append $attributes (printf "service.version=%s" .) -}}{{- end -}}
{{- with $o.cluster -}}{{- $attributes = append $attributes (printf "k8s.cluster.name=%s" .) -}}{{- end -}}
{{- with $o.environment }}
- name: SPACE_ENV
  value: {{ . | quote }}
{{- end }}
- name: SPACE_VERSION
  value: {{ $version | quote }}
{{- if $attributes }}
- name: OTEL_RESOURCE_ATTRIBUTES
  value: {{ join "," $attributes | quote }}
{{- end }}
- name: SPACE_METRICS_SINK
  value: {{ $o.mode | quote }}
- name: SPACE_TRACE_EXPORTER
  value: {{ $traces | quote }}
{{- if eq $o.mode "off" }}
- name: DD_TRACE_ENABLED
  value: "false"
{{- else if eq $o.mode "datadog" }}
- name: DD_ENV
  value: {{ $o.environment | quote }}
- name: DD_VERSION
  value: {{ $version | quote }}
- name: DD_TAGS
  value: {{ printf "kube_cluster_name:%s" $o.cluster | quote }}
- name: DD_TRACE_ENABLED
  value: {{ ternary "false" "true" $traceOff | quote }}
- name: DD_AGENT_HOST
  valueFrom:
    fieldRef:
      fieldPath: status.hostIP
- name: DD_DOGSTATSD_PORT
  value: {{ $o.dogstatsdPort | int | toString | quote }}
{{- if not $traceOff }}
- name: DD_TRACE_AGENT_PORT
  value: {{ $o.apmPort | int | toString | quote }}
{{- end }}
{{- else }}
- name: NODE_IP
  valueFrom:
    fieldRef:
      fieldPath: status.hostIP
- name: OTEL_EXPORTER_OTLP_METRICS_ENDPOINT
  value: {{ printf "http://$(NODE_IP):%d" ($o.nodeLocalPort | int) | quote }}
- name: OTEL_EXPORTER_OTLP_METRICS_PROTOCOL
  value: {{ $o.protocol | quote }}
- name: OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE
  value: "cumulative"
{{- if not $traceOff }}
- name: OTEL_EXPORTER_OTLP_TRACES_ENDPOINT
  value: {{ printf "http://$(NODE_IP):%d" ($o.nodeLocalPort | int) | quote }}
- name: OTEL_EXPORTER_OTLP_TRACES_PROTOCOL
  value: {{ $o.protocol | quote }}
{{- with $o.traceSampler }}
- name: OTEL_TRACES_SAMPLER
  value: {{ . | quote }}
{{- end }}
{{- end }}
{{- end }}
{{- end -}}

{{/*
Names `space-kind-telemetry.env` sets for the selected path and mode,
space-separated, for writers that copy a container's resolved telemetry
environment to a host process (node-manager's kind-node-env). NODE_IP is an
input to the endpoints, not a producer setting, and is left out.
*/}}
{{- define "space-kind-telemetry.envNames" -}}
{{- $names := list -}}
{{- range (include "space-kind-telemetry.env" . | fromYamlArray) -}}
{{- if ne .name "NODE_IP" -}}
{{- $names = append $names .name -}}
{{- end -}}
{{- end -}}
{{- join " " $names -}}
{{- end -}}

{{/*
Every name a host environment writer owns on the global.observability path:
the names `space-kind-telemetry.env` sets in any mode (with a trace sampler),
plus the Kind profile's endpoint names, so switching mode clears what the
previous one wrote. Space-separated.
*/}}
{{- define "space-kind-telemetry.ownedEnvNames" -}}
{{- $names := list "SPACE_CLUSTER_ID" "SPACE_K8S_NODE_NAME" "DD_TRACE_AGENT_PORT" "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT" "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT" "OTEL_TRACES_SAMPLER" -}}
{{- range $mode := list "off" "datadog" "otlp" -}}
{{- $v := deepCopy $.Values -}}
{{- /* Names only: a placeholder identity keeps mode off valid without one. */ -}}
{{- $o := mergeOverwrite (dict "environment" "names" "cluster" "names") (deepCopy $v.global.observability) (dict "mode" $mode) -}}
{{- if not $o.environment -}}{{- $_ := set $o "environment" "names" -}}{{- end -}}
{{- if not $o.cluster -}}{{- $_ := set $o "cluster" "names" -}}{{- end -}}
{{- /* Enumerating names must not activate an unused APM override in off/otlp. */ -}}
{{- $_ := set $o "datadog" (mergeOverwrite (deepCopy ($o.datadog | default dict)) (dict "apmPort" 8126)) -}}
{{- $_ := set $o "otlp" (mergeOverwrite (deepCopy ($o.otlp | default dict)) (dict "traceSampler" "always_on")) -}}
{{- $_ = set $v.global "observability" $o -}}
{{- $_ = set $v "telemetry" (mergeOverwrite (deepCopy ($v.telemetry | default dict)) (dict "traceExporter" "")) -}}
{{- range (splitList " " (include "space-kind-telemetry.envNames" (dict "Values" $v))) -}}
{{- $names = append $names . -}}
{{- end -}}
{{- end -}}
{{- join " " (uniq $names) -}}
{{- end -}}

{{/* The profile owns only SPACE workload collection, not a foreign Agent. */}}
{{- define "space-kind-telemetry.logAnnotations" -}}
{{- if eq (toString .Values.telemetry.excludeDatadogLogs) "true" }}
ad.datadoghq.com/logs_exclude: "true"
{{- end }}
{{- end -}}

{{/*
The SPACE observability mode, read from global.observability.mode. Returns
"" when no global mode is set, which is how every chart here is installed on
its own. YAML 1.1 reads an unquoted `off` as boolean false, so false is
accepted as "off"; any other value fails the render.
*/}}
{{- define "common.observabilityMode" -}}
{{- $global := .Values.global | default dict -}}
{{- $observability := get $global "observability" | default dict -}}
{{- if hasKey $observability "mode" -}}
{{- $raw := get $observability "mode" -}}
{{- $mode := toString $raw -}}
{{- if and (kindIs "bool" $raw) (not $raw) -}}{{- $mode = "off" -}}{{- end -}}
{{- if not (has $mode (list "off" "datadog" "otlp")) -}}
{{- fail (printf "global.observability.mode must be off, datadog or otlp, got %q" $mode) -}}
{{- end -}}
{{- $mode -}}
{{- end -}}
{{- end -}}

{{/*
Whether this OTLP backend chart renders. Standalone installs (no global mode)
always render; under the space-observability umbrella only otlp mode does.
Returns "true" or "".
*/}}
{{- define "common.otlpSelected" -}}
{{- $mode := include "common.observabilityMode" . -}}
{{- if has $mode (list "" "otlp") }}true{{ end -}}
{{- end -}}

{{/*
The cluster name from global.observability.cluster, or "".
*/}}
{{- define "common.observabilityCluster" -}}
{{- $global := .Values.global | default dict -}}
{{- $observability := get $global "observability" | default dict -}}
{{- get $observability "cluster" | default "" -}}
{{- end -}}

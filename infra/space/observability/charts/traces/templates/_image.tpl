{{/*
The Collector digest is shared with the metrics chart and single-sourced in
charts/common. An explicit images.<name> overrides it for this chart only.
*/}}
{{- define "traces.image" -}}
{{- $root := .root -}}
{{- $override := index $root.Values.images .name -}}
{{- if $override }}{{ $override }}{{ else }}{{ (index $root.Values "common").images.collector }}{{ end -}}
{{- end -}}

{{/*
Components this release runs. collector.enabled=false leaves Tempo for an
external Collector labelled app=collector in this namespace, which is how the
space-observability umbrella's single gateway Collector feeds it.
*/}}
{{- define "traces.components" -}}
{{- if .Values.collector.enabled }}["collector","tempo"]{{ else }}["tempo"]{{ end -}}
{{- end -}}

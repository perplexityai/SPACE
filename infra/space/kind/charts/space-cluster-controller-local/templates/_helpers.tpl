{{/*
The control plane's substrate (`substrate`): `kubernetes`, the default, or
`direct`. Any other value fails the render.
*/}}
{{- define "space-cluster-controller-local.substrate" -}}
{{- $substrate := .Values.substrate | default "kubernetes" -}}
{{- if not (has $substrate (list "kubernetes" "direct")) -}}
{{- fail (printf "substrate must be kubernetes or direct, got %q" $substrate) -}}
{{- end -}}
{{- $substrate -}}
{{- end -}}

{{/*
SPACE_K8S_CONTROL_PLANE for `substrate`, as space-api-local renders it: the
controller builds the same substrate as the API (Substrate::from_env,
crates/libs/cluster/controller/server/src/lib.rs).
*/}}
{{- define "space-cluster-controller-local.k8sControlPlane" -}}
{{- eq (include "space-cluster-controller-local.substrate" .) "kubernetes" | ternary "true" "false" | quote -}}
{{- end -}}

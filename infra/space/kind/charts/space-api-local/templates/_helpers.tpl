{{/*
Env for the migrate Job's bootstrap-roles steps. The dev-default passwords are the
same ones the service containers use to connect as space_user / space_creds;
superadmin/ro/migrator exist only inside the kind Postgres and are never
consumed by a service.
*/}}
{{- define "space-api-local.bootstrapRolesEnv" -}}
- name: POSTGRES_CONNECTION_STRING
  valueFrom:
    secretKeyRef:
      name: {{ .Values.postgres.secretName | default "space-postgres" }}
      key: POSTGRES_CONNECTION_STRING
- name: SPACE_DB_BOOTSTRAP_SUPERADMIN_PASSWORD
  {{- include "space-cluster.dbPassword" (dict "ctx" . "role" "SUPERADMIN" "value" .Values.postgres.bootstrapSuperadminPassword) | nindent 2 }}
- name: SPACE_DB_BOOTSTRAP_USER_PASSWORD
  {{- include "space-cluster.dbPassword" (dict "ctx" . "role" "USER" "value" .Values.postgres.managedUserPassword) | nindent 2 }}
- name: SPACE_DB_BOOTSTRAP_RO_PASSWORD
  {{- include "space-cluster.dbPassword" (dict "ctx" . "role" "RO" "value" .Values.postgres.bootstrapReadOnlyPassword) | nindent 2 }}
- name: SPACE_DB_BOOTSTRAP_MIGRATOR_PASSWORD
  {{- include "space-cluster.dbPassword" (dict "ctx" . "role" "MIGRATOR" "value" .Values.postgres.bootstrapMigratorPassword) | nindent 2 }}
- name: SPACE_DB_BOOTSTRAP_CREDS_PASSWORD
  {{- include "space-cluster.dbPassword" (dict "ctx" . "role" "CREDS" "value" .Values.postgres.managedCredentialsPassword) | nindent 2 }}
{{- end }}

{{/*
SPACE_K8S_CONTROL_PLANE for `substrate`: "true" runs the KubernetesSubstrate,
"false" the DirectSubstrate (Substrate::from_env_with_lifecycle in
crates/libs/api/src/controllers/substrate/mod.rs; the flag is read by
KubernetesState::try_from_env in substrate/kubernetes/mod.rs). Any other
substrate fails the render.
*/}}
{{- define "space-api-local.k8sControlPlane" -}}
{{- $substrate := .Values.substrate | default "kubernetes" -}}
{{- if not (has $substrate (list "kubernetes" "direct")) -}}
{{- fail (printf "substrate must be kubernetes or direct, got %q" $substrate) -}}
{{- end -}}
{{- eq $substrate "kubernetes" | ternary "true" "false" | quote -}}
{{- end -}}

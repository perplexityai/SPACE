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

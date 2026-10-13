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
      name: space-postgres
      key: POSTGRES_CONNECTION_STRING
- name: SPACE_DB_BOOTSTRAP_SUPERADMIN_PASSWORD
  value: {{ .Values.postgres.bootstrapSuperadminPassword | quote }}
- name: SPACE_DB_BOOTSTRAP_USER_PASSWORD
  value: {{ .Values.postgres.managedUserPassword | quote }}
- name: SPACE_DB_BOOTSTRAP_RO_PASSWORD
  value: {{ .Values.postgres.bootstrapReadOnlyPassword | quote }}
- name: SPACE_DB_BOOTSTRAP_MIGRATOR_PASSWORD
  value: {{ .Values.postgres.bootstrapMigratorPassword | quote }}
- name: SPACE_DB_BOOTSTRAP_CREDS_PASSWORD
  value: {{ .Values.postgres.managedCredentialsPassword | quote }}
{{- end }}

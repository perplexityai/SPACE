{{/*
Value source for one managed database role password, for the charts that
connect as a managed role or create them (space-api-local's migrate Job).
Kind keeps the inline development default; a deployment outside Kind sets
postgres.rolesSecretName and every password comes from that Secret, under the
key the migrate Job's bootstrap step reads: SPACE_DB_BOOTSTRAP_<ROLE>_PASSWORD.

Context: dict
  ctx    the chart root
  role   SUPERADMIN, USER, RO, MIGRATOR or CREDS
  value  the Kind default
Renders `value: ...` or `valueFrom: ...` for an EnvVar; use with nindent.
*/}}
{{- define "space-cluster.dbPassword" -}}
{{- $secret := .ctx.Values.postgres.rolesSecretName | default "" -}}
{{- if $secret -}}
valueFrom:
  secretKeyRef:
    name: {{ $secret }}
    key: SPACE_DB_BOOTSTRAP_{{ .role }}_PASSWORD
{{- else -}}
value: {{ .value | quote }}
{{- end -}}
{{- end -}}

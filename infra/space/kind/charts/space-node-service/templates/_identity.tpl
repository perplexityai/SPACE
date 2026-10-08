{{/*
Unix identities, read from infra/space/identity/node-identities.sh — the file
packer, the worker image build and direct-node tooling source — through the
`identity/node-identities.sh` link each service chart carries. Reading the
file instead of restating its numbers keeps the pods' UIDs, GIDs and
supplementary groups exactly the ones the image's /etc/passwd and the node
layer create.
*/}}

{{- define "space-node-service.identities" -}}
{{- $ids := .Files.Get "identity/node-identities.sh" -}}
{{- if not $ids -}}
{{- fail "identity/node-identities.sh is missing: link it to infra/space/identity/node-identities.sh" -}}
{{- end -}}
{{- $ids -}}
{{- end -}}

{{/* The number assigned to NAME (e.g. SPACE_SBXMGR_UID) in the identities file. */}}
{{- define "space-node-service.identityNumber" -}}
{{- $ids := include "space-node-service.identities" .ctx -}}
{{- $line := regexFind (printf "(?m)^%s=[0-9]+$" .name) $ids -}}
{{- if not $line -}}
{{- fail (printf "%s is not defined in node-identities.sh" .name) -}}
{{- end -}}
{{- trimPrefix (printf "%s=" .name) $line -}}
{{- end -}}

{{/* The GID SPACE_GROUP_SPECS assigns to group NAME. */}}
{{- define "space-node-service.groupId" -}}
{{- $ids := include "space-node-service.identities" .ctx -}}
{{- $line := regexFind (printf "(?m)^%s:[0-9]+$" .name) $ids -}}
{{- if not $line -}}
{{- fail (printf "group %s is not in SPACE_GROUP_SPECS" .name) -}}
{{- end -}}
{{- trimPrefix (printf "%s:" .name) $line -}}
{{- end -}}

{{/*
The group names in list variable NAME (e.g. SPACE_SANDBOX_MANAGER_GROUPS),
space-separated, with `$OTHER_LIST` references expanded the way the shell
expands them.
*/}}
{{- define "space-node-service.groupNames" -}}
{{- $ids := include "space-node-service.identities" .ctx -}}
{{- $line := regexFind (printf "(?m)^%s=\"[^\"]*\"$" .name) $ids -}}
{{- if not $line -}}
{{- fail (printf "%s is not defined in node-identities.sh" .name) -}}
{{- end -}}
{{- $names := list -}}
{{- range (trimPrefix (printf "%s=" .name) $line | trimAll "\"" | splitList " ") -}}
{{- if hasPrefix "$" . -}}
{{- $names = concat $names (include "space-node-service.groupNames" (dict "ctx" $.ctx "name" (trimPrefix "$" .)) | splitList " ") -}}
{{- else if . -}}
{{- $names = append $names . -}}
{{- end -}}
{{- end -}}
{{- join " " $names -}}
{{- end -}}

{{/*
Pod securityContext fields for a service account: `user` is the alias prefix
(SPACE_SBXMGR → SPACE_SBXMGR_UID / _GID) and `groups` the supplementary list
variable. Mirrors `setpriv --reuid --regid --groups` from the systemd units and
the image's launcher; runAsNonRoot makes the kubelet refuse a UID-0 override.
*/}}
{{- define "space-node-service.runAs" -}}
runAsNonRoot: true
runAsUser: {{ include "space-node-service.identityNumber" (dict "ctx" .ctx "name" (printf "%s_UID" .user)) }}
runAsGroup: {{ include "space-node-service.identityNumber" (dict "ctx" .ctx "name" (printf "%s_GID" .user)) }}
supplementalGroups:
{{- range (include "space-node-service.groupNames" (dict "ctx" .ctx "name" .groups) | splitList " ") }}
  - {{ include "space-node-service.groupId" (dict "ctx" $.ctx "name" .) }}
{{- end }}
{{- end -}}

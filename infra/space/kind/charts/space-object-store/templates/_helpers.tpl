{{/*
The vend role's trust policy: AssumeRoleWithWebIdentity for tokens the STS
bridge's issuer signed for the vend identity's subject and the bridge's
audience, as an EKS role trusts a service account's OIDC subject.
*/}}
{{- define "space-object-store.trustPolicy" -}}
{{- $x := .Values.storageExchange -}}
{{- dict "Version" "2012-10-17" "Statement" (list (dict
  "Effect" "Allow"
  "Principal" (dict "Federated" (printf "arn:aws:iam::000000000000:oidc-provider/%s" $x.issuerHost))
  "Action" "sts:AssumeRoleWithWebIdentity"
  "Condition" (dict "StringEquals" (dict (printf "%s:sub" $x.issuerHost) $x.subject (printf "%s:aud" $x.issuerHost) $x.audience)))) | toJson -}}
{{- end -}}

{{/*
The vend role's own policy, the ceiling of every session: the actions the
authority's exact-object session policies grant
(AuthorizedExchange::session_policy, crates/libs/credential/mgr/server/src/storage_exchange.rs),
on the two buckets' objects and listings.
*/}}
{{- define "space-object-store.rolePolicy" -}}
{{- $objects := list (printf "arn:aws:s3:::%s/*" .Values.snapshotBucket) (printf "arn:aws:s3:::%s/*" .Values.templateBucket) -}}
{{- $buckets := list (printf "arn:aws:s3:::%s" .Values.snapshotBucket) (printf "arn:aws:s3:::%s" .Values.templateBucket) -}}
{{- dict "Version" "2012-10-17" "Statement" (list
  (dict "Effect" "Allow" "Action" (list "s3:GetObject" "s3:PutObject" "s3:AbortMultipartUpload") "Resource" $objects)
  (dict "Effect" "Allow" "Action" (list "s3:ListBucket") "Resource" $buckets)) | toJson -}}
{{- end -}}

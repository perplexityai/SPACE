output "ready" {
  description = "True after the External Secrets Operator release is ready for dependent ExternalSecret resources."
  value       = true

  # Callers only need the External Secrets CRDs before they install their own
  # SecretStore and ExternalSecret objects. Keeping that edge on this output
  # avoids making them depend on every unrelated controller in this module.
  depends_on = [helm_release.external_secrets_operator]
}

output "cluster_autoscaler_discovery_tag_key" {
  description = "The ASG tag key cluster-autoscaler discovers its node groups by. An ASG that carries it is one cluster-autoscaler may still scale."
  value       = local.cluster_autoscaler_discovery_tag_key
}

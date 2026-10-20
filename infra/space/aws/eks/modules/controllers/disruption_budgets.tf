locals {
  # The helm provider diffs a release's `values`, not the contents of a local
  # chart path, so without this a template edit produces no plan diff and never
  # reaches the cluster. The chart ignores the key; its only job is to change.
  # Same pattern as modules/app_deploy.
  disruption_budget_chart_checksum = sha256(join("", [
    for path in sort(fileset(var.disruption_budget_chart_path, "**")) :
    filesha256("${var.disruption_budget_chart_path}/${path}")
  ]))

  # The webhook's budget comes from its chart: with two replicas, an unready one
  # still leaves a healthy count the eviction API can spend. With both unready
  # the default policy refuses evictions until one recovers, because the 0.16.1
  # chart can't render `unhealthyPodEvictionPolicy`. The chart's default
  # `minAvailable: 1` is cleared, since the chart renders each of the two fields
  # that's set and the API rejects a budget with both.
  external_secrets_webhook_disruption_budget = {
    enabled        = true
    minAvailable   = null
    maxUnavailable = 1
  }

  # Single-replica controllers whose own charts can't render
  # `unhealthyPodEvictionPolicy`. Under the default policy the eviction API
  # lets an unready pod go only while the budget's desired-healthy count is
  # above zero, and one replica at `maxUnavailable: 1` desires none, so a
  # crashlooping singleton would block every drain of its node, a node-group
  # roll included.
  # Their budgets come from the in-repo chart instead, which sets `AlwaysAllow`,
  # and their own charts render none. Each selector names the owning release,
  # which also orders the budget after it: the release's upgrade removes any
  # budget its chart used to render before this one installs.
  singleton_disruption_budgets = merge(
    {
      external-secrets = {
        "app.kubernetes.io/name"     = "external-secrets"
        "app.kubernetes.io/instance" = helm_release.external_secrets_operator.name
      }
      external-secrets-cert-controller = {
        "app.kubernetes.io/name"     = "external-secrets-cert-controller"
        "app.kubernetes.io/instance" = helm_release.external_secrets_operator.name
      }
    },
    var.cluster_autoscaler == null ? {} : {
      # The chart names its pods after the cloud provider, not the release.
      cluster-autoscaler = {
        "app.kubernetes.io/name"     = "aws-cluster-autoscaler"
        "app.kubernetes.io/instance" = helm_release.cluster_autoscaler[0].name
      }
    },
  )

  # Preferred, not required: two webhook replicas on one node don't stop
  # cluster-autoscaler from removing it. The planner marks such a node risky and
  # tries it last, and the refused second eviction is retried for up to
  # --max-pod-eviction-time while the first replacement becomes Ready. A
  # required rule would instead leave a rollout's surge pod Pending on a
  # two-node pool, where both nodes already hold a replica.
  external_secrets_webhook_affinity = {
    podAntiAffinity = {
      preferredDuringSchedulingIgnoredDuringExecution = [{
        weight = 100
        podAffinityTerm = {
          topologyKey = "kubernetes.io/hostname"
          labelSelector = {
            matchLabels = {
              "app.kubernetes.io/name"     = "external-secrets-webhook"
              "app.kubernetes.io/instance" = "external-secrets"
            }
          }
        }
      }]
    }
  }
}

# external-dns runs one replica and its chart renders no PodDisruptionBudget, so
# the budget comes from a chart of its own. `maxUnavailable: 1` on one replica
# allows its eviction whenever it's Ready, and the chart's `AlwaysAllow`
# unhealthy-pod policy allows it when it isn't. An eviction only delays record
# changes until the replacement starts: external-dns rebuilds its view from the
# Ingresses and the zone's TXT registry on every sync.
resource "helm_release" "external_dns_disruption_budget" {
  count = var.external_dns_enabled ? 1 : 0

  name      = "external-dns-pdb"
  chart     = var.disruption_budget_chart_path
  namespace = helm_release.external_dns[0].namespace

  values = [yamlencode({
    chart = {
      checksum = local.disruption_budget_chart_checksum
    }
    name = "external-dns"
    # The chart's selector labels: its own name and the release's.
    matchLabels = {
      "app.kubernetes.io/name"     = helm_release.external_dns[0].chart
      "app.kubernetes.io/instance" = helm_release.external_dns[0].name
    }
    maxUnavailable = 1
  })]
}

# Named `<workload>-singleton` so none collides with a budget the workload's own
# chart rendered before.
resource "helm_release" "singleton_disruption_budget" {
  for_each = local.singleton_disruption_budgets

  name      = "${each.key}-pdb"
  chart     = var.disruption_budget_chart_path
  namespace = "kube-system"

  values = [yamlencode({
    chart = {
      checksum = local.disruption_budget_chart_checksum
    }
    name           = "${each.key}-singleton"
    matchLabels    = each.value
    maxUnavailable = 1
  })]
}

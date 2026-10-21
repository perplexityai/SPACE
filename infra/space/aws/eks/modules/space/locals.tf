locals {

  # Physical cluster identity, decoupled from `environment`. `environment`
  # stays the observability/posture selector (the shim's DD_ENV, the
  # Environment tag); `cluster_id` names the cluster. Defaults to
  # `environment`, so a cluster that is its environment's only one needs no
  # cluster_id and is named `${name_prefix}-${environment}`.
  cluster_id = coalesce(var.cluster_id, var.environment)
  name       = "${var.name_prefix}-${local.cluster_id}"

  # An ASG's IAM resource ARN is this prefix plus its name. The `*` stands in
  # for the per-ASG UUID, which AWS assigns at creation time and isn't part of
  # the human-readable name.
  autoscaling_group_arn_prefix = "arn:${data.aws_partition.current.partition}:autoscaling:${var.aws_region}:${data.aws_caller_identity.current.account_id}:autoScalingGroup:*:autoScalingGroupName/"

  azs = var.availability_zones


  worker_common_tags = merge(var.common_tags, {
    Team = "team-space"
  })

  public_ingress_alb_name = "${local.name}-public"

  s3_read_bucket_arns                  = [for name in var.s3_read_bucket_names : "arn:${data.aws_partition.current.partition}:s3:::${name}"]
  s3_read_object_arns                  = [for name in var.s3_read_bucket_names : "arn:${data.aws_partition.current.partition}:s3:::${name}/*"]
  template_artifacts_bucket_arn        = "arn:${data.aws_partition.current.partition}:s3:::${var.template_artifacts_bucket}"
  template_artifacts_templates_objects = "${local.template_artifacts_bucket_arn}/templates/*"
  template_artifacts_build_inputs      = "${local.template_artifacts_bucket_arn}/build-inputs/*"
  snapshots_bucket_arn                 = "arn:${data.aws_partition.current.partition}:s3:::${var.snapshots_bucket}"
  snapshots_bucket_objects             = "${local.snapshots_bucket_arn}/*"
  workers_ami_id                       = var.workers_ami_id != null ? var.workers_ami_id : try(data.aws_ami.workers[0].id, null)
  builders_ami_id                      = var.builders_ami_id != null ? var.builders_ami_id : try(data.aws_ami.builders[0].id, null)
  capacity_reservation_pool_arns       = var.capacity_reservation_pool_arns
  space_capacity_reservation_pool_arn  = try(local.capacity_reservation_pool_arns["space"], null)
  # Resolved capacity-reservation resource-group ARN for the worker LT.
  # Mirrors the logic in `local.workers_cgnat_pool` so the standalone
  # `aws_launch_template.workers` (which doesn't iterate over
  # `local.node_pools`) picks up the same fallback to the SPACE-wide
  # pool when the explicit override is null.
  workers_capacity_reservation_resource_group_arn = var.workers_capacity_reservation_resource_group_arn != null ? var.workers_capacity_reservation_resource_group_arn : local.space_capacity_reservation_pool_arn
  private_subnet_ids = [
    for az in local.azs : data.aws_subnet.private[az].id
  ]
  existing_mng_default_pool_names = [
    for name, config in var.worker_subnet_pools : name if config.default_for_existing_mngs
  ]
  worker_capacity_subnet_pool_names = toset(compact([
    for group in values(var.worker_capacity_groups) : group.subnet_pool
  ]))
  worker_subnet_pool_names_in_use = setunion(
    toset(local.existing_mng_default_pool_names),
    local.worker_capacity_subnet_pool_names,
  )
  # Keep the full pool inventory in tfvars so it can be checked against the VPC
  # stack, but query AWS only for pools selected by an MNG. This lets a VPC
  # change stage a dormant pool without making the EKS plan depend on subnet
  # tags that do not exist until that VPC change is applied. Selecting a pool
  # for any MNG activates the exact-count postcondition in network_data.tf.
  worker_subnet_pools_in_use = {
    for name, config in var.worker_subnet_pools : name => config
    if contains(local.worker_subnet_pool_names_in_use, name)
  }
  worker_subnet_pool_ids = {
    for name, subnets in data.aws_subnets.worker_pool : name => subnets.ids
  }
  secondary_private_subnet_ids = length(local.existing_mng_default_pool_names) == 0 ? (
    data.aws_subnets.secondary_private[0].ids
    ) : local.worker_subnet_pool_ids[
    one(local.existing_mng_default_pool_names)
  ]
  cluster_addon_configuration_values = {
    "metrics-server" = var.metrics_server_resources == null ? null : jsonencode({
      # metrics-server's memory footprint grows with the number of pods it
      # must cache. Keep the CPU request at the addon default while allowing
      # high-pod-count clusters to reserve and cap memory independently.
      resources = {
        requests = {
          cpu    = "100m"
          memory = var.metrics_server_resources.memory_request
        }
        limits = {
          memory = var.metrics_server_resources.memory_limit
        }
      }
    })
    "vpc-cni" = jsonencode({
      enableNetworkPolicy = "true"
      env = merge({
        ENABLE_PREFIX_DELEGATION = tostring(var.vpc_cni_enable_prefix_delegation)
        MINIMUM_IP_TARGET        = tostring(var.vpc_cni_minimum_ip_target)
        }, var.vpc_cni_warm_ip_target == null ? {} : {
        WARM_IP_TARGET = tostring(var.vpc_cni_warm_ip_target)
      })
      # The network-policy agent leaks heap proportionally to cluster-wide
      # PolicyEndpoint churn (sandbox pod create/delete rewrites the
      # namespace-wide sandboxes policies' endpoint lists, and every node's
      # agent ingests every update). aws-node is system-node-critical
      # (oom_score_adj -997), so without a limit the kernel OOM killer
      # starves every other pod on the node before touching the leaker. The
      # memcg limit converts that into a clean container-scoped OOM restart;
      # enforcement is unaffected while the agent restarts (it lives in
      # kernel eBPF), only policy programming for pods created during the
      # seconds-long restart is delayed. Keep the request explicit: an omitted
      # request defaults to the limit. The default covers a fresh agent;
      # high-cardinality clusters override it with a measured per-node
      # working set after checking their smallest node shape. The cpu request
      # restates the addon default so this override cannot drop it.
      nodeAgent = {
        resources = {
          requests = {
            cpu    = "25m"
            memory = var.vpc_cni_node_agent_resources.memory_request
          }
          limits = {
            memory = var.vpc_cni_node_agent_resources.memory_limit
          }
        }
      }
    })
  }

  # `workers_1_headroom_count` is only set during a cluster's
  # workers-cgnat -> workers-1 rollout. While it is set, the target
  # workers-1 MNG and the shared workers IAM role must coexist with the
  # legacy workers-cgnat pool; after the rollout completes,
  # `workers_cgnat_retired` becomes the durable gate.
  workers_1_rollout_active       = var.workers_1_headroom_count != null
  workers_pool_enabled           = var.workers_cgnat_retired || local.workers_1_rollout_active
  workers_shared_identity_needed = local.workers_pool_enabled || length(var.worker_capacity_groups) > 0

  # The legacy `workers-cgnat` pool definition. Hoisted into its own local
  # so the `node_pools` map below can include or exclude it via a single
  # conditional merge, keyed off `var.workers_cgnat_retired`. The pool's
  # launch template is **not** part of this entry — it lives in a
  # standalone `aws_launch_template.workers` resource in `node_groups.tf`
  # so `module.workers_1` can keep referencing the same LT identity after
  # the legacy MNG is retired (the cgnat-named LT is migrated into the
  # standalone resource via a `moved` block, preserving its AWS-side id).
  workers_cgnat_pool = {
    capacity_reservation_preference         = var.workers_capacity_reservation_preference
    capacity_reservation_resource_group_arn = var.workers_capacity_reservation_resource_group_arn != null ? var.workers_capacity_reservation_resource_group_arn : local.space_capacity_reservation_pool_arn
    capacity_type                           = var.workers_capacity_type
    desired_size                            = var.workers_desired_size
    disk_size_gib                           = var.workers_disk_size_gib
    instance_types                          = var.workers_instance_types
    # The `space.pplx.net/node-pool=workers` label is preserved from the
    # retired `workers` pool so existing sandbox pods (which the API creates
    # with that nodeSelector) and the `space-worker` DaemonSet still land
    # here without an API-side selector change. The `subnet-tier` label
    # distinguishes CGNAT nodes for debugging and targeted placement.
    labels = merge(
      {
        "space.pplx.net/node-pool"   = "workers"
        "space.pplx.net/subnet-tier" = "secondary"
        "workload"                   = "workers"
      },
      var.workers_labels,
    )
    max_size = var.workers_max_size
    min_size = var.workers_min_size
    ami_id   = local.workers_ami_id
    service_account = {
      name      = var.workers_service_account_name
      namespace = var.workers_service_account_namespace
    }
    subnet_ids         = local.secondary_private_subnet_ids
    taints             = var.workers_taints
    user_data_template = "worker-bootstrap.sh.tftpl"
    user_data_extra_vars = {
      bootstrap_bucket           = var.bootstrap_bucket
      shim_dogstatsd_socket_path = var.worker_shim_dogstatsd_socket_path
      shim_environment           = var.environment
    }
  }

  # The legacy `workers-cgnat` pool stays in this module: its node group is
  # rollout-managed (`aws_eks_node_group.rollout_managed`), so the rollout
  # script drives its scaling and Terraform ignores it, and it keeps its own
  # role and standalone launch template. The map is empty once the
  # environment flips `workers_cgnat_retired = true`.
  legacy_worker_node_pools = var.workers_cgnat_retired ? {} : { workers-cgnat = local.workers_cgnat_pool }

  node_pools = merge(local.legacy_worker_node_pools, local.core_pool_definitions)

  # Every other pool, built by `module.eks` from `local.core_node_pools`.
  core_pool_definitions = merge(
    {
      builders = {
        capacity_reservation_preference         = var.builders_capacity_reservation_preference
        capacity_reservation_resource_group_arn = var.builders_capacity_reservation_resource_group_arn != null ? var.builders_capacity_reservation_resource_group_arn : local.space_capacity_reservation_pool_arn
        capacity_type                           = var.builders_capacity_type
        desired_size                            = var.builders_desired_size
        disk_size_gib                           = var.builders_disk_size_gib
        instance_types                          = var.builders_instance_types
        labels = merge(
          {
            "space.pplx.net/node-pool" = "builders"
            "workload"                 = "builders"
          },
          var.builders_labels,
        )
        max_size = var.builders_max_size
        min_size = var.builders_min_size
        ami_id   = local.builders_ami_id
        service_account = {
          name      = var.builders_service_account_name
          namespace = var.builders_service_account_namespace
        }
        subnet_ids           = local.private_subnet_ids
        taints               = var.builders_taints
        user_data_template   = "bootstrap.sh.tftpl"
        user_data_extra_vars = {}
      }
      general = {
        capacity_reservation_preference         = null
        capacity_reservation_resource_group_arn = null
        capacity_type                           = var.general_capacity_type
        desired_size                            = var.general_desired_size
        disk_size_gib                           = var.general_disk_size_gib
        instance_types                          = var.general_instance_types
        labels = merge(
          {
            "space.pplx.net/node-pool" = "general"
            "workload"                 = "general"
          },
          var.general_labels,
        )
        max_size = var.general_max_size
        # The recorded floor applies only while general serves. Parked, its
        # minimum is zero, so an operator can scale it to zero through EKS
        # without the next apply raising it again. desired_size is ignored, but
        # the apply that drops the minimum sends the desired size the plan read,
        # so plan it immediately before applying while the pool serves.
        min_size = var.general_serving_node_group == "general" ? var.general_min_size : 0
        ami_id   = null
        service_account = {
          name      = var.general_service_account_name
          namespace = var.general_service_account_namespace
        }
        subnet_ids           = local.private_subnet_ids
        taints               = var.general_taints
        user_data_template   = "bootstrap.sh.tftpl"
        user_data_extra_vars = {}
      }
    },
    # Dedicated pool that carries the egress-endpoint client SG (declared by
    # the calling root) so only credmgr-authority's nodes are admitted by
    # the consumer VPCEs — the SG gates who may reach the endpoints, not where the
    # pods may egress. Tainted so only credmgr-authority (tolerating it via its
    # deploy values) lands here; pod-level SGs aren't an option (ENABLE_POD_ENI
    # is off cluster-wide). Present only when egress is enabled. Sized 1 node by
    # default; clusters with live authority traffic override to 2 for replica
    # spread.
    var.credential_service_egress_enabled ? {
      cred-authority = {
        capacity_reservation_preference         = null
        capacity_reservation_resource_group_arn = null
        capacity_type                           = "ON_DEMAND"
        desired_size                            = var.cred_authority_node_count
        disk_size_gib                           = 50
        instance_types                          = var.cred_authority_instance_types
        labels = {
          "space.pplx.net/node-pool" = "cred-authority"
          "workload"                 = "cred-authority"
        }
        # +1 headroom for rolling surge (no HPA scales this pool).
        max_size = var.cred_authority_node_count + 1
        min_size = var.cred_authority_node_count
        ami_id   = null
        # Per-pool IRSA role is inert for this pool: credmgr-authority uses its
        # own SA with EKS Pod Identity for AWS access, not this node-SA role.
        # Reuses the general SA values only to satisfy the pool schema.
        service_account = {
          name      = var.general_service_account_name
          namespace = var.general_service_account_namespace
        }
        subnet_ids = local.private_subnet_ids
        taints = [{
          effect = "NO_SCHEDULE"
          key    = "space.pplx.net/dedicated"
          value  = "cred-authority"
        }]
        user_data_template   = "bootstrap.sh.tftpl"
        user_data_extra_vars = {}
      }
    } : {},
  )

  oidc_issuer_hostpath = module.eks.oidc_issuer_host

  core_node_pools = {
    for name, pool in local.core_pool_definitions : name => {
      instance_types                          = pool.instance_types
      capacity_type                           = pool.capacity_type
      min_size                                = pool.min_size
      desired_size                            = pool.desired_size
      max_size                                = pool.max_size
      disk_size_gib                           = pool.disk_size_gib
      labels                                  = pool.labels
      taints                                  = pool.taints
      subnet_ids                              = pool.subnet_ids
      ami_id                                  = pool.ami_id
      capacity_reservation_preference         = pool.capacity_reservation_preference
      capacity_reservation_resource_group_arn = pool.capacity_reservation_resource_group_arn
      additional_security_group_ids = concat(
        # general_nodes carries the shared-services access RDS/Redis admit.
        # cred-authority hosts credmgr-authority, which reads DATABASE_URL from
        # space-postgres, so it needs the same SG or it loses Postgres when the
        # authority moves off the general pool.
        contains(["general", "cred-authority"], name) ? [aws_security_group.general_nodes.id] : [],
        # The dedicated cred-authority pool additionally carries the marker
        # client SG the egress endpoints admit; that SG is what gates who may
        # reach them. The pool only exists when credential_service_egress_enabled.
        name == "cred-authority" ? [var.credential_service_client_security_group_id] : [],
      )
      additional_policy_arns = merge(
        length(var.s3_read_bucket_names) > 0 ? { "s3-read" = aws_iam_policy.s3_read[0].arn } : {},
        name == "builders" ? { "template-artifacts-publish" = aws_iam_policy.builder_template_artifacts_publish.arn } : {},
      )
      # An ownership-tag policy can require Team on EBS root volumes; common_tags
      # already supplies the calling root's other ownership tags.
      volume_tags = { Team = "team-space" }
      # general-2 and the per-AZ groups run on a retired general's launch
      # template and node role.
      launch_template_and_role_only = name == "general" && var.general_node_group_retired
    }
  }

  core_node_pool_user_data = {
    for name, pool in local.core_node_pools : name => templatefile(
      "${path.module}/templates/${local.core_pool_definitions[name].user_data_template}",
      merge(
        {
          cluster_endpoint       = module.eks.cluster_endpoint
          cluster_name           = module.eks.cluster_name
          cluster_service_cidr   = var.cluster_service_cidr
          cluster_ca_data        = module.eks.cluster_certificate_authority_data
          space_cache_nvme_mount = file("${path.module}/templates/space-cache-nvme-mount.sh")
        },
        local.core_pool_definitions[name].user_data_extra_vars,
      )
    ) if pool.ami_id != null
  }
}

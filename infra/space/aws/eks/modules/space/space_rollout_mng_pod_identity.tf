# EKS Pod Identity association + IAM role for the `space-rollout-mng`
# Helm-chart Job (`infra/space/aws/eks/charts/space-rollout-mng/`). The
# chart ships the in-cluster variant of `scripts/space-rollout-mng.py`,
# which is the canonical run mode for the Two-MNG worker rollout — the
# local port-forward + Python path is the documented fallback.
#
# Permissions: the orchestrator script is **read-mostly**. It reads
# nodegroup state (`eks:DescribeNodegroup`), reads ASG state
# (`autoscaling:DescribeAutoScalingGroups`), reads EC2 instance state
# (`ec2:DescribeInstances`), and the **only** write path it owns is
# `eks:UpdateNodegroupConfig` to step source/target `scalingConfig`
# wave by wave. The actual node terminations happen inside the
# cluster-controller's `node_drain_reconciler` (which already has
# `autoscaling:TerminateInstanceInAutoScalingGroup` via its own role
# in `cluster_controller_pod_identity.tf`) — the orchestrator just
# asks the SPACE admin API to start a drain, and the reconciler does
# the AWS-side termination once the drain reaches `terminating`.
# Granting direct ASG mutation actions to the Job pod would expand the
# blast radius of a compromised pod or token without being needed for
# any documented orchestrator code path.
#
# Resource scoping uses the same worker nodegroup ARN set the
# cluster-controller already computes, so rollout cohorts
# (`workers-1` and every `workers_rollout_cohorts` target) and the
# disposable smoke-test MNGs (`test-1` / `test-2`) inherit access without
# touching this file.
#
# Why a separate role instead of attaching policies to the existing
# `cluster-controller-db-pod-identity` role: EKS Pod Identity binds one
# role per (cluster, namespace, service_account) tuple, and the chart's
# SA is `space/space-rollout-mng` — distinct from `space-cluster-controller`.

locals {
  space_rollout_mng_namespace            = "space"
  space_rollout_mng_service_account_name = "space-rollout-mng"
  space_rollout_mng_role_name            = "${local.name}-space-rollout-mng-pod-identity"
}

data "aws_iam_policy_document" "space_rollout_mng_assume_role" {
  statement {
    sid = "AllowEksAuthToAssumeRoleForPodIdentity"

    actions = [
      "sts:AssumeRole",
      "sts:TagSession",
    ]

    principals {
      type        = "Service"
      identifiers = ["pods.eks.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:RequestTag/eks-cluster-name"
      values   = [module.eks.cluster_name]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:RequestTag/kubernetes-namespace"
      values   = [local.space_rollout_mng_namespace]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:RequestTag/kubernetes-service-account"
      values   = [local.space_rollout_mng_service_account_name]
    }
  }
}

resource "aws_iam_role" "space_rollout_mng_pod_identity" {
  name               = local.space_rollout_mng_role_name
  assume_role_policy = data.aws_iam_policy_document.space_rollout_mng_assume_role.json
  description        = "EKS Pod Identity role for ${local.space_rollout_mng_namespace}/${local.space_rollout_mng_service_account_name} (Two-MNG rollout orchestrator Job)"

  tags = var.common_tags
}

# EKS nodegroup management. The orchestrator reads scaling state
# (`DescribeNodegroup`) and resizes the source/target MNGs
# (`UpdateNodegroupConfig`) wave by wave. Same nodegroup-ARN scope as
# the cluster-controller's drain reconciler — including the disposable
# `test_1`/`test_2` MNGs that gate on `var.test_mngs_enabled`.
data "aws_iam_policy_document" "space_rollout_mng_eks_nodegroup_manage" {
  statement {
    sid = "RolloutOrchestratorManageWorkerNodegroups"

    actions = [
      "eks:DescribeNodegroup",
      "eks:UpdateNodegroupConfig",
    ]

    resources = local.cluster_controller_worker_nodegroup_arns
  }
}

resource "aws_iam_role_policy" "space_rollout_mng_eks_nodegroup_manage" {
  name   = "${local.name}-space-rollout-mng-eks-nodegroup-manage"
  role   = aws_iam_role.space_rollout_mng_pod_identity.name
  policy = data.aws_iam_policy_document.space_rollout_mng_eks_nodegroup_manage.json
}

# ASG describe is required to look up the source ASG name from the
# nodegroup, read suspended-process state, and verify lifecycle
# transitions during the terminate-and-replace loop. AWS does not
# support resource-level permissions on
# `autoscaling:DescribeAutoScalingGroups`, so the policy must use
# `Resource = "*"`.
data "aws_iam_policy_document" "space_rollout_mng_asg_describe" {
  statement {
    sid = "RolloutOrchestratorDescribeAutoScalingGroups"

    actions = [
      "autoscaling:DescribeAutoScalingGroups",
    ]

    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "space_rollout_mng_asg_describe" {
  name   = "${local.name}-space-rollout-mng-asg-describe"
  role   = aws_iam_role.space_rollout_mng_pod_identity.name
  policy = data.aws_iam_policy_document.space_rollout_mng_asg_describe.json
}

# `ec2:DescribeInstances` for the orchestrator's per-instance state
# checks (instance-lifecycle, private DNS → node-name lookup). EC2
# `DescribeInstances` does not support resource-level permissions, so
# the policy is `Resource = "*"`.
data "aws_iam_policy_document" "space_rollout_mng_ec2_describe_instances" {
  statement {
    sid = "RolloutOrchestratorDescribeInstances"

    actions = [
      "ec2:DescribeInstances",
    ]

    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "space_rollout_mng_ec2_describe_instances" {
  name   = "${local.name}-space-rollout-mng-ec2-describe-instances"
  role   = aws_iam_role.space_rollout_mng_pod_identity.name
  policy = data.aws_iam_policy_document.space_rollout_mng_ec2_describe_instances.json
}

resource "aws_eks_pod_identity_association" "space_rollout_mng" {
  cluster_name    = module.eks.cluster_name
  namespace       = local.space_rollout_mng_namespace
  service_account = local.space_rollout_mng_service_account_name
  role_arn        = aws_iam_role.space_rollout_mng_pod_identity.arn

  tags = var.common_tags
}

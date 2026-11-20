# Optionally suspends the AZRebalance process on the builders node group's
# Auto Scaling group.
#
# The builders group spans every private subnet, so its ASG rebalances
# whenever its zones are uneven: two builders that land in one zone (say the
# other zone had no capacity when the second launched) are later split by
# AZRebalance, which launches in the short zone and terminates a builder.
# EKS drains that termination without honoring PodDisruptionBudgets and
# kills the node after 15 minutes, so builds in flight on it fail. The
# general pool avoids this with one group per zone (general_per_az in
# node_groups.tf). Builders don't, because splitting a live group means
# replacing it, which deletes its nodes; suspending the process leaves the
# group, its nodes and its launch template as they are.
#
# EKS managed node groups don't expose the ASG's suspended processes and the
# AWS provider can only set them on an ASG it creates, so the suspension is a
# call to the AWS CLI from the machine running the apply. That machine needs
# the `aws` CLI and `autoscaling:SuspendProcesses` and
# `autoscaling:ResumeProcesses` on the builders ASG. The call runs when the
# resource is created and again whenever the builders ASG changes (a replaced
# group), and nothing else re-runs it. Turning the variable off resumes the
# process. Something else that resumes it out of band, such as the end of an
# ARC zonal shift, isn't reverted: the check below warns on the next plan,
# and `terraform apply -replace` of this resource suspends it again.
#
# DEFAULT rolls need AZRebalance: EKS places each surge node in the zone of
# the node it replaces through it. MINIMAL terminates first and the ASG
# relaunches into the zone that's short, which doesn't use the process, so
# the suspension is refused unless builders roll MINIMAL. With it suspended,
# nothing moves a builder between zones, so check placement after a builder
# launches and, if both share a zone, replace one by hand (cordon, drain,
# then terminate it without decrementing the desired size, so the ASG
# launches into the empty zone).
locals {
  builders_asg_names = sort(flatten([
    for r in module.eks.node_groups["builders"].resources : [for asg in r.autoscaling_groups : asg.name]
  ]))
}

resource "terraform_data" "builders_az_rebalance_suspended" {
  count = var.builders_az_rebalance_suspended ? 1 : 0

  triggers_replace = {
    asg_names = local.builders_asg_names
    region    = var.aws_region
  }

  provisioner "local-exec" {
    command = "set -e; for asg in $ASG_NAMES; do aws autoscaling suspend-processes --region \"$AWS_REGION\" --auto-scaling-group-name \"$asg\" --scaling-processes AZRebalance; done"
    environment = {
      ASG_NAMES  = join(" ", local.builders_asg_names)
      AWS_REGION = var.aws_region
    }
  }

  # Resumes the process when the variable is turned off or the group's ASG is
  # replaced. A replaced group's old ASG is usually gone already, so a failure
  # here doesn't fail the apply.
  provisioner "local-exec" {
    when       = destroy
    on_failure = continue
    command    = "for asg in $ASG_NAMES; do aws autoscaling resume-processes --region \"$AWS_REGION\" --auto-scaling-group-name \"$asg\" --scaling-processes AZRebalance; done"
    environment = {
      ASG_NAMES  = join(" ", self.triggers_replace.asg_names)
      AWS_REGION = self.triggers_replace.region
    }
  }

  lifecycle {
    precondition {
      condition     = var.builders_update_strategy == "MINIMAL"
      error_message = "builders_az_rebalance_suspended needs builders_update_strategy = \"MINIMAL\": a DEFAULT roll places each surge node through AZRebalance."
    }

    precondition {
      condition     = length(local.builders_asg_names) > 0
      error_message = "builders_az_rebalance_suspended needs the builders node group to exist with its Auto Scaling group: create the cluster first, then turn it on in a later apply."
    }
  }
}

# Read only while the suspension is on, so other clusters' plans make no extra
# AWS call. EKS backs a managed node group with one ASG; a name that's unknown
# at plan time defers the read to the apply.
data "aws_autoscaling_group" "builders" {
  count = var.builders_az_rebalance_suspended ? 1 : 0

  name = one(local.builders_asg_names)

  # Read after the suspension, so the plan that turns it on doesn't warn.
  depends_on = [terraform_data.builders_az_rebalance_suspended]
}

check "builders_az_rebalance_suspended" {
  assert {
    condition = alltrue([
      for asg in data.aws_autoscaling_group.builders : contains(asg.suspended_processes, "AZRebalance")
    ])
    error_message = "AZRebalance isn't suspended on the builders ASG, though builders_az_rebalance_suspended is set: something resumed it out of band. Suspend it again with `terraform apply -replace` of this module's terraform_data.builders_az_rebalance_suspended[0]."
  }
}

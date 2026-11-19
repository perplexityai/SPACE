# Custom-AMI pools' launch template version pin (node_pools[*].launch_template_version).
# Runs against mocked providers, so it needs no AWS credentials:
#   terraform -chdir=infra/space/aws/eks/modules/eks-cluster init -backend=false
#   terraform -chdir=infra/space/aws/eks/modules/eks-cluster test

mock_provider "aws" {
  # Every launch template reports version 7 as its latest.
  mock_resource "aws_launch_template" {
    defaults = {
      id             = "lt-0123456789abcdef0"
      latest_version = 7
    }
  }
  mock_resource "aws_eks_cluster" {
    defaults = {
      arn                   = "arn:aws:eks:us-east-1:111111111111:cluster/test"
      certificate_authority = [{ data = "Y2E=" }]
      identity              = [{ oidc = [{ issuer = "https://oidc.example.com/id/TEST" }] }]
    }
  }
  mock_resource "aws_iam_role" {
    defaults = { arn = "arn:aws:iam::111111111111:role/test" }
  }
  mock_resource "aws_iam_openid_connect_provider" {
    defaults = { arn = "arn:aws:iam::111111111111:oidc-provider/oidc.example.com/id/TEST" }
  }
  mock_data "aws_partition" {
    defaults = { partition = "aws" }
  }
  mock_data "aws_iam_policy_document" {
    defaults = { json = "{}" }
  }
}

mock_provider "tls" {
  mock_data "tls_certificate" {
    defaults = { certificates = [{ sha1_fingerprint = "0000000000000000000000000000000000000000" }] }
  }
}

variables {
  name               = "test"
  vpc_id             = "vpc-0123456789abcdef0"
  subnet_ids         = ["subnet-0123456789abcdef0", "subnet-0123456789abcdef1"]
  kubernetes_version = "1.33"
  cluster_addons = {
    "vpc-cni"    = "v1.23.0-eksbuild.1"
    "kube-proxy" = "v1.23.0-eksbuild.1"
  }
  node_pool_user_data = { pinned = "#!/bin/bash", tracking = "#!/bin/bash" }
  node_pools = {
    pinned = {
      instance_types          = ["m5.large"]
      capacity_type           = "ON_DEMAND"
      min_size                = 1
      desired_size            = 1
      max_size                = 2
      disk_size_gib           = 20
      labels                  = {}
      taints                  = []
      subnet_ids              = ["subnet-0123456789abcdef0"]
      ami_id                  = "ami-0123456789abcdef0"
      launch_template_version = 3
    }
    tracking = {
      instance_types = ["m5.large"]
      capacity_type  = "ON_DEMAND"
      min_size       = 1
      desired_size   = 1
      max_size       = 2
      disk_size_gib  = 20
      labels         = {}
      taints         = []
      subnet_ids     = ["subnet-0123456789abcdef0"]
      ami_id         = "ami-0123456789abcdef0"
    }
  }
}

# A pinned pool stays on its pin, an unpinned one tracks the latest version,
# and the lag warning names only the pinned pool.
run "pinned_and_tracking" {
  command         = apply
  expect_failures = [check.custom_node_groups_on_latest_launch_template]

  assert {
    condition     = aws_eks_node_group.custom["pinned"].launch_template[0].version == "3"
    error_message = "A pinned pool must run its pinned version."
  }

  assert {
    condition     = aws_eks_node_group.custom["tracking"].launch_template[0].version == "7"
    error_message = "An unpinned pool must track the template's latest version."
  }

  assert {
    condition     = keys(local.custom_node_groups_behind_launch_template) == ["pinned"]
    error_message = "Only the pinned pool lags the latest version."
  }
}

run "no_pins_no_warning" {
  command = apply

  variables {
    node_pool_user_data = { tracking = "#!/bin/bash" }
    node_pools = {
      tracking = {
        instance_types = ["m5.large"]
        capacity_type  = "ON_DEMAND"
        min_size       = 1
        desired_size   = 1
        max_size       = 2
        disk_size_gib  = 20
        labels         = {}
        taints         = []
        subnet_ids     = ["subnet-0123456789abcdef0"]
        ami_id         = "ami-0123456789abcdef0"
      }
    }
  }

  assert {
    condition     = length(local.custom_node_groups_behind_launch_template) == 0
    error_message = "With no pins, nothing lags the latest version."
  }
}

run "pin_above_latest_is_rejected" {
  command = apply

  variables {
    node_pool_user_data = { pinned = "#!/bin/bash" }
    node_pools = {
      pinned = {
        instance_types          = ["m5.large"]
        capacity_type           = "ON_DEMAND"
        min_size                = 1
        desired_size            = 1
        max_size                = 2
        disk_size_gib           = 20
        labels                  = {}
        taints                  = []
        subnet_ids              = ["subnet-0123456789abcdef0"]
        ami_id                  = "ami-0123456789abcdef0"
        launch_template_version = "8"
      }
    }
  }

  expect_failures = [aws_eks_node_group.custom["pinned"]]
}

run "non_numeric_pin_is_rejected" {
  command = plan

  variables {
    node_pool_user_data = { pinned = "#!/bin/bash" }
    node_pools = {
      pinned = {
        instance_types          = ["m5.large"]
        capacity_type           = "ON_DEMAND"
        min_size                = 1
        desired_size            = 1
        max_size                = 2
        disk_size_gib           = 20
        labels                  = {}
        taints                  = []
        subnet_ids              = ["subnet-0123456789abcdef0"]
        ami_id                  = "ami-0123456789abcdef0"
        launch_template_version = "$Latest"
      }
    }
  }

  expect_failures = [var.node_pools]
}

run "managed_ami_pin_is_rejected" {
  command = plan

  variables {
    node_pool_user_data = {}
    node_pools = {
      managed = {
        instance_types          = ["m5.large"]
        capacity_type           = "ON_DEMAND"
        min_size                = 1
        desired_size            = 1
        max_size                = 2
        disk_size_gib           = 20
        labels                  = {}
        taints                  = []
        subnet_ids              = ["subnet-0123456789abcdef0"]
        launch_template_version = "3"
      }
    }
  }

  expect_failures = [var.node_pools]
}

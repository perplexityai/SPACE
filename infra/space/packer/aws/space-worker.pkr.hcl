# aws/space-worker.pkr.hcl
#
# AWS AMI for a SPACE worker node (x86_64).
# Worker nodes run Firecracker sandboxes.
#
# This AMI uses the simple AWS layout:
#   - /space lives on the root EBS volume
#   - /space/cache starts as a btrfs loopback image
#   - local NVMe instance store replaces /space/cache at boot when present
#   - hugepages are disabled here and configured by deployment/runtime policy
#
# Provisioning scripts are shared in ../scripts/base/. The SPACE node services
# are not baked in; they run on the node as Kubernetes workloads.

packer {
  required_plugins {
    amazon = {
      version = "= 1.8.2"
      source  = "github.com/hashicorp/amazon"
    }
  }
}

variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "git_sha" {
  type        = string
  default     = "unknown"
  description = "Git commit SHA used to build this AMI"
}

variable "git_branch" {
  type        = string
  default     = "unknown"
  description = "Git branch name used to build this AMI"
}

variable "built_at" {
  type        = string
  default     = "unknown"
  description = "UTC timestamp when this AMI build started"
}

variable "owner" {
  type        = string
  default     = ""
  description = "Owner tag for the AMI"
}

variable "space_managed_by" {
  type        = string
  default     = ""
  description = "Script/tool that created this AMI"
}

variable "space_kind" {
  type        = string
  default     = ""
  description = "SPACE resource kind tag"
}

variable "space_source_name" {
  type        = string
  default     = ""
  description = "SPACE node name associated with this AMI"
}

variable "kubernetes_version" {
  type        = string
  description = "Kubernetes minor for the AL2023 EKS base AMI, e.g. \"1.36\". Required, no default: it decides the kubelet minor of every node launched from the image, which must not be newer than the control plane."
}

variable "instance_type" {
  type        = string
  default     = "c5.2xlarge"
  description = "Instance type for building the AMI (not the runtime instance)"
}

variable "disk_gb" {
  type        = number
  default     = 100
  description = "Root EBS volume size. /space lives on /; cache uses a loopback image unless local NVMe is present at runtime."
}

variable "ssh_public_key" {
  type        = string
  default     = ""
  description = "SSH public key to install for root, deploy, and dev users"
}

variable "disable_root_ssh" {
  type        = bool
  default     = true
  description = "Disable root SSH login (default true for AWS)"
}

variable "subnet_id" {
  type        = string
  default     = ""
  description = "Optional subnet for the Packer build instance"
}

variable "security_group_id" {
  type        = string
  default     = ""
  description = "Optional security group for the Packer build instance"
}

variable "associate_public_ip_address" {
  type        = bool
  default     = true
  description = "Whether to associate a public IP with the Packer build instance"
}

variable "ssh_interface" {
  type        = string
  default     = "public_ip"
  description = "Packer SSH interface to use: public_ip or private_ip"
}

source "amazon-ebs" "al2023" {
  region                      = var.aws_region
  instance_type               = var.instance_type
  ami_name                    = "space-worker-{{timestamp}}"
  subnet_id                   = var.subnet_id != "" ? var.subnet_id : null
  security_group_id           = var.security_group_id != "" ? var.security_group_id : null
  associate_public_ip_address = var.associate_public_ip_address

  source_ami_filter {
    filters = {
      name                = "amazon-eks-node-al2023-x86_64-standard-${var.kubernetes_version}-v*"
      root-device-type    = "ebs"
      virtualization-type = "hvm"
    }

    owners      = ["602401143452"]
    most_recent = true
  }

  launch_block_device_mappings {
    device_name           = "/dev/xvda"
    volume_size           = var.disk_gb
    volume_type           = "gp3"
    delete_on_termination = true
  }

  ssh_username  = "ec2-user"
  ssh_interface = var.ssh_interface

  tags = {
    Name              = "space-worker"
    Role              = "worker"
    Base              = "al2023-eks"
    KubernetesVersion = var.kubernetes_version
    GitSHA            = var.git_sha
    GitBranch         = var.git_branch
    BuiltAt           = var.built_at
    owner             = var.owner

    "space:managed-by"  = var.space_managed_by
    "space:kind"        = var.space_kind
    "space:source-name" = var.space_source_name
  }
}

build {
  sources = ["source.amazon-ebs.al2023"]

  # AWS SSHes as ec2-user (non-root). All scripts assume root, so we wrap with sudo.

  provisioner "shell" {
    execute_command = "chmod +x {{ .Path }}; {{ .Vars }} sudo -E {{ .Path }}"
    script          = "../scripts/base/00-wait-cloud-init.sh"
  }

  provisioner "shell" {
    execute_command = "chmod +x {{ .Path }}; {{ .Vars }} sudo -E {{ .Path }}"
    script          = "../scripts/base/02-users.sh"
  }

  provisioner "shell" {
    execute_command = "chmod +x {{ .Path }}; {{ .Vars }} sudo -E {{ .Path }}"
    script          = "../scripts/base/00-grow-root.sh"
    environment_vars = [
      "MIN_ROOT_GB=${var.disk_gb}",
    ]
  }

  provisioner "shell" {
    execute_command = "chmod +x {{ .Path }}; {{ .Vars }} sudo -E {{ .Path }}"
    script          = "../scripts/base/01-partitions.sh"
    environment_vars = [
      "SIMPLE_LAYOUT=true",
    ]
  }

  provisioner "shell" {
    execute_command = "chmod +x {{ .Path }}; {{ .Vars }} sudo -E {{ .Path }}"
    script          = "../scripts/base/03-packages.sh"
  }

  provisioner "shell" {
    execute_command = "chmod +x {{ .Path }}; {{ .Vars }} sudo -E {{ .Path }}"
    script          = "../scripts/base/04-firecracker.sh"
  }

  provisioner "shell" {
    execute_command = "chmod +x {{ .Path }}; {{ .Vars }} sudo -E {{ .Path }}"
    script          = "../scripts/base/05-grpc-health-probe.sh"
  }

  provisioner "shell" {
    execute_command = "chmod +x {{ .Path }}; {{ .Vars }} sudo -E {{ .Path }}"
    script          = "../scripts/base/06-e2fsprogs.sh"
  }

  provisioner "file" {
    source      = "../../identity/node-identities.sh"
    destination = "/tmp/space-node-identities.sh"
  }

  provisioner "shell" {
    execute_command = "chmod +x {{ .Path }}; {{ .Vars }} sudo -E {{ .Path }}"
    script          = "../scripts/base/07-sysctl.sh"
    environment_vars = [
      "HUGEPAGES=0",
    ]
  }

  provisioner "shell" {
    execute_command = "chmod +x {{ .Path }}; {{ .Vars }} sudo -E {{ .Path }}"
    script          = "../scripts/base/10-ssh-key.sh"
    environment_vars = [
      "SSH_PUBLIC_KEY=${var.ssh_public_key}",
    ]
  }

  provisioner "shell" {
    execute_command = "chmod +x {{ .Path }}; {{ .Vars }} sudo -E {{ .Path }}"
    script          = "../scripts/base/08-sudoers.sh"
  }

  provisioner "shell" {
    execute_command = "chmod +x {{ .Path }}; {{ .Vars }} sudo -E {{ .Path }}"
    script          = "../scripts/base/09-auditd.sh"
  }

  # NVMe instance store auto-detection: formats local NVMe drives as btrfs
  # RAID0 and mounts at /space/cache. No-op on EBS-only instance types.
  provisioner "shell" {
    execute_command = "chmod +x {{ .Path }}; {{ .Vars }} sudo -E {{ .Path }}"
    script          = "../scripts/base/11-nvme-cache.sh"
  }

  # Retry nodeadm-config.service on transient IMDS failures.
  provisioner "shell" {
    execute_command = "chmod +x {{ .Path }}; {{ .Vars }} sudo -E {{ .Path }}"
    script          = "../scripts/base/12-nodeadm-config-retry.sh"
  }

  provisioner "file" {
    source      = "../scripts/base/space-gpu-preflight.sh"
    destination = "/tmp/space-gpu-preflight"
  }

  provisioner "shell" {
    execute_command = "chmod +x {{ .Path }}; {{ .Vars }} sudo -E {{ .Path }}"
    script          = "../scripts/base/13-gpu-passthrough.sh"
  }

  provisioner "shell" {
    execute_command = "chmod +x {{ .Path }}; {{ .Vars }} sudo -E {{ .Path }}"
    script          = "../scripts/base/99-cleanup.sh"
    environment_vars = [
      "DISABLE_ROOT_SSH=${var.disable_root_ssh}",
    ]
  }
}

output "vpc_id" {
  description = "VPC the cluster runs in."
  value       = aws_vpc.this.id
}

output "private_subnet_ids" {
  description = "Private subnet per AZ, keyed by AZ. Node groups and the control-plane ENIs go here."
  value       = { for az, subnet in aws_subnet.private : az => subnet.id }
}

output "public_subnet_ids" {
  description = "Public subnet per AZ, keyed by AZ. Internet-facing load balancers only."
  value       = { for az, subnet in aws_subnet.public : az => subnet.id }
}

output "cluster_name" {
  description = "EKS cluster name. `aws eks update-kubeconfig --name <value> --region <region>` configures kubectl."
  value       = module.eks.cluster_name
}

output "node_pool_sizes" {
  description = "Nodes each pool was sized for at the last apply, keyed by the `space.pplx.net/node-pool` label value. The scheduled job takes the pool names from here and each pool's current size from EKS, since `general`'s desired size can move through the EKS API, within its configured bounds, without a Terraform change."
  value = {
    for pool, group in module.eks.node_groups : pool => group.scaling_config[0].desired_size
  }
}

output "cluster_endpoint" {
  description = "EKS API server endpoint."
  value       = module.eks.cluster_endpoint
}

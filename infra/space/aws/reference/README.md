# Reference EKS deployment

A SPACE cluster that SPACE applies on a schedule, in an AWS account with no
Perplexity workloads. It exists to prove that the public EKS Terraform and the
public node image recipes stand up SPACE's cluster shape from an empty account
with no Perplexity context, and to keep proving it: the schedule surfaces drift
and module regressions without anyone having to remember to check.

## Scope

The target is **an EKS cluster with SPACE's three node pools**: `general` for
cluster services, `workers` that host Firecracker sandboxes, and `builders`
that build templates. The workers and builders are bare-metal nodes running the
SPACE node AMIs baked from `infra/space/packer/`. On top of the cluster run
**three add-ons**: metrics-server, and the PostgreSQL and Valkey SPACE's
control plane uses. Each installs and is verified on its own, and none of them
gates the cluster (see [The add-ons](#the-add-ons)).

Deliberately absent: vendor integrations (no Datadog, no Wiz) and SPACE's own
workloads. This deployment ends at a reachable cluster whose nodes are Ready,
carry SPACE's host setup, and serve the add-ons; it doesn't deploy SPACE's
services onto them, and it doesn't install SPACE's containerd shim on the
workers, which is part of that deployment.

## Layout

| Root | Applied by | State | Creates |
| --- | --- | --- | --- |
| `bootstrap/` | A person, once, and on changes | `bootstrap/terraform.tfstate` in the bucket it creates | The state bucket, the account's GitHub OIDC provider, the role the schedule assumes, and the permissions boundary for the roles that role creates |
| `cluster/` | The scheduled workflow | `cluster/terraform.tfstate` | The VPC, and the EKS cluster with its `general`, `workers` and `builders` node groups from `infra/space/aws/eks/modules/eks-cluster` |
| `addons/` | The scheduled workflow, after `cluster/` | `addons/terraform.tfstate` | metrics-server, the EBS CSI driver and its IAM role, PostgreSQL and Valkey, on the cluster `cluster/` created |

The node AMIs are a further input, baked by a person with
`infra/space/packer/aws/build.sh` (step 3) and looked up by the cluster root.

`bootstrap/` is separate on purpose. The schedule's role must not be able to
rewrite its own trust policy, so the root that defines the role is never run by
the schedule. `addons/` is separate from `cluster/` so that an add-on failure
can't stop the cluster from applying, and so its Kubernetes providers are
configured from a cluster that already exists when it plans.

State locking is S3-native (`use_lockfile`, Terraform 1.10+), so there's no
DynamoDB lock table. Every backend is a partial configuration: the bucket name
contains the account id and is passed at `init` time rather than committed.

## The cluster

`cluster/eks.tf` calls `infra/space/aws/eks/modules/eks-cluster` by path, so
the schedule applies whatever `main` holds and a module regression shows up
here first. It creates the control plane, the `vpc-cni`, `kube-proxy` and
`coredns` addons, and three node groups in the private subnets:

| Node group | Instances | AMI | Taint |
| --- | --- | --- | --- |
| `general` | one `m6i.large`, in the NAT gateway's AZ | EKS-managed AL2023 | none |
| `workers` | `m5d.metal`, none by default | the newest `Role=worker` SPACE node AMI | none |
| `builders` | `m5d.metal`, none by default | the newest `Role=builder` SPACE node AMI | `space.pplx.net/workload=builders:NoSchedule` |

`general` runs in one AZ, the NAT gateway's, because it hosts PostgreSQL,
whose EBS volume attaches only in the AZ it was created in; a replacement
node in another AZ would leave the database Pending. Losing that AZ already
cuts every node's egress. What it does add: when that AZ is out of
`m6i.large` capacity, EC2 can't place the node elsewhere. The AZ is the first
of `availability_zones` in sorted order, so once PostgreSQL's volume exists,
keep that AZ first: a list that sorts another one ahead moves the gateway and
`general` there, and leaves the database Pending. Workers and builders
are bare metal because Firecracker needs `/dev/kvm`. Their
labels and taints are the ones `modules/space` gives the same pools, so SPACE's
node workloads would land on them unchanged. The AMI lookup matches the tag
`KubernetesVersion` to the cluster's version and `GitBranch` to
`node_ami_branch` (`main` by default), so a bake at another minor or from a
feature branch is never picked up; a newer matching bake rolls the pool on the
next apply, draining each node before its replacement launches. Terraform
gives a roll 40 minutes, and the apply job 210 (a Kubernetes minor bump chains
the control-plane upgrade, addons and custom-AMI rolls, 180 minutes at their
Terraform limits, before the readiness check), so a stalled roll fails the
apply rather than having the job killed with the state lock held. Nodes roll
one at a time, so 40 minutes covers a pool of one metal node, and the sizes
parameter rejects anything above 1; `node_group_update_timeout` in
`cluster/eks.tf`, and the job's limit, need raising in the same change as that
cap. The 180 minutes cover a version bump, with or without a pool resize in
the same apply: a change that replaces a node group (its instance types or subnets) or adds other cluster changes can run
past the job's limit alongside one, so apply that combination locally, as in
step 5, rather than through the schedule. The branch filter trusts the tag `build.sh` writes, which is the
local branch name, so bake from a clean, current checkout of `main` (see
`infra/space/packer/README.md`).

The lookup runs on every plan, even with a pool at zero, because the pool's
launch template names the image. Don't deregister the last `main` bake at the
cluster's minor: every plan, including the schedule's drift check, fails until
there's another.

The custom-AMI nodes join through `modules/space`'s
builder bootstrap script (NVMe cache mount, then `nodeadm`), which this root
renders from that module's templates rather than copying.

The workers and builders pools are sized by hand and stay at zero between uses
(see [Scaling the metal pools](#scaling-the-metal-pools)). Nothing resizes them
dynamically.

Each node role carries
`AmazonEC2ContainerRegistryPullOnly`, so nodes can pull from the account's own
ECR repositories with no repository policy. Nodes set the IMDS hop limit to 1,
so pods on the pod network can't read the node role's credentials. The SPACE
cluster module itself can't be used here: it always installs SPACE's
application layer (External Secrets, the load balancer controller, and secrets
read from the RDS and ElastiCache stacks SPACE's own environments run, which
this deployment doesn't have), with no `count` or `for_each` guard to switch
it off.

`eks-cluster` takes `vpc_id` and `subnet_ids` as inputs rather than finding
them by tag. Tag discovery, as the SPACE cluster module does it, reads data
sources at plan time, before the VPC exists, so a first apply fails; it also
expects secondary CG-NAT subnets this network doesn't have.

The API endpoint is public, so the scheduled job can reach it from a
GitHub-hosted runner and check that the nodes are Ready. Every request still
needs an EKS access entry. The apply role has one because it created the
cluster; people get one through bootstrap's `cluster_admin_principal_arns`,
which `bootstrap/` publishes as an SSM parameter the cluster root reads, so
every plan of the cluster root sees the same list.

Kubernetes and addon versions are pinned in `cluster/variables.tf`. A new
cluster takes the newest version in EKS standard support, and each addon
version must be one EKS lists for it
(`aws eks describe-addon-versions --kubernetes-version <version> --addon-name <name>`).

To move to a new minor: bake both AMIs at it (step 3). `build.sh` refuses to
bake a commit that already has an image of the same role at another minor, so
bake from a later commit on `main` than the last bake, or pass
`--skip-conflict-check`: the reference cluster selects by `KubernetesVersion`,
not `GitSHA`, so the ambiguity that check guards against can't affect it
(it can affect a `modules/space` cluster in the same account). Then bump
`kubernetes_version` and the addon versions, in `cluster/variables.tf` and in
`addons/variables.tf` (`metrics_server_version`, `ebs_csi_driver_version`),
and let an apply upgrade the control plane, roll the metal pools, and then
update the add-ons. The `general` pool doesn't follow:
`eks-cluster` leaves an EKS-managed pool at the version it was created with,
because `CreateNodegroup` only accepts the cluster's current version. Upgrade
it after the control plane, with administrator credentials:
`aws eks update-nodegroup-version --cluster-name space-reference --nodegroup-name general`.

## The add-ons

`addons/` installs three add-ons on the cluster, each in its own file and
each with its own check in `addons/verify.sh`:

| Add-on | How it runs | Verified by (`./verify.sh <name>`) |
| --- | --- | --- |
| metrics-server | The EKS `metrics-server` addon, the same one SPACE's own clusters run, installed from `addons/` rather than the cluster module's `cluster_addons` so a failure there can't hold up the cluster | `metrics-server`: the resource metrics API reports every Ready node |
| PostgreSQL 17 | A StatefulSet on `general` (`addons/charts/postgresql`), its data on an encrypted 20 GiB gp3 EBS volume | `postgresql`: the volume claim is Bound, and a password login through the Service runs a query |
| Valkey 9.0 | A Deployment on `general` (`addons/charts/valkey`), in memory only | `valkey`: an authenticated `PING` through the Service answers, and an unauthenticated one is refused |

The versions are PostgreSQL 17 and Valkey 9.0, the ones SPACE is built and
tested against. Both run in the cluster, from local
charts, with images pinned by digest so an unattended apply never picks up a
retagged image. They run in the `space` namespace, and each writes a Secret a
SPACE deployment can read. `space-postgres` holds `POSTGRES_USER`,
`POSTGRES_PASSWORD`, `POSTGRES_DB` and `POSTGRES_CONNECTION_STRING`, the same
Secret and keys SPACE's services read in the Kind harness. `space-valkey`
holds `SPACE_REDIS_URL`, the variable SPACE's API reads its rate-limiter store
from; Kind sets that variable directly rather than from a Secret.
Terraform generates both passwords and keeps them in `addons/` state.

Valkey holds SPACE's rate-limiter counters, a guardrail rather than a source
of truth, so it keeps nothing on disk and a restart just empties it. PostgreSQL
is SPACE's desired-state store, so its data lives on a volume that survives
restarts, release upgrades and uninstalls (a StatefulSet's claims outlive it).
The volume's size is set when the volume is created: to grow it, resize the
`data-space-postgres-0` claim, which the storage class allows, since an edit to
`postgresql_storage_size` fails the apply. For the same reason, destroying
`addons/` in one step leaves the claim and its volume behind, and takes the
driver that would delete them with it. Tear it down in order instead: destroy
the release (`terraform destroy -target=helm_release.postgresql`), which
stops the pod still using the claim; delete the claim
(`kubectl delete pvc data-space-postgres-0 -n space`) and wait until
`kubectl get pv` no longer lists its volume; then destroy the rest.

The EBS CSI driver is there for PostgreSQL's volume: EKS ships no volume
provisioner. Its controller calls EC2 through its own IRSA role,
`<cluster>-ebs-csi-sa`, which carries only `AmazonEBSCSIDriverPolicy` under the
role boundary, so a pod on a node can't create or attach volumes.

`addons/` reads the cluster by name (`data "aws_eks_cluster"`), so it plans
only once the cluster exists, and the scheduled job runs it only once every
pool is Ready. So while the cluster root has changes to apply, a plan-only
run (`apply` unchecked) stops before `addons/`; on first setup, the first run
that plans `addons/` is the one that applies the cluster. The Helm provider
authenticates with `aws eks get-token`, so
running it locally needs the AWS CLI and credentials with an access entry.

## The apply role

`bootstrap/` grants the apply role two inline policies:

- `github_oidc.tf`: the EC2 actions the network needs, plus the `cluster/`
  and `addons/` state objects and locks (and `s3:ListBucket` on the state
  bucket, which lists
  object keys and metadata, never contents).
- `cluster_apply.tf`: the EKS cluster, its node groups, addons and access
  entries, the control-plane log group, the cluster's IAM roles and
  IRSA OIDC provider, and the EBS CSI driver's role that `addons/` creates.
  EKS, IAM and log permissions are limited to resources named from
  `cluster_name`, which all three roots must agree on, except where a name
  can't be known in advance or the API takes no resource: `eks:CreateCluster`,
  `logs:DescribeLogGroups`, EKS's service-linked roles, and the IRSA OIDC
  provider (below).

The IAM half is what an unattended role could turn into more access. The
header of `cluster_apply.tf` explains how it's fenced: every role it touches
must carry the `<cluster>-role-boundary` permissions boundary, which it can't
change or remove; it may attach only the six AWS managed policies
`eks-cluster` and `addons/` use, each only to the role that carries it there,
and pass those roles only to EKS. It can't manage the account's GitHub OIDC
provider.

The same header states what the fence leaves open. A role it creates can trust
any principal, the cluster role included, since IAM can't condition on
trust-policy contents, and as cluster admin it can reach a node's, the CNI's or
the EBS CSI driver's credentials. So it can reach what the cluster role, a
node, the CNI or the EBS CSI driver can do, within the boundary and the
region. The cluster policy is the widest of those:
region-wide EC2 volume, route, security-group and instance-attribute changes,
and load-balancer writes, none scoped by resource. The rest adds the SSM agent's
actions, KMS through EC2 for the EBS CSI driver's role only (so it can create
volumes encrypted with the account's `aws/ebs` key, which also means **any
`aws/ebs`-encrypted volume or snapshot in the region can be attached,
restored, snapshotted, copied or modified through that role**, and the apply
role can act as it), and **ECR
pulls from every repository in the account**, because that's
how nodes pull from it, SPACE's private bundle image repositories included. It
can also manage
any EKS cluster's IRSA provider in the region, because the issuer id isn't
known until the cluster exists.

One thing the boundary takes away outright: no role under it can delete,
lock or fast-restore an EBS snapshot. The EBS CSI driver's managed policy
allows deletes and locks on any snapshot tagged as the driver's, and fast
restore on any snapshot at all, and the apply role can tag any snapshot in the
region and act as the driver. A compliance-mode lock can't be lifted by anyone
until it expires, and fast restore bills per snapshot per AZ-hour. Nothing
here takes volume snapshots, so the boundary denies `ec2:DeleteSnapshot`,
`ec2:LockSnapshot` and `ec2:EnableFastSnapshotRestores`. Revisit that
statement in the same change that starts using `VolumeSnapshot`s.

What isn't fenced by resource: the EC2 actions, including `ec2:RunInstances`
(EKS checks it against the caller of `CreateNodegroup` when the launch template
tags instances), are bound by the region and the account. They could change
other EC2 resources in the same region: another stack's network, its Elastic
IPs, its launch templates (a new version with different user data runs under
that stack's instance profile), or any EC2 resource's tags. Use a dedicated
account or scope these permissions further before sharing it with other stacks.

`bootstrap/` is applied by a person, never by the schedule, so a change to it
takes effect only once someone re-applies it (see
[Changing bootstrap](#changing-bootstrap)). Until then, a scheduled run that
needs the change fails and reports it.

## First-time setup

These steps need administrator credentials for the target account. Nothing
here runs in CI.

### 1. Protect the environment

The apply role trusts only tokens issued to the `reference-deployment`
environment, so the environment's branch restriction is what stops another
branch from assuming it. Create it, restricted to exactly `main`, before the
bootstrap apply creates that trust: a job naming an environment that doesn't
exist creates it on the fly with no restriction. Creating an environment needs
repository admin.

### 2. Bootstrap the state backend

The bootstrap root keeps its own state in the bucket it creates, so its first
apply runs against local state and then migrates. Every bootstrap `apply` and
`plan` needs `-var 'cluster_admin_principal_arns=[...]'`, the IAM principals
that get cluster admin (such as `["arn:aws:iam::123456789012:role/admin"]`,
each already existing; `[]` for none). Outside `us-east-1`, also add
`-var region=<region>`, and add `-var create_github_oidc_provider=false` if the
account already has a GitHub Actions OIDC provider (details after the block):

```bash
cd infra/space/aws/reference/bootstrap

# Temporary: the bucket this root's backend names doesn't exist yet.
cat > zz_backend_override.tf <<'EOF'
terraform {
  backend "local" {}
}
EOF
terraform init
terraform apply

bucket="$(terraform output -raw state_bucket)"
region="$(terraform output -raw state_region)"
rm zz_backend_override.tf
terraform init -migrate-state \
  -backend-config="bucket=${bucket}" \
  -backend-config="region=${region}"
```

Confirm the migration before deleting the local copy: `terraform plan` must
report no changes against the S3 backend. Then remove `terraform.tfstate` and
`terraform.tfstate.backup`.

If the account already has a GitHub Actions OIDC provider, pass
`-var create_github_oidc_provider=false`; an account holds only one per issuer.

Pass the same `-var` values to every bootstrap `apply` and `plan`,
including the post-migration check: `region` outside `us-east-1`,
`create_github_oidc_provider=false` if you used it, and the same
`cluster_admin_principal_arns`. That one has no default, so leaving it out
fails rather than emptying the list; a different list takes effect at the next
scheduled cluster apply, which adds and removes access entries to match.
Flipping
`create_github_oidc_provider` to `false` after the default would plan to
destroy the account's OIDC provider, which every GitHub OIDC role in the
account needs; `prevent_destroy` makes that plan fail instead.
The region sets the state bucket's location and limits the apply role's EC2
permissions, so a bare `apply` outside `us-east-1` would move those
permissions back to `us-east-1`.

Each principal must be the role itself, not a session of it. When you sign in
through IAM Identity Center, `aws sts get-caller-identity` returns the session,
`arn:aws:sts::<account>:assumed-role/<role>/<session>`, which an access entry
can't use. The role's own ARN also has a path the session ARN leaves out
(`role/aws-reserved/sso.amazonaws.com/...`), so look it up rather than
rebuilding it by hand:

```bash
role="$(aws sts get-caller-identity --query Arn --output text | cut -d/ -f2)"
aws iam get-role --role-name "${role}" --query Role.Arn --output text
```

The repository issues immutable OIDC subjects
(`repo:<owner>@<id>/<name>@<id>:...`), not `repo:<owner>/<name>:...`, so the
trust is built from `github_oidc_subject_prefixes`, one prefix per trusted
repository. If `AssumeRoleWithWebIdentity` is denied, compare the list with
`gh api repos/<owner>/<name>/actions/oidc/customization/sub` for the repository
whose job was denied.

The default list trusts `perplexityai/SPACE` and the subject its replacement
repository is expected to issue once it's renamed to `SPACE`. The replacement's
pre-rename subject isn't listed, so nothing can mint the second entry until the
rename. That entry is unverified until then, but only the repository with that
id can mint it, so it's safe to list early. `StringEquals` is case-sensitive, so
it matches only if the new name is exactly `SPACE`. Remove the old repository's
entry once the replacement has taken over.

A repository can mint its listed subjects as soon as it holds the listed name.
From then on it must:

- have a `github_environment` (`reference-deployment`) environment restricted
  to `main`. A job that names a missing environment creates it without
  protection, so until then any branch can mint the environment subject.
- protect `main` with rulesets that block deletion and force-pushes and
  require a pull request with an approval, the required checks, and the merge
  queue. An approval isn't dismissed by later pushes, so anyone with write
  access can still land unreviewed commits after one; the roles trust every
  writer to that extent. Restricting the environment to `main` gates
  nothing while anyone with write access can push to `main` directly, and the
  same `main` subject is what the OSS registry's push role trusts. A ruleset
  bypass actor can still push to `main` directly, and so can assume both roles.
- limit admin access to the people trusted with both roles. An admin can
  change the rulesets, their bypass list and the environment's branch policy.

Put these in place on a replacement repository before it's renamed.

### 3. Bake the node AMIs

The cluster root looks up a worker and a builder AMI in the account, so a plan
fails until both exist. Bake them with administrator credentials, from the
repository root, at the cluster's Kubernetes minor (`kubernetes_version` in
`cluster/variables.tf`):

```bash
infra/space/packer/aws/build.sh --kubernetes-version 1.36 --type worker
infra/space/packer/aws/build.sh --kubernetes-version 1.36 --type builder
```

Each bake launches a temporary build instance with a public IP in the region's
default VPC and needs `packer` on the path; `infra/space/packer/README.md` has
the details. Bake from `main`: the cluster root uses only bakes tagged with
`node_ami_branch`. Re-bake to pick up a change to the recipes, or when moving
the cluster to a new minor: bake at the new minor first, or the plan finds no
AMI. The schedule never bakes, so a regression in the recipes shows up only at
the next bake.

Before planning, check that the cluster root's lookup finds both images. This
filters on the tags that lookup filters on, so it lists exactly the images a
plan can choose from, and the newest of each role wins:

```bash
aws ec2 describe-images --owners self --region us-east-1 \
  --filters Name=tag:KubernetesVersion,Values=1.36 Name=tag:GitBranch,Values=main Name=state,Values=available \
  --query 'sort_by(Images,&CreationDate)[].[Tags[?Key==`Role`]|[0].Value,ImageId,Name,CreationDate]' \
  --output table
```

A role with no row fails the plan. A bake that's missing here but shows up
without the filters was baked from another branch or at another minor.

The node-pool sizes start at zero, so the first apply runs no metal instances.

### 4. Configure the repository

Set these **repository** variables (not environment variables — the job's
`if` is evaluated before the environment is entered, so environment variables
aren't visible to it):

- `REFERENCE_DEPLOYMENT_ROLE_ARN` — the bootstrap output `apply_role_arn`
- `REFERENCE_TF_STATE_BUCKET` — the bootstrap output `state_bucket`
- optionally, outside `us-east-1`, both `REFERENCE_AWS_REGION` and
  `REFERENCE_AVAILABILITY_ZONES` (that region's AZs as a list such as
  `["eu-west-1a","eu-west-1b","eu-west-1c"]`; the root's default AZs are
  us-east-1's, and a plan rejects AZs outside the region). The region must be
  the one bootstrap was applied with (`-var region=...`): the state bucket
  lives there, and the apply role's EC2 permissions are limited to it.

Until the first two are set, the scheduled job is skipped rather than failing.

### 5. Apply the cluster root

Run the workflow by hand with `apply` unchecked first, and read its plan. This
proves the OIDC chain and the apply role before anything is created:

```bash
gh workflow run reference-deployment.yml --repo perplexityai/SPACE --ref main -f apply=false
```

Then run it again with `apply=true`, or wait for the schedule. A first apply
creates the network, the control plane, its addons and the three node groups.
Don't cancel it: see [The schedule](#the-schedule) for what an interrupted
apply leaves behind.

To work on the root locally, from the repository root, with administrator
credentials and `bootstrap/` initialized against its S3 backend as in step 2
(`terraform output` reads bootstrap's state):

```bash
cd infra/space/aws/reference/cluster
bucket="$(terraform -chdir=../bootstrap output -raw state_bucket)"
region="$(terraform -chdir=../bootstrap output -raw state_region)"
terraform init -backend-config="bucket=${bucket}" -backend-config="region=${region}"
terraform plan -var="region=${region}"   # plus -var='availability_zones=[...]' outside us-east-1
```

### 6. Check the cluster

As one of bootstrap's `cluster_admin_principal_arns`:

```bash
aws eks update-kubeconfig --region us-east-1 --name space-reference
kubectl get nodes -L space.pplx.net/node-pool
```

With the metal pools at zero, that's the one `general` node, `Ready`. The
apply job checks the same thing; running it yourself also proves your own
access entry works.

Then check that a node can pull from the account's ECR. Every node role
carries `AmazonEC2ContainerRegistryPullOnly`, so the `general` node proves it
without scaling a metal pool. This needs an image in one of the account's
repositories, and nothing here creates one:

```bash
image=<account>.dkr.ecr.us-east-1.amazonaws.com/<repository>:<tag>
kubectl run ecr-pull-check --image="${image}" --image-pull-policy=Always --restart=Never
kubectl get events --field-selector involvedObject.name=ecr-pull-check
kubectl delete pod ecr-pull-check
```

A `Pulled` event means the pull worked, even if the container exits or crashes
afterwards. `ErrImagePull` with a 401 or 403 means it didn't. To check a metal
pool's role instead, scale it up (see
[Scaling the metal pools](#scaling-the-metal-pools)) and add
`--overrides='{"spec":{"nodeSelector":{"space.pplx.net/node-pool":"workers"}}}'`.
A pod on `builders` also needs a toleration for its taint.

`addons/` works the same way once the cluster exists, and `verify.sh` checks
each add-on against kubectl's current context:

```bash
cd ../addons
terraform init -backend-config="bucket=${bucket}" -backend-config="region=${region}"
terraform plan -var="region=${region}"
./verify.sh metrics-server && ./verify.sh postgresql && ./verify.sh valkey
```

## Changing bootstrap

A change under `bootstrap/` lands on `main` without taking effect: someone with
administrator credentials re-applies it, and the schedule's next run picks it
up. From the repository root, with the same `-var` values as every earlier
bootstrap apply (see step 2):

```bash
cd infra/space/aws/reference/bootstrap
account="$(aws sts get-caller-identity --query Account --output text)"
region=us-east-1   # the region bootstrap was first applied with
# The state bucket is `<name_prefix>-tfstate-<account>`.
terraform init -backend-config="bucket=space-reference-tfstate-${account}" -backend-config="region=${region}"
terraform plan -out=bootstrap.tfplan \
  -var 'cluster_admin_principal_arns=["arn:aws:iam::<account>:role/<admin>"]'
  # plus -var region=... and -var create_github_oidc_provider=false if you used them
terraform apply bootstrap.tfplan
```

Read the plan before applying it. It should change only the resources the
merged diff touches. A policy edit is usually an in-place update to
`aws_iam_role_policy.apply_cluster`, `aws_iam_role_policy.apply` or the
`aws_iam_policy.role_boundary`. List what the plan changes, then compare each
changed policy (including a role's trust policy) before and after, not just
its name. A policy Terraform can't
compute until apply (it happens when one of its SSM parameters changes, or on
a first bootstrap) prints as `known after apply` instead of a diff: plan again
after the parameters exist, or read the document from
`terraform console`, before applying it.

```bash
terraform show -json bootstrap.tfplan \
  | jq -r '.resource_changes[] | select(.change.actions != ["no-op"]) | "\(.change.actions | join(",")) \(.address)"'
terraform show -json bootstrap.tfplan \
  | jq '.resource_changes[] | select(.change.actions != ["no-op"]) | . as $r
        | ("policy", "assume_role_policy") as $k
        | select(($r.change.before // {})[$k] != null or ($r.change.after // {})[$k] != null
                 or ($r.change.after_unknown // {})[$k] == true)
        | {address: $r.address, attribute: $k, actions: $r.change.actions,
           before: (($r.change.before // {})[$k] // "null" | fromjson),
           after: (if ($r.change.after_unknown // {})[$k] == true then "known after apply"
                   else (($r.change.after // {})[$k] // "null" | fromjson) end)}'
```

A change to the state bucket, the OIDC provider or the apply role's trust
means the `-var` values don't match the earlier apply.

The cluster root's plan also needs the node AMIs, so bake them from `main`
(step 3) before this if they don't exist yet; until both the re-apply and the
bakes are done, every plan fails.

Then prove the role works before the schedule applies with it: run the
workflow by hand with `apply` unchecked
(`gh workflow run reference-deployment.yml --repo perplexityai/SPACE -f apply=false`)
and read its plan, then run it again with `apply` checked.

## Scaling the metal pools

The workers and builders pools run bare-metal instances, so they stay at zero
nodes except while someone needs them, for a demo or a smoke run. Their sizes
live in the `/<cluster>/node-pool-sizes` SSM parameter, which `bootstrap/`
creates at zero and never resets, so a local plan and the schedule always read
the same sizes. To scale up, with administrator credentials:

```bash
aws ssm put-parameter --overwrite --name /space-reference/node-pool-sizes \
  --value '{"workers":1,"builders":1}'
gh workflow run reference-deployment.yml --repo perplexityai/SPACE -f apply=true
```

Scale back down the same way with `{"workers":0,"builders":0}`. Each pool takes
0 or 1. The size holds
until someone changes the parameter; the schedule applies whatever it says. A
smoke-test job could make the same two calls, given a role that may write the
parameter and dispatch the workflow; none has that today.

Metal instances count against the account's on-demand vCPU quota for standard
instance families (`L-1216C47A`). Each `m5d.metal` is 96 vCPUs, so one of each
needs 192 on top of the `general` node's 2, and a fresh account's quota is often
lower. The metal pools roll with EKS's `MINIMAL` update strategy, which drains a
node before launching its replacement, so a roll after a new bake needs no quota
beyond the pools' sizes (EKS's default strategy launches up to two extra nodes
per AZ first). Check the quota before scaling up
(`aws service-quotas get-service-quota --service-code ec2 --quota-code L-1216C47A`)
and request an increase if needed.

## The schedule

`.github/workflows/reference-deployment.yml` runs daily and on demand:

- **Pull requests** touching this directory, the `eks-cluster` module, the
  `modules/space` templates the cluster root renders, or the node AMI recipes
  get `fmt` and `validate` on all three roots, `helm lint` on the add-on
  charts, `shellcheck` on `verify.sh`, and `packer fmt` and `packer validate`
  on both recipes, with no AWS credentials. A plan executes provider code, and a pull
  request can come from a fork.
- **The schedule** plans the cluster root and applies whatever the plan
  contains. A run with nothing to change applies nothing.
- **After an apply, or a plan with nothing to change**, the job waits up to
  twenty minutes for each pool to have exactly as many Ready nodes as EKS says
  its node group is sized for, so a pool that should have nodes and has none fails the
  run. A cluster
  that exists but can't run anything counts as a failure.
- **Once every pool is Ready**, a second job plans `addons/` and applies it on
  the same terms (on the schedule, or when `apply` is checked), then runs
  `verify.sh` for each add-on as its own step. Every check runs even when an
  earlier one fails, so one broken add-on doesn't hide the state of the
  others. It has its own role session and time limit, and a failure there
  leaves the cluster as it was.
- **A manual run** plans, and applies only when `apply` is checked.
- **A failed scheduled run** posts to the CI Slack channel through the
  `internal-slack` environment, the same path main CI failures take, so a
  failure reaches someone without anyone watching the Actions tab.

A run in progress is never cancelled: an interrupted apply leaves a held state
lock and a half-created resource behind. A run still waiting for its turn can
be replaced by a newer one. If a run did die mid-apply (a timeout, a runner
loss), the next one fails on the held lock; clear it with
`terraform force-unlock <lock-id>` from the root that holds it (`cluster/` or
`addons/`), initialized as in step 5 with administrator credentials, once no
run is in progress. A run that died during an `addons/` apply can also leave a
Helm release mid-operation, and every later apply fails with "another
operation (install/upgrade/rollback) is in progress" until it's cleared:
`helm history <release> -n space` shows it `pending-install` or
`pending-upgrade`; `helm rollback <release> -n space` clears an upgrade, and
`helm uninstall <release> -n space` a first install (PostgreSQL's claim and
data survive either).

## Cost

Running around the clock: the EKS control plane, one `m6i.large` node and its
50 GiB gp3 volume, PostgreSQL's 20 GiB gp3 volume, one NAT gateway and its
Elastic IP, the control plane's
audit and authenticator logs in CloudWatch (30-day retention), and the
snapshots behind the baked AMIs. The metal workers and builders, each with a
100 GiB volume, cost only while they're scaled up, and dominate the bill when
they are. The single
gateway is a deliberate trade: losing its AZ cuts egress for every private
subnet, which is acceptable for a reference deployment and not for production.

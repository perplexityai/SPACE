# OSS account image registry

Seven **private** ECR repositories, one for each image
`.github/workflows/artifact-ci.yml` builds (`space/worker`, `space/builder`,
`space/api`, `space/data-plane`, `space/cluster-controller`, `space/ui`,
`space/rollout-mng`), and the `space-oss-image-push` role that `main` CI
assumes to push to them. They serve SPACE's own deployments in the OSS AWS
account. They have no repository policy, so they aren't a distribution
channel: nothing outside the account can pull from them.

A person applies this root, never CI: the push role must not be able to
rewrite its own trust policy.

The repositories and their lifecycle rules come from the shared
[`ecr-repositories`](../modules/ecr-repositories/README.md) module. This root
keeps only what's specific to the OSS account: the repository list, the
settings it passes the module, and the push role. Earlier revisions declared
the repositories here directly, and `moved` blocks in `main.tf` carry a state
written by one of them onto the module's addresses without replacing anything.

## Behavior

- **Who pushes.** Every push to `main` builds all seven images and, once
  `SPACE_ECR_PUSH_ROLE_ARN` is set, pushes them as `<commit sha>`. Pull
  requests and the merge queue don't run these builds, and a manual run of the
  workflow never publishes. The push role trusts only the `refs/heads/main`
  subject. Its permissions are AWS's documented push set (which includes
  reading manifests) on these seven repositories, plus the registry login.
- **One workflow on `main` may mint the token.** The `ref:refs/heads/main`
  subject is issued to every job on `main` that has `id-token: write`, not only
  to `main-ci`. So no other workflow that runs on `main` may grant
  `id-token: write`, least of all one that handles untrusted input
  (`issue_comment`, `workflow_run`). Narrowing the trust to one workflow needs
  the repository's OIDC subject template to include `job_workflow_ref`, which
  changes every role's subject in the account.
- **Tools.** A publishing build runs the build script on the runner's own
  `python3` and uses the runner's `docker` and `aws`. It never downloads
  and runs an installer, because the job holds registry credentials and can
  mint its OIDC token, and the BuildKit daemon that does the push is pinned by
  digest.
- **Tags.** Each image carries only its commit tag, and tags are immutable,
  so a deployment pins an exact commit and nothing can be retagged under it.
  There is no moving tag: a deployment that wants the newest image resolves a
  commit first.
- **Re-runs.** A re-run whose commit tag already exists skips the build, so
  either re-run button is safe.
- **Retention.** Each repository keeps its newest 300 images
  (`image_retention_count`), so the retention window depends on build frequency.
  Untagged images expire after 7 days.
- **Subject format.** The repository issues immutable OIDC subjects
  (`repo:<owner>@<id>/<name>@<id>:...`), not `repo:<owner>/<name>:...`, so the
  trust is built from `github_oidc_subject_prefixes`, one prefix per trusted
  repository. If `AssumeRoleWithWebIdentity` is denied, compare the list with
  `gh api repos/<owner>/<name>/actions/oidc/customization/sub` for the
  repository whose job was denied.
- **Trusted repositories.** The default list trusts `perplexityai/SPACE` and
  the subject its replacement repository is expected to issue once it's
  renamed to `SPACE`. The replacement's pre-rename subject isn't listed, so
  nothing can mint the second entry until the rename. That entry is unverified
  until then, but only the repository with that id can mint it, so it's safe to
  list early. `StringEquals` is case-sensitive, so it matches only if the new
  name is exactly `SPACE`. A repository holding a listed name must protect
  `main` with rulesets that block deletion and force-pushes and require a pull
  request with an approval, the required checks, and the merge queue. The role
  trusts whatever reaches that repository's `main`: a direct push by a ruleset
  bypass actor, commits pushed after an approval (later pushes don't dismiss
  it), and anything its admins allow by changing the rulesets. Put that
  protection, and an admin list no wider than the old repository's, on a
  replacement repository before it's renamed. Remove the old repository's
  entry once the replacement has taken over.

`docs/space/diagrams/oss-image-publish.png` draws the flow.

## Setup

These steps need administrator credentials for the OSS account.

1. **Apply the root.** It reads the account's existing GitHub Actions OIDC
   provider instead of creating one, since an account holds one per issuer.
   State goes in the account's Terraform state bucket under
   `oss-registry/terraform.tfstate`:

   ```bash
   cd infra/space/aws/oss-registry
   terraform init -backend-config="bucket=<state bucket>" -backend-config="region=<bucket region>"
   terraform apply
   ```

2. **Turn publishing on.** Set the `SPACE_ECR_PUSH_ROLE_ARN` repository
   variable to the `image_push_role_arn` output, and `SPACE_ECR_REGION` when
   the root's `region` isn't `us-east-1`. Set it only after the apply has
   created the role: once it's set, a push that can't assume the role fails
   every `main` run's artifact job.

The next `main` push publishes all seven images.

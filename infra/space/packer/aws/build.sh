#!/usr/bin/env bash
# Build a SPACE AWS AMI.
#
# Usage: ./build.sh --kubernetes-version VER [--type TYPE] [--region REGION]
#                   [--skip-conflict-check] [packer-var-args...]
#   --kubernetes-version VER  Kubernetes minor for the AL2023 EKS base AMI, e.g. 1.36 (required)
#   --type TYPE               Image type: worker (default), gpu-worker, or builder
#   --region REGION           AWS region (default: us-east-1)
#   --skip-conflict-check     Skip the same-GitSHA/different-minor AMI check
#
# --kubernetes-version selects the AL2023 EKS base image, so it decides the
# kubelet minor of every node launched from the result. It has no default: a
# default in this script would silently rot as clusters move, and a kubelet
# newer than its control plane isn't supported. Bake at the minor of the oldest
# control plane the image will join.
#
# Prerequisites:
#   - AWS credentials configured (aws configure, env vars, or instance profile)
#   - packer on PATH (this script runs `packer init`)
#
# Examples:
#   ./build.sh --kubernetes-version 1.36                  # worker AMI in us-east-1
#   ./build.sh --kubernetes-version 1.36 --type builder
#   ./build.sh --kubernetes-version 1.36 --region us-west-2
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

vcs_sha() {
  if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    git rev-parse HEAD
    return
  fi

  if command -v sl >/dev/null 2>&1 && sl root >/dev/null 2>&1; then
    sl log -r . -T '{node}'
    return
  fi

  echo "unknown"
}

vcs_branch() {
  if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    git rev-parse --abbrev-ref HEAD
    return
  fi

  if command -v sl >/dev/null 2>&1 && sl root >/dev/null 2>&1; then
    local bookmark
    bookmark="$(sl log -r . -T '{bookmarks}')"
    if [[ -n "$bookmark" ]]; then
      echo "$bookmark"
      return
    fi
    sl log -r . -T '{branch}'
    return
  fi

  echo "unknown"
}

IMAGE_TYPE="worker"
REGION=""
KUBERNETES_VERSION=""
SKIP_CONFLICT_CHECK="false"
PACKER_ARGS=()
GIT_SHA="$(vcs_sha)"
GIT_BRANCH="$(vcs_branch)"
BUILT_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

require_value() {
  if [[ $# -lt 2 || -z "$2" ]]; then
    echo "$1 needs a value." >&2
    exit 1
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --type)
      require_value "$@"
      IMAGE_TYPE="$2"
      shift 2
      ;;
    --region)
      require_value "$@"
      REGION="$2"
      shift 2
      ;;
    --kubernetes-version)
      require_value "$@"
      KUBERNETES_VERSION="$2"
      shift 2
      ;;
    --skip-conflict-check)
      SKIP_CONFLICT_CHECK="true"
      shift
      ;;
    kubernetes_version=*|-var=kubernetes_version=*)
      # A second source for the same variable; packer would silently take the
      # last one. Keep --kubernetes-version the only spelling.
      echo "Set the Kubernetes minor with --kubernetes-version; drop '$1'." >&2
      exit 1
      ;;
    aws_region=*|-var=aws_region=*)
      # The region has to be one value: the conflict check below queries it and
      # packer builds in it. Keep --region the only spelling.
      echo "Set the region with --region; drop '$1'." >&2
      exit 1
      ;;
    gpu_passthrough=*|-var=gpu_passthrough=*)
      echo "Select GPU passthrough with --type gpu-worker; drop '$1'." >&2
      exit 1
      ;;
    *)
      PACKER_ARGS+=("$1")
      shift
      ;;
  esac
done

if [[ -z "$KUBERNETES_VERSION" ]]; then
  echo "Pass --kubernetes-version, the Kubernetes minor to bake for (e.g. 1.36)." >&2
  exit 1
fi

if [[ ! "$KUBERNETES_VERSION" =~ ^[0-9]+\.[0-9]+$ ]]; then
  echo "--kubernetes-version must be a MAJOR.MINOR string such as 1.36, got: $KUBERNETES_VERSION" >&2
  exit 1
fi

# Map type to HCL file
case "$IMAGE_TYPE" in
  worker)
    HCL_FILE="space-worker.pkr.hcl"
    AMI_ROLE="worker"
    ;;
  gpu-worker)
    HCL_FILE="space-worker.pkr.hcl"
    AMI_ROLE="gpu-worker"
    PACKER_ARGS+=("-var" "gpu_passthrough=true")
    ;;
  builder)
    HCL_FILE="space-builder.pkr.hcl"
    AMI_ROLE="builder"
    ;;
  *)
    echo "Unknown image type: $IMAGE_TYPE. Use: worker, gpu-worker, builder" >&2
    exit 1
    ;;
esac

# Resolve the region once and pass it explicitly, rather than letting packer
# fall back to the recipe default. These `-var`s come after anything the caller
# passed, and packer takes the last value, so a `-var-file` or PKR_VAR_aws_region
# cannot move the build to a region the conflict check below did not look at.
REGION="${REGION:-us-east-1}"

PACKER_ARGS+=(
  "-var" "aws_region=$REGION"
  "-var" "git_sha=$GIT_SHA"
  "-var" "git_branch=$GIT_BRANCH"
  "-var" "built_at=$BUILT_AT"
  "-var" "kubernetes_version=$KUBERNETES_VERSION"
)

# `modules/space`'s `data.aws_ami.workers` / `.builders` resolve an AMI from
# GitSHA + Role with most_recent = true. Baking one commit at two minors puts
# two images behind that pair, and the later bake then wins for every cluster
# using that SHA —
# including one whose control plane is still on the older minor, which would
# leave its kubelet ahead of its API server. Refuse to create the ambiguity.
if [[ "$SKIP_CONFLICT_CHECK" == "false" ]]; then
  # Selects same-SHA, same-Role images that do NOT carry this exact minor —
  # a different minor, or none at all because they predate the tag. Either way
  # the pair is ambiguous.
  conflict_query="$(printf \
    "Images[?length(Tags[?Key=='KubernetesVersion' && Value=='%s'])==\`0\`].ImageId" \
    "$KUBERNETES_VERSION")"

  if ! conflicting="$(aws ec2 describe-images \
    --owners self \
    --region "$REGION" \
    --filters "Name=tag:GitSHA,Values=$GIT_SHA" "Name=tag:Role,Values=$AMI_ROLE" \
    --query "$conflict_query" \
    --output text)"; then
    echo "Could not list existing AMIs to check for a GitSHA/minor conflict." >&2
    echo "Fix AWS access — packer needs it too — or pass --skip-conflict-check." >&2
    exit 1
  fi

  if [[ -n "$conflicting" ]]; then
    echo "Refusing to bake: commit $GIT_SHA already has a $IMAGE_TYPE AMI that is" >&2
    echo "not tagged KubernetesVersion=$KUBERNETES_VERSION: $conflicting" >&2
    echo "Both would carry the same GitSHA and Role tags, making role lookup ambiguous." >&2
    echo "Bake this minor from its own commit." >&2
    exit 1
  fi
fi

echo "Building SPACE $IMAGE_TYPE AMI ($HCL_FILE)..."
echo "Base AMI: amazon-eks-node-al2023-x86_64-standard-$KUBERNETES_VERSION-v*"
echo "Kubernetes version: $KUBERNETES_VERSION (tagged on the AMI as KubernetesVersion)"
echo "Git SHA: $GIT_SHA"
echo "Git branch: $GIT_BRANCH"

packer init "$HCL_FILE"
packer build "${PACKER_ARGS[@]}" "$HCL_FILE"

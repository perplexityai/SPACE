#!/usr/bin/env bash
# Activate the Kind-only root@local login after API migrations complete.
# Run by Tilt's provision-local-console resource; the password is never rendered
# into a Kubernetes resource, and --if-locked preserves an existing login.
set -euo pipefail
set +x

KIND_CLUSTER="${SPACE_KIND_CLUSTER:-space-local}"
KCTX="kind-$KIND_CLUSTER"
NAMESPACE=space
CONSOLE_PASSWORD="${SPACE_CONSOLE_PASSWORD-password}"
export -n CONSOLE_PASSWORD
unset SPACE_CONSOLE_PASSWORD

[[ -n "$CONSOLE_PASSWORD" ]] || { printf 'SPACE_CONSOLE_PASSWORD must not be empty\n' >&2; exit 1; }
[[ "$CONSOLE_PASSWORD" != *$'\n'* ]] || { printf 'SPACE_CONSOLE_PASSWORD must not contain a newline\n' >&2; exit 1; }
password_bytes=$(printf '%s' "$CONSOLE_PASSWORD" | wc -c)
(( password_bytes <= 72 )) || { printf 'SPACE_CONSOLE_PASSWORD must be at most 72 bytes\n' >&2; exit 1; }

kubectl --context "$KCTX" -n "$NAMESPACE" rollout status deploy/space-api \
  --timeout="${SPACE_BOOTSTRAP_TIMEOUT:-600}s"
printf '[provision-local-console] activating root@local if locked\n'
printf '%s\n' "$CONSOLE_PASSWORD" |
  kubectl --context "$KCTX" -n "$NAMESPACE" exec -i deploy/space-api -c api -- sh -c '
    IFS= read -r SPACE_USER_PASSWORD || exit 1
    export SPACE_USER_PASSWORD
    exec /space/bin/space-db-migrator set-user-password \
      --database-url "$POSTGRES_CONNECTION_STRING" \
      --email root@local --if-locked
  '
unset CONSOLE_PASSWORD

# Previous Kind revisions persisted this password. No current pod consumes
# the Secret, so remove only that obsolete resource after provisioning works.
kubectl --context "$KCTX" -n "$NAMESPACE" delete secret space-console-bootstrap --ignore-not-found

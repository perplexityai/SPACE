#!/usr/bin/env bash
# Discover SPACE templates in a configured S3 bucket and register each one
# as a template release in the cluster Postgres. Templates are NOT downloaded
# to the local node — once the rows below exist, every worker's tmplmgr
# prefetcher (or an on-demand `GetTemplate` driven by a sandbox create)
# pulls the artifacts from S3 and installs them locally with the
# subvolume + `received_uuid` shape.
#
# This script is intentionally idempotent and runs ONCE against the
# cluster Postgres — not per-stack. The four rows it writes are the only
# state the cluster needs to know a template exists and where to fetch
# it from:
#
#   templates                          (name)
#   template_releases                  (template_name + content_hash)
#   template_release_tags              (latest → release_id)
#   template_release_storage_locations (release_id → S3 URI)
#
# Bucket layout discovered:
#
#   s3://$BUCKET/<name>/manifest.json                       (flat / legacy)
#   s3://$BUCKET/templates/<name>/sha256:.../manifest.json  (hashed / canonical)
#
# Templates that exist in both layouts use the hashed (canonical) one;
# flat-only templates fall back to `s3://$BUCKET/<name>/manifest.json`.
#
# Env:
#   TEMPLATE_BOOTSTRAP_BUCKET   — required; S3 bucket name (no scheme)
#   TEMPLATE_BOOTSTRAP_NAMES    — optional; comma/space-separated list of
#                                 template names. Skips bucket-root scan.
#   TEMPLATE_BOOTSTRAP_FILTER   — optional; glob applied to discovered
#                                 names when TEMPLATE_BOOTSTRAP_NAMES is
#                                 unset (default: '*')
#   POSTGRES_CONNECTION_STRING  — required; shared cluster DB
#   AWS_REGION                  — optional; defaults to us-east-1
#   AWS_PROFILE / AWS_*         — passed through to aws CLI
set -euo pipefail

LOG_PREFIX=bootstrap-templates
log() { printf '[%s] %s\n' "$LOG_PREFIX" "$*" >&2; }
die() { printf '[%s] ERROR: %s\n' "$LOG_PREFIX" "$*" >&2; exit 1; }

[[ -n "${TEMPLATE_BOOTSTRAP_BUCKET:-}" ]] || die "TEMPLATE_BOOTSTRAP_BUCKET is required"
[[ "$TEMPLATE_BOOTSTRAP_BUCKET" != *://* ]] || die "TEMPLATE_BOOTSTRAP_BUCKET must be a bucket name, not a URI"
[[ -n "${POSTGRES_CONNECTION_STRING:-}" ]] || die "POSTGRES_CONNECTION_STRING is required"
command -v aws  >/dev/null 2>&1 || die "aws CLI is required"
command -v jq   >/dev/null 2>&1 || die "jq is required"
command -v psql >/dev/null 2>&1 || die "psql is required"

# The rule that picks a digest's original release, shared with the internal
# API, the API and the warm pool: a whole SQL predicate over a candidate row,
# with `{release}` standing for the row's alias. Reading it here, rather than
# spelling the rule again, keeps this script attaching its tag and storage
# location to the release every other caller resolves.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DIGEST_ORIGINAL_SQL="$SCRIPT_DIR/../crates/l30_storage/template/release/original/src/digest_original.sql"
[[ -r "$DIGEST_ORIGINAL_SQL" ]] || die "missing $DIGEST_ORIGINAL_SQL; run this script from a SPACE checkout"
DIGEST_ORIGINAL="$(<"$DIGEST_ORIGINAL_SQL")"
DIGEST_ORIGINAL="${DIGEST_ORIGINAL//\{release\}/r}"

BUCKET="$TEMPLATE_BOOTSTRAP_BUCKET"
FILTER="${TEMPLATE_BOOTSTRAP_FILTER:-*}"
export AWS_REGION="${AWS_REGION:-us-east-1}"

list_template_prefixes() {
  # Flat-layout names: CommonPrefixes at the bucket root.
  aws s3api list-objects-v2 \
    --bucket "$BUCKET" \
    --delimiter / \
    --query 'CommonPrefixes[].Prefix' \
    --output text \
    | tr '\t' '\n' \
    | sed 's:/$::' \
    | grep -v '^$' || true
}

list_hashed_prefixes() {
  # Hashed-layout names: CommonPrefixes under `templates/`.
  aws s3api list-objects-v2 \
    --bucket "$BUCKET" \
    --prefix templates/ \
    --delimiter / \
    --query 'CommonPrefixes[].Prefix' \
    --output text 2>/dev/null \
    | tr '\t' '\n' \
    | sed -E 's:templates/(.*)/$:\1:' \
    | grep -v '^$' || true
}

# For a template `name`, locate the newest hashed-layout prefix by the
# LastModified of its `metadata.json`. Echoes the full S3 prefix
# (without trailing slash), or empty if no hashed build exists.
latest_hashed_prefix() {
  local name="$1"
  aws s3api list-objects-v2 \
    --bucket "$BUCKET" \
    --prefix "templates/$name/" \
    --query 'reverse(sort_by(Contents,&LastModified))[?ends_with(Key, `/metadata.json`)] | [0].Key' \
    --output text 2>/dev/null \
    | sed 's:/metadata.json$::' \
    | grep -v '^None$' || true
}

register_template_in_postgres() {
  local name="$1"
  local hash="$2"
  local storage_uri="$3"

  # psql `-v` + `:'name'` placeholders quote values safely; never
  # interpolate bash $vars directly into the SQL body. `:digest_original` is
  # the repository's own predicate, not a value, so it goes in unquoted.
  psql "$POSTGRES_CONNECTION_STRING" -v ON_ERROR_STOP=1 \
    -v name="$name" -v hash="$hash" -v storage_uri="$storage_uri" \
    -v digest_original="$DIGEST_ORIGINAL" <<'SQL'
BEGIN;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM clusters WHERE singleton) THEN
    RAISE EXCEPTION 'missing cluster singleton for template materialization';
  END IF;
END $$;

WITH managing_cluster AS (
  SELECT managing_org_id
  FROM clusters
  WHERE singleton
),
new_id AS (
  -- gen_random_uuid is v4; flip the version nibble to satisfy the
  -- templates_id_uuid_v7 check constraint. Ordering is not load-bearing
  -- here, so we don't bother packing a real ms timestamp.
  SELECT (substring(u::text, 1, 14) || '7' || substring(u::text, 16))::uuid AS id
  FROM (SELECT gen_random_uuid() AS u) t
)
INSERT INTO templates (id, name, org_id, network_policy_json, network_policy_generation)
SELECT
  id,
  :'name',
  managing_org_id,
  '{"version":"v1","egress":{"default_action":"deny"}}',
  1
FROM new_id
CROSS JOIN managing_cluster
ON CONFLICT (name) DO UPDATE
SET org_id = COALESCE(templates.org_id, EXCLUDED.org_id)
WHERE templates.org_id IS NULL;

-- A release is unique per template, hash and execution environment, so a hash
-- can have twins bound to other environments. This registration carries no
-- execution metadata, so it inserts a release only when the digest has none
-- under either spelling (bare `<hex>` beside `sha256:<hex>`). The tag and
-- storage location below go to the digest's original, whichever spelling it's
-- stored under. The internal API registers under this same template row lock
-- and stamps created_at with clock_timestamp(), so the existence check can't
-- race its insert and insert order is registration order.
SELECT 1 FROM templates WHERE name = :'name' FOR UPDATE;

INSERT INTO template_releases (template_name, type, hash, org_id, created_at)
SELECT :'name', 'fc', :'hash', org_id, clock_timestamp()
FROM templates
WHERE name = :'name'
  AND NOT EXISTS (
    SELECT 1
    FROM template_releases other
    WHERE other.template_name = :'name'
      AND other.hash IN (replace(:'hash', 'sha256:', ''),
                         'sha256:' || replace(:'hash', 'sha256:', ''))
  )
ON CONFLICT DO NOTHING;

-- An owner filled in later goes to every release of this spelling that has
-- none, twins included.
UPDATE template_releases
SET org_id = templates.org_id
FROM templates
WHERE templates.name = :'name'
  AND template_releases.template_name = :'name'
  AND template_releases.hash = :'hash'
  AND template_releases.org_id IS NULL;

-- The digest's original can be stored under the other spelling, which the
-- update above doesn't reach, so give it the template's owner here too.
UPDATE template_releases r
SET org_id = t.org_id
FROM templates t
WHERE t.name = :'name'
  AND r.template_name = :'name'
  AND r.hash IN (replace(:'hash', 'sha256:', ''),
                 'sha256:' || replace(:'hash', 'sha256:', ''))
  AND r.org_id IS NULL
  AND :digest_original;

INSERT INTO template_release_tags (template_release_id, template_name, tag)
SELECT r.id, r.template_name, 'latest'
FROM template_releases r
WHERE r.template_name = :'name'
  AND r.hash IN (replace(:'hash', 'sha256:', ''),
                 'sha256:' || replace(:'hash', 'sha256:', ''))
  AND :digest_original
ON CONFLICT (template_name, tag) DO UPDATE
SET template_release_id = EXCLUDED.template_release_id;

INSERT INTO template_release_storage_locations (
  template_release_id,
  storage_connection_string,
  org_id
)
SELECT r.id, :'storage_uri', r.org_id
FROM template_releases r
WHERE r.template_name = :'name'
  AND r.hash IN (replace(:'hash', 'sha256:', ''),
                 'sha256:' || replace(:'hash', 'sha256:', ''))
  AND :digest_original
ON CONFLICT (template_release_id, storage_connection_string) DO UPDATE
SET org_id = COALESCE(
  template_release_storage_locations.org_id,
  EXCLUDED.org_id
)
WHERE template_release_storage_locations.org_id IS NULL;

COMMIT;
SQL
}

# Each install runs in a subshell so its `trap '…' EXIT` cleanup is
# scoped to the call. RETURN traps on the parent shell are global, which
# is the fragility psi-review-bot flagged in the previous shape.
install_template() (
  set -euo pipefail
  local name="$1"
  log "==> $name"

  # Layout precedence: flat first (`s3://$BUCKET/<name>/manifest.json`)
  # because the SPACE template build pipeline treats the flat path as
  # the "latest blessed" copy, which for seed can be newer than the
  # most-recent hashed-layout release. Fall back to hashed
  # (`templates/<name>/sha256:.../manifest.json`) for templates that
  # only publish a hashed build. Probing flat first only needs HeadObject
  # /GetObject and works on creds that don't carry s3:ListBucket.
  #
  # Opt-in override: TEMPLATE_BOOTSTRAP_PREFER=hashed forces the hashed
  # layout. Use this when the flat-layout copy in the bucket lacks the
  # rootfs.ext4.zst variant that the worker's volmgr pull path expects
  # (the kind harness hits this — its sandbox S3 only stages .zst under
  # the hashed `templates/<name>/sha256:.../` prefix).
  local s3_prefix="" hash
  if [[ "${TEMPLATE_BOOTSTRAP_PREFER:-flat}" == "hashed" ]]; then
    s3_prefix="$(latest_hashed_prefix "$name")"
    if [[ -z "$s3_prefix" ]]; then
      log "$name: TEMPLATE_BOOTSTRAP_PREFER=hashed set but no hashed-layout release found — skipping"
      return 0
    fi
  elif aws s3 ls "s3://$BUCKET/$name/manifest.json" >/dev/null 2>&1; then
    s3_prefix="$name"
  else
    s3_prefix="$(latest_hashed_prefix "$name")"
    if [[ -z "$s3_prefix" ]]; then
      log "$name: no manifest.json under flat or hashed layout — skipping"
      return 0
    fi
  fi

  tmp="$(mktemp -d)"
  trap 'rm -rf "$tmp"' EXIT
  aws s3 cp "s3://$BUCKET/$s3_prefix/manifest.json" "$tmp/manifest.json" >/dev/null \
    || { log "$name: failed to fetch manifest.json from s3://$BUCKET/$s3_prefix/"; return 0; }

  hash="$(jq -r '.content_hash // empty' "$tmp/manifest.json")"
  [[ -n "$hash" ]] || { log "$name: manifest.json missing content_hash — skipping"; return 0; }

  local storage_uri="s3://$BUCKET/$s3_prefix/?region=$AWS_REGION"
  register_template_in_postgres "$name" "$hash" "$storage_uri" >&2
  log "$name: registered $hash (storage=$storage_uri)"
  printf '%s\n' "$hash"
)

main() {
  local prefixes name hash registered=0
  if [[ -n "${TEMPLATE_BOOTSTRAP_NAMES:-}" ]]; then
    prefixes="$(printf '%s' "$TEMPLATE_BOOTSTRAP_NAMES" | tr ', ' '\n')"
  else
    prefixes="$(printf '%s\n%s\n' \
      "$(list_template_prefixes)" \
      "$(list_hashed_prefixes)" \
      | sort -u)"
  fi
  [[ -n "$prefixes" ]] || die "no template names resolved"

  while read -r name; do
    [[ -z "$name" ]] && continue
    [[ "$name" == "templates" ]] && continue  # parent prefix, not a template
    # shellcheck disable=SC2254
    case "$name" in
      $FILTER)
        hash="$(install_template "$name")"
        if [[ -n "$hash" ]]; then
          registered=$((registered + 1))
        fi
        ;;
      *) log "skipping $name (filter=$FILTER)" ;;
    esac
  done <<< "$prefixes"

  (( registered > 0 )) || die "no templates registered from bucket $BUCKET; check bucket access, template names, and manifests"
  log "done: registered $registered template(s)"
}

main "$@"

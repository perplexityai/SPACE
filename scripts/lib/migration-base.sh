# shellcheck shell=bash
# The base a migration guard compares a change with, and how it is kept
# current. scripts/check-migration-versions.sh,
# scripts/check-sqlite-migrations-rollback.sh,
# scripts/check-postgres-migrations-isolated.sh and
# scripts/check-postgres-migrations-squawk.sh source this; each names its own
# override variables, default branch and merge-queue policy through
# migration_base_configure, then calls migration_base_ensure once before
# handing migration_base_ref to git and printing migration_base_name.
#
# GitHub's Migration guards step sets BUILDKITE_BUILD_ID=github-actions; it is
# the only CI marker these guards read.
# A trunk-merge/* BUILDKITE_BRANCH is a merge-queue build.
#
# Sourced, never executed.

migration_base_override=""
migration_base_branch=main
migration_base_queue_exempt=false

# migration_base_configure <override> <default-branch> <merge-queue>
#
# <override> is the ref the guard's own variables pin, or empty to compare with
# origin/<default-branch>. <merge-queue> is `exempt` when a trunk merge-queue
# build keeps the local cached-ref fallback, or `strict` when it fails closed
# like any other CI build.
migration_base_configure() {
  migration_base_override="$1"
  migration_base_branch="$2"
  case "$3" in
    exempt) migration_base_queue_exempt=true ;;
    strict) migration_base_queue_exempt=false ;;
    *)
      echo "migration_base_configure: merge-queue policy must be exempt or strict, not '$3'." >&2
      return 2
      ;;
  esac
}

# Prints the branch an override of the form `origin/<branch>` names, and fails
# for any other override, or none.
migration_base_override_branch() {
  local override="${migration_base_override}"
  local override_branch="${override#origin/}"
  [[ "${override}" == origin/* ]] \
    && [[ -n "${override_branch}" ]] \
    && [[ "${override_branch}" != HEAD ]] \
    && [[ "${override_branch}" != *[~^:]* ]] \
    && [[ "${override_branch}" != *"@{"* ]] \
    && git check-ref-format --branch "${override_branch}" >/dev/null 2>&1 \
    && printf '%s\n' "${override_branch}"
}

# Prints the ref the guard hands to git. The default base and an
# `origin/<branch>` override are spelled refs/remotes/origin/<branch>, the ref
# migration_base_ensure refreshes: git resolves a bare origin/<branch> to a
# local branch or tag of that name first, which would shadow it. Any other
# override, such as a SHA, HEAD~1 or upstream/main, is passed through as given.
migration_base_ref() {
  local branch
  if [[ -z "${migration_base_override}" ]]; then
    printf 'refs/remotes/origin/%s\n' "${migration_base_branch}"
  elif branch="$(migration_base_override_branch)"; then
    printf 'refs/remotes/origin/%s\n' "${branch}"
  else
    printf '%s\n' "${migration_base_override}"
  fi
}

# Prints the base as messages name it: the override as given, or
# origin/<default-branch>.
migration_base_name() {
  printf '%s\n' "${migration_base_override:-origin/${migration_base_branch}}"
}

# Succeeds when a base that can't be established or verified must fail the
# guard: in CI, except a merge-queue build of a guard configured `exempt`.
migration_base_fails_closed() {
  [[ -n "${BUILDKITE_BUILD_ID:-}" ]] || return 1
  [[ "${migration_base_queue_exempt}" == true ]] || return 0
  [[ "${BUILDKITE_BRANCH:-}" != trunk-merge/* ]]
}

# Freshens the base ref from origin. Returns 1 only when the guard must fail
# closed; a base that is still missing afterwards is left for the guard's own
# no-base gate to report.
migration_base_ensure() {
  local base_branch="${migration_base_branch}"
  local override_branch

  # Freshen the ref the caller actually asked for. A pin of the form
  # `origin/<branch>` names a remote-tracking ref, so fetch *that* branch rather
  # than the default base — otherwise pinning a stacked PR's parent fetches
  # `main`, leaves the pin unresolved, and now hard-fails in CI on a base the
  # fetch never even tried to get. Any other override shape (a raw SHA, a local
  # tag) has no branch to derive, so leave base_branch alone.
  if [[ -n "${migration_base_override}" ]]; then
    if override_branch="$(migration_base_override_branch)"; then
      base_branch="${override_branch}"
    else
      # A raw SHA, `upstream/main`, `HEAD~1`: no branch to derive. Fetching the
      # default base instead would force-update a ref nobody asked about — in a
      # ref store shared with every sibling worktree — without making the
      # requested base any more resolvable. Leave the pin alone.
      return 0
    fi
  fi

  # Skip locally only when there is genuinely nothing to freshen from. That is
  # the self-tests' synthetic repos, which are `git init` plus a hand-written
  # refs/remotes/origin/<base> and never configure a remote. CI must fail
  # closed here: a cached ref cannot prove itself current when the remote
  # needed to refresh it is absent. An `exempt` guard's merge-queue runs retain
  # their exemption.
  #
  # Deliberately *not* gated on an override being set. The natural value to
  # pin is a remote-tracking ref, which is precisely the thing that goes stale
  # on a persistent agent, so keying the skip off the override would disable
  # freshening for exactly the case that needs it — and gating on "override
  # resolves" inverts it outright, skipping when the ref is present (can be
  # stale) and fetching when it is absent (cannot be).
  if ! git remote get-url origin >/dev/null 2>&1; then
    if migration_base_fails_closed; then
      echo "ERROR: origin remote is not configured; cannot verify" \
        "$(migration_base_name) is current in CI." >&2
      return 1
    fi
    return 0
  fi

  # Freshen unconditionally, not just when the ref is absent: a persistent CI
  # agent can carry a present-but-stale `origin/<base>` from an earlier build,
  # which yields an old merge-base and makes migrations this branch merely
  # rebased onto look "newly added" — a legacy-numbered migration surfacing
  # that way trips the version guard's legacy-cutoff check and fails the build
  # for a migration the branch never touched.
  #
  # Every character of the refspec earns its place, and a stale ref diverges
  # rather than merely lagging, so both halves matter:
  #   * the leading `+` forces the update. Without it the fetch *refuses* a
  #     non-fast-forward and leaves the stale ref exactly as it was.
  #   * the source must be fully qualified as refs/heads/<base>. With a short
  #     source ("main:refs/remotes/origin/main") git deletes the destination
  #     and then fails to recreate it ("cannot lock ref ... unable to resolve
  #     reference"), leaving the guard on a *weaker* base than it started with.
  # `--force` would be redundant here — `+` already carries it per-refspec.
  #
  # `--deepen=50` matters on a shallow checkout, which is what a Buildkite PR
  # build is: cloning the *head* branch shallowly puts the shallow root on the
  # head lineage, above the base's fork point. Fetching the base without
  # deepening then leaves `merge-base origin/<base> HEAD` unresolvable, and the
  # three-dot diff empty — so the guard drops to the weaker two-tree fallback
  # and stops seeing which files the branch actually added. Measured on a
  # `clone --depth=1 --branch <head>` fixture: without it merge-base fails and
  # the added-file diff is empty; with it merge-base returns the true fork point
  # and the diff is correct. `--no-tags` keeps the refspec from dragging tags in.
  # This fetch deliberately touches only the base; it does not deepen the
  # head branch to keep HEAD from remaining a shallow root. `--deepen=50`
  # bounds how far back a fork point can be and still be reachable — a branch
  # with more own history than that leaves merge-base unresolvable, and the
  # caller falls back to a two-tree diff that over-reports.
  #
  # Side effect worth knowing: in a linked worktree this writes to the shared
  # ref store, so it moves `origin/<base>` for every sibling worktree too.
  #
  # A failed refresh is fatal in CI even when the cached ref resolves, except in
  # an `exempt` guard's merge queue: that ref may be stale enough to hide or
  # invent changes relative to the real base. Local runs retain the cached-ref
  # fallback. Report only a static diagnostic: Git and remote helpers can
  # include a remote URL (and credentials embedded in it) in stderr. The branch
  # identifies the operation safely; the fetch exit status drives the
  # fail-closed behavior.
  # Block terminal prompts without replacing how the checkout authenticates.
  # Buildkite may supply credentials through credential.helper, core.askPass,
  # GIT_ASKPASS, SSH_ASKPASS, GIT_SSH_COMMAND, core.sshCommand, or GIT_SSH. Keep
  # each configured mechanism in CI; discarding one can make a valid fetch fail
  # and trip the fail-closed gate below. Outside Buildkite, clear askpass helpers
  # so a desktop IDE integration cannot open a UI or hang this local check.
  #
  # Do not append OpenSSH flags to caller-provided commands: the command may be
  # a non-OpenSSH wrapper, and BatchMode would disable SSH_ASKPASS. Install a
  # noninteractive default only when no custom SSH transport or CI askpass is
  # active. GIT_TERMINAL_PROMPT still suppresses Git's own terminal prompts.
  local configured_ssh_command
  configured_ssh_command="$(git config --get core.sshCommand 2>/dev/null || true)"

  local -a fetch_env=(env GIT_TERMINAL_PROMPT=0)
  local -a git_command=(git)
  if [[ -z "${BUILDKITE_BUILD_ID:-}" ]]; then
    fetch_env+=(GIT_ASKPASS= SSH_ASKPASS=)
    git_command+=(-c core.askPass=)
  fi
  if [[ -z "${GIT_SSH_COMMAND:-}" ]] \
    && [[ -z "${configured_ssh_command}" ]] \
    && [[ -z "${GIT_SSH:-}" ]] \
    && { [[ -z "${BUILDKITE_BUILD_ID:-}" ]] || [[ -z "${SSH_ASKPASS:-}" ]]; }; then
    fetch_env+=(GIT_SSH_COMMAND="ssh -oBatchMode=yes")
  fi

  # The refresh is unconditional, so it needs its own wall-clock bound below
  # the CI step timeout. GNU coreutils calls the binary `timeout` on Linux and
  # usually `gtimeout` under Homebrew. If neither exists, don't run an
  # unbounded fetch: CI fails closed and local runs retain the same cached-ref
  # fallback as any other refresh failure. The wrapper sits outside
  # `env git ...`, so it doesn't replace or modify the configured auth path.
  local fetch_timeout_secs="${MIGRATION_GUARD_FETCH_TIMEOUT_SECS:-60}"
  local timeout_bin=""
  local fetch_failure_reason=""
  local candidate
  for candidate in timeout gtimeout; do
    if command -v "${candidate}" >/dev/null 2>&1; then
      timeout_bin="$(command -v "${candidate}")"
      break
    fi
  done
  if [[ ! "${fetch_timeout_secs}" =~ ^[1-9][0-9]*$ ]]; then
    fetch_failure_reason="the configured fetch timeout is invalid"
  elif [[ -z "${timeout_bin}" ]]; then
    fetch_failure_reason="timeout/gtimeout is unavailable"
  fi

  local -a timeout_command=()
  if [[ -z "${fetch_failure_reason}" ]]; then
    timeout_command=("${timeout_bin}" --kill-after=5s "${fetch_timeout_secs}s")
  fi
  local -a fetch_command=("${git_command[@]}" fetch --no-tags origin)

  if [[ -z "${fetch_failure_reason}" ]] \
    && ! "${timeout_command[@]}" "${fetch_env[@]}" "${fetch_command[@]}" \
    "+refs/heads/${base_branch}:refs/remotes/origin/${base_branch}" \
    >/dev/null 2>&1; then
    fetch_failure_reason="git fetch failed or timed out"
  fi
  if [[ -n "${fetch_failure_reason}" ]]; then
    echo "WARN: could not freshen origin/${base_branch};" \
      "${fetch_failure_reason}." >&2
    if migration_base_fails_closed; then
      if git rev-parse --verify --quiet "$(migration_base_ref)^{commit}" >/dev/null; then
        echo "ERROR: could not verify origin/${base_branch} is current in CI." >&2
        echo "  Refusing to compare against a cached ref after refresh failed; it" >&2
        echo "  may be stale and can hide or invent changes relative to the real base." >&2
        return 1
      fi
      # Let the guard's no-base gate report the more specific missing-ref
      # failure. It still fails closed before any comparison runs.
      return 0
    fi
    echo "WARN: comparing against the cached ref, which may be stale." >&2
    return 0
  fi

  # The ordinary refresh is enough on a complete checkout, or when an earlier
  # guard (or the Buildkite pre-command hook) already exposed the fork point.
  # Deepen only the shallow checkout that still lacks a merge base: the guards
  # run in one lane, so deepening every successful refresh would download
  # another 50 commits per guard and keep deepening reused agent worktrees.
  if git merge-base "$(migration_base_ref)" HEAD >/dev/null 2>&1 \
    || [[ "$(git rev-parse --is-shallow-repository 2>/dev/null || true)" != true ]]; then
    return 0
  fi

  local -a deepen_command=("${git_command[@]}" fetch --no-tags --deepen=50 origin)
  if ! "${timeout_command[@]}" "${fetch_env[@]}" "${deepen_command[@]}" \
    "+refs/heads/${base_branch}:refs/remotes/origin/${base_branch}" \
    >/dev/null 2>&1; then
    echo "WARN: could not deepen history for origin/${base_branch};" \
      "the direct tree fallback may include inherited files." >&2
  fi
}

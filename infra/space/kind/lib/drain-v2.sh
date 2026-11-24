#!/usr/bin/env bash
# drain-v2.sh — whether the Kind bench runs drain v2 (SPACE_DRAIN_V2_ENABLED on
# the API and the cluster controller), selected by SPACE_KIND_DRAIN_V2 for
# every bring-up path through the Tiltfile, which sets each chart's
# drainV2.enabled from it:
#
#   off  (default) drain v2 off, as a release installs it: the API accepts v1
#        drains, and the smoke runner skips the drain v2 scenarios by mode.
#   on   drain v2 on: the drain v2 scenarios run, and the API refuses every v1
#        :drain, so the runner skips the v1 drain scenarios by mode.
#
# Run it to print the chart value, `true` or `false`; it fails on any other
# setting.
set -euo pipefail

case "${SPACE_KIND_DRAIN_V2:-off}" in
  on) printf 'true\n' ;;
  off) printf 'false\n' ;;
  *)
    printf 'SPACE_KIND_DRAIN_V2 must be on or off, not %q\n' "$SPACE_KIND_DRAIN_V2" >&2
    exit 1
    ;;
esac

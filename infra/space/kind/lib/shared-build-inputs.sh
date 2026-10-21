# shellcheck shell=bash
# The build inputs every checkout on the host shares: the Rust artifact image
# tag (space-rust-build-kind:local, written by build-rust-artifacts.sh) and the
# seed directory (~/.space-devbox/kind/seed/build, written by build-seed.sh).
# Another checkout's run of either can replace what this checkout's next
# runtime image copies in between this checkout's own run and that image's
# docker build. infra/space/kind/scripts/image-provenance.py therefore
# certifies an image as what its commit builds only when no other checkout's
# writer was running when the image's build chain started or started before
# its docker build ended, which it reads from the marks made here.
#
# space_kind_mark_shared_input_writer <repo root>
#   Takes a shared lock on this checkout's mark, held until the calling script
#   exits (fd 8, inherited by its children), then touches the mark. Locking
#   first means a writer is either seen running or seen starting. The mark's
#   name is the first 16 hex digits of the sha256 of the checkout's physical
#   path, as image-provenance.py computes it.
space_kind_mark_shared_input_writer() {
  local root dir mark
  root="$(cd "$1" && pwd -P)"
  dir="${SPACE_TILT_STATE_DIR:-$HOME/.space-devbox/kind}/provenance/writers"
  mkdir -p "$dir"
  mark="$dir/$(printf '%s' "$root" | sha256sum | cut -c1-16)"
  exec 8>>"$mark"
  # Off Linux there may be no flock; a running writer is then seen only by
  # the touch below.
  if command -v flock >/dev/null 2>&1; then
    flock -s 8
  fi
  printf '%s\n' "$root" >"$mark"
}

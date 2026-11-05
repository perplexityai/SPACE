#!/usr/bin/env bash
# Build an existing process fixture from an explicitly chosen SPACE checkout.
set -euo pipefail
[[ $# == 3 ]] || { echo 'Usage: build-fixture.sh SOURCE_CHECKOUT IMAGE_TAG NEW_OUTPUT_DIR' >&2; exit 2; }
source_dir=$(cd -- "$1" && pwd)
image_tag=$2
mkdir -- "$3"
output_dir=$(cd -- "$3" && pwd)
harness_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
[[ -f $source_dir/crates/l40_api/service/host/tests/logging_process.rs ]] || { echo 'Selected source does not contain the logging process fixture' >&2; exit 1; }
[[ -z $(git -C "$source_dir" status --porcelain --untracked-files=no) ]] || { echo 'Require a clean source checkout for provenance' >&2; exit 1; }
revision=$(git -C "$source_dir" rev-parse HEAD)
git -C "$source_dir" rev-parse HEAD > "$output_dir/source.txt"
(
  cd -- "$source_dir"
  cargo test --locked -p pplx-space-service-host --test logging_process \
    --no-default-features --no-run --message-format=json
) > "$output_dir/build.jsonl"
fixture=$(jq -r 'select(.reason == "compiler-artifact" and .target.name == "logging_process") | .executable // empty' "$output_dir/build.jsonl")
[[ -f $fixture ]] || { echo 'Build did not produce the fixture binary' >&2; exit 1; }
"$fixture" > "$output_dir/process-tests.log"
install -m 755 "$fixture" "$output_dir/logging_process"
sha256sum "$output_dir/logging_process" > "$output_dir/binary.sha256"
docker build -t "$image_tag" --build-arg "FIXTURE_SOURCE=$revision" \
  -f "$harness_dir/fixture.Dockerfile" "$output_dir"
docker image inspect "$image_tag" > "$output_dir/image.json"
echo 'Fixture built. Verify local Kind with local.sh status before loading the image.'

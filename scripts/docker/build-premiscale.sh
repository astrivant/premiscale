#! /usr/bin/env bash
# Build the production image from this checkout.

PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
IMAGE_TAG="${1:-premiscale:local}"

docker build --target production --tag "$IMAGE_TAG" --file "$PROJECT_ROOT/Dockerfile" "$PROJECT_ROOT"

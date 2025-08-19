#!/bin/bash

VERSION=$(grep -E 'VERSION = "[^"]+"' src/server/config.go | sed -E 's/.*VERSION = "([^"]+)".*/\1/')
if [ -z "$VERSION" ]; then
  echo "Error: Could not extract version"
  exit 1
fi

echo "Building bemidb version $VERSION for linux/arm64"
docker buildx build --build-arg PLATFORM=linux/arm64 -t ghcr.io/bemihq/bemidb:$VERSION-arm64 .

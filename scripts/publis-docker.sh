#!/bin/bash

version=$(grep -E 'VERSION = "[^"]+"' src/config.go | sed -E 's/.*VERSION = "([^"]+)".*/\1/')
if [ -z "$version" ]; then
  echo "Error: Could not extract version from config.go"
  exit 1
fi

echo "Pushing bemidb version $version to ghcr.io"

docker push ghcr.io/bemihq/bemidb:$version-amd64
docker push ghcr.io/bemihq/bemidb:$version-arm64

docker manifest create ghcr.io/bemihq/bemidb:$version \
  ghcr.io/bemihq/bemidb:$version-amd64 \
  ghcr.io/bemihq/bemidb:$version-arm64
docker manifest annotate ghcr.io/bemihq/bemidb:$version ghcr.io/bemihq/bemidb:$version-amd64 --arch amd64
docker manifest annotate ghcr.io/bemihq/bemidb:$version ghcr.io/bemihq/bemidb:$version-arm64 --arch arm64

docker manifest push ghcr.io/bemihq/bemidb:$version

docker tag ghcr.io/bemihq/bemidb:$version ghcr.io/bemihq/bemidb:latest
docker push ghcr.io/bemihq/bemidb:latest

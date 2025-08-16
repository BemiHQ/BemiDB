#!/bin/bash

VERSION=$(grep -E 'VERSION = "[^"]+"' src/common/common_config.go | sed -E 's/.*VERSION = "([^"]+)".*/\1/')
if [ -z "$VERSION" ]; then
  echo "Error: Could not extract version"
  exit 1
fi

echo "Pushing bemidb version $VERSION to ghcr.io"

docker push ghcr.io/bemihq/bemidb:$VERSION-amd64
docker push ghcr.io/bemihq/bemidb:$VERSION-arm64

docker manifest inspect ghcr.io/bemihq/bemidb:$VERSION &> /dev/null && docker manifest rm ghcr.io/bemihq/bemidb:$VERSION
docker manifest create ghcr.io/bemihq/bemidb:$VERSION ghcr.io/bemihq/bemidb:$VERSION-amd64 ghcr.io/bemihq/bemidb:$VERSION-arm64
docker manifest annotate ghcr.io/bemihq/bemidb:$VERSION ghcr.io/bemihq/bemidb:$VERSION-amd64 --arch amd64
docker manifest annotate ghcr.io/bemihq/bemidb:$VERSION ghcr.io/bemihq/bemidb:$VERSION-arm64 --arch arm64
docker manifest push ghcr.io/bemihq/bemidb:$VERSION

docker manifest inspect ghcr.io/bemihq/bemidb:latest &> /dev/null && docker manifest rm ghcr.io/bemihq/bemidb:latest
docker manifest create ghcr.io/bemihq/bemidb:latest ghcr.io/bemihq/bemidb:$VERSION-amd64 ghcr.io/bemihq/bemidb:$VERSION-arm64
docker manifest annotate ghcr.io/bemihq/bemidb:latest ghcr.io/bemihq/bemidb:$VERSION-amd64 --arch amd64
docker manifest annotate ghcr.io/bemihq/bemidb:latest ghcr.io/bemihq/bemidb:$VERSION-arm64 --arch arm64
docker manifest push ghcr.io/bemihq/bemidb:latest

echo
echo "See https://github.com/orgs/BemiHQ/packages/container/package/bemidb"

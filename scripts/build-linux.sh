#!/bin/bash

platforms=("linux/amd64" "linux/arm64")

version=$(grep -E 'VERSION = "[^"]+"' src/config.go | sed -E 's/.*VERSION = "([^"]+)".*/\1/')
if [ -z "$version" ]; then
  echo "Error: Could not extract version from config.go"
  exit 1
fi

for platform in "${platforms[@]}"
do
  os="${platform%/*}"
  arch="${platform#*/}"
  tag="ghcr.io/bemihq/bemidb:$version-$arch"

  echo "Building bemidb version $version for $os/$arch"

  docker buildx build \
    --build-arg PLATFORM=$platform \
    --build-arg GOOS=$os \
    --build-arg GOARCH="$arch" \
    -t $tag .

  docker create --name temp-container $tag
  docker cp temp-container:/app/bemidb ./build/bemidb-$os-$arch
  docker rm temp-container
done

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
